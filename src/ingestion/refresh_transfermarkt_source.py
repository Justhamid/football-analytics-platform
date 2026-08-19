"""
Rafraîchit le snapshot Transfermarkt (Kaggle) en le retéléchargeant et
en le stockant dans MinIO (bucket raw-transfermarkt), avec validation de
structure/volume et sauvegarde de l'ancien objet avant remplacement.
"""

from pathlib import Path
from io import BytesIO
import os
import json
import tempfile
from datetime import datetime, timezone
import pandas as pd
from minio import Minio
from minio.commonconfig import CopySource
from dotenv import load_dotenv

load_dotenv()

KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")

if not KAGGLE_USERNAME or not KAGGLE_KEY:
    raise ValueError(
        "KAGGLE_USERNAME / KAGGLE_KEY introuvables dans le fichier .env"
    )

os.environ["KAGGLE_USERNAME"] = KAGGLE_USERNAME
os.environ["KAGGLE_KEY"] = KAGGLE_KEY

DATASET_SLUG = "davidcariboo/player-scores"
BUCKET = "raw-transfermarkt"
METADATA_OBJECT = "_refresh_metadata.json"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9002")
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)

COLONNES_ATTENDUES = {
    "players.csv": {
        "player_id", "first_name", "last_name", "name", "date_of_birth",
        "position", "sub_position", "foot", "height_in_cm",
        "country_of_birth", "country_of_citizenship", "current_club_id",
        "current_club_name", "current_club_domestic_competition_id",
        "last_season", "market_value_in_eur", "highest_market_value_in_eur",
        "international_caps", "international_goals",
    },
    "clubs.csv": {
        "club_id", "name", "domestic_competition_id", "squad_size",
        "average_age", "foreigners_number", "foreigners_percentage",
        "national_team_players", "stadium_name", "stadium_seats",
        "coach_name", "last_season",
    },
    "player_valuations.csv": {
        "player_id", "date", "market_value_in_eur", "current_club_name",
        "current_club_id", "player_club_domestic_competition_id",
    },
    "transfers.csv": {
        "player_id", "player_name", "transfer_date", "transfer_season",
        "from_club_id", "from_club_name", "to_club_id", "to_club_name",
        "transfer_fee", "market_value_in_eur",
    },
    "appearances.csv": {
        "appearance_id", "game_id", "player_id", "player_club_id", "date",
        "competition_id", "goals", "assists", "yellow_cards", "red_cards",
        "minutes_played",
    },
    "games.csv": {
        "game_id", "competition_id", "date",
        "home_club_name", "away_club_name",
        "home_club_goals", "away_club_goals",
    },
}

SEUIL_ALERTE = 0.90
SEUIL_BLOQUANT = 0.30


def assurer_bucket() -> None:
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
        print(f"  -> Bucket '{BUCKET}' cree")


def telecharger_dataset(dest_dir: Path) -> None:
    """Telecharge et decompresse le dataset Kaggle localement (temporaire)."""
    _original_makedirs = os.makedirs
    def _makedirs_safe(name, mode=0o777, exist_ok=False):
        return _original_makedirs(name, mode=mode, exist_ok=True)
    os.makedirs = _makedirs_safe
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    finally:
        os.makedirs = _original_makedirs

    print(f"Authentification Kaggle ({KAGGLE_USERNAME})...")
    api = KaggleApi()
    api.authenticate()

    print(f"Telechargement du dataset {DATASET_SLUG}...")
    api.dataset_download_files(DATASET_SLUG, path=str(dest_dir), unzip=True, quiet=False)
    print(f"  -> telecharge dans {dest_dir}")


def compter_lignes_minio(object_name: str) -> int:
    """Compte les lignes d'un objet CSV deja present dans MinIO (0 si absent)."""
    try:
        response = minio_client.get_object(BUCKET, object_name)
        nb_lignes = -1  # ne pas compter l'en-tete
        for _ in response.stream(32 * 1024):
            pass
        # Alternative fiable : compter via un flux, sans tout garder en memoire
        response.close()
        response.release_conn()

        response2 = minio_client.get_object(BUCKET, object_name)
        nb_lignes = -1
        buffer = b""
        for chunk in response2.stream(64 * 1024):
            buffer += chunk
            nb_lignes += buffer.count(b"\n")
            buffer = buffer[buffer.rfind(b"\n") + 1:] if b"\n" in buffer else buffer
        response2.close()
        response2.release_conn()
        return max(nb_lignes, 0)
    except Exception:
        return 0


def valider_snapshot(nouveau_dir: Path) -> None:
    """
    Valide structure et volume avant d'accepter. Leve une exception si
    anomalie detectee -- l'ancien snapshot MinIO reste alors intact.
    """
    for fichier, colonnes_requises in COLONNES_ATTENDUES.items():
        chemin = nouveau_dir / fichier

        if not chemin.exists():
            raise FileNotFoundError(
                f"Fichier attendu absent du nouveau snapshot : {fichier}"
            )

        df_nouveau = pd.read_csv(chemin, nrows=5, encoding="utf-8", encoding_errors="replace")
        colonnes_manquantes = colonnes_requises - set(df_nouveau.columns)
        if colonnes_manquantes:
            raise ValueError(
                f"{fichier} : colonnes manquantes {colonnes_manquantes} -- "
                f"le schema Kaggle a peut-etre change."
            )

        nb_ancien = compter_lignes_minio(fichier)
        if nb_ancien > 0:
            nb_nouveau = -1
            with open(chemin, "rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    nb_nouveau += chunk.count(b"\n")
            ratio = nb_nouveau / nb_ancien

            if ratio < SEUIL_BLOQUANT:
                raise ValueError(
                    f"{fichier} : volume catastrophiquement bas -- {nb_nouveau} "
                    f"lignes contre {nb_ancien} precedemment (ratio {ratio:.0%}). "
                    f"Refresh refuse, snapshot MinIO conserve."
                )
            elif ratio < SEUIL_ALERTE:
                print(
                    f"  ATTENTION {fichier} : volume en baisse notable -- {nb_nouveau} "
                    f"lignes contre {nb_ancien} precedemment (ratio {ratio:.0%}). "
                    f"Accepte mais a verifier manuellement."
                )

        print(f"  OK {fichier} valide")


def sauvegarder_ancien_snapshot() -> None:
    """Copie chaque objet MinIO existant vers une cle de backup avant remplacement."""
    for fichier in COLONNES_ATTENDUES.keys():
        try:
            minio_client.stat_object(BUCKET, fichier)
            minio_client.copy_object(
                BUCKET, f"{fichier}.backup",
                CopySource(BUCKET, fichier),
            )
        except Exception:
            pass  # pas d'objet existant, premier upload


def remplacer_snapshot(nouveau_dir: Path) -> None:
    """Upload chaque CSV valide vers MinIO."""
    for fichier in COLONNES_ATTENDUES.keys():
        chemin = nouveau_dir / fichier
        minio_client.fput_object(BUCKET, fichier, str(chemin), content_type="text/csv")
    print(f"  -> snapshot mis a jour dans MinIO ({BUCKET})")


def ecrire_metadata() -> None:
    contenu = json.dumps(
        {
            "derniere_actualisation": datetime.now(timezone.utc).isoformat(),
            "source": DATASET_SLUG,
        },
        indent=2,
    ).encode("utf-8")
    minio_client.put_object(
        BUCKET, METADATA_OBJECT,
        data=BytesIO(contenu), length=len(contenu),
        content_type="application/json",
    )


def main():
    print("\n===== REFRESH TRANSFERMARKT (KAGGLE -> MinIO) =====\n")
    assurer_bucket()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        try:
            telecharger_dataset(tmp_path)
            print("\nValidation du nouveau snapshot...")
            valider_snapshot(tmp_path)
        except Exception as e:
            raise RuntimeError(
                f"Refresh Transfermarkt echoue, snapshot MinIO conserve inchange : {e}"
            )

        print("\nSauvegarde de l'ancien snapshot...")
        sauvegarder_ancien_snapshot()

        print("\nRemplacement du snapshot...")
        remplacer_snapshot(tmp_path)
        ecrire_metadata()

    print("\nRefresh Transfermarkt termine avec succes.")


if __name__ == "__main__":
    main()