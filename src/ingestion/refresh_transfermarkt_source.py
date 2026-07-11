"""
Rafraîchit le snapshot local du dataset Transfermarkt (Kaggle) en le
retéléchargeant, avec validation de structure et sauvegarde de l'ancien
snapshot avant remplacement (rollback possible en cas d'échec de la
validation en aval).
"""

from pathlib import Path
import os
import shutil
import json
import tempfile
from datetime import datetime, timezone
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")

if not KAGGLE_USERNAME or not KAGGLE_KEY:
    raise ValueError(
        "KAGGLE_USERNAME / KAGGLE_KEY introuvables dans le fichier .env"
    )

# kaggle lit ces deux variables directement depuis os.environ à l'authentification
os.environ["KAGGLE_USERNAME"] = KAGGLE_USERNAME
os.environ["KAGGLE_KEY"] = KAGGLE_KEY

DATASET_SLUG = "davidcariboo/player-scores"
TM_DIR = Path("data/brut/transfermarkt")
BACKUP_DIR = Path("data/brut/transfermarkt_backup")
METADATA_FILE = TM_DIR / "_refresh_metadata.json"

# Colonnes minimales attendues par transform_players.py pour chaque fichier
# (vérification structurelle : ces colonnes doivent être présentes, des
# colonnes supplémentaires ajoutées par Kaggle ne posent pas de problème)
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
}

# Tolérance sur le volume — deux seuils :
# - en dessous de SEUIL_ALERTE : accepté mais signalé (variation notable,
#   éventuellement légitime côté source)
# - en dessous de SEUIL_BLOQUANT : refusé (téléchargement probablement
#   tronqué ou corrompu, protection contre une perte de données silencieuse)
SEUIL_ALERTE = 0.90
SEUIL_BLOQUANT = 0.30


def telecharger_dataset(dest_dir: Path) -> None:
    """Télécharge et décompresse le dataset Kaggle dans dest_dir."""
    # Patch pour un bug connu de la lib Kaggle : os.makedirs échoue si
    # le dossier ~/.config/kaggle existe déjà (FileExistsError)
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

    print(f"Téléchargement du dataset {DATASET_SLUG}...")
    api.dataset_download_files(DATASET_SLUG, path=str(dest_dir), unzip=True, quiet=False)

    print(f"  → téléchargé dans {dest_dir}")


def valider_snapshot(nouveau_dir: Path) -> None:
    """
    Valide la structure et le volume du nouveau snapshot avant de
    l'accepter. Lève une exception si une anomalie est détectée —
    l'ancien snapshot reste alors intact.
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
                f"{fichier} : colonnes manquantes {colonnes_manquantes} — "
                f"le schéma Kaggle a peut-être changé."
            )

        # Comparaison de volume avec l'ancien snapshot, si disponible
        ancien_chemin = TM_DIR / fichier
        if ancien_chemin.exists():
            nb_nouveau = sum(1 for _ in open(chemin, encoding="utf-8", errors="replace")) - 1
            nb_ancien = sum(1 for _ in open(ancien_chemin, encoding="utf-8", errors="replace")) - 1

            if nb_ancien > 0:
                ratio = nb_nouveau / nb_ancien
                if ratio < SEUIL_BLOQUANT:
                    raise ValueError(
                        f"{fichier} : volume catastrophiquement bas — {nb_nouveau} "
                        f"lignes contre {nb_ancien} précédemment (ratio {ratio:.0%}). "
                        f"Refresh refusé, snapshot conservé."
                    )
                elif ratio < SEUIL_ALERTE:
                    print(
                        f"  ⚠️ {fichier} : volume en baisse notable — {nb_nouveau} "
                        f"lignes contre {nb_ancien} précédemment (ratio {ratio:.0%}). "
                        f"Accepté mais à vérifier manuellement."
                    )

        print(f"  ✓ {fichier} validé")


def sauvegarder_ancien_snapshot() -> None:
    """Copie l'ancien snapshot dans un dossier de backup avant remplacement."""
    if BACKUP_DIR.exists():
        shutil.rmtree(BACKUP_DIR)
    if TM_DIR.exists() and any(TM_DIR.iterdir()):
        shutil.copytree(TM_DIR, BACKUP_DIR)
        print(f"  → ancien snapshot sauvegardé dans {BACKUP_DIR}")


def remplacer_snapshot(nouveau_dir: Path) -> None:
    """Remplace le snapshot local par le nouveau, une fois validé."""
    TM_DIR.mkdir(parents=True, exist_ok=True)
    for fichier in nouveau_dir.glob("*.csv"):
        shutil.copyfile(fichier, TM_DIR / fichier.name)
    print(f"  → snapshot mis à jour dans {TM_DIR}")


def ecrire_metadata() -> None:
    METADATA_FILE.write_text(
        json.dumps(
            {
                "derniere_actualisation": datetime.now(timezone.utc).isoformat(),
                "source": DATASET_SLUG,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def main():
    print("\n===== REFRESH TRANSFERMARKT (KAGGLE) =====\n")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        try:
            telecharger_dataset(tmp_path)
            print("\nValidation du nouveau snapshot...")
            valider_snapshot(tmp_path)
        except Exception as e:
            raise RuntimeError(
                f"Refresh Transfermarkt échoué, snapshot local conservé inchangé : {e}"
            )

        print("\nSauvegarde de l'ancien snapshot...")
        sauvegarder_ancien_snapshot()

        print("\nRemplacement du snapshot...")
        remplacer_snapshot(tmp_path)
        ecrire_metadata()

    print("\n✅ Refresh Transfermarkt terminé avec succès.")


if __name__ == "__main__":
    main()