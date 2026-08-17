"""
Rafraîchit les snapshots football-datasets (GitHub) et les stocke dans
MinIO (bucket raw-football-datasets), pour les 5 grandes ligues.
Téléchargement conditionnel (ETag) pour éviter les re-téléchargements
inutiles, validation structurelle avant remplacement, sauvegarde de
l'ancien objet dans MinIO avant écrasement.

Aucune authentification requise (dépôt public).
Source : https://github.com/datasets/football-datasets
"""

from pathlib import Path
from typing import Optional
from io import BytesIO
import os
import requests
from minio import Minio
from minio.commonconfig import CopySource
from dotenv import load_dotenv

load_dotenv()

RAW_BASE_URL = "https://raw.githubusercontent.com/datasets/football-datasets/main/datasets"

LIGUES = {
    "premier_league": "premier-league",
    "la_liga": "la-liga",
    "bundesliga": "bundesliga",
    "serie_a": "serie-a",
    "ligue_1": "ligue-1",
}

COLONNES_ATTENDUES = {
    "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR",
}

SAISONS_A_RAFRAICHIR = ["season-2425.csv", "season-2526.csv"]

BUCKET = "raw-football-datasets"

# Petit dossier LOCAL, uniquement pour l'empreinte ETag de comparaison
# (metadonnee de verification, pas une copie des vraies donnees)
ETAG_DIR = Path("data/etags/football_datasets")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9002")
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)


def assurer_bucket() -> None:
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
        print(f"  -> Bucket '{BUCKET}' cree")


def url_saison(dossier_github: str, fichier: str) -> str:
    return f"{RAW_BASE_URL}/{dossier_github}/{fichier}"


def etag_local(chemin_etag: Path) -> Optional[str]:
    if chemin_etag.exists():
        return chemin_etag.read_text(encoding="utf-8").strip()
    return None


def valider_contenu(contenu: bytes, nom_fichier: str) -> None:
    premiere_ligne = contenu.split(b"\n", 1)[0].decode("utf-8", errors="replace")
    colonnes = set(premiere_ligne.strip().split(","))
    colonnes_manquantes = COLONNES_ATTENDUES - colonnes
    if colonnes_manquantes:
        raise ValueError(
            f"{nom_fichier} : colonnes manquantes {colonnes_manquantes} — "
            f"structure inattendue, refresh refuse."
        )
    nb_lignes = contenu.count(b"\n")
    if nb_lignes < 10:
        raise ValueError(
            f"{nom_fichier} : seulement {nb_lignes} lignes recues — "
            f"contenu probablement tronque ou erreur serveur, refresh refuse."
        )


def rafraichir_fichier(dossier_local: str, dossier_github: str, fichier: str) -> str:
    """
    Retourne : 'mis_a_jour', 'inchange', 'absent', ou leve une exception.
    Le contenu est ecrit dans MinIO. L'ETag GitHub est memorise en local,
    dans un petit fichier texte (pas une copie des donnees).
    """
    object_name = f"{dossier_local}/{fichier}"

    etag_dir_local = ETAG_DIR / dossier_local
    etag_dir_local.mkdir(parents=True, exist_ok=True)
    chemin_etag = etag_dir_local / f"{fichier}.etag"

    url = url_saison(dossier_github, fichier)
    headers = {}
    etag_existant = etag_local(chemin_etag)
    if etag_existant:
        headers["If-None-Match"] = etag_existant

    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code == 304:
        return "inchange"

    if response.status_code == 404:
        print(f"  ATTENTION {dossier_local}/{fichier} : absent sur GitHub (404), ignore")
        return "absent"

    if response.status_code != 200:
        raise RuntimeError(
            f"{dossier_local}/{fichier} : status {response.status_code} inattendu"
        )

    valider_contenu(response.content, f"{dossier_local}/{fichier}")

    # Sauvegarde de l'ancien objet MinIO avant remplacement (copie interne)
    try:
        minio_client.stat_object(BUCKET, object_name)
        minio_client.copy_object(
            BUCKET, f"{object_name}.backup",
            CopySource(BUCKET, object_name),
        )
    except Exception:
        pass  # pas d'objet existant, premier upload pour ce fichier

    minio_client.put_object(
        BUCKET,
        object_name,
        data=BytesIO(response.content),
        length=len(response.content),
        content_type="text/csv",
    )

    if "ETag" in response.headers:
        chemin_etag.write_text(response.headers["ETag"], encoding="utf-8")

    return "mis_a_jour"


def main():
    print("\n===== REFRESH FOOTBALL-DATASETS (GitHub -> MinIO) =====\n")
    assurer_bucket()

    resultats = {"mis_a_jour": [], "inchange": [], "absent": []}
    erreurs = []

    for dossier_local, dossier_github in LIGUES.items():
        for fichier in SAISONS_A_RAFRAICHIR:
            try:
                statut = rafraichir_fichier(dossier_local, dossier_github, fichier)
                resultats[statut].append(f"{dossier_local}/{fichier}")
                print(f"  {dossier_local}/{fichier} : {statut}")
            except Exception as e:
                erreurs.append(f"{dossier_local}/{fichier} : {e}")
                print(f"  ERREUR {dossier_local}/{fichier} : {e}")

    print(f"\nResume : {len(resultats['mis_a_jour'])} mis a jour, "
          f"{len(resultats['inchange'])} inchanges, "
          f"{len(resultats['absent'])} absents, {len(erreurs)} erreurs")

    if erreurs:
        raise RuntimeError(f"{len(erreurs)} erreur(s) lors du refresh : {erreurs}")


if __name__ == "__main__":
    main()