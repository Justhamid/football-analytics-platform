"""
Rafraîchit les snapshots locaux football-datasets (GitHub, source
football-data.co.uk) pour les 5 grandes ligues européennes. Téléchargement
conditionnel (ETag) pour éviter les re-téléchargements inutiles, validation
structurelle avant remplacement, sauvegarde de l'ancien snapshot.

Aucune authentification requise (dépôt public).
Source : https://github.com/datasets/football-datasets
Mise à jour quotidienne par le mainteneur.
"""

from pathlib import Path
from typing import Optional
import json
import requests
from datetime import datetime, timezone

RAW_BASE_URL = "https://raw.githubusercontent.com/datasets/football-datasets/main/datasets"

# Correspondance nom local (underscore) -> nom GitHub (tiret)
LIGUES = {
    "premier_league": "premier-league",
    "la_liga": "la-liga",
    "bundesliga": "bundesliga",
    "serie_a": "serie-a",
    "ligue_1": "ligue-1",
}

# Colonnes minimales attendues (utilisées par build_unified_matches.py)
COLONNES_ATTENDUES = {
    "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR",
}

# Saisons à rafraîchir : uniquement les plus récentes (les anciennes ne
# changent jamais, inutile de les re-télécharger à chaque fois)
SAISONS_A_RAFRAICHIR = ["season-2425.csv", "season-2526.csv"]

BASE_DIR = Path("data/brut/football_datasets")


def url_saison(dossier_github: str, fichier: str) -> str:
    return f"{RAW_BASE_URL}/{dossier_github}/{fichier}"


def etag_local(chemin_etag: Path) -> Optional[str]:
    if chemin_etag.exists():
        return chemin_etag.read_text(encoding="utf-8").strip()
    return None


def valider_contenu(contenu: bytes, nom_fichier: str) -> None:
    """Valide la structure du CSV téléchargé avant de l'accepter."""
    premiere_ligne = contenu.split(b"\n", 1)[0].decode("utf-8", errors="replace")
    colonnes = set(premiere_ligne.strip().split(","))

    colonnes_manquantes = COLONNES_ATTENDUES - colonnes
    if colonnes_manquantes:
        raise ValueError(
            f"{nom_fichier} : colonnes manquantes {colonnes_manquantes} — "
            f"structure inattendue, refresh refusé."
        )

    nb_lignes = contenu.count(b"\n")
    if nb_lignes < 10:
        raise ValueError(
            f"{nom_fichier} : seulement {nb_lignes} lignes reçues — "
            f"contenu probablement tronqué ou erreur serveur, refresh refusé."
        )


def rafraichir_fichier(dossier_local: str, dossier_github: str, fichier: str) -> str:
    """
    Retourne : 'mis_a_jour', 'inchange', ou lève une exception en cas d'échec.
    """
    dest_dir = BASE_DIR / dossier_local
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_csv = dest_dir / fichier
    dest_etag = dest_dir / f"{fichier}.etag"

    url = url_saison(dossier_github, fichier)
    headers = {}
    etag_existant = etag_local(dest_etag)
    if etag_existant:
        headers["If-None-Match"] = etag_existant

    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code == 304:
        return "inchange"

    if response.status_code == 404:
        # Normal pour season-2526.csv en tout début de saison (pas encore créé)
        print(f"  ⚠️  {dossier_local}/{fichier} : absent sur GitHub (404), ignoré")
        return "absent"

    if response.status_code != 200:
        raise RuntimeError(
            f"{dossier_local}/{fichier} : status {response.status_code} inattendu"
        )

    valider_contenu(response.content, f"{dossier_local}/{fichier}")

    # Sauvegarde de l'ancien fichier avant remplacement
    if dest_csv.exists():
        backup = dest_dir / f"{fichier}.backup"
        dest_csv.replace(backup)

    dest_csv.write_bytes(response.content)

    if "ETag" in response.headers:
        dest_etag.write_text(response.headers["ETag"], encoding="utf-8")

    return "mis_a_jour"


def main():
    print("\n===== REFRESH FOOTBALL-DATASETS (GitHub) =====\n")

    resultats = {"mis_a_jour": [], "inchange": [], "absent": []}
    erreurs = []

    for dossier_local, dossier_github in LIGUES.items():
        for fichier in SAISONS_A_RAFRAICHIR:
            try:
                statut = rafraichir_fichier(dossier_local, dossier_github, fichier)
                resultats[statut].append(f"{dossier_local}/{fichier}")
                print(f"  {dossier_local}/{fichier} : {statut}")
            except Exception as e:
                print(f"  ❌ {dossier_local}/{fichier} : {e}")
                erreurs.append(f"{dossier_local}/{fichier} : {e}")

    print(f"\nRésumé : {len(resultats['mis_a_jour'])} mis à jour, "
          f"{len(resultats['inchange'])} inchangés, "
          f"{len(resultats['absent'])} absents, "
          f"{len(erreurs)} erreurs")

    metadata_file = BASE_DIR / "_refresh_metadata.json"
    metadata_file.write_text(
        json.dumps(
            {
                "derniere_actualisation": datetime.now(timezone.utc).isoformat(),
                "mis_a_jour": resultats["mis_a_jour"],
                "inchange": resultats["inchange"],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    if erreurs:
        resume = "\n".join(erreurs)
        raise RuntimeError(f"{len(erreurs)} échec(s) sur le refresh football-datasets :\n{resume}")

    print("\n✅ Refresh football-datasets terminé.")


if __name__ == "__main__":
    main()