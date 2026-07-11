from pathlib import Path
import json
import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("FOOTBALL_DATA_API_TOKEN")

if not API_TOKEN:
    raise ValueError("FOOTBALL_DATA_API_TOKEN introuvable dans le fichier .env")

HEADERS = {"X-Auth-Token": API_TOKEN}

COMPETITIONS = ["PL", "PD", "SA", "BL1", "FL1"]
SEASONS = [2023, 2024]

# Seuils minimums plausibles par compétition (nb de matchs sur une saison complète)
# PL/PD/SA : 20 équipes -> 380 matchs. BL1/FL1 : 18 équipes -> 306 matchs.
# Seuil fixé avec marge de sécurité pour détecter une collecte anormalement basse
# sans être trop strict (ex: saison en cours, retards de calendrier).
MIN_MATCHES_ATTENDUS = {

    "PL": 350, "PD": 350, "SA": 350,

    "BL1": 280, "FL1": 280,

}

CHAMPS_OBLIGATOIRES = {"homeTeam", "awayTeam", "score", "utcDate", "status"}

OUTPUT_DIR = Path("data/brut/api")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def valider_reponse(matches: list, competition_code: str, season: int) -> None:
    """
    Contrôle qualité post-collecte. Lève une exception si une anomalie
    est détectée, afin de déclencher l'alerte Airflow (on_failure_callback)
    même quand l'appel HTTP a techniquement réussi (200 OK) mais que les
    données renvoyées sont vides, incomplètes ou anormalement faibles.
    """
    if len(matches) == 0:
        raise ValueError(
            f"Collecte vide pour {competition_code}/{season} — "
            f"l'API a peut-être changé de format ou est indisponible."
        )

    seuil = MIN_MATCHES_ATTENDUS.get(competition_code, 250)
    if len(matches) < seuil:
        raise ValueError(
            f"Volume anormalement bas pour {competition_code}/{season} : "
            f"{len(matches)} matchs reçus (attendu >= {seuil})."
        )

    # Vérification structurelle sur un échantillon
    echantillon = matches[:5]
    for m in echantillon:
        champs_manquants = CHAMPS_OBLIGATOIRES - m.keys()
        if champs_manquants:
            raise ValueError(
                f"Structure JSON inattendue pour {competition_code}/{season} : "
                f"champs manquants {champs_manquants}."
            )


def collect_matches(competition_code: str, season: int) -> None:
    url = f"https://api.football-data.org/v4/competitions/{competition_code}/matches?season={season}"
    response = requests.get(url, headers=HEADERS, timeout=30)

    print(f"{competition_code} {season} -> status {response.status_code}")

    if response.status_code != 200:
        raise RuntimeError(
            f"Erreur API pour {competition_code}/{season} : "
            f"status {response.status_code} — {response.text}"
        )

    data = response.json()

    if "matches" not in data:
        raise ValueError(
            f"Réponse API malformée pour {competition_code}/{season} : "
            f"clé 'matches' absente."
        )

    matches = data["matches"]
    valider_reponse(matches, competition_code, season)

    output_file = OUTPUT_DIR / f"{competition_code}_{season}_matches.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Enregistré : {output_file} | {len(matches)} matchs")


def main():
    erreurs = []

    for competition in COMPETITIONS:
        for season in SEASONS:
            try:
                collect_matches(competition, season)
            except Exception as e:
                print(f"❌ Échec {competition}/{season} : {e}")
                erreurs.append(f"{competition}/{season} : {e}")
            time.sleep(6)  # pour respecter les limites du plan free

    if erreurs:
        resume = "\n".join(erreurs)
        raise RuntimeError(
            f"{len(erreurs)} collecte(s) en échec sur {len(COMPETITIONS) * len(SEASONS)} :\n{resume}"
        )

    print("✅ Toutes les collectes ont réussi.")


if __name__ == "__main__":
    main()