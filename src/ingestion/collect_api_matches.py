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

OUTPUT_DIR = Path("data/brut/api")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def collect_matches(competition_code: str, season: int) -> None:
    url = f"https://api.football-data.org/v4/competitions/{competition_code}/matches?season={season}"
    response = requests.get(url, headers=HEADERS, timeout=30)

    print(f"{competition_code} {season} -> status {response.status_code}")

    if response.status_code != 200:
        print(f"Erreur API pour {competition_code} {season}: {response.text}")
        return

    data = response.json()

    if "matches" not in data or len(data["matches"]) == 0:
        print(f"Aucun match pour {competition_code} {season}")
        return

    output_file = OUTPUT_DIR / f"{competition_code}_{season}_matches.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Enregistré : {output_file} | {len(data['matches'])} matchs")


def main():
    for competition in COMPETITIONS:
        for season in SEASONS:
            collect_matches(competition, season)
            time.sleep(6)  # pour respecter les limites du plan free


if __name__ == "__main__":
    main()