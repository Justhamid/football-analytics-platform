import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{POSTGRES_HOST}:5432/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)


def construire_competitions() -> pd.DataFrame:
    print("Construction table competitions...")

    competitions = pd.DataFrame([
        {
            "competition_id":   "PL",
            "competition_name": "Premier League",
            "country":          "Angleterre",
            "confederation":    "UEFA",
            "tier":             1,
            "nb_clubs":         20,
            "nb_matchs_saison": 380,
            "transfermarkt_id": "GB1",
            "football_data_id": "PL",
        },
        {
            "competition_id":   "PD",
            "competition_name": "La Liga",
            "country":          "Espagne",
            "confederation":    "UEFA",
            "tier":             1,
            "nb_clubs":         20,
            "nb_matchs_saison": 380,
            "transfermarkt_id": "ES1",
            "football_data_id": "PD",
        },
        {
            "competition_id":   "SA",
            "competition_name": "Serie A",
            "country":          "Italie",
            "confederation":    "UEFA",
            "tier":             1,
            "nb_clubs":         20,
            "nb_matchs_saison": 380,
            "transfermarkt_id": "IT1",
            "football_data_id": "SA",
        },
        {
            "competition_id":   "BL1",
            "competition_name": "Bundesliga",
            "country":          "Allemagne",
            "confederation":    "UEFA",
            "tier":             1,
            "nb_clubs":         18,
            "nb_matchs_saison": 306,
            "transfermarkt_id": "L1",
            "football_data_id": "BL1",
        },
        {
            "competition_id":   "FL1",
            "competition_name": "Ligue 1",
            "country":          "France",
            "confederation":    "UEFA",
            "tier":             1,
            "nb_clubs":         18,
            "nb_matchs_saison": 306,
            "transfermarkt_id": "FR1",
            "football_data_id": "FL1",
        },
    ])

    competitions.to_sql(
        name="competitions",
        con=engine,
        schema="marts_clubs",
        if_exists="replace",
        index=False
    )

    print(f"  → PostgreSQL : marts_clubs.competitions ({len(competitions)} lignes)")
    print("\n  Détail :")
    print(competitions[[
        "competition_id", "competition_name",
        "country", "nb_clubs", "transfermarkt_id"
    ]].to_string())

    return competitions


def main():
    print("\n===== TABLE COMPETITIONS =====\n")
    construire_competitions()
    print("\n✅ Table competitions créée.")


if __name__ == "__main__":
    main()