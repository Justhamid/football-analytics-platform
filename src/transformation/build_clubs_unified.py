import pandas as pd
from sqlalchemy import create_engine, text
from src.utils.team_mapping import normaliser_nom
from dotenv import load_dotenv
import os

load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{POSTGRES_HOST}:5432/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)


def construire_clubs_unified() -> pd.DataFrame:
    print("Construction table clubs unifiée...")

    # Charger les clubs Transfermarkt
    clubs_tm = pd.read_sql("""
        SELECT
            club_id,
            name,
            domestic_competition_id,
            squad_size,
            average_age,
            foreigners_number,
            foreigners_percentage,
            national_team_players,
            stadium_name,
            stadium_seats,
            coach_name,
            last_season
        FROM marts_players.clubs
    """, engine)

    # Mapping compétition Transfermarkt → code standard
    comp_mapping = {
        "GB1": "PL", "ES1": "PD", "IT1": "SA",
        "L1":  "BL1", "FR1": "FL1",
    }

    # Normalisation du nom
    clubs_tm["name_normalized"] = clubs_tm["name"].apply(normaliser_nom)
    clubs_tm["competition"]     = clubs_tm["domestic_competition_id"].map(
        comp_mapping
    )

    # Noms lisibles
    comp_labels = {
        "PL": "Premier League", "PD": "La Liga",
        "SA": "Serie A", "BL1": "Bundesliga", "FL1": "Ligue 1",
    }
    clubs_tm["competition_name"] = clubs_tm["competition"].map(comp_labels)

    print(f"  → {len(clubs_tm)} clubs Transfermarkt")
    print(f"  → {clubs_tm['name_normalized'].nunique()} noms normalisés uniques")

    # Vérification — clubs sans mapping
    non_mappes = clubs_tm[
        clubs_tm["name_normalized"] == clubs_tm["name"]
    ][["club_id", "name", "domestic_competition_id"]]

    if len(non_mappes) > 0:
        print(f"\n  ⚠️  {len(non_mappes)} clubs sans mapping normalisé :")
        print(non_mappes.head(20).to_string())
    else:
        print(f"  ✅ Tous les clubs sont normalisés")

    return clubs_tm


def charger_postgres(df: pd.DataFrame, table: str, schema: str) -> None:
    try:
        with engine.connect() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS {schema}.{table} CASCADE'))
            conn.execute(text('COMMIT'))
        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False
        )
        print(f"  → PostgreSQL : {schema}.{table} ({len(df)} lignes)")
    except Exception as e:
        raise RuntimeError(f"Erreur chargement PostgreSQL {schema}.{table} : {e}")


def main():
    print("\n===== CLUBS UNIFIÉS =====\n")
    clubs = construire_clubs_unified()
    charger_postgres(clubs, "clubs_unified", "marts_clubs")
    print("\n✅ Clubs unifiés construits.")


if __name__ == "__main__":
    main()