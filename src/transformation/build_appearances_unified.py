import pandas as pd
from sqlalchemy import create_engine
from src.utils.team_mapping import normaliser_nom
from dotenv import load_dotenv
import os
import hashlib

load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{POSTGRES_HOST}:5432/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)


def generer_match_id(date: str, home: str, away: str, competition: str) -> str:
    cle = f"{date}_{competition}_{home}_{away}"
    return hashlib.md5(cle.encode()).hexdigest()[:16]


def construire_appearances_unified() -> pd.DataFrame:
    print("Chargement des données...")

    # Apparitions Transfermarkt depuis 2017
    appearances = pd.read_sql("""
        SELECT
            appearance_id,
            game_id,
            player_id,
            player_club_id,
            date,
            competition_id,
            goals,
            assists,
            minutes_played,
            yellow_cards,
            red_cards,
            player_name,
            position,
            market_value_in_eur
        FROM marts_players.appearances
        WHERE date >= '2017-01-01'
    """, engine)
    print(f"  → {len(appearances)} apparitions chargées")

    # Matchs Transfermarkt avec noms clubs
    games_tm = pd.read_csv(
        "data/brut/transfermarkt/games.csv",
        encoding="utf-8",
        encoding_errors="replace"
    )
    games_tm["date"] = pd.to_datetime(games_tm["date"], errors="coerce")
    games_tm = games_tm[games_tm["date"] >= "2017-01-01"]

    # Mapping compétition Transfermarkt → code standard
    comp_mapping = {
        "GB1": "PL", "ES1": "PD", "IT1": "SA",
        "L1": "BL1", "FR1": "FL1",
    }
    games_tm["competition"] = games_tm["competition_id"].map(comp_mapping)
    games_tm = games_tm.dropna(subset=["competition"])

    # Normalisation noms
    games_tm["home_norm"] = games_tm["home_club_name"].apply(normaliser_nom)
    games_tm["away_norm"] = games_tm["away_club_name"].apply(normaliser_nom)
    games_tm["date_str"]  = games_tm["date"].dt.strftime("%Y-%m-%d")

    # Génération match_id pour chaque match Transfermarkt
    print("Génération des match_id pour les matchs Transfermarkt...")
    games_tm["match_id"] = games_tm.apply(
        lambda r: generer_match_id(
            r["date_str"], r["home_norm"],
            r["away_norm"], r["competition"]
        ), axis=1
    )

    # Table de correspondance game_id TM → match_id unifié
    correspondance = games_tm[[
        "game_id", "match_id", "competition",
        "date_str", "home_norm", "away_norm",
        "home_club_goals", "away_club_goals"
    ]].rename(columns={"date_str": "date"})

    print(f"  → {len(correspondance)} matchs Transfermarkt avec match_id")

    # Jointure apparitions ↔ correspondance
    print("Jointure apparitions ↔ match_id...")
    appearances["date"] = pd.to_datetime(
        appearances["date"], errors="coerce"
    )

    appearances_unified = appearances.merge(
        correspondance[["game_id", "match_id", "competition",
                        "home_norm", "away_norm"]],
        on="game_id",
        how="left"
    )

    # Stats de couverture
    avec_match_id = appearances_unified["match_id"].notna().sum()
    sans_match_id = appearances_unified["match_id"].isna().sum()
    pct           = round(avec_match_id / len(appearances_unified) * 100, 1)

    print(f"\n  Couverture match_id :")
    print(f"  → Avec match_id    : {avec_match_id:,} ({pct}%)")
    print(f"  → Sans match_id    : {sans_match_id:,} ({100-pct}%)")
    print(f"  → Total            : {len(appearances_unified):,}")

    return appearances_unified


def charger_postgres(df: pd.DataFrame, table: str, schema: str) -> None:
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False
    )
    print(f"  → PostgreSQL : {schema}.{table} ({len(df)} lignes)")


def main():
    print("\n===== APPARITIONS UNIFIÉES =====\n")
    df = construire_appearances_unified()
    charger_postgres(df, "appearances_unified", "marts_players")
    print("\n✅ Apparitions unifiées construites.")


if __name__ == "__main__":
    main()