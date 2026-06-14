import duckdb
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv
import os

load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{POSTGRES_HOST}:{POSTGRES_PORT}/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)

TM_DIR = Path("data/brut/transfermarkt")

# une seule connexion DuckDB partagée
con = duckdb.connect()


def charger_csv(fichier: str) -> pd.DataFrame:
    path = TM_DIR / fichier
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")
    df = pd.read_csv(path, encoding="utf-8", encoding_errors="replace")
    print(f"  → {fichier} : {len(df)} lignes")
    return df


def transformer_players(df: pd.DataFrame) -> pd.DataFrame:
    print("Transformation players...")

    df_clean = con.execute("""
        SELECT
            player_id,
            first_name,
            last_name,
            name,
            CAST(date_of_birth AS DATE)                    AS date_of_birth,
            position,
            sub_position,
            foot,
            CAST(height_in_cm AS INTEGER)                  AS height_in_cm,
            country_of_birth,
            country_of_citizenship,
            current_club_id,
            current_club_name,
            current_club_domestic_competition_id,
            CAST(last_season AS INTEGER)                   AS last_season,
            -- CORRECTION 2 — contrôle qualité valeur >= 0
            CASE
                WHEN CAST(market_value_in_eur AS DOUBLE) >= 0
                THEN CAST(market_value_in_eur AS DOUBLE)
                ELSE NULL
            END AS market_value_in_eur,
            CASE
                WHEN CAST(highest_market_value_in_eur AS DOUBLE) >= 0
                THEN CAST(highest_market_value_in_eur AS DOUBLE)
                ELSE NULL
            END AS highest_market_value_in_eur,
            CAST(international_caps AS INTEGER)            AS international_caps,
            CAST(international_goals AS INTEGER)           AS international_goals
        FROM df
        WHERE player_id IS NOT NULL
          AND name IS NOT NULL
    """).df()

    # déduplication sur player_id
    nb_avant = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["player_id"])
    if nb_avant != len(df_clean):
        print(f"  ⚠️  {nb_avant - len(df_clean)} doublons supprimés sur player_id")

    print(f"  → {len(df_clean)} joueurs valides")
    return df_clean


def transformer_clubs(df: pd.DataFrame) -> pd.DataFrame:
    print("Transformation clubs...")

    df_clean = con.execute("""
        SELECT
            club_id,
            name,
            domestic_competition_id,
            squad_size,
            CAST(average_age AS FLOAT)          AS average_age,
            foreigners_number,
            CAST(foreigners_percentage AS FLOAT) AS foreigners_percentage,
            national_team_players,
            stadium_name,
            CAST(stadium_seats AS INTEGER)       AS stadium_seats,
            coach_name,
            CAST(last_season AS INTEGER)         AS last_season
        FROM df
        WHERE club_id IS NOT NULL
    """).df()

    # déduplication clubs
    df_clean = df_clean.drop_duplicates(subset=["club_id"])
    print(f"  → {len(df_clean)} clubs valides")
    return df_clean


def transformer_valuations(df: pd.DataFrame) -> pd.DataFrame:
    print("Transformation valuations...")

    df_clean = con.execute("""
        SELECT
            player_id,
            CAST(date AS DATE)                      AS date,
            -- CORRECTION 5 — contrôle qualité valeur >= 0
            CASE
                WHEN CAST(market_value_in_eur AS DOUBLE) >= 0
                THEN CAST(market_value_in_eur AS DOUBLE)
                ELSE NULL
            END AS market_value_in_eur,
            current_club_name,
            current_club_id,
            player_club_domestic_competition_id
        FROM df
        WHERE player_id IS NOT NULL
          AND market_value_in_eur IS NOT NULL
    """).df()

    print(f"  → {len(df_clean)} valuations valides")
    return df_clean


def transformer_transfers(df: pd.DataFrame) -> pd.DataFrame:
    print("Transformation transfers...")

    df_clean = con.execute("""
        SELECT
            player_id,
            player_name,
            -- CORRECTION 6 — filtrage dates futures
            CASE
                WHEN CAST(transfer_date AS DATE) <= CURRENT_DATE
                THEN CAST(transfer_date AS DATE)
                ELSE NULL
            END AS transfer_date,
            transfer_season,
            from_club_id,
            from_club_name,
            to_club_id,
            to_club_name,
            -- CORRECTION 7 — contrôle qualité transfer_fee >= 0
            CASE
                WHEN CAST(transfer_fee AS DOUBLE) >= 0
                THEN CAST(transfer_fee AS DOUBLE)
                ELSE NULL
            END AS transfer_fee,
            CASE
                WHEN CAST(market_value_in_eur AS DOUBLE) >= 0
                THEN CAST(market_value_in_eur AS DOUBLE)
                ELSE NULL
            END AS market_value_in_eur
        FROM df
        WHERE player_id IS NOT NULL
    """).df()

    print(f"  → {len(df_clean)} transferts valides")
    return df_clean


def transformer_appearances(df_app: pd.DataFrame,
                            df_players: pd.DataFrame) -> pd.DataFrame:
    print("Transformation appearances...")

    df_clean = con.execute("""
        SELECT
            a.appearance_id,
            a.game_id,
            a.player_id,
            a.player_club_id,
            CAST(a.date AS DATE)            AS date,
            a.competition_id,
            -- CORRECTION 8 — contrôle qualité stats >= 0
            CASE WHEN CAST(a.goals AS INTEGER) >= 0
                 THEN CAST(a.goals AS INTEGER) ELSE 0 END          AS goals,
            CASE WHEN CAST(a.assists AS INTEGER) >= 0
                 THEN CAST(a.assists AS INTEGER) ELSE 0 END        AS assists,
            CASE WHEN CAST(a.yellow_cards AS INTEGER) >= 0
                 THEN CAST(a.yellow_cards AS INTEGER) ELSE 0 END   AS yellow_cards,
            CASE WHEN CAST(a.red_cards AS INTEGER) >= 0
                 THEN CAST(a.red_cards AS INTEGER) ELSE 0 END      AS red_cards,
            CASE WHEN CAST(a.minutes_played AS INTEGER) >= 0
                 THEN CAST(a.minutes_played AS INTEGER) ELSE 0 END AS minutes_played,
            p.name       AS player_name,
            p.position,
            p.market_value_in_eur
        FROM df_app a
        LEFT JOIN df_players p ON a.player_id = p.player_id
        WHERE a.minutes_played > 0
          AND a.player_id IS NOT NULL
    """).df()

    # déduplication sur appearance_id
    nb_avant = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["appearance_id"])
    if nb_avant != len(df_clean):
        print(f"  ⚠️  {nb_avant - len(df_clean)} doublons supprimés sur appearance_id")

    print(f"  → {len(df_clean)} apparitions valides")
    return df_clean


def construire_player_performance(df_app: pd.DataFrame) -> pd.DataFrame:
    print("Construction player_performance...")

    df_perf = con.execute("""
        SELECT
            player_id,
            player_name,
            position,
            market_value_in_eur,
            competition_id,
            COUNT(*)                                            AS matchs_joues,
            SUM(goals)                                          AS total_goals,
            SUM(assists)                                        AS total_assists,
            SUM(minutes_played)                                 AS total_minutes,
            SUM(yellow_cards)                                   AS total_yellow_cards,
            SUM(red_cards)                                      AS total_red_cards,
            ROUND(SUM(goals) * 90.0 /
                NULLIF(SUM(minutes_played), 0), 3)              AS goals_per_90,
            ROUND(SUM(assists) * 90.0 /
                NULLIF(SUM(minutes_played), 0), 3)              AS assists_per_90,
            ROUND((SUM(goals) + SUM(assists)) * 90.0 /
                NULLIF(SUM(minutes_played), 0), 3)              AS goal_contributions_per_90,
            ROUND(AVG(minutes_played), 1)                       AS avg_minutes_per_match,
            ROUND(
                (SUM(goals) + SUM(assists)) * 90.0 /
                NULLIF(SUM(minutes_played), 0) /
                NULLIF(market_value_in_eur / 1000000.0, 0),
            4)                                                  AS value_efficiency
        FROM df_app
        WHERE market_value_in_eur IS NOT NULL
          AND market_value_in_eur > 0
        GROUP BY player_id, player_name, position,
                 market_value_in_eur, competition_id
        HAVING SUM(minutes_played) >= 90
        ORDER BY goals_per_90 DESC
    """).df()

    print(f"  → {len(df_perf)} profils de performance")
    return df_perf


def charger_postgres(df: pd.DataFrame, table: str, schema: str) -> None:
    try:
        # DROP CASCADE pour gérer les clés étrangères
        with engine.connect() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS {schema}.{table} CASCADE'))
            conn.execute(text('COMMIT'))

        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists="append",  # append car on vient de dropper
            index=False
        )
        print(f"  → PostgreSQL : {schema}.{table} ({len(df)} lignes)")
    except Exception as e:
        raise RuntimeError(f"Erreur chargement PostgreSQL {schema}.{table} : {e}")


def main():
    print("\n===== PIPELINE JOUEURS =====\n")

    # Chargement brut
    print("Chargement CSV Transfermarkt...")
    df_players_raw = charger_csv("players.csv")
    df_appearances = charger_csv("appearances.csv")
    df_clubs       = charger_csv("clubs.csv")
    df_valuations  = charger_csv("player_valuations.csv")
    df_transfers   = charger_csv("transfers.csv")

    # Staging — données brutes telles quelles
    print("\nChargement staging...")
    charger_postgres(df_players_raw, "stg_players",     "staging")
    charger_postgres(df_appearances, "stg_appearances", "staging")
    charger_postgres(df_valuations,  "stg_valuations",  "staging")
    charger_postgres(df_transfers,   "stg_transfers",   "staging")

    # Transformation — données nettoyées
    print("\nTransformation...")
    df_players_clean = transformer_players(df_players_raw)
    df_clubs_clean   = transformer_clubs(df_clubs)
    df_val_clean     = transformer_valuations(df_valuations)
    df_tr_clean      = transformer_transfers(df_transfers)

    charger_postgres(df_players_clean, "players",           "marts_players")
    charger_postgres(df_clubs_clean,   "clubs",             "marts_players")
    charger_postgres(df_val_clean,     "player_valuations", "marts_players")
    charger_postgres(df_tr_clean,      "transfers",         "marts_players")

    # Appearances + performance
    df_app_clean = transformer_appearances(df_appearances, df_players_clean)
    charger_postgres(df_app_clean, "appearances", "marts_players")

    df_perf = construire_player_performance(df_app_clean)
    charger_postgres(df_perf, "player_performance", "marts_players")

    print("\n✅ Pipeline joueurs terminé.")


if __name__ == "__main__":
    main()