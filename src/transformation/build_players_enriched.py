import pandas as pd
from sqlalchemy import create_engine
from src.utils.team_mapping import normaliser_nom
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)


def construire_players_enriched() -> pd.DataFrame:
    print("Construction table joueurs enrichie...")

    players = pd.read_sql("""
        SELECT
            p.player_id,
            p.first_name,
            p.last_name,
            p.name,
            p.date_of_birth,
            p.position,
            p.sub_position,
            p.foot,
            p.height_in_cm,
            p.country_of_birth,
            p.country_of_citizenship,
            p.current_club_id,
            p.current_club_name,
            p.current_club_domestic_competition_id,
            p.last_season,
            p.market_value_in_eur,
            p.highest_market_value_in_eur,
            p.international_caps,
            p.international_goals
        FROM marts_players.players p
        WHERE p.position IS NOT NULL
          AND p.position != 'Missing'
    """, engine)

    # Normalisation du nom du club actuel
    players["current_club_normalized"] = players["current_club_name"].apply(
        lambda x: normaliser_nom(str(x)) if pd.notnull(x) else None
    )

    # Mapping compétition actuelle → code standard
    comp_mapping = {
        "GB1": "PL", "ES1": "PD", "IT1": "SA",
        "L1": "BL1", "FR1": "FL1",
    }
    comp_labels = {
        "PL": "Premier League", "PD": "La Liga",
        "SA": "Serie A", "BL1": "Bundesliga", "FL1": "Ligue 1",
    }

    players["current_competition"] = players[
        "current_club_domestic_competition_id"
    ].map(comp_mapping)

    players["current_competition_name"] = players[
        "current_competition"
    ].map(comp_labels)

    # Calcul âge actuel
    players["date_of_birth"] = pd.to_datetime(
        players["date_of_birth"], errors="coerce"
    )
    today = pd.Timestamp.now()
    players["age"] = (
        (today - players["date_of_birth"]).dt.days / 365.25
    ).round(1)

    # Jointure avec player_performance pour les stats agrégées
    perf = pd.read_sql("""
        SELECT
            player_id,
            SUM(matchs_joues)                   AS matchs_joues_total,
            SUM(total_goals)                     AS total_goals,
            SUM(total_assists)                   AS total_assists,
            SUM(total_minutes)                   AS total_minutes,
            ROUND(AVG(goals_per_90)::NUMERIC, 3) AS goals_per_90,
            ROUND(AVG(assists_per_90)::NUMERIC, 3) AS assists_per_90,
            ROUND(AVG(goal_contributions_per_90)::NUMERIC, 3)
                AS goal_contributions_per_90,
            ROUND(AVG(value_efficiency)::NUMERIC, 4) AS value_efficiency
        FROM marts_players.player_performance
        GROUP BY player_id
    """, engine)

    players_enriched = players.merge(perf, on="player_id", how="left")

    # Jointure avec dernière prédiction ML
    predictions = pd.read_sql("""
        SELECT DISTINCT ON (player_id)
            player_id,
            predicted_value,
            actual_value,
            difference_pct,
            evaluation
        FROM marts_ml.predictions_market_value_temporal
        ORDER BY player_id, date_t1 DESC
    """, engine)

    players_enriched = players_enriched.merge(
        predictions, on="player_id", how="left"
    )

    print(f"  → {len(players_enriched)} joueurs enrichis")
    print(f"  → {players_enriched['current_competition'].notna().sum()} joueurs dans les 5 grandes ligues")
    print(f"  → {players_enriched['predicted_value'].notna().sum()} joueurs avec prédiction ML")

    return players_enriched


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
    print("\n===== JOUEURS ENRICHIS =====\n")
    df = construire_players_enriched()
    charger_postgres(df, "players_enriched", "marts_players")
    print("\n✅ Joueurs enrichis construits.")
    print(f"\nAperçu :")
    print(df[[
        "name", "position", "age",
        "current_club_normalized", "current_competition_name",
        "market_value_in_eur", "predicted_value", "evaluation"
    ]].dropna(subset=["predicted_value"]).head(10).to_string())


if __name__ == "__main__":
    main()