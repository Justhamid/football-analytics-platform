import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)


def construire_features() -> pd.DataFrame:
    print("Construction dataset features ML...")

    query = """
        SELECT
            p.player_id,
            p.name,
            p.position,
            p.foot,
            p.country_of_citizenship,
            p.international_caps,
            p.international_goals,
            p.highest_market_value_in_eur,
            p.market_value_in_eur AS target_market_value,

            COALESCE(pp.matchs_joues, 0)           AS matchs_joues,
            COALESCE(pp.total_goals, 0)            AS total_goals,
            COALESCE(pp.total_assists, 0)          AS total_assists,
            COALESCE(pp.total_minutes, 0)          AS total_minutes,

            ROUND(
                (COALESCE(pp.total_goals, 0) * 90.0 /
                NULLIF(COALESCE(pp.total_minutes, 0), 0))::NUMERIC
            , 3) AS goals_per_90,

            ROUND(
                (COALESCE(pp.total_assists, 0) * 90.0 /
                NULLIF(COALESCE(pp.total_minutes, 0), 0))::NUMERIC
            , 3) AS assists_per_90,

            ROUND(
                ((COALESCE(pp.total_goals, 0) + COALESCE(pp.total_assists, 0)) * 90.0 /
                NULLIF(COALESCE(pp.total_minutes, 0), 0))::NUMERIC
            , 3) AS goal_contributions_per_90,

            COALESCE(pp.avg_minutes_per_match, 0)  AS avg_minutes_per_match

        FROM marts_players.players p
        LEFT JOIN (
            SELECT
                player_id,
                SUM(matchs_joues)          AS matchs_joues,
                SUM(total_goals)           AS total_goals,
                SUM(total_assists)         AS total_assists,
                SUM(total_minutes)         AS total_minutes,
                AVG(avg_minutes_per_match) AS avg_minutes_per_match
            FROM marts_players.player_performance
            GROUP BY player_id
        ) pp ON p.player_id = pp.player_id

        WHERE p.market_value_in_eur IS NOT NULL
        AND p.market_value_in_eur > 0
        AND p.position IS NOT NULL
    """

    df = pd.read_sql(query, engine)
    print(f"  → {len(df)} joueurs avec valeur marchande connue")
    return df


def encoder_features(df: pd.DataFrame) -> pd.DataFrame:
    print("Encodage des variables catégorielles...")

    # fusionner Turkey / Türkiye
    df["country_of_citizenship"] = df["country_of_citizenship"].replace(
        "Türkiye", "Turkey"
    )

    # remplacer "Missing" par NaN puis imputer
    df["position"] = df["position"].replace("Missing", None)
    df = df.dropna(subset=["position"])

    # One-hot encoding position
    positions = pd.get_dummies(df["position"], prefix="pos")
    df = pd.concat([df, positions], axis=1)

    # One-hot encoding pied
    foot_dummies = pd.get_dummies(df["foot"], prefix="foot")
    df = pd.concat([df, foot_dummies], axis=1)

    # Encodage nationalité — top 20 pays + "other"
    top_pays = df["country_of_citizenship"].value_counts().head(20).index
    df["nationality_group"] = df["country_of_citizenship"].apply(
        lambda x: x if x in top_pays else "other"
    )
    nationality_dummies = pd.get_dummies(df["nationality_group"], prefix="nat")
    df = pd.concat([df, nationality_dummies], axis=1)

    # Remplacement des nulls numériques
    df["international_caps"]          = df["international_caps"].fillna(0)
    df["international_goals"]         = df["international_goals"].fillna(0)
    df["highest_market_value_in_eur"] = df["highest_market_value_in_eur"].fillna(
        df["target_market_value"]
    )
    df["goals_per_90"]              = df["goals_per_90"].fillna(0)
    df["assists_per_90"]            = df["assists_per_90"].fillna(0)
    df["goal_contributions_per_90"] = df["goal_contributions_per_90"].fillna(0)

    print(f"  → {len(df)} joueurs encodés")
    print(f"  → Colonnes créées : {[c for c in df.columns if c.startswith(('pos_', 'foot_', 'nat_'))]}")

    return df


def sauvegarder_features(df: pd.DataFrame) -> None:

    # On identifie automatiquement toutes les colonnes one-hot créées
    colonnes_one_hot = [c for c in df.columns
                        if c.startswith(("pos_", "foot_", "nat_"))]

    colonnes_features = [
        "player_id",
        "name",
        "position",
        "target_market_value",
        "international_caps",
        "international_goals",
        "highest_market_value_in_eur",
        "matchs_joues",
        "total_goals",
        "total_assists",
        "total_minutes",
        "goals_per_90",
        "assists_per_90",
        "goal_contributions_per_90",
        "avg_minutes_per_match",
    ] + colonnes_one_hot

    df[colonnes_features].to_sql(
        name="features_market_value",
        con=engine,
        schema="marts_ml",
        if_exists="replace",
        index=False
    )
    print(f"  → PostgreSQL : marts_ml.features_market_value ({len(df)} lignes)")
    print(f"  → {len(colonnes_features)} colonnes au total")


def main():
    print("\n===== CONSTRUCTION FEATURES ML =====\n")
    df = construire_features()
    df = encoder_features(df)
    sauvegarder_features(df)
    print("\n✅ Features ML construites.")


if __name__ == "__main__":
    main()