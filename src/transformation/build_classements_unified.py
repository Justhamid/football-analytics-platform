import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{POSTGRES_HOST}:5432/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)


def charger_unified_matches() -> pd.DataFrame:
    print("Chargement unified_matches...")
    df = pd.read_sql("""
        SELECT
            match_id,
            competition,
            date,
            season,
            home_team,
            away_team,
            home_goals,
            away_goals,
            result,
            total_goals,
            home_shots,
            away_shots,
            home_shots_on_target,
            away_shots_on_target,
            home_corners,
            away_corners,
            home_yellow_cards,
            away_yellow_cards,
            home_red_cards,
            away_red_cards
        FROM marts_clubs.unified_matches
        WHERE home_goals IS NOT NULL
          AND away_goals IS NOT NULL
    """, engine)

    # Noms lisibles pour les compétitions
    comp_labels = {
        "PL":  "Premier League",
        "PD":  "La Liga",
        "SA":  "Serie A",
        "BL1": "Bundesliga",
        "FL1": "Ligue 1",
    }
    df["competition_name"] = df["competition"].map(comp_labels).fillna(df["competition"])

    # Points
    df["home_points"] = df["result"].map({"home": 3, "draw": 1, "away": 0})
    df["away_points"] = df["result"].map({"away": 3, "draw": 1, "home": 0})

    print(f"  → {len(df)} matchs chargés")
    print(f"  → Saisons : {sorted(df['season'].dropna().unique())}")
    return df


def construire_classements(df: pd.DataFrame) -> pd.DataFrame:
    print("\nConstruction classements...")

    con = duckdb.connect()

    classements = con.execute("""
        WITH home_stats AS (
            SELECT
                competition,
                competition_name,
                season,
                home_team      AS club_name,
                COUNT(*)       AS matchs_joues,
                SUM(home_points)     AS points,
                SUM(home_goals)      AS buts_marques,
                SUM(away_goals)      AS buts_encaisses,
                SUM(CASE WHEN result = 'home' THEN 1 ELSE 0 END) AS victoires,
                SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END) AS nuls,
                SUM(CASE WHEN result = 'away' THEN 1 ELSE 0 END) AS defaites
            FROM df
            GROUP BY competition, competition_name, season, home_team
        ),
        away_stats AS (
            SELECT
                competition,
                competition_name,
                season,
                away_team      AS club_name,
                COUNT(*)       AS matchs_joues,
                SUM(away_points)     AS points,
                SUM(away_goals)      AS buts_marques,
                SUM(home_goals)      AS buts_encaisses,
                SUM(CASE WHEN result = 'away' THEN 1 ELSE 0 END) AS victoires,
                SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END) AS nuls,
                SUM(CASE WHEN result = 'home' THEN 1 ELSE 0 END) AS defaites
            FROM df
            GROUP BY competition, competition_name, season, away_team
        ),
        total AS (
            SELECT
                competition,
                competition_name,
                season,
                club_name,
                SUM(matchs_joues)   AS matchs_joues,
                SUM(points)         AS points,
                SUM(buts_marques)   AS buts_marques,
                SUM(buts_encaisses) AS buts_encaisses,
                SUM(victoires)      AS victoires,
                SUM(nuls)           AS nuls,
                SUM(defaites)       AS defaites
            FROM (
                SELECT * FROM home_stats
                UNION ALL
                SELECT * FROM away_stats
            )
            GROUP BY competition, competition_name, season, club_name
        )
        SELECT
            *,
            buts_marques - buts_encaisses AS diff_buts,
            ROUND(
                CAST(buts_marques AS FLOAT) / NULLIF(matchs_joues, 0)
            , 2) AS moy_buts_par_match,
            ROUND(
                CAST(buts_encaisses AS FLOAT) / NULLIF(matchs_joues, 0)
            , 2) AS moy_buts_encaisses_par_match
        FROM total
        ORDER BY
            competition,
            season DESC,
            points      DESC,
            diff_buts   DESC,
            buts_marques DESC
    """).df()

    print(f"  → {len(classements)} lignes classement")
    print(f"  → Saisons couvertes : {sorted(classements['season'].unique())}")
    return classements


def construire_stats_avantage_domicile(df: pd.DataFrame) -> pd.DataFrame:
    print("\nConstruction avantage domicile...")

    con = duckdb.connect()

    stats = con.execute("""
        SELECT
            competition,
            competition_name,
            season,
            COUNT(*) AS total_matchs,
            SUM(CASE WHEN result = 'home' THEN 1 ELSE 0 END) AS victoires_domicile,
            SUM(CASE WHEN result = 'away' THEN 1 ELSE 0 END) AS victoires_exterieur,
            SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END) AS nuls,
            ROUND(
                SUM(CASE WHEN result = 'home' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(*), 0)
            , 1) AS pct_victoires_domicile,
            ROUND(AVG(total_goals), 2) AS moy_buts_par_match,
            ROUND(AVG(home_goals), 2)  AS moy_buts_domicile,
            ROUND(AVG(away_goals), 2)  AS moy_buts_exterieur
        FROM df
        GROUP BY competition, competition_name, season
        ORDER BY competition, season DESC
    """).df()

    print(f"  → {len(stats)} lignes avantage domicile")
    return stats


def construire_stats_matchs_enrichis(df: pd.DataFrame) -> pd.DataFrame:
    print("\nConstruction matches enrichis...")

    # On garde les colonnes stats supplémentaires de football-datasets
    cols = [
        "match_id", "competition", "competition_name", "season", "date",
        "home_team", "away_team", "home_goals", "away_goals",
        "result", "total_goals",
        "home_shots", "away_shots",
        "home_shots_on_target", "away_shots_on_target",
        "home_corners", "away_corners",
        "home_yellow_cards", "away_yellow_cards",
        "home_red_cards", "away_red_cards",
        "home_points", "away_points"
    ]
    df_enrichi = df[cols].copy()
    print(f"  → {len(df_enrichi)} matchs enrichis")
    return df_enrichi


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
    print("\n===== CLASSEMENTS UNIFIÉS 2017-2025 =====\n")

    df = charger_unified_matches()

    classements  = construire_classements(df)
    avantage_dom = construire_stats_avantage_domicile(df)
    matches_enrichis = construire_stats_matchs_enrichis(df)

    charger_postgres(classements,      "classements_equipes_unified", "marts_clubs")
    charger_postgres(avantage_dom,     "avantage_domicile",           "marts_clubs")
    charger_postgres(matches_enrichis, "matches_enrichis",            "marts_clubs")

    print("\n✅ Classements unifiés construits.")
    print(f"\nAperçu classement Premier League 2024 :")
    pl_2024 = classements[
        (classements["competition"] == "PL") &
        (classements["season"] == "2024")
    ][["club_name", "points", "victoires", "diff_buts"]].head(10)
    print(pl_2024.to_string())


if __name__ == "__main__":
    main()