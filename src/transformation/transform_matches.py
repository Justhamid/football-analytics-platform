import duckdb
import pandas as pd
import json
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{POSTGRES_HOST}:5432/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)

API_DIR = Path("data/brut/api")


def charger_matches_api() -> pd.DataFrame:
    print("Chargement fichiers API...")

    # vérification dossier vide
    fichiers = sorted(API_DIR.glob("*.json"))
    if not fichiers:
        raise FileNotFoundError(f"Aucun fichier JSON trouvé dans {API_DIR}")

    toutes_lignes = []

    for fichier in fichiers:
        # gestion JSON mal formé
        try:
            with open(fichier, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"  ⚠️  Fichier JSON invalide : {fichier.name} — {e}")
            continue

        competition = data.get("competition", {})
        matches = data.get("matches", [])

        if not matches:
            print(f"  ⚠️  Aucun match dans {fichier.name}")
            continue

        for match in matches:
            toutes_lignes.append({
                "game_id":          match.get("id"),
                "competition_id":   competition.get("code"),
                "competition_name": competition.get("name"),
                "season":           match.get("season", {}).get("startDate", "")[:4],
                "date":             match.get("utcDate", "")[:10],
                "round":            match.get("matchday"),
                "home_club_name":   match.get("homeTeam", {}).get("name"),
                "away_club_name":   match.get("awayTeam", {}).get("name"),
                "home_club_id":     match.get("homeTeam", {}).get("id"),
                "away_club_id":     match.get("awayTeam", {}).get("id"),
                "home_club_goals":  match.get("score", {}).get("fullTime", {}).get("home"),
                "away_club_goals":  match.get("score", {}).get("fullTime", {}).get("away"),
                "status":           match.get("status"),
            })

    df = pd.DataFrame(toutes_lignes)

    # déduplication sur game_id
    nb_avant = len(df)
    df = df.drop_duplicates(subset=["game_id"])
    nb_apres = len(df)
    if nb_avant != nb_apres:
        print(f"  ⚠️  {nb_avant - nb_apres} doublons supprimés sur game_id")

    print(f"  → {len(df)} matchs chargés")
    return df


def transformer_matches(df: pd.DataFrame) -> pd.DataFrame:
    print("Transformation matches...")

    con = duckdb.connect()

    df_clean = con.execute("""
        SELECT
            game_id,
            competition_id,
            competition_name,
            CAST(season AS INTEGER) AS season,
            CAST(date AS DATE) AS date,
            CAST(round AS INTEGER) AS round,
            home_club_id,
            home_club_name,
            away_club_id,
            away_club_name,
            -- CORRECTION 4 — contrôle qualité buts >= 0
            CASE
                WHEN CAST(home_club_goals AS INTEGER) >= 0
                THEN CAST(home_club_goals AS INTEGER)
                ELSE NULL
            END AS home_club_goals,
            CASE
                WHEN CAST(away_club_goals AS INTEGER) >= 0
                THEN CAST(away_club_goals AS INTEGER)
                ELSE NULL
            END AS away_club_goals,
            status,
            CASE
                WHEN home_club_goals > away_club_goals  THEN 'home'
                WHEN away_club_goals > home_club_goals  THEN 'away'
                WHEN home_club_goals = away_club_goals
                     AND home_club_goals IS NOT NULL    THEN 'draw'
                ELSE 'unknown'
            END AS result,
            CASE
                WHEN home_club_goals IS NOT NULL
                 AND away_club_goals IS NOT NULL
                THEN home_club_goals + away_club_goals
                ELSE NULL
            END AS total_goals
        FROM df
        WHERE status = 'FINISHED'
    """).df()

    print(f"  → {len(df_clean)} matchs terminés")
    return df_clean


def enrichir_matches(df: pd.DataFrame) -> pd.DataFrame:
    print("Enrichissement matches...")

    con = duckdb.connect()

    df_enrichi = con.execute("""
        SELECT
            *,
            CASE
                WHEN result = 'home' THEN 3
                WHEN result = 'draw' THEN 1
                ELSE 0
            END AS home_points,
            CASE
                WHEN result = 'away' THEN 3
                WHEN result = 'draw' THEN 1
                ELSE 0
            END AS away_points
        FROM df
    """).df()

    print(f"  → {len(df_enrichi)} matchs enrichis")
    return df_enrichi


def construire_classements(df: pd.DataFrame) -> pd.DataFrame:
    print("Construction classements...")

    con = duckdb.connect()

    classements = con.execute("""
        WITH home_stats AS (
            SELECT
                competition_id,
                competition_name,
                season,
                home_club_id   AS club_id,
                home_club_name AS club_name,
                COUNT(*)       AS matchs_joues,
                SUM(home_points)      AS points,
                SUM(home_club_goals)  AS buts_marques,
                SUM(away_club_goals)  AS buts_encaisses,
                SUM(CASE WHEN result = 'home' THEN 1 ELSE 0 END) AS victoires,
                SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END) AS nuls,
                SUM(CASE WHEN result = 'away' THEN 1 ELSE 0 END) AS defaites
            FROM df
            GROUP BY competition_id, competition_name, season,
                     home_club_id, home_club_name
        ),
        away_stats AS (
            SELECT
                competition_id,
                competition_name,
                season,
                away_club_id   AS club_id,
                away_club_name AS club_name,
                COUNT(*)       AS matchs_joues,
                SUM(away_points)      AS points,
                SUM(away_club_goals)  AS buts_marques,
                SUM(home_club_goals)  AS buts_encaisses,
                SUM(CASE WHEN result = 'away' THEN 1 ELSE 0 END) AS victoires,
                SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END) AS nuls,
                SUM(CASE WHEN result = 'home' THEN 1 ELSE 0 END) AS defaites
            FROM df
            GROUP BY competition_id, competition_name, season,
                     away_club_id, away_club_name
        ),
        total AS (
            SELECT
                competition_id,
                competition_name,
                season,
                club_id,
                club_name,
                SUM(matchs_joues)    AS matchs_joues,
                SUM(points)          AS points,
                SUM(buts_marques)    AS buts_marques,
                SUM(buts_encaisses)  AS buts_encaisses,
                SUM(victoires)       AS victoires,
                SUM(nuls)            AS nuls,
                SUM(defaites)        AS defaites
            FROM (
                SELECT * FROM home_stats
                UNION ALL
                SELECT * FROM away_stats
            )
            GROUP BY competition_id, competition_name, season,
                     club_id, club_name
        )
        SELECT
            *,
            buts_marques - buts_encaisses AS diff_buts,
            ROUND(
                CAST(buts_marques AS FLOAT) / NULLIF(matchs_joues, 0),
            2) AS moy_buts_par_match
        FROM total
        -- CORRECTION 5 — tri complet avec diff_buts et buts_marques
        ORDER BY
            competition_id,
            season,
            points       DESC,
            diff_buts    DESC,
            buts_marques DESC
    """).df()

    print(f"  → {len(classements)} lignes classement")
    return classements


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
    print("\n===== PIPELINE MATCHES =====\n")

    df_raw     = charger_matches_api()
    charger_postgres(df_raw, "stg_matches_api", "staging")

    df_clean   = transformer_matches(df_raw)
    df_enrichi = enrichir_matches(df_clean)
    charger_postgres(df_enrichi, "matches", "marts_clubs")
    charger_postgres(
        construire_classements(df_enrichi),
        "classements_equipes",
        "marts_clubs"
    )

    print("\n✅ Pipeline matches terminé.")


if __name__ == "__main__":
    main()