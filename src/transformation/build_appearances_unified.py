import pandas as pd
from sqlalchemy import create_engine, text
from src.utils.team_mapping import normaliser_nom
from dotenv import load_dotenv
import io
import os
import hashlib

load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{POSTGRES_HOST}:{POSTGRES_PORT}/{os.getenv('POSTGRES_DB')}"
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
            player_name
        FROM staging.stg_appearances
        WHERE date >= '2012-01-01'
    """, engine)
    print(f"  → {len(appearances)} apparitions chargées")

    # Matchs Transfermarkt avec noms clubs
    games_tm = pd.read_csv(
        "data/brut/transfermarkt/games.csv",
        encoding="utf-8",
        encoding_errors="replace"
    )
    games_tm["date"] = pd.to_datetime(games_tm["date"], errors="coerce")
    games_tm = games_tm[games_tm["date"] >= "2012-01-01"]

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


def charger_postgres(df, table: str, schema: str) -> None:
    try:
        # 1. S'assurer que le schéma existe
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

        # 2. Méthode d'insertion rapide native PostgreSQL (COPY)
        def psql_insert_copy(table_obj, conn_obj, keys, data_iter):
            dbapi_conn = conn_obj.connection
            with dbapi_conn.cursor() as cur:
                s_buf = io.StringIO()
                for row in data_iter:
                    s_buf.write(
                        "\t".join(
                            [str(val) if val is not None else "" for val in row]
                        )
                        + "\n"
                    )
                s_buf.seek(0)
                columns = ", ".join([f'"{k}"' for k in keys])
                table_name = f'"{table_obj.schema}"."{table_obj.name}"'
                sql = f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT csv, DELIMITER '\t', NULL '')"
                cur.copy_expert(sql=sql, file=s_buf)

        # 3. Écriture ultra-rapide
        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            method=psql_insert_copy,  # <-- Écriture instantanée
        )
        print(f"  → PostgreSQL : {schema}.{table} ({len(df)} lignes)")
    except Exception as e:
        raise RuntimeError(
            f"Erreur chargement PostgreSQL {schema}.{table} : {e}"
        )


def main():
    print("\n===== APPARITIONS UNIFIÉES =====\n")
    df = construire_appearances_unified()
    charger_postgres(df, "appearances_unified", "marts_players")
    print("\n✅ Apparitions unifiées construites.")


if __name__ == "__main__":
    main()