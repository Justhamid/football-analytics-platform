import pandas as pd
import json
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)


def audit_api() -> pd.DataFrame:
    print("\n===== SOURCE 1 : API football-data =====")

    lignes = []
    for fichier in sorted(Path("data/brut/api").glob("*.json")):
        with open(fichier, "r", encoding="utf-8") as f:
            data = json.load(f)

        competition = data.get("competition", {}).get("code", "")
        for match in data.get("matches", []):
            lignes.append({
                "source":     "api",
                "competition": competition,
                "date":       match.get("utcDate", "")[:10],
                "home_team":  match.get("homeTeam", {}).get("name", ""),
                "away_team":  match.get("awayTeam", {}).get("name", ""),
                "home_goals": match.get("score", {}).get("fullTime", {}).get("home"),
                "away_goals": match.get("score", {}).get("fullTime", {}).get("away"),
                "status":     match.get("status", ""),
            })

    df = pd.DataFrame(lignes)
    df = df[df["status"] == "FINISHED"]
    print(f"Matchs terminés : {len(df)}")
    print(f"Compétitions    : {sorted(df['competition'].unique())}")
    print(f"Dates           : {df['date'].min()} → {df['date'].max()}")
    print(f"\nExemple noms équipes API :")
    print(df[["competition", "home_team"]].drop_duplicates().head(20).to_string())
    return df


def audit_football_datasets() -> pd.DataFrame:
    print("\n===== SOURCE 2 : football-datasets =====")

    mapping_ligues = {
        "premier_league": "PL",
        "la_liga":        "PD",
        "serie_a":        "SA",
        "bundesliga":     "BL1",
        "ligue_1":        "FL1",
    }

    lignes = []
    base = Path("data/brut/football_datasets")

    for dossier, code in mapping_ligues.items():
        chemin = base / dossier
        if not chemin.exists():
            continue

        for fichier in sorted(chemin.glob("season-*.csv")):
            # On ne garde que les saisons depuis 2017
            nom = fichier.stem  # ex: season-1718
            annee_debut = int("20" + nom[-4:-2]) if int(nom[-4:-2]) <= 30 else int("19" + nom[-4:-2])
            if annee_debut < 2017:
                continue

            try:
                df_s = pd.read_csv(fichier, encoding="utf-8", encoding_errors="replace")
                df_s["source"]      = "football_datasets"
                df_s["competition"] = code
                df_s["saison_fichier"] = nom
                lignes.append(df_s)
            except Exception as e:
                print(f"  ⚠️  Erreur {fichier.name} : {e}")

    if not lignes:
        print("Aucun fichier chargé.")
        return pd.DataFrame()

    df = pd.concat(lignes, ignore_index=True)

    # Colonnes utiles
    cols_utiles = ["source", "competition", "saison_fichier",
                   "Date", "HomeTeam", "AwayTeam",
                   "FTHG", "FTAG"]
    df = df[[c for c in cols_utiles if c in df.columns]]
    df = df.rename(columns={
        "Date":     "date_raw",
        "HomeTeam": "home_team",
        "AwayTeam": "away_team",
        "FTHG":     "home_goals",
        "FTAG":     "away_goals",
    })
    df = df.dropna(subset=["home_team", "away_team"])

    print(f"Matchs chargés  : {len(df)}")
    print(f"Compétitions    : {sorted(df['competition'].unique())}")
    print(f"Saisons         : {sorted(df['saison_fichier'].unique())}")
    print(f"\nExemple noms équipes football_datasets :")
    print(df[["competition", "home_team"]].drop_duplicates().head(20).to_string())

    return df


def audit_transfermarkt() -> pd.DataFrame:
    print("\n===== SOURCE 3 : Transfermarkt games =====")

    df = pd.read_csv(
        "data/brut/transfermarkt/games.csv",
        encoding="utf-8",
        encoding_errors="replace"
    )

    # Filtre 2017+
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"] >= "2017-01-01"]

    # Filtre 5 grandes ligues
    ligues_tm = ["GB1", "ES1", "IT1", "L1", "FR1"]
    df = df[df["competition_id"].isin(ligues_tm)]

    print(f"Matchs chargés  : {len(df)}")
    print(f"Compétitions    : {sorted(df['competition_id'].unique())}")
    print(f"Dates           : {df['date'].min()} → {df['date'].max()}")
    print(f"\nExemple noms équipes Transfermarkt :")
    print(df[["competition_id", "home_club_name"]].drop_duplicates().head(20).to_string())

    return df


def comparer_noms(df_api: pd.DataFrame,
                  df_fd: pd.DataFrame,
                  df_tm: pd.DataFrame) -> None:
    print("\n===== COMPARAISON DES NOMS D'ÉQUIPES =====")

    # Noms uniques par source pour PL
    noms_api = set(df_api[df_api["competition"] == "PL"]["home_team"].unique())
    noms_fd  = set(df_fd[df_fd["competition"]  == "PL"]["home_team"].unique()) if not df_fd.empty else set()
    noms_tm  = set(df_tm[df_tm["competition_id"] == "GB1"]["home_club_name"].unique())

    print(f"\n--- Premier League ---")
    print(f"API ({len(noms_api)}) : {sorted(noms_api)}")
    print(f"FD  ({len(noms_fd)})  : {sorted(noms_fd)}")
    print(f"TM  ({len(noms_tm)})  : {sorted(noms_tm)}")

    # Noms dans API mais pas dans football_datasets
    if noms_fd:
        print(f"\nDans API mais pas FD  : {sorted(noms_api - noms_fd)}")
        print(f"Dans FD mais pas API  : {sorted(noms_fd - noms_api)}")

    # Noms dans API mais pas dans Transfermarkt
    print(f"\nDans API mais pas TM  : {sorted(noms_api - noms_tm)}")
    print(f"Dans TM mais pas API  : {sorted(noms_tm - noms_api)}")


def main():
    df_api = audit_api()
    df_fd  = audit_football_datasets()
    df_tm  = audit_transfermarkt()
    comparer_noms(df_api, df_fd, df_tm)

    print("\n✅ Audit terminé.")


if __name__ == "__main__":
    main()