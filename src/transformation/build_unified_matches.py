import pandas as pd
import json
import hashlib
from pathlib import Path
from sqlalchemy import create_engine
from src.utils.team_mapping import normaliser_nom
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)

# Mapping codes compétition vers notre standard
COMP_MAPPING = {
    # API
    "PL":  "PL", "BL1": "BL1", "FL1": "FL1", "PD": "PD", "SA": "SA",
    # football-datasets
    # (déjà mappé dans audit_jointure)
    # Transfermarkt
    "GB1": "PL", "L1": "BL1", "FR1": "FL1", "ES1": "PD", "IT1": "SA",
}


def generer_match_id(date: str, home: str, away: str, competition: str) -> str:
    """
    Génère un ID unique et déterministe pour un match
    basé sur date + équipes normalisées + compétition.
    """
    cle = f"{date}_{competition}_{home}_{away}"
    return hashlib.md5(cle.encode()).hexdigest()[:16]


def charger_api() -> pd.DataFrame:
    print("Chargement API football-data...")
    lignes = []

    for fichier in sorted(Path("data/brut/api").glob("*.json")):
        with open(fichier, "r", encoding="utf-8") as f:
            data = json.load(f)

        competition_code = data.get("competition", {}).get("code", "")

        for match in data.get("matches", []):
            if match.get("status") != "FINISHED":
                continue

            date       = match.get("utcDate", "")[:10]
            home_raw   = match.get("homeTeam", {}).get("name", "")
            away_raw   = match.get("awayTeam", {}).get("name", "")
            home_norm  = normaliser_nom(home_raw)
            away_norm  = normaliser_nom(away_raw)
            competition = COMP_MAPPING.get(competition_code, competition_code)

            lignes.append({
                "match_id":       generer_match_id(date, home_norm, away_norm, competition),
                "source":         "api",
                "competition":    competition,
                "date":           date,
                "season":         match.get("season", {}).get("startDate", "")[:4],
                "round":          match.get("matchday"),
                "home_team":      home_norm,
                "away_team":      away_norm,
                "home_goals":     match.get("score", {}).get("fullTime", {}).get("home"),
                "away_goals":     match.get("score", {}).get("fullTime", {}).get("away"),
            })

    df = pd.DataFrame(lignes)
    print(f"  → {len(df)} matchs API")
    return df


def charger_football_datasets() -> pd.DataFrame:
    print("Chargement football-datasets...")

    mapping_dossiers = {
        "premier_league": "PL",
        "la_liga":        "PD",
        "serie_a":        "SA",
        "bundesliga":     "BL1",
        "ligue_1":        "FL1",
    }

    # Mapping saison fichier → année début
    def annee_debut(nom_fichier: str) -> int:
        code = nom_fichier.replace("season-", "")
        debut = int(code[:2])
        return 2000 + debut if debut <= 30 else 1900 + debut

    lignes = []
    base = Path("data/brut/football_datasets")

    for dossier, competition in mapping_dossiers.items():
        chemin = base / dossier
        if not chemin.exists():
            continue

        for fichier in sorted(chemin.glob("season-*.csv")):
            annee = annee_debut(fichier.stem)
            if annee < 2017:
                continue

            try:
                df_s = pd.read_csv(
                    fichier,
                    encoding="utf-8",
                    encoding_errors="replace"
                )
            except Exception as e:
                print(f"  ⚠️  Erreur {fichier.name} : {e}")
                continue

            if "HomeTeam" not in df_s.columns:
                continue

            for _, row in df_s.iterrows():
                try:
                    # Parsing de la date — football-datasets utilise DD/MM/YYYY
                    date_raw = str(row.get("Date", ""))
                    try:
                        date = pd.to_datetime(
                            date_raw, dayfirst=True
                        ).strftime("%Y-%m-%d")
                    except Exception:
                        continue

                    home_norm = normaliser_nom(str(row.get("HomeTeam", "")))
                    away_norm = normaliser_nom(str(row.get("AwayTeam", "")))

                    home_goals = row.get("FTHG")
                    away_goals = row.get("FTAG")

                    if pd.isna(home_goals) or pd.isna(away_goals):
                        continue

                    lignes.append({
                        "match_id":    generer_match_id(
                            date, home_norm, away_norm, competition
                        ),
                        "source":      "football_datasets",
                        "competition": competition,
                        "date":        date,
                        "season":      str(annee),
                        "round":       None,
                        "home_team":   home_norm,
                        "away_team":   away_norm,
                        "home_goals":  int(home_goals),
                        "away_goals":  int(away_goals),
                        # Stats supplémentaires disponibles dans football-datasets
                        "home_shots":  row.get("HS"),
                        "away_shots":  row.get("AS"),
                        "home_shots_on_target": row.get("HST"),
                        "away_shots_on_target": row.get("AST"),
                        "home_corners": row.get("HC"),
                        "away_corners": row.get("AC"),
                        "home_yellow_cards": row.get("HY"),
                        "away_yellow_cards": row.get("AY"),
                        "home_red_cards": row.get("HR"),
                        "away_red_cards": row.get("AR"),
                    })
                except Exception:
                    continue

    df = pd.DataFrame(lignes)
    print(f"  → {len(df)} matchs football-datasets")
    return df


def charger_transfermarkt() -> pd.DataFrame:
    print("Chargement Transfermarkt games...")

    df = pd.read_csv(
        "data/brut/transfermarkt/games.csv",
        encoding="utf-8",
        encoding_errors="replace"
    )

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"] >= "2017-01-01"]
    df = df[df["competition_id"].isin(["GB1", "ES1", "IT1", "L1", "FR1"])]
    df = df.dropna(subset=["home_club_name", "away_club_name", "date"])

    lignes = []
    for _, row in df.iterrows():
        date        = row["date"].strftime("%Y-%m-%d")
        competition = COMP_MAPPING.get(row["competition_id"], row["competition_id"])
        home_norm   = normaliser_nom(str(row["home_club_name"]))
        away_norm   = normaliser_nom(str(row["away_club_name"]))

        lignes.append({
            "match_id":       generer_match_id(date, home_norm, away_norm, competition),
            "source":         "transfermarkt",
            "competition":    competition,
            "date":           date,
            "season":         str(row.get("season", "")),
            "round":          row.get("round"),
            "home_team":      home_norm,
            "away_team":      away_norm,
            "home_goals":     row.get("home_club_goals"),
            "away_goals":     row.get("away_club_goals"),
            "tm_game_id":     row.get("game_id"),
        })

    df_out = pd.DataFrame(lignes)
    print(f"  → {len(df_out)} matchs Transfermarkt")
    return df_out


def fusionner_sources(
    df_api: pd.DataFrame,
    df_fd: pd.DataFrame,
    df_tm: pd.DataFrame
) -> pd.DataFrame:
    print("\nFusion des sources...")

    # Concaténation complète
    df_all = pd.concat([df_api, df_fd, df_tm], ignore_index=True)

    # Déduplication sur match_id
    # En cas de doublon, on garde la source dans cet ordre de priorité :
    # api > football_datasets > transfermarkt
    priorite = {"api": 0, "football_datasets": 1, "transfermarkt": 2}
    df_all["priorite"] = df_all["source"].map(priorite)
    df_all = df_all.sort_values("priorite")
    df_all = df_all.drop_duplicates(subset=["match_id"], keep="first")
    df_all = df_all.drop(columns=["priorite"])

    # Nettoyage
    df_all["date"]       = pd.to_datetime(df_all["date"])
    df_all["home_goals"] = pd.to_numeric(df_all["home_goals"], errors="coerce")
    df_all["away_goals"] = pd.to_numeric(df_all["away_goals"], errors="coerce")
    df_all = df_all.dropna(subset=["home_goals", "away_goals"])
    df_all["home_goals"] = df_all["home_goals"].astype(int)
    df_all["away_goals"] = df_all["away_goals"].astype(int)

    # Filtre qualité — scores aberrants
    df_all = df_all[
        (df_all["home_goals"] >= 0) & (df_all["home_goals"] <= 20) &
        (df_all["away_goals"] >= 0) & (df_all["away_goals"] <= 20)
    ]

    # Enrichissement
    df_all["result"] = df_all.apply(
        lambda r: "home" if r["home_goals"] > r["away_goals"]
        else ("away" if r["away_goals"] > r["home_goals"] else "draw"),
        axis=1
    )
    df_all["total_goals"] = df_all["home_goals"] + df_all["away_goals"]

    print(f"  → {len(df_all)} matchs uniques après fusion")
    print(f"\n  Répartition par source :")
    print(df_all["source"].value_counts().to_string())
    print(f"\n  Répartition par compétition :")
    print(df_all["competition"].value_counts().to_string())
    print(f"\n  Période couverte :")
    print(f"  {df_all['date'].min().date()} → {df_all['date'].max().date()}")

    return df_all


def verifier_coherence(df: pd.DataFrame) -> None:
    print("\nVérification cohérence des scores...")

    # Vérifier que les matchs en doublon ont les mêmes scores
    df_tm_raw = charger_transfermarkt()
    df_fd_raw = charger_football_datasets()

    # Matchs présents dans API et football-datasets
    ids_api = set(df[df["source"] == "api"]["match_id"])
    ids_fd  = set(df_fd_raw["match_id"])
    ids_communs = ids_api & ids_fd

    print(f"  Matchs communs API ∩ FD : {len(ids_communs)}")

    if ids_communs:
        # Comparer les scores pour quelques matchs communs
        df_api_sub = df[df["match_id"].isin(ids_communs)][
            ["match_id", "home_team", "away_team", "home_goals", "away_goals"]
        ].rename(columns={"home_goals": "api_hg", "away_goals": "api_ag"})

        df_fd_sub = df_fd_raw[df_fd_raw["match_id"].isin(ids_communs)][
            ["match_id", "home_goals", "away_goals"]
        ].rename(columns={"home_goals": "fd_hg", "away_goals": "fd_ag"})

        comp = df_api_sub.merge(df_fd_sub, on="match_id")
        conflits = comp[
            (comp["api_hg"] != comp["fd_hg"]) |
            (comp["api_ag"] != comp["fd_ag"])
        ]

        if len(conflits) > 0:
            print(f"  ⚠️  {len(conflits)} conflits de scores détectés :")
            print(conflits.head(10).to_string())
        else:
            print(f"  ✅ Scores cohérents entre API et football-datasets")


def main():
    print("\n===== CONSTRUCTION DATASET UNIFIÉ =====\n")

    df_api = charger_api()
    df_fd  = charger_football_datasets()
    df_tm  = charger_transfermarkt()

    df_unified = fusionner_sources(df_api, df_fd, df_tm)
    verifier_coherence(df_unified)

    # Sauvegarde PostgreSQL
    df_unified.to_sql(
        name="unified_matches",
        con=engine,
        schema="marts_clubs",
        if_exists="replace",
        index=False
    )
    print(f"\n  → PostgreSQL : marts_clubs.unified_matches ({len(df_unified)} lignes)")

    # Sauvegarde CSV local aussi
    df_unified.to_csv(
        "data/traite/unified_matches.csv",
        index=False,
        encoding="utf-8"
    )
    print(f"  → CSV : data/traite/unified_matches.csv")

    print("\n✅ Dataset unifié construit.")


if __name__ == "__main__":
    main()