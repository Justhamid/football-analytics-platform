import pandas as pd
import json
from pathlib import Path
from src.utils.team_mapping import normaliser_nom, TEAM_MAPPING


def valider_mapping():
    print("\n===== VALIDATION DU MAPPING =====\n")

    # Collecte tous les noms des 3 sources
    noms_api = set()
    for fichier in sorted(Path("data/brut/api").glob("*.json")):
        with open(fichier, "r", encoding="utf-8") as f:
            data = json.load(f)
        for match in data.get("matches", []):
            if match.get("status") == "FINISHED":
                noms_api.add(match.get("homeTeam", {}).get("name", ""))
                noms_api.add(match.get("awayTeam", {}).get("name", ""))

    noms_fd = set()
    base = Path("data/brut/football_datasets")
    for fichier in base.rglob("season-*.csv"):
        nom = fichier.stem
        annee = int("20" + nom[-4:-2]) if int(nom[-4:-2]) <= 30 \
                else int("19" + nom[-4:-2])
        if annee < 2017:
            continue
        try:
            df = pd.read_csv(fichier, encoding_errors="replace")
            if "HomeTeam" in df.columns:
                noms_fd.update(df["HomeTeam"].dropna().unique())
                noms_fd.update(df["AwayTeam"].dropna().unique())
        except Exception:
            pass

    df_tm = pd.read_csv(
        "data/brut/transfermarkt/games.csv",
        encoding_errors="replace"
    )
    df_tm = df_tm[df_tm["competition_id"].isin(["GB1","ES1","IT1","L1","FR1"])]
    noms_tm = set(df_tm["home_club_name"].dropna().unique()) | \
              set(df_tm["away_club_name"].dropna().unique())

    tous_les_noms = noms_api | noms_fd | noms_tm
    tous_les_noms.discard("")

    # Vérification
    non_mappes = []
    for nom in sorted(tous_les_noms):
        resultat = normaliser_nom(nom)
        if resultat == nom and nom not in TEAM_MAPPING:
            non_mappes.append(nom)

    if non_mappes:
        print(f"⚠️  {len(non_mappes)} noms NON mappés :")
        for nom in sorted(non_mappes):
            print(f"   → '{nom}'")
    else:
        print(f"✅ Tous les noms sont mappés ({len(tous_les_noms)} noms uniques)")

    # Stats
    print(f"\nNoms API          : {len(noms_api)}")
    print(f"Noms FD           : {len(noms_fd)}")
    print(f"Noms Transfermarkt: {len(noms_tm)}")
    print(f"Total unique      : {len(tous_les_noms)}")
    print(f"Mappés            : {len(tous_les_noms) - len(non_mappes)}")


if __name__ == "__main__":
    valider_mapping()