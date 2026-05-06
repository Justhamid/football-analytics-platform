from pathlib import Path
import json
import pandas as pd


def check_transfermarkt():
    print("\n===== VALIDATION TRANSFERMARKT =====")

    base = Path("data/brut/transfermarkt")

    files = {
        "players": "players.csv",
        "appearances": "appearances.csv",
        "games": "games.csv",
        "clubs": "clubs.csv",
        "player_valuations": "player_valuations.csv",
        "transfers": "transfers.csv",
    }

    dataframes = {}

    for name, filename in files.items():
        path = base / filename
        df = pd.read_csv(path)
        dataframes[name] = df
        print(f"\n{name}: {df.shape}")
        print(f"Colonnes: {list(df.columns)}")

    # Vérifications ciblées
    players = dataframes["players"]
    appearances = dataframes["appearances"]
    games = dataframes["games"]
    valuations = dataframes["player_valuations"]
    transfers = dataframes["transfers"]

    print("\n--- Nulls critiques ---")
    print("players.market_value_in_eur null:", players["market_value_in_eur"].isna().sum())
    print("players.position null:", players["position"].isna().sum())
    print("players.date_of_birth null:", players["date_of_birth"].isna().sum())
    print("transfers.transfer_fee null:", transfers["transfer_fee"].isna().sum())

    print("\n--- Clés uniques ---")
    print("players.player_id uniques:", players["player_id"].nunique())
    print("appearances.player_id uniques:", appearances["player_id"].nunique())
    print("games.game_id uniques:", games["game_id"].nunique())
    print("valuations.player_id uniques:", valuations["player_id"].nunique())

    print("\n--- Tests de jointure ---")
    join_ap_players = appearances.merge(players[["player_id", "name", "position", "market_value_in_eur"]], on="player_id", how="inner")
    print("appearances x players:", join_ap_players.shape)

    join_tr_players = transfers.merge(players[["player_id", "name"]], on="player_id", how="inner")
    print("transfers x players:", join_tr_players.shape)

    join_val_players = valuations.merge(players[["player_id", "name"]], on="player_id", how="inner")
    print("valuations x players:", join_val_players.shape)

    print("\n--- Échantillon dataset ML potentiel ---")
    ml_sample = join_ap_players[["player_id", "name", "position", "goals", "assists", "minutes_played", "market_value_in_eur"]].dropna()
    print("sample ML shape:", ml_sample.shape)
    print(ml_sample.head())


def check_api():
    print("\n===== VALIDATION API =====")

    base = Path("data/brut/api")
    files = sorted(base.glob("*.json"))

    if not files:
        print("Aucun fichier JSON trouvé dans data/brut/api")
        return

    total_matches = 0

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        matches = data.get("matches", [])
        total_matches += len(matches)

        print(f"{file.name}: {len(matches)} matchs")

    print(f"\nTotal matchs API: {total_matches}")


def check_football_datasets():
    print("\n===== VALIDATION FOOTBALL_DATASETS =====")

    base = Path("data/brut/football_datasets")
    csv_files = sorted(base.rglob("*.csv"))

    print(f"Nombre de fichiers CSV: {len(csv_files)}")

    # Vérification rapide d’un échantillon
    if csv_files:
        sample = pd.read_csv(csv_files[0])
        print(f"Exemple fichier: {csv_files[0].name}")
        print("Shape:", sample.shape)
        print("Colonnes:", list(sample.columns))


def main():
    check_transfermarkt()
    check_api()
    check_football_datasets()


if __name__ == "__main__":
    main()