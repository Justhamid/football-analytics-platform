import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)

# Fenêtre temporelle — marché post-Neymar (2017 = début ère moderne des transferts)
DATE_DEBUT = "2017-01-01"

# Joueurs actifs récemment — on exclut les retraités anciens
LAST_SEASON_MIN = 2022


def charger_donnees() -> tuple:
    print("Chargement des données...")

    # CORRECTION 1 — fenêtre 8 ans sur les valuations
    valuations = pd.read_sql(f"""
        SELECT
            player_id,
            date,
            market_value_in_eur
        FROM marts_players.player_valuations
        WHERE market_value_in_eur IS NOT NULL
          AND market_value_in_eur > 0
          AND date >= '{DATE_DEBUT}'
        ORDER BY player_id, date
    """, engine)
    print(f"  → {len(valuations)} valuations chargées (depuis {DATE_DEBUT})")

    # CORRECTION 2 — filtre joueurs actifs récemment
    players = pd.read_sql(f"""
        SELECT
            player_id,
            position,
            foot,
            country_of_citizenship,
            international_caps,
            international_goals,
            date_of_birth
        FROM marts_players.players
        WHERE position IS NOT NULL
          AND position != 'Missing'
          AND last_season >= {LAST_SEASON_MIN}
    """, engine)
    print(f"  → {len(players)} joueurs actifs chargés (last_season >= {LAST_SEASON_MIN})")

    # Apparitions depuis 2017 uniquement — inutile de charger 1.8M lignes
    appearances = pd.read_sql(f"""
        SELECT
            player_id,
            date,
            goals,
            assists,
            minutes_played,
            yellow_cards,
            red_cards
        FROM marts_players.appearances
        WHERE minutes_played > 0
          AND date >= '{DATE_DEBUT}'
    """, engine)
    print(f"  → {len(appearances)} apparitions chargées (depuis {DATE_DEBUT})")

    return valuations, appearances, players


def construire_dataset_temporel(
    valuations: pd.DataFrame,
    appearances: pd.DataFrame,
    players: pd.DataFrame
) -> pd.DataFrame:

    print("\nConstruction du dataset temporel...")

    # Conversion des dates
    valuations["date"]       = pd.to_datetime(valuations["date"])
    appearances["date"]      = pd.to_datetime(appearances["date"])
    players["date_of_birth"] = pd.to_datetime(
        players["date_of_birth"], errors="coerce"
    )

    # Index pour accélérer les lookups
    players_idx     = players.set_index("player_id")
    appearances_idx = appearances.set_index("player_id")

    lignes = []
    groupes = valuations.groupby("player_id")
    total   = len(groupes)

    for i, (player_id, val_joueur) in enumerate(groupes):

        if i % 5000 == 0:
            print(f"  → Traitement joueur {i}/{total}...")

        # Il faut que le joueur soit dans notre table players filtrée
        if player_id not in players_idx.index:
            continue

        val_joueur = val_joueur.sort_values("date").reset_index(drop=True)

        # Il faut au moins 2 points de valorisation
        if len(val_joueur) < 2:
            continue

        profil     = players_idx.loc[player_id]

        # Apparitions du joueur
        if player_id in appearances_idx.index:
            app_joueur = appearances_idx.loc[[player_id]].copy()
        else:
            app_joueur = pd.DataFrame(
                columns=appearances.columns
            )

        # Pour chaque paire de valuations consécutives
        for j in range(len(val_joueur) - 1):

            date_t0 = val_joueur.loc[j,   "date"]
            date_t1 = val_joueur.loc[j+1, "date"]
            val_t0  = val_joueur.loc[j,   "market_value_in_eur"]
            val_t1  = val_joueur.loc[j+1, "market_value_in_eur"]

            # Performances entre les deux dates
            if not app_joueur.empty:
                masque = (
                    (app_joueur["date"] >= date_t0) &
                    (app_joueur["date"] <  date_t1)
                )
                perf_periode = app_joueur[masque]
            else:
                perf_periode = pd.DataFrame()

            if perf_periode.empty:
                matchs         = 0
                buts           = 0
                assists        = 0
                minutes        = 0
                cartons_jaunes = 0
                cartons_rouges = 0
            else:
                matchs         = len(perf_periode)
                buts           = int(perf_periode["goals"].sum())
                assists        = int(perf_periode["assists"].sum())
                minutes        = int(perf_periode["minutes_played"].sum())
                cartons_jaunes = int(perf_periode["yellow_cards"].sum())
                cartons_rouges = int(perf_periode["red_cards"].sum())

            # Calcul âge à date_t1
            dob = profil["date_of_birth"]
            if pd.notnull(dob):
                age = round((date_t1 - dob).days / 365.25, 2)
            else:
                age = np.nan

            # Durée de la période
            duree_jours = (date_t1 - date_t0).days

            # Ratios par 90 minutes
            g90 = round(buts    * 90 / minutes, 3) if minutes > 0 else 0.0
            a90 = round(assists * 90 / minutes, 3) if minutes > 0 else 0.0

            lignes.append({
                "player_id":           player_id,
                "date_t0":             date_t0,
                "date_t1":             date_t1,
                "duree_jours":         duree_jours,
                "age_a_t1":            age,

                # Valeurs marchandes
                "market_value_t0":     val_t0,
                "market_value_t1":     val_t1,
                "delta_value":         val_t1 - val_t0,
                "delta_pct":           round(
                    (val_t1 - val_t0) / val_t0 * 100, 2
                ),

                # Performances de la période
                "matchs_periode":      matchs,
                "buts_periode":        buts,
                "assists_periode":     assists,
                "minutes_periode":     minutes,
                "cartons_j_periode":   cartons_jaunes,
                "cartons_r_periode":   cartons_rouges,
                "goals_per_90":        g90,
                "assists_per_90":      a90,

                # Profil
                "position":            profil["position"],
                "foot":                profil["foot"],
                "country":             profil["country_of_citizenship"],
                "international_caps":  profil["international_caps"],
                "international_goals": profil["international_goals"],
            })

    df = pd.DataFrame(lignes)
    print(f"\n  → {len(df)} paires de valorisation construites")
    print(f"  → {df['player_id'].nunique()} joueurs uniques")
    return df


def encoder_et_nettoyer(df: pd.DataFrame) -> pd.DataFrame:
    print("\nEncodage et nettoyage...")

    # Suppression périodes aberrantes
    df = df[
        (df["duree_jours"] >= 7) &
        (df["duree_jours"] <= 1100)
    ].copy()

    # Suppression variations extrêmes
    df = df[
        (df["delta_pct"] >= -90) &
        (df["delta_pct"] <= 500)
    ].copy()

    # Filtre âge réaliste (joueurs entre 15 et 45 ans)
    df = df[
        (df["age_a_t1"] >= 15) &
        (df["age_a_t1"] <= 45)
    ].copy()

    # One-hot encoding position
    pos_dummies = pd.get_dummies(df["position"], prefix="pos")
    df = pd.concat([df, pos_dummies], axis=1)

    # One-hot encoding pied
    foot_dummies = pd.get_dummies(df["foot"], prefix="foot")
    df = pd.concat([df, foot_dummies], axis=1)

    # Encodage nationalité top 20
    df["country"] = df["country"].replace("Türkiye", "Turkey")
    top_pays = df["country"].value_counts().head(20).index
    df["nationality_group"] = df["country"].apply(
        lambda x: x if x in top_pays else "other"
    )
    nat_dummies = pd.get_dummies(df["nationality_group"], prefix="nat")
    df = pd.concat([df, nat_dummies], axis=1)

    # Remplacement nulls
    df["age_a_t1"]           = df["age_a_t1"].fillna(df["age_a_t1"].median())
    df["international_caps"] = df["international_caps"].fillna(0)
    df["international_goals"]= df["international_goals"].fillna(0)

    print(f"  → {len(df)} lignes après nettoyage")
    print(f"  → {df['player_id'].nunique()} joueurs uniques")
    print(f"\n  Répartition par position :")
    print(df["position"].value_counts().to_string())
    print(f"\n  Distribution âge :")
    print(df["age_a_t1"].describe().round(1).to_string())
    print(f"\n  Distribution delta_pct :")
    print(df["delta_pct"].describe().round(1).to_string())

    return df


def sauvegarder(df: pd.DataFrame) -> None:
    print("\nSauvegarde dans PostgreSQL...")

    df.to_sql(
        name="features_temporal",
        con=engine,
        schema="marts_ml",
        if_exists="replace",
        index=False
    )
    print(f"  → marts_ml.features_temporal ({len(df)} lignes)")


def main():
    print("\n===== CONSTRUCTION FEATURES TEMPORELLES (2017-2025) =====\n")

    valuations, appearances, players = charger_donnees()
    df = construire_dataset_temporel(valuations, appearances, players)
    df = encoder_et_nettoyer(df)
    sauvegarder(df)

    print("\n✅ Features temporelles construites.")
    print(f"\nAperçu :")
    print(df[[
        "player_id", "age_a_t1",
        "market_value_t0", "market_value_t1",
        "delta_pct", "buts_periode", "minutes_periode"
    ]].head(5).to_string())


if __name__ == "__main__":
    main()