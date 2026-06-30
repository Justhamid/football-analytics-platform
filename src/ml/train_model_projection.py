import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pickle
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{POSTGRES_HOST}:{POSTGRES_PORT}/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)

MODELS_DIR = Path("models")
MODELS_DIR.mkdir(exist_ok=True)


def charger_dataset() -> pd.DataFrame:
    print("Construction du dataset de projection...")

    query = """
    WITH stats_jeunes AS (
        SELECT
            a.player_id,
            COUNT(*)                                          AS matchs_jeune,
            SUM(a.goals)                                     AS buts_jeune,
            SUM(a.assists)                                   AS passes_jeune,
            SUM(a.minutes_played)                            AS minutes_jeune,
            ROUND((SUM(a.goals) * 90.0
                / NULLIF(SUM(a.minutes_played), 0))::numeric, 4)
                                                             AS goals_per_90_jeune,
            ROUND((SUM(a.assists) * 90.0
                / NULLIF(SUM(a.minutes_played), 0))::numeric, 4)
                                                             AS assists_per_90_jeune,
            MIN(DATE_PART('year', AGE(a.date::date,
                p.date_of_birth::date)))                     AS age_premier_match,
            COUNT(DISTINCT a.competition_id)                 AS nb_competitions_jeune
        FROM marts_players.appearances_unified a
        JOIN marts_players.players p ON a.player_id = p.player_id
        WHERE DATE_PART('year', AGE(a.date::date, p.date_of_birth::date))
              BETWEEN 16 AND 21
        GROUP BY a.player_id
        HAVING SUM(a.minutes_played) >= 500
    ),
    -- Amélioration 3 : valeur dans 2 ans après la période jeune
    valeurs_futures AS (
        SELECT DISTINCT ON (player_id)
            player_id,
            market_value_in_eur AS valeur_2ans,
            date                AS date_evaluation
        FROM staging.stg_valuations
        ORDER BY player_id, date DESC
    ),
    -- Amélioration 2 : niveau du club formateur
    club_score AS (
        SELECT
            current_club_id,
            ROUND(AVG(market_value_in_eur))  AS valeur_moyenne_club,
            COUNT(*)                          AS nb_joueurs_club
        FROM marts_players.players
        WHERE market_value_in_eur IS NOT NULL
        GROUP BY current_club_id
    )
    SELECT
        sj.*,
        p.position,
        p.foot,
        p.height_in_cm,
        p.international_caps,
        p.international_goals,
        p.current_club_id,
        COALESCE(cs.valeur_moyenne_club, 0)   AS valeur_moyenne_club,
        DATE_PART('year', AGE(p.date_of_birth)) AS age_actuel,
        -- Amélioration 3 : cible = valeur future à 2 ans
        COALESCE(vf.valeur_2ans,
                 p.market_value_in_eur)        AS valeur_cible
    FROM stats_jeunes sj
    JOIN marts_players.players p ON sj.player_id = p.player_id
    LEFT JOIN valeurs_futures vf ON sj.player_id = vf.player_id
    LEFT JOIN club_score cs ON p.current_club_id = cs.current_club_id
    WHERE COALESCE(vf.valeur_2ans, p.market_value_in_eur) IS NOT NULL
      AND DATE_PART('year', AGE(p.date_of_birth)) BETWEEN 22 AND 35
    """

    df = pd.read_sql(query, engine)
    print(f"  → {len(df)} joueurs chargés")
    print(f"  → Valeur cible moyenne : {df['valeur_cible'].mean():,.0f} €")
    return df


def preparer_features(df: pd.DataFrame):
    print("\nPréparation des features...")

    le_position = LabelEncoder()
    le_foot = LabelEncoder()

    df["position_enc"] = le_position.fit_transform(
        df["position"].fillna("Unknown")
    )
    df["foot_enc"] = le_foot.fit_transform(
        df["foot"].fillna("Unknown")
    )

    features = [
        "matchs_jeune",
        "buts_jeune",
        "passes_jeune",
        "minutes_jeune",
        "goals_per_90_jeune",
        "assists_per_90_jeune",
        "age_premier_match",
        "nb_competitions_jeune",
        "height_in_cm",
        "international_caps",
        "international_goals",
        "valeur_moyenne_club",      # Amélioration 2
        "position_enc",
        "foot_enc",
    ]

    df_clean = df[features + ["valeur_cible", "position"]].dropna()
    print(f"  → {len(df_clean)} joueurs après suppression NaN")

    X = df_clean[features]
    y = np.log1p(df_clean["valeur_cible"])

    return X, y, features, df_clean, le_position, le_foot


def entrainer_modele_global(X, y):
    print("\nEntraînement du modèle global...")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = GradientBoostingRegressor(
        n_estimators=200, max_depth=4,
        learning_rate=0.05, random_state=42
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae = mean_absolute_error(np.expm1(y_test), np.expm1(y_pred))
    r2 = r2_score(y_test, y_pred)

    cv = cross_val_score(model, X_train, y_train, cv=5, scoring='r2')
    print(f"  → R²         : {r2:.4f}")
    print(f"  → MAE        : {mae:,.0f} €")
    print(f"  → R² CV      : {cv.mean():.4f} ± {cv.std():.4f}")

    return model, r2, mae


# Amélioration 1 — Segmentation par poste
def entrainer_par_poste(df_clean, features, y):
    print("\nEntraînement par poste...")

    postes = df_clean["position"].unique()
    modeles_poste = {}
    resultats = []

    for poste in postes:
        mask = df_clean["position"] == poste
        if mask.sum() < 100:
            print(f"  ⚠️  {poste:12} : pas assez ({mask.sum()} joueurs)")
            continue

        X_p = df_clean[mask][features]
        y_p = y[mask]

        X_train, X_test, y_train, y_test = train_test_split(
            X_p, y_p, test_size=0.2, random_state=42
        )

        model = GradientBoostingRegressor(
            n_estimators=200, max_depth=4,
            learning_rate=0.05, random_state=42
        )
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        r2  = r2_score(y_test, y_pred)
        mae = mean_absolute_error(np.expm1(y_test), np.expm1(y_pred))

        modeles_poste[poste] = {"model": model, "r2": r2, "mae": mae}
        resultats.append({
            "Poste": poste, "Nb joueurs": mask.sum(),
            "R²": round(r2, 4), "MAE (€)": f"{mae:,.0f}"
        })

        print(f"  {poste:12} : R²={r2:.4f} | MAE={mae:,.0f}€ | "
              f"n={mask.sum()}")

    return modeles_poste


def graphique_feature_importance(model, features):
    print("\nGénération des graphiques feature importance...")

    importances = pd.Series(
        model.feature_importances_,
        index=features
    ).sort_values(ascending=False)

    # Dictionnaire de noms lisibles
    noms_lisibles = {
        "valeur_moyenne_club":   "Niveau du club formateur",
        "matchs_jeune":          "Matchs joués (16-21 ans)",
        "international_caps":    "Sélections nationales",
        "minutes_jeune":         "Minutes jouées (16-21 ans)",
        "goals_per_90_jeune":    "Buts / 90 min (16-21 ans)",
        "assists_per_90_jeune":  "Passes dé. / 90 min (16-21 ans)",
        "buts_jeune":            "Buts totaux (16-21 ans)",
        "passes_jeune":          "Passes déc. totales (16-21 ans)",
        "height_in_cm":          "Taille (cm)",
        "international_goals":   "Buts internationaux",
        "age_premier_match":     "Âge au 1er match pro",
        "nb_competitions_jeune": "Nb compétitions (16-21 ans)",
        "position_enc":          "Poste",
        "foot_enc":              "Pied préférentiel",
    }

    # ── Graphique 1 : vue globale (noms techniques) ────────
    imp_globale = importances.copy()
    imp_globale.index = [noms_lisibles.get(i, i)
                         for i in imp_globale.index]

    fig, ax = plt.subplots(figsize=(10, 6))
    couleurs = ["#0F6E56" if i == 0 else "#9FE1CB"
                for i in range(len(imp_globale))]
    ax.barh(imp_globale.index[::-1], imp_globale.values[::-1],
            color=couleurs[::-1])
    ax.set_xlabel("Importance", fontsize=12)
    ax.set_title(
        "Importance des variables — vue globale\n"
        "Le niveau du club formateur est le meilleur prédicteur",
        fontsize=12, fontweight='bold'
    )
    plt.tight_layout()
    chemin1 = MODELS_DIR / "projection_feature_importance_globale.png"
    plt.savefig(chemin1, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → Graphique 1 : {chemin1}")

    # ── Graphique 2 : sans valeur_moyenne_club ─────────────
    # On travaille sur importances ORIGINALES (avant renommage)
    imp_sans_club = importances.drop(
        "valeur_moyenne_club", errors="ignore"
    ).head(12)
    total = imp_sans_club.sum()
    imp_relatives = (imp_sans_club / total * 100).round(2)

    # Renommer maintenant
    imp_relatives.index = [noms_lisibles.get(i, i)
                            for i in imp_relatives.index]

    fig, ax = plt.subplots(figsize=(10, 6))
    couleurs2 = ["#534AB7" if i < 3 else "#CECBF6"
                 for i in range(len(imp_relatives))]
    ax.barh(imp_relatives.index[::-1], imp_relatives.values[::-1],
            color=couleurs2[::-1])
    ax.set_xlabel("Importance relative (%)", fontsize=12)
    ax.set_title(
        "Importance relative des variables de performance\n"
        "(hors niveau du club formateur)",
        fontsize=12, fontweight='bold'
    )
    plt.tight_layout()
    chemin2 = MODELS_DIR / "projection_feature_importance_performance.png"
    plt.savefig(chemin2, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → Graphique 2 : {chemin2}")


def sauvegarder_modele(model, modeles_poste, features,
                        le_position, le_foot, r2, mae):
    chemin = MODELS_DIR / "model_projection_carriere.pkl"
    with open(chemin, "wb") as f:
        pickle.dump({
            "model_global":  model,
            "modeles_poste": modeles_poste,
            "features":      features,
            "le_position":   le_position,
            "le_foot":       le_foot,
            "r2":            r2,
            "mae":           mae
        }, f)
    print(f"\n  → Modèle sauvegardé : {chemin}")


def generer_predictions_jeunes(model, modeles_poste,
                                features, le_position, le_foot):
    print("\nGénération des prédictions pour les joueurs U22...")

    query = """
    WITH stats_jeunes AS (
        SELECT
            a.player_id,
            COUNT(*)                                          AS matchs_jeune,
            SUM(a.goals)                                     AS buts_jeune,
            SUM(a.assists)                                   AS passes_jeune,
            SUM(a.minutes_played)                            AS minutes_jeune,
            ROUND((SUM(a.goals) * 90.0
                / NULLIF(SUM(a.minutes_played), 0))::numeric, 4)
                                                             AS goals_per_90_jeune,
            ROUND((SUM(a.assists) * 90.0
                / NULLIF(SUM(a.minutes_played), 0))::numeric, 4)
                                                             AS assists_per_90_jeune,
            MIN(DATE_PART('year', AGE(a.date::date,
                p.date_of_birth::date)))                     AS age_premier_match,
            COUNT(DISTINCT a.competition_id)                 AS nb_competitions_jeune
        FROM marts_players.appearances_unified a
        JOIN marts_players.players p ON a.player_id = p.player_id
        WHERE DATE_PART('year', AGE(a.date::date, p.date_of_birth::date))
              BETWEEN 16 AND 21
        GROUP BY a.player_id
        HAVING SUM(a.minutes_played) >= 500
    ),
    club_score AS (
        SELECT
            current_club_id,
            ROUND(AVG(market_value_in_eur)) AS valeur_moyenne_club
        FROM marts_players.players
        WHERE market_value_in_eur IS NOT NULL
        GROUP BY current_club_id
    )
    SELECT
        sj.*,
        p.name,
        p.position,
        p.foot,
        p.height_in_cm,
        p.international_caps,
        p.international_goals,
        p.current_club_name,
        p.current_club_id,
        COALESCE(cs.valeur_moyenne_club, 0) AS valeur_moyenne_club,
        DATE_PART('year', AGE(p.date_of_birth)) AS age_actuel,
        p.market_value_in_eur               AS valeur_actuelle
    FROM stats_jeunes sj
    JOIN marts_players.players p ON sj.player_id = p.player_id
    LEFT JOIN club_score cs ON p.current_club_id = cs.current_club_id
    WHERE DATE_PART('year', AGE(p.date_of_birth)) BETWEEN 16 AND 22
    """

    df = pd.read_sql(query, engine)
    print(f"  → {len(df)} joueurs U22 trouvés")

    if len(df) == 0:
        print("  ⚠️ Aucun joueur U22 trouvé")
        return

    df["position_enc"] = le_position.transform(
        df["position"].fillna("Unknown")
    )
    df["foot_enc"] = le_foot.transform(
        df["foot"].fillna("Unknown")
    )

    # Au lieu de la boucle ligne par ligne
    # Remplace par une prédiction vectorisée

    df_clean = df[features].fillna(0)

    # Prédiction globale d'abord
    predictions_global = np.expm1(model.predict(df_clean))

    # Prédiction par poste si disponible
    predictions_finales = predictions_global.copy()
    for poste, info in modeles_poste.items():
        mask = df["position"] == poste
        if mask.any():
            X_poste = df_clean[mask]
            predictions_finales[mask] = np.expm1(
                info["model"].predict(X_poste)
            )

    df["valeur_projetee"] = predictions_finales.round(0)

    df_output = df[[
        "player_id", "name", "position", "age_actuel",
        "current_club_name", "valeur_actuelle",
        "goals_per_90_jeune", "assists_per_90_jeune",
        "minutes_jeune", "matchs_jeune",
        "valeur_projetee"
    ]].copy()

    df_output.to_sql(
        name="predictions_projection_carriere",
        con=engine,
        schema="marts_ml",
        if_exists="replace",
        index=False
    )

    print(f"  → {len(df_output)} prédictions sauvegardées")
    print(f"\n  Top 10 joueurs U22 :")
    print(df_output.nlargest(10, "valeur_projetee")[[
        "name", "age_actuel", "position",
        "valeur_actuelle", "valeur_projetee"
    ]].to_string(index=False))


def main():
    print("\n===== MODÈLE DE PROJECTION DE CARRIÈRE (V2) =====\n")

    df = charger_dataset()
    X, y, features, df_clean, le_position, le_foot = preparer_features(df)

    # Modèle global
    model, r2, mae = entrainer_modele_global(X, y)

    # Amélioration 1 — par poste
    modeles_poste = entrainer_par_poste(df_clean, features, y)

    graphique_feature_importance(model, features)
    sauvegarder_modele(model, modeles_poste, features,
                       le_position, le_foot, r2, mae)
    generer_predictions_jeunes(model, modeles_poste,
                                features, le_position, le_foot)

    print("\n===== RÉSUMÉ FINAL =====")
    print(f"  Modèle global  : R²={r2:.4f} | MAE={mae:,.0f}€")
    print(f"  Améliorations  : segmentation poste + niveau club + cible 2 ans")
    print(f"  Dataset        : {len(df_clean)} joueurs")
    print("\n✅ Modèle de projection V2 entraîné.")


if __name__ == "__main__":
    main()