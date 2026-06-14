import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
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

TARGET = "market_value_t1"


def definir_features(df: pd.DataFrame) -> list:
    """Construit dynamiquement la liste des features disponibles."""

    features_fixes = [
        # Valeur précédente — feature la plus importante
        "market_value_t0",

        # Temporel
        "age_a_t1",
        "duree_jours",

        # Performances de la période
        "matchs_periode",
        "buts_periode",
        "assists_periode",
        "minutes_periode",
        "cartons_j_periode",
        "cartons_r_periode",
        "goals_per_90",
        "assists_per_90",

        # Profil
        "international_caps",
        "international_goals",
    ]

    # Colonnes one-hot générées dynamiquement
    features_onehot = [
        c for c in df.columns
        if c.startswith(("pos_", "foot_", "nat_"))
    ]

    return features_fixes + features_onehot


def charger_dataset() -> pd.DataFrame:
    print("Chargement du dataset temporel...")
    df = pd.read_sql(
        "SELECT * FROM marts_ml.features_temporal",
        engine
    )
    print(f"  → {len(df)} lignes chargées")
    print(f"  → {df['player_id'].nunique()} joueurs uniques")
    return df


def preparer_dataset(df: pd.DataFrame, features: list):
    print("\nPréparation du dataset...")

    df_clean = df[features + [TARGET, "player_id",
                              "delta_pct", "position"]].dropna()
    print(f"  → {len(df_clean)} lignes après suppression NaN")

    X = df_clean[features]
    y = np.log1p(df_clean[TARGET])

    X_train, X_test, y_train, y_test, idx_train, idx_test = train_test_split(
        X, y, df_clean.index,
        test_size=0.2,
        random_state=42
    )

    print(f"  → Train : {len(X_train)} lignes")
    print(f"  → Test  : {len(X_test)} lignes")

    return X_train, X_test, y_train, y_test, df_clean


def entrainer_modeles(X_train, X_test, y_train, y_test) -> dict:
    print("\nEntraînement des modèles...")

    resultats = {}

    # Modèle 1 — Régression linéaire
    print("\n  [1/3] Linear Regression...")
    scaler        = StandardScaler()
    X_train_sc    = scaler.fit_transform(X_train)
    X_test_sc     = scaler.transform(X_test)
    lr            = LinearRegression()
    lr.fit(X_train_sc, y_train)
    y_pred        = lr.predict(X_test_sc)
    mae           = mean_absolute_error(np.expm1(y_test), np.expm1(y_pred))
    r2            = r2_score(y_test, y_pred)
    print(f"     MAE : {mae:,.0f} €  |  R² : {r2:.4f}")
    resultats["linear_regression"] = {
        "model": lr, "scaler": scaler, "mae": mae, "r2": r2
    }

    # Modèle 2 — Random Forest
    print("\n  [2/3] Random Forest...")
    rf = RandomForestRegressor(
        n_estimators=200,
        max_depth=15,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1
    )
    rf.fit(X_train, y_train)
    y_pred = rf.predict(X_test)
    mae    = mean_absolute_error(np.expm1(y_test), np.expm1(y_pred))
    r2     = r2_score(y_test, y_pred)
    print(f"     MAE : {mae:,.0f} €  |  R² : {r2:.4f}")
    resultats["random_forest"] = {
        "model": rf, "scaler": None, "mae": mae, "r2": r2
    }

    # Modèle 3 — Gradient Boosting
    print("\n  [3/3] Gradient Boosting...")
    gb = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.05,
        random_state=42
    )
    gb.fit(X_train, y_train)
    y_pred = gb.predict(X_test)
    mae    = mean_absolute_error(np.expm1(y_test), np.expm1(y_pred))
    r2     = r2_score(y_test, y_pred)
    print(f"     MAE : {mae:,.0f} €  |  R² : {r2:.4f}")
    resultats["gradient_boosting"] = {
        "model": gb, "scaler": None, "mae": mae, "r2": r2
    }

    return resultats


def selectionner_meilleur(resultats: dict) -> tuple:
    print("\nSélection du meilleur modèle...")
    nom     = min(resultats, key=lambda k: resultats[k]["mae"])
    meilleur = resultats[nom]
    print(f"  → Meilleur modèle : {nom}")
    print(f"     MAE : {meilleur['mae']:,.0f} €")
    print(f"     R²  : {meilleur['r2']:.4f}")
    return nom, meilleur


def sauvegarder_modele(nom: str, info: dict, features: list) -> None:
    with open(MODELS_DIR / "best_model_temporal.pkl", "wb") as f:
        pickle.dump({
            "model":      info["model"],
            "scaler":     info["scaler"],
            "features":   features,
            "model_name": nom
        }, f)
    print(f"\n  → Sauvegardé : models/best_model_temporal.pkl")


def importance_features(nom: str, info: dict, features: list) -> None:
    """Affiche les features les plus importantes (RF et GB seulement)."""
    if nom not in ("random_forest", "gradient_boosting"):
        return

    importances = pd.Series(
        info["model"].feature_importances_,
        index=features
    ).sort_values(ascending=False).head(15)

    print("\nTop 15 features les plus importantes :")
    for feat, imp in importances.items():
        barre = "█" * int(imp * 200)
        print(f"  {feat:<40} {imp:.4f}  {barre}")


def generer_predictions(
    df: pd.DataFrame,
    features: list,
    nom: str,
    info: dict
) -> pd.DataFrame:
    print("\nGénération des prédictions...")

    # On récupère l'ordre exact depuis le modèle lui-même
    if hasattr(info["model"], "feature_names_in_"):
        features_modele = list(info["model"].feature_names_in_)
    else:
        features_modele = features

    colonnes_necessaires = features_modele + [
        TARGET, "player_id", "delta_pct", "position",
        "market_value_t0", "age_a_t1", "date_t1"
    ]

    # On garde uniquement les colonnes qui existent dans df
    colonnes_dispo = [c for c in colonnes_necessaires if c in df.columns]
    df_clean = df[colonnes_dispo].dropna()

    # On ajoute les colonnes manquantes avec 0
    for col in features_modele:
        if col not in df_clean.columns:
            df_clean[col] = 0

    # Supprimer les doublons de colonnes avant sélection
    df_clean = df_clean.loc[:, ~df_clean.columns.duplicated()]

    # On sélectionne dans l'ordre exact du modèle
    X = df_clean[features_modele].copy()

    if info["scaler"]:
        X_pred = info["scaler"].transform(X)
    else:
        X_pred = X

    if hasattr(info["model"], "feature_names_in_"):
        modele_cols = list(info["model"].feature_names_in_)
        x_cols      = list(X.columns)
        print(f"\n  Modèle attend  ({len(modele_cols)}) : {modele_cols}")
        print(f"  X a            ({len(x_cols)})      : {x_cols}")
        diff = [(i, m, x) for i, (m, x) in enumerate(zip(modele_cols, x_cols)) if m != x]
        print(f"  Différences    : {diff}")

    y_pred_log = info["model"].predict(X_pred)

    df_pred = df_clean[[
        "player_id", "position",
        "age_a_t1", "date_t1",
        "market_value_t0", TARGET
    ]].copy()

    df_pred["predicted_value"] = np.expm1(y_pred_log).round(0)
    df_pred["actual_value"]    = df_pred[TARGET]
    df_pred["difference"]      = df_pred["predicted_value"] - df_pred["actual_value"]
    df_pred["difference_pct"]  = (
        df_pred["difference"] / df_pred["actual_value"] * 100
    ).round(1)
    df_pred["evaluation"] = df_pred["difference_pct"].apply(
        lambda x: "sous-évalué"  if x > 20
        else ("surévalué" if x < -20 else "bien évalué")
    )
    df_pred["model_name"] = nom

    print(f"  → {len(df_pred)} prédictions générées")
    print(f"\n  Répartition :")
    print(df_pred["evaluation"].value_counts().to_string())

    return df_pred


def charger_predictions_postgres(df: pd.DataFrame) -> None:
    df.to_sql(
        name="predictions_market_value_temporal",
        con=engine,
        schema="marts_ml",
        if_exists="replace",
        index=False
    )
    print(f"\n  → PostgreSQL : marts_ml.predictions_market_value_temporal ({len(df)} lignes)")


def main():
    print("\n===== ENTRAÎNEMENT MODÈLE TEMPOREL =====\n")

    df       = charger_dataset()
    features = definir_features(df)
    print(f"\n  → {len(features)} features utilisées")

    X_train, X_test, y_train, y_test, df_clean = preparer_dataset(df, features)

    # CORRECTION — on récupère l'ordre exact des colonnes après le split
    features_ordre_exact = list(X_train.columns)
    print(f"  → {len(features_ordre_exact)} features dans l'ordre exact du train")

    resultats     = entrainer_modeles(X_train, X_test, y_train, y_test)
    nom, meilleur = selectionner_meilleur(resultats)

    importance_features(nom, meilleur, features_ordre_exact)

    # CORRECTION — on sauvegarde avec l'ordre exact
    sauvegarder_modele(nom, meilleur, features_ordre_exact)

    # CORRECTION — on passe l'ordre exact à generer_predictions
    df_pred = generer_predictions(df, features_ordre_exact, nom, meilleur)
    charger_predictions_postgres(df_pred)

    print("\n✅ Modèle temporel entraîné et prédictions chargées.")


if __name__ == "__main__":
    main()