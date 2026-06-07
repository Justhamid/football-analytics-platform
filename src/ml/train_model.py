import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor
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
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{POSTGRES_HOST}:5432/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)

MODELS_DIR = Path("models")
MODELS_DIR.mkdir(exist_ok=True)

FEATURES = [
    # Performance statistique
    "matchs_joues",
    "total_goals",
    "total_assists",
    "total_minutes",
    "goals_per_90",
    "assists_per_90",
    "goal_contributions_per_90",
    "avg_minutes_per_match",

    # Profil joueur
    "international_caps",
    "international_goals",
    "highest_market_value_in_eur",

    # Position (one-hot)
    "pos_Attack",
    "pos_Defender",
    "pos_Goalkeeper",
    "pos_Midfield",

    # Pied (one-hot)
    "foot_both",
    "foot_left",
    "foot_right",

    # Nationalité (one-hot)
    "nat_Argentina",
    "nat_Belgium",
    "nat_Brazil",
    "nat_Colombia",
    "nat_Denmark",
    "nat_England",
    "nat_France",
    "nat_Germany",
    "nat_Greece",
    "nat_Italy",
    "nat_Japan",
    "nat_Netherlands",
    "nat_Portugal",
    "nat_Russia",
    "nat_Scotland",
    "nat_Serbia",
    "nat_Spain",
    "nat_Sweden",
    "nat_Turkey",
    "nat_Ukraine",
    "nat_other",
]

TARGET = "target_market_value"


def charger_features() -> pd.DataFrame:
    print("Chargement features depuis PostgreSQL...")
    df = pd.read_sql("SELECT * FROM marts_ml.features_market_value", engine)
    print(f"  → {len(df)} joueurs chargés")
    return df


def preparer_dataset(df: pd.DataFrame):
    print("Préparation du dataset...")

    # Suppression des lignes avec des NaN dans les features
    df_clean = df[FEATURES + [TARGET, "player_id", "name", "position"]].dropna()
    print(f"  → {len(df_clean)} joueurs après suppression des NaN")

    X = df_clean[FEATURES]
    y = df_clean[TARGET]

    # Log-transformation de la target
    # La valeur marchande suit une distribution très asymétrique
    # (quelques joueurs valent 100M+, la majorité vaut < 1M)
    # Le log rend la distribution plus normale et améliore le modèle
    y_log = np.log1p(y)

    # Split train / test : 80% entraînement, 20% test
    X_train, X_test, y_train, y_test, idx_train, idx_test = train_test_split(
        X, y_log, df_clean.index,
        test_size=0.2,
        random_state=42
    )

    print(f"  → Train : {len(X_train)} joueurs")
    print(f"  → Test  : {len(X_test)} joueurs")

    return X_train, X_test, y_train, y_test, df_clean


def entrainer_modeles(X_train, X_test, y_train, y_test):
    print("\nEntraînement des modèles...")

    resultats = {}

    # Modèle 1 — Régression linéaire
    print("\n  [1/2] Linear Regression...")
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled  = scaler.transform(X_test)

    lr = LinearRegression()
    lr.fit(X_train_scaled, y_train)
    y_pred_lr = lr.predict(X_test_scaled)

    mae_lr = mean_absolute_error(
        np.expm1(y_test),
        np.expm1(y_pred_lr)
    )
    r2_lr = r2_score(y_test, y_pred_lr)

    print(f"     MAE : {mae_lr:,.0f} €")
    print(f"     R²  : {r2_lr:.4f}")

    resultats["linear_regression"] = {
        "model": lr,
        "scaler": scaler,
        "mae": mae_lr,
        "r2": r2_lr
    }

    # Modèle 2 — Random Forest
    print("\n  [2/2] Random Forest...")
    rf = RandomForestRegressor(
        n_estimators=200,
        max_depth=15,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1
    )
    rf.fit(X_train, y_train)
    y_pred_rf = rf.predict(X_test)

    mae_rf = mean_absolute_error(
        np.expm1(y_test),
        np.expm1(y_pred_rf)
    )
    r2_rf = r2_score(y_test, y_pred_rf)

    print(f"     MAE : {mae_rf:,.0f} €")
    print(f"     R²  : {r2_rf:.4f}")

    resultats["random_forest"] = {
        "model": rf,
        "scaler": None,
        "mae": mae_rf,
        "r2": r2_rf
    }

    return resultats


def selectionner_meilleur_modele(resultats: dict) -> tuple:
    print("\nSélection du meilleur modèle...")

    meilleur_nom = min(resultats, key=lambda k: resultats[k]["mae"])
    meilleur = resultats[meilleur_nom]

    print(f"  → Meilleur modèle : {meilleur_nom}")
    print(f"     MAE : {meilleur['mae']:,.0f} €")
    print(f"     R²  : {meilleur['r2']:.4f}")

    return meilleur_nom, meilleur


def sauvegarder_modele(nom: str, modele_info: dict) -> None:
    print(f"\nSauvegarde du modèle {nom}...")

    with open(MODELS_DIR / "best_model.pkl", "wb") as f:
        pickle.dump({
            "model": modele_info["model"],
            "scaler": modele_info["scaler"],
            "features": FEATURES,
            "model_name": nom
        }, f)

    print(f"  → Sauvegardé : models/best_model.pkl")


def generer_predictions(df: pd.DataFrame, nom: str, modele_info: dict) -> pd.DataFrame:
    print("\nGénération des prédictions...")

    df_clean = df[FEATURES + [TARGET, "player_id", "name", "position"]].dropna()
    X = df_clean[FEATURES]

    if modele_info["scaler"]:
        X_scaled = modele_info["scaler"].transform(X)
        y_pred_log = modele_info["model"].predict(X_scaled)
    else:
        y_pred_log = modele_info["model"].predict(X)

    df_pred = df_clean[["player_id", "name", "position", TARGET]].copy()
    df_pred["predicted_value"]  = np.expm1(y_pred_log).round(0)
    df_pred["actual_value"]     = df_pred[TARGET]
    df_pred["difference"]       = df_pred["predicted_value"] - df_pred["actual_value"]
    df_pred["difference_pct"]   = (
        df_pred["difference"] / df_pred["actual_value"] * 100
    ).round(1)

    # Catégorisation
    df_pred["evaluation"] = df_pred["difference_pct"].apply(
        lambda x: "sous-évalué" if x > 20
        else ("surévalué" if x < -20 else "bien évalué")
    )

    df_pred["model_name"] = nom

    print(f"  → {len(df_pred)} prédictions générées")
    print(f"\n  Répartition :")
    print(df_pred["evaluation"].value_counts().to_string())

    return df_pred


def charger_predictions_postgres(df: pd.DataFrame) -> None:
    df.to_sql(
        name="predictions_market_value",
        con=engine,
        schema="marts_ml",
        if_exists="replace",
        index=False
    )
    print(f"\n  → PostgreSQL : marts_ml.predictions_market_value ({len(df)} lignes)")


def main():
    print("\n===== ENTRAÎNEMENT MODÈLE ML =====\n")

    df            = charger_features()
    X_train, X_test, y_train, y_test, df_clean = preparer_dataset(df)
    resultats     = entrainer_modeles(X_train, X_test, y_train, y_test)
    nom, meilleur = selectionner_meilleur_modele(resultats)

    sauvegarder_modele(nom, meilleur)
    df_pred = generer_predictions(df, nom, meilleur)
    charger_predictions_postgres(df_pred)

    print("\n✅ Modèle ML entraîné et prédictions chargées.")


if __name__ == "__main__":
    main()