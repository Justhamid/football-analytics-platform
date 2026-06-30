import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # mode sans interface graphique (Docker)
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
    features_fixes = [
        "market_value_t0",
        "age_a_t1",
        "duree_jours",
        "matchs_periode",
        "buts_periode",
        "assists_periode",
        "minutes_periode",
        "cartons_j_periode",
        "cartons_r_periode",
        "goals_per_90",
        "assists_per_90",
        "international_caps",
        "international_goals",
    ]
    features_onehot = [
        c for c in df.columns
        if c.startswith(("pos_", "foot_", "nat_"))
    ]
    return features_fixes + features_onehot


def charger_dataset() -> pd.DataFrame:
    print("Chargement du dataset temporel...")
    df = pd.read_sql("SELECT * FROM marts_ml.features_temporal", engine)
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
    scaler = StandardScaler()
    X_train_sc = scaler.fit_transform(X_train)
    X_test_sc = scaler.transform(X_test)
    lr = LinearRegression()
    lr.fit(X_train_sc, y_train)
    y_pred = lr.predict(X_test_sc)
    mae = mean_absolute_error(np.expm1(y_test), np.expm1(y_pred))
    r2 = r2_score(y_test, y_pred)
    print(f"     MAE : {mae:,.0f} €  |  R² : {r2:.4f}")
    resultats["linear_regression"] = {
        "model": lr, "scaler": scaler, "mae": mae, "r2": r2
    }

    # Modèle 2 — Random Forest
    print("\n  [2/3] Random Forest...")
    rf = RandomForestRegressor(
        n_estimators=200, max_depth=15,
        min_samples_leaf=5, random_state=42, n_jobs=-1
    )
    rf.fit(X_train, y_train)
    y_pred = rf.predict(X_test)
    mae = mean_absolute_error(np.expm1(y_test), np.expm1(y_pred))
    r2 = r2_score(y_test, y_pred)
    print(f"     MAE : {mae:,.0f} €  |  R² : {r2:.4f}")
    resultats["random_forest"] = {
        "model": rf, "scaler": None, "mae": mae, "r2": r2
    }

    # Modèle 3 — Gradient Boosting
    print("\n  [3/3] Gradient Boosting...")
    gb = GradientBoostingRegressor(
        n_estimators=200, max_depth=5,
        learning_rate=0.05, random_state=42
    )
    gb.fit(X_train, y_train)
    y_pred = gb.predict(X_test)
    mae = mean_absolute_error(np.expm1(y_test), np.expm1(y_pred))
    r2 = r2_score(y_test, y_pred)
    print(f"     MAE : {mae:,.0f} €  |  R² : {r2:.4f}")
    resultats["gradient_boosting"] = {
        "model": gb, "scaler": None, "mae": mae, "r2": r2
    }

    return resultats


def selectionner_meilleur(resultats: dict) -> tuple:
    print("\nSélection du meilleur modèle...")
    nom = min(resultats, key=lambda k: resultats[k]["mae"])
    meilleur = resultats[nom]
    print(f"  → Meilleur modèle : {nom}")
    print(f"     MAE : {meilleur['mae']:,.0f} €")
    print(f"     R²  : {meilleur['r2']:.4f}")
    return nom, meilleur


def sauvegarder_modele(nom: str, info: dict, features: list) -> None:
    with open(MODELS_DIR / "best_model_temporal.pkl", "wb") as f:
        pickle.dump({
            "model": info["model"],
            "scaler": info["scaler"],
            "features": features,
            "model_name": nom
        }, f)
    print(f"\n  → Sauvegardé : models/best_model_temporal.pkl")


# ============================================================
# NOUVEAU : Graphique feature importance
# ============================================================
def graphique_feature_importance(nom: str, info: dict, features: list) -> None:
    if nom not in ("random_forest", "gradient_boosting"):
        return

    print("\nGénération des graphiques feature importance...")

    importances = pd.Series(
        info["model"].feature_importances_,
        index=features
    ).sort_values(ascending=False)

    # ── Graphique 1 : toutes les features ──────────────────
    top15 = importances.head(15)
    fig, ax = plt.subplots(figsize=(10, 6))
    couleurs = ["#0F6E56" if i == 0 else "#9FE1CB"
                for i in range(len(top15))]
    ax.barh(top15.index[::-1], top15.values[::-1],
            color=couleurs[::-1])
    ax.set_xlabel("Importance", fontsize=12)
    ax.set_title(
        "Importance des variables — vue globale\n"
        "(market_value_t0 domine : la valeur passée prédit la valeur future)",
        fontsize=12, fontweight='bold'
    )
    plt.tight_layout()
    chemin1 = MODELS_DIR / "feature_importance_globale.png"
    plt.savefig(chemin1, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → Graphique 1 sauvegardé : {chemin1}")

    # ── Graphique 2 : sans market_value_t0 ─────────────────
    importances_sans_t0 = importances.drop(
        "market_value_t0", errors="ignore"
    ).head(14)

    # Recalcul en % relatif pour que la somme = 100%
    total = importances_sans_t0.sum()
    importances_relatives = (importances_sans_t0 / total * 100).round(2)

    fig, ax = plt.subplots(figsize=(10, 6))
    couleurs2 = ["#534AB7" if i == 0 else "#CECBF6"
                 for i in range(len(importances_relatives))]
    ax.barh(importances_relatives.index[::-1],
            importances_relatives.values[::-1],
            color=couleurs2[::-1])
    ax.set_xlabel("Importance relative (%)", fontsize=12)
    ax.set_title(
        "Importance des variables de performance\n"
        "(hors valeur marchande passée — contribution relative)",
        fontsize=12, fontweight='bold'
    )
    plt.tight_layout()
    chemin2 = MODELS_DIR / "feature_importance_performance.png"
    plt.savefig(chemin2, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → Graphique 2 sauvegardé : {chemin2}")


# ============================================================
# NOUVEAU : Validation croisée 5-fold
# ============================================================
def validation_croisee(nom: str, info: dict,
                        X_train: pd.DataFrame, y_train: pd.Series) -> None:
    if nom not in ("random_forest", "gradient_boosting"):
        return

    print("\nValidation croisée 5-fold...")
    scores = cross_val_score(
        info["model"], X_train, y_train,
        cv=5, scoring='r2', n_jobs=-1
    )
    print(f"  → R² moyen  : {scores.mean():.4f}")
    print(f"  → Écart-type : {scores.std():.4f}")
    print(f"  → Scores par fold : {[round(s, 4) for s in scores]}")

    if scores.std() < 0.02:
        print("  ✅ Le modèle généralise bien (faible variance entre les folds)")
    else:
        print("  ⚠️  Variance élevée — possible surapprentissage")


# ============================================================
# NOUVEAU : Analyse des résidus
# ============================================================
def analyse_residus(nom: str, info: dict,
                    X_test: pd.DataFrame, y_test: pd.Series) -> None:
    print("\nAnalyse des résidus...")

    if info["scaler"]:
        X_pred = info["scaler"].transform(X_test)
    else:
        X_pred = X_test

    y_pred_log = info["model"].predict(X_pred)
    y_reel = np.expm1(y_test)
    y_pred = np.expm1(y_pred_log)
    residus = y_reel - y_pred

    mae_global = mean_absolute_error(y_reel, y_pred)
    print(f"  → MAE globale : {mae_global:,.0f} €")

    # Graphique résidus vs valeurs prédites
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Graphique 1 : résidus vs prédictions
    axes[0].scatter(y_pred / 1e6, residus / 1e6, alpha=0.3,
                    color="#534AB7", s=10)
    axes[0].axhline(0, color='red', linestyle='--', linewidth=1.5)
    axes[0].set_xlabel("Valeur prédite (M€)", fontsize=11)
    axes[0].set_ylabel("Résidu (M€)", fontsize=11)
    axes[0].set_title("Résidus vs valeurs prédites", fontsize=12,
                      fontweight='bold')

    # Graphique 2 : valeurs réelles vs prédites
    max_val = max(y_reel.max(), y_pred.max()) / 1e6
    axes[1].scatter(y_reel / 1e6, y_pred / 1e6, alpha=0.3,
                    color="#0F6E56", s=10)
    axes[1].plot([0, max_val], [0, max_val], 'r--', linewidth=1.5,
                 label="Prédiction parfaite")
    axes[1].set_xlabel("Valeur réelle (M€)", fontsize=11)
    axes[1].set_ylabel("Valeur prédite (M€)", fontsize=11)
    axes[1].set_title("Valeurs réelles vs prédites", fontsize=12,
                      fontweight='bold')
    axes[1].legend()

    plt.suptitle(f"Analyse des résidus — {nom.replace('_', ' ').title()}",
                 fontsize=13, fontweight='bold', y=1.01)
    plt.tight_layout()
    chemin = MODELS_DIR / "analyse_residus.png"
    plt.savefig(chemin, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → Graphique sauvegardé : {chemin}")


# ============================================================
# NOUVEAU : Analyse par poste
# ============================================================
def analyse_par_poste(df_pred: pd.DataFrame) -> None:
    print("\nAnalyse de l'évaluation par poste...")

    if "position" not in df_pred.columns:
        print("  (colonne position absente, analyse ignorée)")
        return

    stats_poste = df_pred.groupby("position").agg(
        nb_joueurs=("player_id", "count"),
        pct_sous_evalues=("evaluation",
                          lambda x: (x == "sous-évalué").mean() * 100),
        pct_bien_evalues=("evaluation",
                          lambda x: (x == "bien évalué").mean() * 100),
        pct_surevalues=("evaluation",
                        lambda x: (x == "surévalué").mean() * 100),
        ecart_moyen_pct=("difference_pct", "mean")
    ).round(1).sort_values("pct_sous_evalues", ascending=False)

    print(stats_poste.to_string())

    # Graphique
    postes = stats_poste.index.tolist()
    x = np.arange(len(postes))
    width = 0.25

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x - width, stats_poste["pct_sous_evalues"],
           width, label="Sous-évalués", color="#0F6E56")
    ax.bar(x, stats_poste["pct_bien_evalues"],
           width, label="Bien évalués", color="#F4A261")
    ax.bar(x + width, stats_poste["pct_surevalues"],
           width, label="Surévalués", color="#993C1D")

    ax.set_xlabel("Poste", fontsize=11)
    ax.set_ylabel("Pourcentage de joueurs (%)", fontsize=11)
    ax.set_title("Répartition sous-évalués / bien évalués / surévalués\npar poste",
                 fontsize=12, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(postes, rotation=30, ha='right')
    ax.legend()
    plt.tight_layout()
    chemin = MODELS_DIR / "evaluation_par_poste.png"
    plt.savefig(chemin, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → Graphique sauvegardé : {chemin}")


def generer_predictions(df, features, nom, info) -> pd.DataFrame:
    print("\nGénération des prédictions...")

    if hasattr(info["model"], "feature_names_in_"):
        features_modele = list(info["model"].feature_names_in_)
    else:
        features_modele = features

    colonnes_necessaires = features_modele + [
        TARGET, "player_id", "delta_pct", "position",
        "market_value_t0", "age_a_t1", "date_t1"
    ]
    colonnes_dispo = [c for c in colonnes_necessaires if c in df.columns]
    df_clean = df[colonnes_dispo].dropna()

    for col in features_modele:
        if col not in df_clean.columns:
            df_clean[col] = 0

    df_clean = df_clean.loc[:, ~df_clean.columns.duplicated()]
    X = df_clean[features_modele].copy()

    if info["scaler"]:
        X_pred = info["scaler"].transform(X)
    else:
        X_pred = X

    y_pred_log = info["model"].predict(X_pred)

    df_pred = df_clean[[
        "player_id", "position",
        "age_a_t1", "date_t1",
        "market_value_t0", TARGET
    ]].copy()

    df_pred["predicted_value"] = np.expm1(y_pred_log).round(0)
    df_pred["actual_value"] = df_pred[TARGET]
    df_pred["difference"] = df_pred["predicted_value"] - df_pred["actual_value"]
    df_pred["difference_pct"] = (
        df_pred["difference"] / df_pred["actual_value"] * 100
    ).round(1)
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
        name="predictions_market_value_temporal",
        con=engine,
        schema="marts_ml",
        if_exists="replace",
        index=False
    )
    print(f"\n  → PostgreSQL : marts_ml.predictions_market_value_temporal ({len(df)} lignes)")


def main():
    print("\n===== ENTRAÎNEMENT MODÈLE TEMPOREL =====\n")

    df = charger_dataset()
    features = definir_features(df)
    print(f"\n  → {len(features)} features utilisées")

    X_train, X_test, y_train, y_test, df_clean = preparer_dataset(
        df, features)

    features_ordre_exact = list(X_train.columns)
    print(f"  → {len(features_ordre_exact)} features dans l'ordre exact du train")

    resultats = entrainer_modeles(X_train, X_test, y_train, y_test)
    nom, meilleur = selectionner_meilleur(resultats)

    # ── Nouvelles analyses ──────────────────────────────────
    graphique_feature_importance(nom, meilleur, features_ordre_exact)
    validation_croisee(nom, meilleur, X_train, y_train)
    analyse_residus(nom, meilleur, X_test, y_test)
    # ────────────────────────────────────────────────────────

    sauvegarder_modele(nom, meilleur, features_ordre_exact)

    df_pred = generer_predictions(df, features_ordre_exact, nom, meilleur)

    # ── Analyse par poste ───────────────────────────────────
    analyse_par_poste(df_pred)
    # ────────────────────────────────────────────────────────

    charger_predictions_postgres(df_pred)

    print("\n===== RÉSUMÉ FINAL =====")
    print(f"  Modèle retenu    : {nom.replace('_', ' ').title()}")
    print(f"  R²               : {meilleur['r2']:.4f}")
    print(f"  MAE              : {meilleur['mae']:,.0f} €")
    print(f"  Prédictions      : {len(df_pred):,}")
    print(f"  Graphiques       : models/feature_importance.png")
    print(f"                     models/analyse_residus.png")
    print(f"                     models/evaluation_par_poste.png")
    print("\n✅ Modèle temporel entraîné et prédictions chargées.")


if __name__ == "__main__":
    main()