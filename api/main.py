"""
Football Analytics Platform — API REST de prédiction ML
Expose les prédictions du modèle de projection de carrière via FastAPI.

Endpoints :
  GET  /                    → santé de l'API
  GET  /predictions         → liste des 1 147 prédictions U22 depuis PostgreSQL
  POST /predict             → prédiction à la volée pour un nouveau joueur
  GET  /predictions/{id}    → prédiction d'un joueur spécifique par player_id
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import pickle
import numpy as np
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

# ── Chargement du modèle ──────────────────────────────────────────────────────
MODEL_PATH = Path("models/model_projection_carriere.pkl")

if not MODEL_PATH.exists():
    raise FileNotFoundError(f"Modèle introuvable : {MODEL_PATH}")

with open(MODEL_PATH, "rb") as f:
    bundle = pickle.load(f)

model_global  = bundle["model_global"]
modeles_poste = bundle["modeles_poste"]
features      = bundle["features"]
le_position   = bundle["le_position"]
le_foot       = bundle["le_foot"]

# ── Connexion PostgreSQL ──────────────────────────────────────────────────────
DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5433')}"
    f"/{os.getenv('POSTGRES_DB')}"
)
engine = create_engine(DB_URL)

# ── Application FastAPI ───────────────────────────────────────────────────────
app = FastAPI(
    title="Football Analytics — API de projection de carrière",
    description=(
        "API REST exposant les prédictions du modèle Gradient Boosting "
        "(R²=0,61) entraîné sur 3 289 joueurs professionnels. "
        "Permet de prédire la valeur marchande future d'un jeune joueur "
        "(U16–U21) à partir de ses statistiques de formation."
    ),
    version="1.0.0",
)


# ── Schémas Pydantic ──────────────────────────────────────────────────────────
class PlayerInput(BaseModel):
    """Statistiques d'un joueur sur sa période de formation (16–21 ans)."""
    name:                str   = Field(..., example="Adam B.")
    position:            str   = Field(..., example="Attack",
                                       description="Attack / Midfield / Defender / Goalkeeper")
    foot:                str   = Field("right", example="right",
                                       description="right / left / both")
    age_actuel:          int   = Field(..., example=17, ge=14, le=25)
    matchs_jeune:        int   = Field(..., example=25, ge=1)
    buts_jeune:          int   = Field(..., example=10, ge=0)
    passes_jeune:        int   = Field(..., example=5, ge=0)
    minutes_jeune:       int   = Field(..., example=2000, ge=100)
    nb_competitions_jeune: int = Field(..., example=2, ge=1, le=10)
    height_in_cm:        int   = Field(..., example=178, ge=150, le=210)
    age_premier_match:   int   = Field(..., example=16, ge=13, le=21)
    international_caps:  int   = Field(0, example=3, ge=0)
    international_goals: int   = Field(0, example=1, ge=0)
    valeur_actuelle_eur: float = Field(0.0, example=500000.0, ge=0,
                                       description="Valeur actuelle en euros (0 si inconnue)")
    valeur_moyenne_club: float = Field(..., example=4000000.0, ge=0,
                                       description="Valeur marchande moyenne des joueurs du club actuel")


class PredictionOutput(BaseModel):
    """Résultat de la projection de carrière."""
    name:               str
    position:           str
    age_actuel:         int
    valeur_actuelle_eur: float
    valeur_projetee_eur: float
    progression_pct:    float
    age_pic_min:        int
    age_pic_max:        int
    modele_utilise:     str
    interpretation:     str


# ── Helpers ───────────────────────────────────────────────────────────────────
def fmt_eur(v: float) -> str:
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f} M€"
    return f"{v / 1_000:.0f} K€"


def interpret(valeur: float) -> str:
    if valeur >= 50_000_000:
        return "Talent exceptionnel — profil de joueur international de haut niveau"
    elif valeur >= 20_000_000:
        return "Très grand potentiel — profil de joueur de top club européen"
    elif valeur >= 5_000_000:
        return "Bon potentiel — profil de joueur professionnel de championnat majeur"
    elif valeur >= 1_000_000:
        return "Potentiel correct — profil de joueur professionnel L2 ou équivalent"
    return "Potentiel limité selon les données actuelles"


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/", tags=["Santé"])
def root():
    """Vérifie que l'API est opérationnelle."""
    return {
        "status": "ok",
        "service": "Football Analytics — API de projection de carrière",
        "version": "1.0.0",
        "modele": "Gradient Boosting · R²=0,61 · 14 features · 4 modèles par poste",
    }


@app.post("/predict", response_model=PredictionOutput, tags=["Prédiction"])
def predict(player: PlayerInput):
    """
    Prédit la valeur marchande future d'un jeune joueur (U16–U21)
    à partir de ses statistiques de formation.
    """
    # Calcul des ratios
    goals_per_90   = round(player.buts_jeune * 90 / max(player.minutes_jeune, 1), 4)
    assists_per_90 = round(player.passes_jeune * 90 / max(player.minutes_jeune, 1), 4)

    # Encodage
    pos_label  = player.position if player.position in le_position.classes_ else "Missing"
    foot_label = player.foot if player.foot in le_foot.classes_ else "Unknown"
    position_enc = int(le_position.transform([pos_label])[0])
    foot_enc     = int(le_foot.transform([foot_label])[0])

    # Vecteur de features
    X = pd.DataFrame([{
        "matchs_jeune":           player.matchs_jeune,
        "buts_jeune":             player.buts_jeune,
        "passes_jeune":           player.passes_jeune,
        "minutes_jeune":          player.minutes_jeune,
        "goals_per_90_jeune":     goals_per_90,
        "assists_per_90_jeune":   assists_per_90,
        "age_premier_match":      player.age_premier_match,
        "nb_competitions_jeune":  player.nb_competitions_jeune,
        "height_in_cm":           player.height_in_cm,
        "international_caps":     player.international_caps,
        "international_goals":    player.international_goals,
        "valeur_moyenne_club":    player.valeur_moyenne_club,
        "position_enc":           position_enc,
        "foot_enc":               foot_enc,
    }])[features]

    # Prédiction
    if player.position in modeles_poste:
        valeur_log     = modeles_poste[player.position]["model"].predict(X)[0]
        modele_utilise = f"Modèle spécialisé {player.position}"
    else:
        valeur_log     = model_global.predict(X)[0]
        modele_utilise = "Modèle global"

    valeur_projetee = float(np.expm1(valeur_log))

    # Progression
    progression_pct = 0.0
    if player.valeur_actuelle_eur > 0:
        progression_pct = round(
            (valeur_projetee - player.valeur_actuelle_eur)
            / player.valeur_actuelle_eur * 100, 1
        )

    # Âge pic
    age_pic_min = max(player.age_actuel + (21 - player.age_actuel) + 2, 22)
    age_pic_max = age_pic_min + 2

    return PredictionOutput(
        name=player.name,
        position=player.position,
        age_actuel=player.age_actuel,
        valeur_actuelle_eur=player.valeur_actuelle_eur,
        valeur_projetee_eur=round(valeur_projetee, 0),
        progression_pct=progression_pct,
        age_pic_min=age_pic_min,
        age_pic_max=age_pic_max,
        modele_utilise=modele_utilise,
        interpretation=interpret(valeur_projetee),
    )


@app.get("/predictions", tags=["Données"])
def get_predictions(limit: int = 20, offset: int = 0):
    """
    Retourne les prédictions U22 stockées dans PostgreSQL
    (table marts_ml.predictions_projection_carriere).
    """
    try:
        query = text("""
            SELECT player_id, name, position, age_actuel,
                   current_club_name,
                   valeur_actuelle, valeur_projetee,
                   goals_per_90_jeune, assists_per_90_jeune,
                   matchs_jeune
            FROM marts_ml.predictions_projection_carriere
            ORDER BY valeur_projetee DESC
            LIMIT :limit OFFSET :offset
        """)
        with engine.connect() as conn:
            rows = conn.execute(query, {"limit": limit, "offset": offset}).fetchall()
        return {
            "total_returned": len(rows),
            "limit": limit,
            "offset": offset,
            "predictions": [dict(row._mapping) for row in rows],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predictions/{player_id}", tags=["Données"])
def get_prediction_by_id(player_id: int):
    """
    Retourne la prédiction d'un joueur spécifique par son player_id.
    """
    try:
        query = text("""
            SELECT player_id, name, position, age_actuel,
                   current_club_name,
                   valeur_actuelle, valeur_projetee,
                   goals_per_90_jeune, assists_per_90_jeune,
                   matchs_jeune
            FROM marts_ml.predictions_projection_carriere
            WHERE player_id = :player_id
        """)
        with engine.connect() as conn:
            row = conn.execute(query, {"player_id": player_id}).fetchone()
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"Joueur {player_id} non trouvé dans les prédictions U22"
            )
        return dict(row._mapping)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
