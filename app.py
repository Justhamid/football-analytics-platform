import streamlit as st
import pickle
import numpy as np
import pandas as pd
from pathlib import Path

# ── Configuration page ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Football Analytics — Projection de Carrière",
    page_icon="⚽",
    layout="centered"
)

# ── Chargement du modèle ──────────────────────────────────────────────────────
@st.cache_resource
def charger_modele():
    chemin = Path("models/model_projection_carriere.pkl")
    with open(chemin, "rb") as f:
        return pickle.load(f)

bundle = charger_modele()
model_global   = bundle["model_global"]
modeles_poste  = bundle["modeles_poste"]
features       = bundle["features"]
le_position    = bundle["le_position"]
le_foot        = bundle["le_foot"]

# ── Header ────────────────────────────────────────────────────────────────────
st.title("⚽ Projection de Carrière — Jeunes Joueurs")
st.markdown(
    "**Simulation** : entrez les statistiques d'un jeune joueur (16–21 ans) "
    "et obtenez une estimation de sa valeur marchande future."
)
st.divider()

# ── Formulaire ────────────────────────────────────────────────────────────────
st.subheader("📋 Profil du joueur")

col1, col2 = st.columns(2)

with col1:
    nom        = st.text_input("Nom du joueur", value="Adam B.")
    position   = st.selectbox("Poste", ["Attack", "Midfield", "Defender", "Goalkeeper"])
    pied       = st.selectbox("Pied préférentiel", ["right", "left", "both"])
    taille     = st.slider("Taille (cm)", 160, 200, 178)
    age_match  = st.slider("Âge au 1er match pro", 15, 21, 16)

with col2:
    matchs          = st.slider("Matchs joués (16–21 ans)", 1, 150, 25)
    minutes         = st.slider("Minutes jouées (16–21 ans)", 100, 12000, 2000)
    buts            = st.slider("Buts totaux (16–21 ans)", 0, 80, 10)
    passes          = st.slider("Passes décisives (16–21 ans)", 0, 60, 5)
    nb_competitions = st.slider("Nombre de compétitions", 1, 8, 2)

st.divider()
st.subheader("📊 Indicateurs avancés")

col3, col4 = st.columns(2)

with col3:
    goals_per_90   = st.number_input("Buts / 90 min", 0.0, 3.0,
                                      round(buts * 90 / max(minutes, 1), 2),
                                      step=0.01, format="%.2f")
    assists_per_90 = st.number_input("Passes déc. / 90 min", 0.0, 3.0,
                                      round(passes * 90 / max(minutes, 1), 2),
                                      step=0.01, format="%.2f")

with col4:
    caps           = st.slider("Sélections nationales (U17/U19/U21)", 0, 50, 3)
    int_goals      = st.slider("Buts en sélection", 0, 20, 1)

st.divider()
st.subheader("🏟️ Niveau du club formateur")

niveau_club = st.selectbox(
    "Catégorie du club formateur",
    options=[
        "Amateur / Régional (< 500K€)",
        "Semi-pro / N2-N3 (500K€ – 2M€)",
        "Professionnel L2 / 3e div. (2M€ – 8M€)",
        "Ligue 1 / Championship (8M€ – 25M€)",
        "Top 5 européen (25M€ – 80M€)",
        "Elite européenne (> 80M€)",
    ],
    index=2
)

valeur_club_map = {
    "Amateur / Régional (< 500K€)":              250_000,
    "Semi-pro / N2-N3 (500K€ – 2M€)":          1_000_000,
    "Professionnel L2 / 3e div. (2M€ – 8M€)":  4_000_000,
    "Ligue 1 / Championship (8M€ – 25M€)":     15_000_000,
    "Top 5 européen (25M€ – 80M€)":            45_000_000,
    "Elite européenne (> 80M€)":               120_000_000,
}
valeur_moyenne_club = valeur_club_map[niveau_club]

# ── Prédiction ────────────────────────────────────────────────────────────────
st.divider()

if st.button("🔮 Prédire la valeur marchande future", type="primary", use_container_width=True):

    # Encodage position et pied
    pos_label = position if position in le_position.classes_ else "Missing"
    foot_label = pied if pied in le_foot.classes_ else "Unknown"

    position_enc = int(le_position.transform([pos_label])[0])
    foot_enc     = int(le_foot.transform([foot_label])[0])

    # Construction du vecteur de features dans l'ordre exact du modèle
    X = pd.DataFrame([{
        "matchs_jeune":          matchs,
        "buts_jeune":            buts,
        "passes_jeune":          passes,
        "minutes_jeune":         minutes,
        "goals_per_90_jeune":    goals_per_90,
        "assists_per_90_jeune":  assists_per_90,
        "age_premier_match":     age_match,
        "nb_competitions_jeune": nb_competitions,
        "height_in_cm":          taille,
        "international_caps":    caps,
        "international_goals":   int_goals,
        "valeur_moyenne_club":   valeur_moyenne_club,
        "position_enc":          position_enc,
        "foot_enc":              foot_enc,
    }])[features]

    # Prédiction : modèle par poste en priorité, global en fallback
    if position in modeles_poste:
        valeur_log = modeles_poste[position]["model"].predict(X)[0]
        modele_utilise = f"Modèle {position}"
    else:
        valeur_log = model_global.predict(X)[0]
        modele_utilise = "Modèle global"

    valeur_projetee = np.expm1(valeur_log)

    # ── Résultat ──────────────────────────────────────────────────────────────
    st.divider()
    st.subheader("📈 Résultat de la projection")

    if valeur_projetee >= 1_000_000:
        valeur_affichee = f"{valeur_projetee / 1_000_000:.1f} M€"
    else:
        valeur_affichee = f"{valeur_projetee / 1_000:.0f} K€"

    st.metric(
        label=f"Valeur marchande projetée — {nom}",
        value=valeur_affichee
    )

    # Profil du joueur
    col5, col6, col7 = st.columns(3)
    col5.metric("Poste", position)
    col6.metric("Buts / 90", f"{goals_per_90:.2f}")
    col7.metric("Sélections nationales", caps)

    # Interprétation
    st.divider()
    if valeur_projetee >= 50_000_000:
        st.success("🌟 **Talent élite** — Potentiel de joueur de classe mondiale.")
    elif valeur_projetee >= 20_000_000:
        st.success("⭐ **Très haut potentiel** — Profil de recrutement prioritaire.")
    elif valeur_projetee >= 5_000_000:
        st.info("📊 **Bon potentiel** — Joueur intéressant pour un club professionnel.")
    elif valeur_projetee >= 1_000_000:
        st.warning("👀 **Potentiel modéré** — À suivre sur les prochaines saisons.")
    else:
        st.error("⚠️ **Potentiel limité** selon les données actuelles.")

    # Détail technique
    with st.expander("🔧 Détail technique"):
        st.write(f"**Modèle utilisé :** {modele_utilise}")
        st.write(f"**R² global :** 0,61 | **MAE global :** 4 651 790 €")
        st.write(f"**Valeur log-transformée :** {valeur_log:.4f}")
        st.write(f"**Valeur brute :** {valeur_projetee:,.0f} €")
        st.write("**Vecteur de features :**")
        st.dataframe(X)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Football Analytics Platform · RNCP 39586 · "
    "Gradient Boosting · R²=0,61 · 1 147 prédictions U22"
)
