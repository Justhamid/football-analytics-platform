import streamlit as st
import pickle
import numpy as np
import pandas as pd
from pathlib import Path

# ── Configuration page ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Football Analytics — Potentiel de Carrière",
    page_icon="⚽",
    layout="centered"
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .result-box {
        background-color: #f0f7ff;
        border-left: 5px solid #1a73e8;
        padding: 20px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .warning-box {
        background-color: #fff5f5;
        border-left: 5px solid #e74c3c;
        padding: 20px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .value-card {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        padding: 15px;
        border-radius: 8px;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .age-badge {
        background-color: #1a73e8;
        color: white;
        padding: 6px 14px;
        border-radius: 20px;
        font-weight: bold;
        font-size: 0.95em;
        display: inline-block;
        margin-top: 6px;
    }
    .info-text { color: #666666; font-size: 0.9em; }
    .fiabilite-haute  { color: #2e7d32; font-weight: bold; }
    .fiabilite-moyenne { color: #f57c00; font-weight: bold; }
    .fiabilite-faible { color: #c62828; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# ── Chargement du modèle ──────────────────────────────────────────────────────
@st.cache_resource
def charger_modele():
    chemin = Path("models/model_projection_carriere.pkl")
    with open(chemin, "rb") as f:
        return pickle.load(f)

bundle        = charger_modele()
model_global  = bundle["model_global"]
modeles_poste = bundle["modeles_poste"]
features      = bundle["features"]
le_position   = bundle["le_position"]
le_foot       = bundle["le_foot"]

# ── Header ────────────────────────────────────────────────────────────────────
st.title("⚽ Outil d'évaluation du potentiel")
st.markdown(
    "**Pour les centres de formation et cellules de scouting.**  \n"
    "Entrez le profil d'un jeune joueur (U16/U17/U18) et découvrez "
    "**combien il pourrait valoir quand il sera pleinement établi en professionnel**, "
    "en comparant son profil à plus de **3 000 trajectoires de joueurs pros**."
)
st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Identité
# ══════════════════════════════════════════════════════════════════════════════
st.subheader("👤 Identité du joueur")

col1, col2 = st.columns(2)
with col1:
    nom       = st.text_input("Nom du joueur", value="Adam B.")
    position  = st.selectbox(
        "Poste",
        ["Attack", "Midfield", "Defender", "Goalkeeper"],
        help="Attack = Attaquant · Midfield = Milieu · Defender = Défenseur · Goalkeeper = Gardien"
    )
    pied      = st.selectbox(
        "Pied préférentiel",
        ["right", "left", "both"],
        help="right = droitier · left = gaucher · both = ambidextre"
    )

with col2:
    age_actuel = st.slider(
        "Âge actuel du joueur",
        14, 21, 16,
        help="L'âge actuel permet de calculer à quel âge il atteindra son potentiel"
    )
    taille     = st.slider("Taille (cm)", 160, 205, 178)
    age_match  = st.slider(
        "Âge au 1er match en compétition",
        13, 21, 15,
        help="L'âge auquel il a joué son premier match officiel"
    )
    valeur_actuelle_m = st.number_input(
        "💶 Valeur actuelle (M€) — 0 si inconnue",
        min_value=0.0,
        max_value=500.0,
        value=0.5,
        step=0.1,
        format="%.1f",
        help="Sa valeur marchande actuelle selon Transfermarkt. Mettez 0 si non référencé."
    )

# Calcul âge pic et fiabilité
age_pic_min = max(age_actuel + (21 - age_actuel) + 2, 22)
age_pic_max = age_pic_min + 2

if age_actuel <= 15:
    fiabilite_label = "⚠️ Indicative"
    fiabilite_class = "fiabilite-faible"
    fiabilite_note  = "Peu de données disponibles à cet âge — à utiliser comme indicateur de tendance."
elif age_actuel <= 17:
    fiabilite_label = "🟡 Modérée"
    fiabilite_class = "fiabilite-moyenne"
    fiabilite_note  = "Estimation correcte pour orienter les décisions de formation."
else:
    fiabilite_label = "🟢 Bonne"
    fiabilite_class = "fiabilite-haute"
    fiabilite_note  = "Suffisamment de données pour une estimation fiable."

st.markdown(f"""
<div class="result-box">
    📅 <b>Quand verra-t-on ce potentiel ?</b><br><br>
    Si <b>{nom}</b> continue sa progression, il devrait atteindre
    sa valeur de plein potentiel vers
    <span class="age-badge">🎯 {age_pic_min} – {age_pic_max} ans</span>
    <br><br>
    <span class="info-text">
    Fiabilité de l'estimation : <span class="{fiabilite_class}">{fiabilite_label}</span>
    — {fiabilite_note}
    </span>
</div>
""", unsafe_allow_html=True)

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Statistiques
# ══════════════════════════════════════════════════════════════════════════════
st.subheader("📊 Ses statistiques actuelles")
st.caption(
    "Renseignez ses stats depuis ses débuts en compétition. "
    "Plus il a joué, plus l'estimation sera précise."
)

col3, col4 = st.columns(2)
with col3:
    matchs  = st.slider("Matchs joués", 1, 150, 20,
                         help="Total de matchs joués depuis ses débuts")
    minutes = st.slider("Minutes jouées", 100, 12000, 1500,
                         help="Total des minutes disputées")
    buts    = st.slider("Buts marqués", 0, 80, 8)
    passes  = st.slider("Passes décisives", 0, 60, 4)

with col4:
    nb_competitions = st.slider(
        "Compétitions différentes jouées",
        1, 8, 2,
        help="Ex : championnat régional + coupe = 2 compétitions"
    )
    caps      = st.slider(
        "Sélections en équipe nationale jeune",
        0, 50, 2,
        help="U15 / U16 / U17 / U19 / U21 — tous niveaux confondus"
    )
    int_goals = st.slider("Buts en sélection", 0, 20, 0)

# Calcul automatique ratios
goals_per_90   = round(buts * 90 / max(minutes, 1), 2)
assists_per_90 = round(passes * 90 / max(minutes, 1), 2)

# Indicateur volume de données
if minutes >= 1000:
    volume_icon = "🟢"
    volume_msg  = "Volume suffisant — estimation fiable"
elif minutes >= 500:
    volume_icon = "🟡"
    volume_msg  = "Volume correct — estimation possible"
else:
    volume_icon = "🔴"
    volume_msg  = "Peu de minutes jouées — estimation à titre indicatif"

st.markdown(f"""
<div class="result-box">
    <b>📐 Indicateurs calculés automatiquement</b><br><br>
    ⚽ <b>Buts par match de 90 min</b> : {goals_per_90:.2f}
    &nbsp;&nbsp;
    🎯 <b>Passes déc. par match de 90 min</b> : {assists_per_90:.2f}
    <br><br>
    {volume_icon} <span class="info-text">{volume_msg}
    ({minutes:,} minutes jouées)</span>
</div>
""", unsafe_allow_html=True)

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Club
# ══════════════════════════════════════════════════════════════════════════════
st.subheader("🏟️ Son club actuel")
st.caption(
    "C'est le facteur le plus important du modèle. "
    "Un joueur formé dans un grand club a statistiquement plus de chances de percer."
)

niveau_club = st.selectbox(
    "Niveau du club",
    options=[
        "🏘️  Club amateur / régional        — ex : club de district, U17 régionale",
        "🥈  Club semi-pro                   — ex : N2, N3, championnat national",
        "🥇  Club professionnel L2 / 3e div  — ex : Metz, Auxerre, Grenoble",
        "⭐  Club Ligue 1 / Championship     — ex : Lens, Strasbourg, Marseille",
        "🌟  Top 5 européen                  — ex : Lyon, Arsenal, Atletico Madrid",
        "👑  Elite européenne                — ex : PSG, Barcelone, Man City, Real",
    ],
    index=2
)

valeur_club_map = {
    "🏘️  Club amateur / régional        — ex : club de district, U17 régionale":      250_000,
    "🥈  Club semi-pro                   — ex : N2, N3, championnat national":       1_000_000,
    "🥇  Club professionnel L2 / 3e div  — ex : Metz, Auxerre, Grenoble":           4_000_000,
    "⭐  Club Ligue 1 / Championship     — ex : Lens, Strasbourg, Marseille":       15_000_000,
    "🌟  Top 5 européen                  — ex : Lyon, Arsenal, Atletico Madrid":    45_000_000,
    "👑  Elite européenne                — ex : PSG, Barcelone, Man City, Real":   120_000_000,
}
valeur_moyenne_club = valeur_club_map[niveau_club]

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# BOUTON
# ══════════════════════════════════════════════════════════════════════════════
predict_btn = st.button(
    f"🔮 Évaluer le potentiel de {nom}",
    type="primary",
    use_container_width=True
)

if predict_btn:

    # Encodage
    pos_label  = position if position in le_position.classes_ else "Missing"
    foot_label = pied if pied in le_foot.classes_ else "Unknown"
    position_enc = int(le_position.transform([pos_label])[0])
    foot_enc     = int(le_foot.transform([foot_label])[0])

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

    # Prédiction
    if position in modeles_poste:
        valeur_log     = modeles_poste[position]["model"].predict(X)[0]
        modele_utilise = f"Modèle spécialisé {position}"
    else:
        valeur_log     = model_global.predict(X)[0]
        modele_utilise = "Modèle global"

    valeur_projetee = np.expm1(valeur_log)
    valeur_actuelle = valeur_actuelle_m * 1_000_000

    # Progression
    if valeur_actuelle > 0:
        progression_pct = ((valeur_projetee - valeur_actuelle) / valeur_actuelle) * 100
    else:
        progression_pct = None

    # Formatage
    def fmt(v):
        if v >= 1_000_000:
            return f"{v / 1_000_000:.1f} M€"
        return f"{v / 1_000:.0f} K€"

    valeur_projetee_str = fmt(valeur_projetee)
    valeur_actuelle_str = fmt(valeur_actuelle) if valeur_actuelle > 0 else "Non référencé"

    # ── RÉSULTATS ─────────────────────────────────────────────────────────────
    st.divider()
    st.subheader(f"📈 Potentiel estimé — {nom}")

    # 3 cartes
    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown(f"""
        <div class="value-card">
            <div class="info-text">Il vaut aujourd'hui</div>
            <div style="font-size:1.5em; font-weight:bold; color:#555; margin:8px 0">
                {valeur_actuelle_str}
            </div>
            <div class="info-text">valeur actuelle</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        if progression_pct is not None:
            arrow = "🔼" if progression_pct >= 0 else "🔽"
            color = "#2e7d32" if progression_pct >= 0 else "#c62828"
            sign  = "+" if progression_pct >= 0 else ""
            prog_text = f"{arrow} {sign}{progression_pct:.0f}%"
            prog_sub  = "de progression estimée"
        else:
            prog_text = "—"
            prog_sub  = "valeur actuelle non renseignée"
            color     = "#888888"

        st.markdown(f"""
        <div class="value-card">
            <div class="info-text">Sa progression</div>
            <div style="font-size:1.5em; font-weight:bold; color:{color}; margin:8px 0">
                {prog_text}
            </div>
            <div class="info-text">{prog_sub}</div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        st.markdown(f"""
        <div class="value-card">
            <div class="info-text">Il pourrait valoir</div>
            <div style="font-size:1.5em; font-weight:bold; color:#1a73e8; margin:8px 0">
                {valeur_projetee_str}
            </div>
            <div class="info-text">vers <b>{age_pic_min}–{age_pic_max} ans</b></div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Interprétation simple ──────────────────────────────────────────────────
    if valeur_projetee >= 50_000_000:
        st.success(
            f"🌟 **Talent exceptionnel** — {nom} a le profil d'un futur joueur international "
            f"de très haut niveau. Sa valeur pourrait atteindre **{valeur_projetee_str}** "
            f"vers {age_pic_min}–{age_pic_max} ans. **Investissement de formation fortement recommandé.**"
        )
    elif valeur_projetee >= 20_000_000:
        st.success(
            f"⭐ **Très grand potentiel** — {nom} a le profil d'un futur joueur de top club européen. "
            f"Valeur estimée à **{valeur_projetee_str}** vers {age_pic_min}–{age_pic_max} ans. "
            f"**Investissement de formation recommandé.**"
        )
    elif valeur_projetee >= 5_000_000:
        st.info(
            f"📊 **Bon potentiel** — {nom} a le profil d'un futur joueur professionnel "
            f"de championnat majeur (Ligue 1 / équivalent). "
            f"Valeur estimée à **{valeur_projetee_str}** vers {age_pic_min}–{age_pic_max} ans."
        )
    elif valeur_projetee >= 1_000_000:
        st.warning(
            f"👀 **Potentiel correct** — {nom} pourrait évoluer en professionnel "
            f"(L2 ou championnat secondaire). "
            f"Valeur estimée à **{valeur_projetee_str}** vers {age_pic_min}–{age_pic_max} ans. "
            f"À suivre sur les prochaines saisons."
        )
    else:
        st.error(
            f"⚠️ **Potentiel limité** selon les données actuelles. "
            f"Valeur estimée à **{valeur_projetee_str}** vers {age_pic_min}–{age_pic_max} ans. "
            f"Des facteurs non mesurables (travail, mental, blessures) peuvent changer la trajectoire."
        )

    # ── Signal sous/sur-évalué ─────────────────────────────────────────────────
    if progression_pct is not None and progression_pct > 20:
        st.markdown(f"""
        <div class="result-box">
            💡 <b>Opportunité identifiée</b><br><br>
            {nom} est actuellement valorisé à <b>{valeur_actuelle_str}</b>
            mais son profil suggère une valeur de <b>{valeur_projetee_str}</b>
            vers {age_pic_min}–{age_pic_max} ans (+{progression_pct:.0f}%).<br><br>
            <span class="info-text">
            Ce joueur est potentiellement sous-évalué par le marché actuel.
            C'est une opportunité de recrutement ou de revalorisation de contrat.
            </span>
        </div>
        """, unsafe_allow_html=True)
    elif progression_pct is not None and progression_pct < -20:
        st.markdown(f"""
        <div class="warning-box">
            ⚠️ <b>Attention — surévaluation possible</b><br><br>
            {nom} est valorisé à <b>{valeur_actuelle_str}</b>
            mais son profil statistique suggère <b>{valeur_projetee_str}</b>
            vers {age_pic_min}–{age_pic_max} ans ({progression_pct:.0f}%).<br><br>
            <span class="info-text">
            Le marché semble surévaluer ce joueur par rapport à son profil de formation.
            </span>
        </div>
        """, unsafe_allow_html=True)

    # ── Fiabilité ──────────────────────────────────────────────────────────────
    st.markdown(f"""
    <div class="result-box" style="background-color:#f9f9f9; border-left-color:#888;">
        📋 <b>Fiabilité de cette estimation</b> :
        <span class="{fiabilite_class}">{fiabilite_label}</span><br>
        <span class="info-text">{fiabilite_note}</span><br><br>
        <span class="info-text">
        ℹ️ Ce résultat est basé sur l'analyse de <b>3 289 trajectoires</b> de joueurs
        professionnels. Plus {nom} accumule de matchs et de minutes, plus
        l'estimation s'affine.
        </span>
    </div>
    """, unsafe_allow_html=True)

    # ── Détail technique ───────────────────────────────────────────────────────
    with st.expander("🔧 Détail technique (pour les analystes data)"):
        st.markdown(f"""
        | Paramètre | Valeur |
        |---|---|
        | Modèle utilisé | {modele_utilise} |
        | R² global | 0,61 |
        | MAE global | 4 651 790 € |
        | Valeur log-transformée | {valeur_log:.4f} |
        | Valeur brute projetée | {valeur_projetee:,.0f} € |
        | Buts / 90 min | {goals_per_90:.2f} |
        | Assists / 90 min | {assists_per_90:.2f} |
        | Âge pic estimé | {age_pic_min}–{age_pic_max} ans |
        """)

# ── Footer ─────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "⚽ Football Analytics Platform · RNCP 39586 · "
    "Gradient Boosting · R²=0,61 · Entraîné sur 3 289 joueurs · 1 147 prédictions U22"
)
