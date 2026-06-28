# Football Analytics Platform

Plateforme d'analyse football construite comme un MVP de SaaS analytique data engineering,
destinée aux clubs professionnels, cellules de scouting et centres de formation.

## Objectifs

- Analyser les performances des équipes sur 5 ligues européennes (2017–2025)
- Scouting individuel des joueurs avec indicateurs avancés (goals/90, assists/90)
- Projection de carrière des jeunes joueurs (16–21 ans) par apprentissage automatique

## Architecture

```
Sources → Data Lake (MinIO S3) → ETL (Airflow) → Data Warehouse (PostgreSQL) → ML → BI (Metabase)
```

## Prérequis

| Outil | Version minimale |
|---|---|
| Python | 3.12.x |
| Docker Desktop | 24.x ou supérieur |
| Git | 2.x |

> ⚠️ PySpark nécessite Java 11+ installé sur la machine hôte.  
> Sur Windows, `winutils.exe` doit être configuré (voir [documentation PySpark Windows](https://spark.apache.org/docs/latest/)).

## Sources de données

| Source | Contenu | Volume |
|---|---|---|
| API football-data.org | Matchs, compétitions, classements | 3 502 matchs (2023–2025) |
| Transfermarkt (Kaggle) | Joueurs, apparitions, valorisations | 1,86M apparitions (2012–2026) |
| football-datasets (GitHub) | Historique matchs | 15 827 matchs (2017–2025) |

**Résultat après unification** : 16 802 matchs uniques · 47 702 joueurs · 616 377 valorisations

## Stack technique

| Composant | Outil | Rôle |
|---|---|---|
| Orchestration | Apache Airflow 2.8 | 3 DAGs · scheduling hebdomadaire |
| Data Lake | MinIO (S3-compatible) | Stockage brut · idempotence |
| Transformation | Python · pandas · DuckDB · PySpark | ETL selon volume |
| Data Warehouse | PostgreSQL 15 | Schéma en étoile · 4 schémas |
| ML | scikit-learn (Gradient Boosting) | Projection carrière U22 |
| BI | Metabase | 3 dashboards interactifs |
| Infra | Docker Compose | 6 services · réseau isolé |
| Versionning | Git · GitHub | Conventional Commits |

## Installation

```bash
# 1. Cloner le repo
git clone https://github.com/Justhamid/football-analytics-platform.git
cd football-analytics-platform

# 2. Créer l'environnement Python
python -m venv venv
.\venv\Scripts\Activate.ps1  # Windows
# source venv/bin/activate   # Linux / macOS
pip install -r requirements.txt

# 3. Configurer les variables d'environnement
cp .env.example .env
# Remplir .env avec vos valeurs (voir section Variables d'environnement)

# 4. Lancer l'infrastructure (6 conteneurs)
docker compose up -d

# 5. Initialiser MinIO et PostgreSQL
python src/storage/setup_minio.py
python src/storage/setup_postgres.py
```

## Variables d'environnement (.env)

```bash
POSTGRES_USER=football_admin
POSTGRES_PASSWORD=your_password
POSTGRES_DB=football_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5433          # port exposé vers l'hôte (interne : 5432)
FOOTBALL_DATA_API_TOKEN=your_token
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=your_secret
```

## Pipeline complet

Les 3 DAGs s'exécutent automatiquement chaque lundi via Airflow (`localhost:8080`).
Exécution manuelle possible script par script :

```bash
# ── DAG 1 : pipeline_clubs (6h00) ──────────────────────────
python src/ingestion/collect_api_matches.py
python src/transformation/transform_matches.py
python src/transformation/build_unified_matches.py
python src/transformation/build_classements_unified.py

# ── DAG 2 : pipeline_players (7h00) ────────────────────────
python src/transformation/transform_players.py
python src/transformation/build_appearances_unified.py   # couvre 2012-2026
python src/transformation/build_players_enriched.py

# ── DAG 3 : pipeline_ml (8h00) ─────────────────────────────
python src/ml/train_model_projection.py
```

## Modèle ML — Projection de carrière

Prédit la valeur marchande future d'un joueur à partir de ses statistiques entre 16 et 21 ans.

| Modèle | Dataset | R² | MAE |
|---|---|---|---|
| Régression linéaire | 3 289 joueurs | 0,58 | — |
| **Gradient Boosting (retenu)** | **3 289 joueurs** | **0,61** | **4 651 790 €** |

> Le R² de 0,61 est cohérent avec la littérature en football analytics pour
> la projection à long terme (facteurs non quantifiables : blessures, transferts...).

**Résultats par poste :**

| Poste | R² | N joueurs |
|---|---|---|
| Attaquants | 0,68 | 944 |
| Défenseurs | 0,65 | 1 135 |
| Milieux | 0,64 | 1 046 |
| Gardiens | 0,53 | 164 |

**Variables les plus importantes :**
- Niveau du club formateur : ~85%
- Matchs joués (16–21 ans) : ~5%
- Sélections nationales : ~3%

**Prédictions générées :** 1 147 joueurs U22 → table `marts_ml.predictions_projection_carriere`

## Dashboards Metabase (localhost:3000)

| Dashboard | Contenu principal |
|---|---|
| Clubs & Ligues | Classements · avantage domicile · top offensives/défenses |
| Scouting Joueurs | Goals/90 · assists/90 · joueurs sous-évalués |
| Projection de Carrière | Top U22 · pépites <5M€ · valeur projetée vs actuelle |

## Démo interactive — Projection de carrière

Une application Streamlit permet de simuler la projection de carrière
d'un jeune joueur en temps réel.

```bash
streamlit run app.py
```

Accessible sur `http://localhost:8501` — entrez les statistiques d'un
joueur (16–21 ans) et obtenez une estimation de sa valeur marchande future.

## Structure du projet

```
football-analytics-platform/
├── dags/
│   ├── dag_pipeline_clubs.py
│   ├── dag_pipeline_players.py
│   └── dag_pipeline_ml.py
├── src/
│   ├── ingestion/        # Collecte API et CSV
│   ├── transformation/   # ETL et pipelines
│   ├── ml/               # train_model_projection.py
│   ├── storage/          # Setup MinIO et PostgreSQL
│   └── utils/            # team_mapping.py (412 correspondances)
├── models/               # Modèles entraînés .pkl (non versionné)
├── data/
│   └── brut/             # Données brutes (non versionné)
├── images/               # Schémas architecture et dashboards
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── .gitignore            # .env · *.pkl · *.pyc · data/
```

## Sécurité

- Secrets via `.env` (jamais versionné)
- 3 rôles PostgreSQL : `football_reader` (Metabase) · `football_writer` (Airflow) · `football_user` (admin)
- Réseau Docker isolé : `172.18.0.0/16`
- Authentification PostgreSQL : SCRAM-SHA-256
- Données personnelles : Article 4 RGPD · activité professionnelle publique · pas de données sensibles (Art. 9)
