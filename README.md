# Football Analytics Platform

Plateforme d'analyse football construite comme un MVP de SaaS analytique data engineering.

## Objectifs

- Analyser les performances des équipes sur 5 ligues européennes (2017-2025)
- Scouting individuel des joueurs avec indicateurs avancés
- Estimation de la valeur marchande des joueurs par apprentissage automatique

## Architecture

Sources → Data Lake (MinIO) → ETL (Python/DuckDB) → Data Warehouse (PostgreSQL) → BI (Metabase)

## Sources de données

| Source | Contenu | Volume |
|---|---|---|
| API football-data.org | Matchs, compétitions, classements | 3 502 matchs |
| Transfermarkt (Kaggle) | Joueurs, apparitions, valuations | 1.8M apparitions |
| football-datasets (GitHub) | Historique matchs 1993-2025 | 15 827 matchs |

## Stack technique

- **Ingestion** : Python, requests, pandas
- **Stockage brut** : MinIO (compatible S3)
- **Transformation** : Python, DuckDB, pandas
- **Data Warehouse** : PostgreSQL
- **ML** : scikit-learn (Gradient Boosting, R²=0.96)
- **BI** : Metabase
- **Orchestration** : GitHub Actions
- **Environnement** : Docker Compose

## Installation

```bash
# 1. Cloner le repo
git clone https://github.com/Justhamid/football-analytics-platform.git
cd football-analytics-platform

# 2. Créer l'environnement Python
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 3. Configurer les variables d'environnement
cp .env.example .env
# Remplir .env avec vos valeurs

# 4. Lancer l'infrastructure
docker compose up -d

# 5. Initialiser MinIO et PostgreSQL
python src/storage/setup_minio.py
python src/storage/setup_postgres.py
```

## Pipeline complet

```bash
# Ingestion
python src/ingestion/collect_api_matches.py
python src/storage/upload_to_minio.py

# Transformation
python src/transformation/transform_matches.py
python src/transformation/transform_players.py
python src/transformation/build_unified_matches.py
python src/transformation/build_classements_unified.py
python src/transformation/build_clubs_unified.py
python src/transformation/build_appearances_unified.py
python src/transformation/build_players_enriched.py

# ML
python src/ml/build_features_temporal.py
python src/ml/train_model_temporal.py
```

## Modèle ML

| Modèle | MAE | R² |
|---|---|---|
| Linear Regression | 586 567 204 € | 0.58 |
| Random Forest | 540 944 € | 0.96 |
| **Gradient Boosting** | **531 795 €** | **0.96** |

## Structure du projet

projet_football_data/
├── src/
│   ├── ingestion/        # Collecte des données
│   ├── transformation/   # ETL et pipelines
│   ├── ml/               # Modèle ML
│   ├── storage/          # Setup MinIO et PostgreSQL
│   ├── validation/       # Scripts de qualité
│   └── utils/            # Team mapping et utilitaires
├── data/
│   ├── brut/             # Données brutes (non versionné)
│   └── traite/           # Données transformées (non versionné)
├── models/               # Modèles entraînés (non versionné)
├── docker-compose.yml
├── requirements.txt
└── .env.example

