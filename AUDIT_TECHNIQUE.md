# AUDIT TECHNIQUE COMPLET — Football Analytics Platform
> Responsable audit : GitHub Copilot — Date : août 2026  
> Version auditée : état courant du dépôt `Justhamid/football-analytics-platform`

---

## TABLE DES MATIÈRES

1. [Structure générale](#1-structure-générale)
2. [Ingestion](#2-ingestion-srcingestion)
3. [Transformation](#3-transformation-srctransformation)
4. [DAGs Airflow](#4-dags-airflow-dags)
5. [Base de données PostgreSQL](#5-base-de-données-postgresql)
6. [Machine Learning](#6-machine-learning-srcml)
7. [API REST](#7-api-rest-api)
8. [Infrastructure](#8-infrastructure)
9. [Documentation existante](#9-documentation-existante)
10. [Tests](#10-tests)

---

## 1. STRUCTURE GÉNÉRALE

### 1.1 Arborescence complète

```
football-analytics-platform/
├── .github/
│   └── workflows/
│       └── ci.yml                          # Pipeline CI/CD GitHub Actions (4 jobs)
├── api/
│   ├── Dockerfile                          # Image Docker pour le service FastAPI
│   ├── main.py                             # API REST FastAPI (4 endpoints)
│   └── requirements_api.txt               # Dépendances Python de l'API
├── certs/                                  # (dossier réservé certificats TLS)
├── dags/
│   ├── dag_pipeline_clubs.py              # DAG Airflow pipeline clubs
│   ├── dag_pipeline_ml.py                 # DAG Airflow pipeline ML
│   └── dag_pipeline_players.py            # DAG Airflow pipeline joueurs
├── data/
│   ├── brut/
│   │   ├── api/                           # Fichiers JSON collectés (football-data.org)
│   │   │   ├── BL1_2023_matches.json
│   │   │   ├── BL1_2024_matches.json
│   │   │   ├── FL1_2023_matches.json
│   │   │   ├── FL1_2024_matches.json
│   │   │   ├── PD_2023_matches.json
│   │   │   ├── PD_2024_matches.json
│   │   │   ├── PL_2023_matches.json
│   │   │   ├── PL_2024_matches.json
│   │   │   ├── SA_2023_matches.json
│   │   │   └── SA_2024_matches.json
│   │   ├── football_datasets/             # CSV historique matchs (GitHub)
│   │   │   ├── _refresh_metadata.json
│   │   │   ├── bundesliga/
│   │   │   ├── la_liga/
│   │   │   ├── ligue_1/
│   │   │   ├── premier_league/
│   │   │   └── serie_a/
│   │   ├── transfermarkt/                 # CSV Transfermarkt (Kaggle)
│   │   │   ├── _refresh_metadata.json
│   │   │   ├── appearances.csv
│   │   │   ├── club_games.csv
│   │   │   ├── clubs.csv
│   │   │   ├── competitions.csv
│   │   │   ├── countries.csv
│   │   │   ├── game_events.csv
│   │   │   ├── games.csv
│   │   │   ├── player_valuations.csv
│   │   │   ├── players.csv
│   │   │   └── transfers.csv
│   │   └── transfermarkt_backup/          # Snapshot précédent (rollback)
│   └── traite/
│       ├── unified_matches.csv            # Sortie brute fusion matchs
│       └── spark_player_performance/      # Sorties PySpark
├── logs/
│   └── airflow/                           # Logs d'exécution des DAGs
│       ├── dag_id=pipeline_clubs/
│       ├── dag_id=pipeline_ml/
│       ├── dag_id=pipeline_players/
│       ├── dag_processor_manager/
│       └── scheduler/
├── models/                                # Fichiers .pkl (exclus du git)
├── scripts/
│   └── check_feature_importance.py        # Script d'analyse importance features
├── src/
│   ├── __init__.py
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── collect_api_matches.py         # Collecte API football-data.org
│   │   ├── refresh_football_datasets_source.py  # Refresh GitHub (ETag)
│   │   └── refresh_transfermarkt_source.py      # Refresh Kaggle + validation
│   ├── ml/
│   │   ├── __init__.py
│   │   ├── build_features.py              # Construction features ML valeur marchande
│   │   ├── build_features_temporal.py     # Construction features temporelles
│   │   ├── train_model.py                 # Entraînement LR + RandomForest (valeur marchande)
│   │   ├── train_model_projection.py      # Entraînement GBR projection carrière (modèle retenu)
│   │   └── train_model_temporal.py        # Entraînement modèles temporels (LR/RF/GBR)
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── setup_constraints.py           # Clés primaires, FK, CHECK, index PostgreSQL
│   │   ├── setup_minio.py                 # Création buckets MinIO
│   │   ├── setup_postgres.py              # Création schémas PostgreSQL
│   │   ├── setup_roles.py                 # Création rôles PostgreSQL
│   │   └── upload_to_minio.py             # Upload données vers MinIO
│   ├── transformation/
│   │   ├── __init__.py
│   │   ├── build_appearances_unified.py   # Jointure apparitions ↔ match_id unifié
│   │   ├── build_classements_unified.py   # Classements + avantage domicile
│   │   ├── build_clubs_unified.py         # Table clubs normalisée
│   │   ├── build_competitions.py          # Table compétitions
│   │   ├── build_players_enriched.py      # Joueurs enrichis (stats + ML)
│   │   ├── build_unified_matches.py       # Fusion 3 sources + déduplication MD5
│   │   ├── spark_appearances.py           # Traitement PySpark apparitions
│   │   ├── transform_matches.py           # ETL matchs API → staging
│   │   └── transform_players.py           # ETL joueurs Transfermarkt → staging
│   ├── utils/
│   │   ├── __init__.py
│   │   └── team_mapping.py                # Dictionnaire normalisation noms d'équipes
│   └── validation/
│       ├── __init__.py
│       ├── audit_jointure.py              # Audit comparaison noms entre sources
│       ├── check_api_data.py              # Vérification données API
│       ├── check_transfermarkt_data.py    # Vérification données Transfermarkt
│       ├── inventaire_donnees.py          # Inventaire des données disponibles
│       ├── validation_initiale.py         # Validation structurelle initiale
│       └── valider_mapping.py             # Validation du mapping équipes
├── app.py                                 # Application Streamlit (démo projection carrière)
├── docker-compose.yml                     # Infrastructure 7 services Docker Compose
├── EXPLOITATION.md                        # Feuille de route d'exploitation
├── fix_cascade_drop.py                    # Script correctif DROP CASCADE
├── fix_compose.py                         # Script correctif docker-compose
├── fix_postgres_port.py                   # Script correctif port PostgreSQL
├── INCIDENT.md                            # Rapport d'incident (ModuleNotFoundError)
├── pyproject.toml                         # Configuration projet Python
├── README.md                              # Documentation principale
├── RECETTES.md                            # Cahier de recettes (tests)
├── requirements.txt                       # Dépendances Python du projet
└── test_hypothese_domicile.py             # Test statistique avantage domicile
```

### 1.2 Inventaire des fichiers par rôle

| Fichier | Rôle |
|---|---|
| `app.py` | Interface Streamlit de démonstration de la projection de carrière U16–U21 |
| `docker-compose.yml` | Définit et orchestre 7 services Docker (PostgreSQL, MinIO, Metabase, Airflow x3, FastAPI) |
| `EXPLOITATION.md` | Feuille de route opérationnelle : maintenance récurrente, commandes, indicateurs, procédures de reprise |
| `fix_cascade_drop.py` | Script correctif ponctuel pour les DROP TABLE ... CASCADE |
| `fix_compose.py` | Script correctif ponctuel pour la configuration docker-compose |
| `fix_postgres_port.py` | Script correctif ponctuel pour le port PostgreSQL |
| `INCIDENT.md` | Post-mortem de l'incident critique ModuleNotFoundError sur les 3 DAGs Airflow |
| `pyproject.toml` | Configuration de l'outil de build Python |
| `README.md` | Documentation principale : architecture, sources, installation, pipeline, ML, dashboards, API |
| `RECETTES.md` | Cahier de 19 tests fonctionnels, structurels et de sécurité avec résultats obtenus |
| `requirements.txt` | Dépendances Python globales du projet |
| `test_hypothese_domicile.py` | Test statistique sur l'avantage à domicile |
| `dags/dag_pipeline_clubs.py` | DAG Airflow pipeline clubs (6 tâches, schedule lundi 6h00) |
| `dags/dag_pipeline_players.py` | DAG Airflow pipeline joueurs (4 tâches + 1 sensor, schedule lundi 7h00) |
| `dags/dag_pipeline_ml.py` | DAG Airflow pipeline ML (1 tâche + 1 sensor, schedule lundi 8h00) |
| `api/main.py` | Serveur FastAPI exposant 4 endpoints de prédiction ML |
| `api/Dockerfile` | Image Docker pour le service FastAPI |
| `api/requirements_api.txt` | Dépendances Python de l'API FastAPI |
| `src/ingestion/collect_api_matches.py` | Collecte les matchs sur l'API football-data.org (5 ligues × 2 saisons) |
| `src/ingestion/refresh_football_datasets_source.py` | Refresh conditionnel (ETag) des CSV football-datasets depuis GitHub |
| `src/ingestion/refresh_transfermarkt_source.py` | Téléchargement Kaggle + validation structurelle + backup + remplacement |
| `src/transformation/transform_matches.py` | Chargement JSON API → DuckDB → cleaning → staging PostgreSQL |
| `src/transformation/build_unified_matches.py` | Fusion 3 sources (API + football-datasets + Transfermarkt) avec déduplication MD5 |
| `src/transformation/build_classements_unified.py` | Construction classements + avantage domicile depuis `unified_matches` |
| `src/transformation/build_clubs_unified.py` | Construction table clubs normalisée depuis `marts_players.clubs` |
| `src/transformation/transform_players.py` | ETL complet des 5 fichiers Transfermarkt (players, clubs, valuations, transfers, appearances) |
| `src/transformation/build_appearances_unified.py` | Jointure apparitions Transfermarkt ↔ match_id unifié |
| `src/transformation/build_players_enriched.py` | Enrichissement joueurs avec stats agrégées + prédictions ML |
| `src/transformation/build_competitions.py` | Construction table compétitions |
| `src/transformation/spark_appearances.py` | Traitement PySpark des apparitions (grand volume) |
| `src/ml/build_features.py` | Construction features ML valeur marchande courante (one-hot encoding) |
| `src/ml/build_features_temporal.py` | Construction dataset temporel (paires de valorisations consécutives) |
| `src/ml/train_model.py` | Entraînement Régression Linéaire + Random Forest (valeur marchande statique) |
| `src/ml/train_model_projection.py` | Entraînement Gradient Boosting projection carrière U22 — **modèle de production** |
| `src/ml/train_model_temporal.py` | Entraînement 3 modèles (LR / RF / GBR) sur dataset temporel |
| `src/storage/setup_postgres.py` | Création des 4 schémas PostgreSQL |
| `src/storage/setup_minio.py` | Création des 3 buckets MinIO |
| `src/storage/setup_roles.py` | Création et configuration des rôles PostgreSQL |
| `src/storage/setup_constraints.py` | Ajout des clés primaires, clés étrangères, contraintes CHECK et index |
| `src/storage/upload_to_minio.py` | Upload des données brutes vers MinIO |
| `src/utils/team_mapping.py` | Dictionnaire de normalisation des noms d'équipes (5 ligues, multi-sources) |
| `src/validation/audit_jointure.py` | Audit de cohérence des noms d'équipes entre les 3 sources |
| `src/validation/check_api_data.py` | Vérification structurelle des données API |
| `src/validation/check_transfermarkt_data.py` | Vérification des données Transfermarkt |
| `src/validation/inventaire_donnees.py` | Inventaire complet des données disponibles |
| `src/validation/validation_initiale.py` | Validation structurelle initiale des sources |
| `src/validation/valider_mapping.py` | Validation de la couverture du dictionnaire de mapping |
| `.github/workflows/ci.yml` | Pipeline CI/CD : 4 jobs (qualité code, sécurité, tests unitaires, résumé) |

---

## 2. INGESTION (`src/ingestion/`)

### 2.1 `collect_api_matches.py`

**Source :** API football-data.org v4  
**Authentification :** Header `X-Auth-Token` lu depuis la variable d'environnement `FOOTBALL_DATA_API_TOKEN`. Lève `ValueError` si absent.

**Paramètres :**
- `COMPETITIONS = ["PL", "PD", "SA", "BL1", "FL1"]` — 5 ligues européennes
- `SEASONS = [2023, 2024]` — 2 saisons → 10 requêtes au total
- `MIN_MATCHES_ATTENDUS = {"PL": 350, "PD": 350, "SA": 350, "BL1": 280, "FL1": 280}` — seuils de volume
- `CHAMPS_OBLIGATOIRES = {"homeTeam", "awayTeam", "score", "utcDate", "status"}` — validation structurelle
- Pause de **7 secondes** entre chaque requête (plan gratuit : 10 req/min)

**Mécanisme de collecte — `collect_matches(competition_code, season)` :**
1. Construit l'URL : `https://api.football-data.org/v4/competitions/{code}/matches?season={year}`
2. Appelle `Effectuer_requete_securisee()` (max 3 retries)
3. Gestion **rate limit 429** : lit `X-Request-Counter-Reset` ou attend 15s par défaut
4. En cas de statut ≠ 200 : lève `RuntimeError` avec le code HTTP et le corps de réponse
5. Vérifie la présence de la clé `"matches"` : lève `ValueError` sinon
6. Appelle `valider_reponse()` :
   - 0 match → `ValueError`
   - Volume < seuil (`MIN_MATCHES_ATTENDUS`) → `ValueError`
   - Vérification structurelle sur les 5 premiers matchs : champs manquants → `ValueError`
7. Écrit `data/brut/api/{CODE}_{YEAR}_matches.json`

**Sortie :** 10 fichiers JSON dans `data/brut/api/`

**Exceptions propagées :**
- `ValueError` : `FOOTBALL_DATA_API_TOKEN` absent, volume anormal, structure inattendue
- `RuntimeError` : erreur HTTP non 200 ou au moins 1 collecte en échec (levée dans `main()`)

---

### 2.2 `refresh_football_datasets_source.py`

**Source :** Dépôt GitHub public `datasets/football-datasets` (football-data.co.uk)  
**Authentification :** Aucune (dépôt public)

**Paramètres :**
- `LIGUES` : dict de 5 entrées `{nom_local: nom_github}` → `{"premier_league": "premier-league", "la_liga": "la-liga", "bundesliga": "bundesliga", "serie_a": "serie-a", "ligue_1": "ligue-1"}`
- `SAISONS_A_RAFRAICHIR = ["season-2425.csv", "season-2526.csv"]` — uniquement les saisons récentes
- `COLONNES_ATTENDUES = {"Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"}` — validation structurelle
- URL base : `https://raw.githubusercontent.com/datasets/football-datasets/main/datasets`
- Timeout requêtes : 30s

**Mécanisme — `rafraichir_fichier()` :**
1. Lit l'ETag local (`{fichier}.etag`) s'il existe
2. Envoie `GET` avec header `If-None-Match: {etag}` → si **304** : retourne `"inchange"` (aucun téléchargement)
3. Si **404** : retourne `"absent"` (normal pour `season-2526.csv` en début de saison)
4. Si autre code ≠ 200 : lève `RuntimeError`
5. Appelle `valider_contenu()` : vérifie les colonnes minimales dans la première ligne + au moins 10 lignes → lève `ValueError` sinon
6. Sauvegarde l'ancien fichier sous `{fichier}.backup` avant remplacement
7. Écrit le nouveau CSV et met à jour le fichier `.etag`
8. Écrit `_refresh_metadata.json` (timestamp, listes mis_a_jour/inchangés)

**Statuts de retour possibles :** `"mis_a_jour"`, `"inchange"`, `"absent"`

**Exceptions propagées :**
- `RuntimeError` : statut HTTP inattendu, ou au moins une erreur dans `main()`
- `ValueError` : colonnes manquantes ou contenu tronqué

---

### 2.3 `refresh_transfermarkt_source.py`

**Source :** Dataset Kaggle `davidcariboo/player-scores`  
**Authentification :** Variables `KAGGLE_USERNAME` et `KAGGLE_KEY` lues depuis `.env`, injectées dans `os.environ`. Lève `ValueError` si absentes.

**Fichiers validés et colonnes minimales attendues :**

| Fichier | Colonnes minimales requises |
|---|---|
| `players.csv` | `player_id`, `first_name`, `last_name`, `name`, `date_of_birth`, `position`, `sub_position`, `foot`, `height_in_cm`, `country_of_birth`, `country_of_citizenship`, `current_club_id`, `current_club_name`, `current_club_domestic_competition_id`, `last_season`, `market_value_in_eur`, `highest_market_value_in_eur`, `international_caps`, `international_goals` |
| `clubs.csv` | `club_id`, `name`, `domestic_competition_id`, `squad_size`, `average_age`, `foreigners_number`, `foreigners_percentage`, `national_team_players`, `stadium_name`, `stadium_seats`, `coach_name`, `last_season` |
| `player_valuations.csv` | `player_id`, `date`, `market_value_in_eur`, `current_club_name`, `current_club_id`, `player_club_domestic_competition_id` |
| `transfers.csv` | `player_id`, `player_name`, `transfer_date`, `transfer_season`, `from_club_id`, `from_club_name`, `to_club_id`, `to_club_name`, `transfer_fee`, `market_value_in_eur` |
| `appearances.csv` | `appearance_id`, `game_id`, `player_id`, `player_club_id`, `date`, `competition_id`, `goals`, `assists`, `yellow_cards`, `red_cards`, `minutes_played` |

**Seuils de volume :**
- `SEUIL_ALERTE = 0.90` — ratio nouveau/ancien < 90% → accepté avec avertissement dans les logs
- `SEUIL_BLOQUANT = 0.30` — ratio < 30% → `ValueError`, snapshot conservé

**Mécanisme — `main()` :**
1. Télécharge dans un dossier temporaire via `KaggleApi.dataset_download_files()` avec `unzip=True`
   - Patch connu : `os.makedirs` remplacé temporairement pour éviter `FileExistsError` sur `~/.config/kaggle`
2. Appelle `valider_snapshot()` : vérifie présence de chaque fichier, colonnes minimales (lecture 5 lignes), ratios de volume
3. Si validation OK → `sauvegarder_ancien_snapshot()` : `shutil.rmtree(BACKUP_DIR)` puis `shutil.copytree(TM_DIR, BACKUP_DIR)`
4. `remplacer_snapshot()` : copie tous les `.csv` du dossier temporaire dans `TM_DIR`
5. Écrit `_refresh_metadata.json`

**Exceptions propagées :**
- `ValueError` : variables Kaggle absentes, colonnes manquantes, volume catastrophique
- `FileNotFoundError` : fichier attendu absent du snapshot
- `RuntimeError` : enveloppe toute exception de téléchargement ou validation

---

## 3. TRANSFORMATION (`src/transformation/`)

### 3.1 `transform_matches.py`

**Entrée :** Fichiers JSON dans `data/brut/api/` (10 fichiers)  
**Sortie :** Tables PostgreSQL dans le schéma `staging` (via SQLAlchemy)

**Fonctions et logique :**

**`charger_matches_api()`**
- Lit tous les `.json` dans `data/brut/api/`
- Extrait pour chaque match : `game_id`, `competition_id`, `competition_name`, `season` (4 premiers chars de `startDate`), `date` (10 premiers chars de `utcDate`), `round` (matchday), `home_club_name`, `away_club_name`, `home_club_id`, `away_club_id`, `home_club_goals` (score.fullTime.home), `away_club_goals`, `status`
- Déduplication sur `game_id`, log si doublons
- Gestion `json.JSONDecodeError` : fichier ignoré avec avertissement

**`transformer_matches(df)`** via DuckDB :
- Caste `season` en `INTEGER`, `date` en `DATE`, `round` en `INTEGER`
- Contrôle qualité buts ≥ 0 (CASE WHEN) → `NULL` sinon
- Filtre `WHERE status = 'FINISHED'`
- Calcule `result` : `'home'` / `'away'` / `'draw'` / `'unknown'`
- Calcule `total_goals`

**`enrichir_matches(df)`** via DuckDB :
- Ajoute `home_points` (3/1/0) et `away_points` (3/1/0)

**`construire_classements(df)`** via DuckDB :
- CTE `home_stats` + `away_stats` + `total` : agrège par `(competition_id, competition_name, season, club_id, club_name)`
- Colonnes produites : `matchs_joues`, `points`, `buts_marques`, `buts_encaisses`, `victoires`, `nuls`, `defaites`
- Ajoute `diff_buts`, `moy_buts_par_match`, `moy_buts_encaisses_par_match`
- Tri par `competition, season DESC, points DESC, diff_buts DESC, buts_marques DESC`

---

### 3.2 `build_unified_matches.py`

**Entrée :** 3 sources brutes simultanément :
1. `data/brut/api/*.json` — matchs API 2023–2024
2. `data/brut/football_datasets/{ligue}/season-*.csv` — matchs historiques 2016–2025 (filtre `annee >= 2015`)
3. `data/brut/transfermarkt/games.csv` — matchs depuis 2017

**Sortie :** Table `marts_clubs.unified_matches` dans PostgreSQL

**Colonnes produites dans `unified_matches` :**
`match_id`, `source`, `competition`, `date`, `season`, `round`, `home_team`, `away_team`, `home_goals`, `away_goals`, `result`, `total_goals`, `home_shots`, `away_shots`, `home_shots_on_target`, `away_shots_on_target`, `home_corners`, `away_corners`, `home_yellow_cards`, `away_yellow_cards`, `home_red_cards`, `away_red_cards`, `tm_game_id` (Transfermarkt uniquement)

**Mécanisme de déduplication MD5 — `generer_match_id()` :**
```
cle = f"{date}_{competition}_{home_norm}_{away_norm}"
match_id = hashlib.md5(cle.encode()).hexdigest()[:16]
```
Les noms sont normalisés via `normaliser_nom()` avant le hash.

**Mapping des codes compétition :**
```
API : PL, BL1, FL1, PD, SA  →  PL, BL1, FL1, PD, SA
Transfermarkt : GB1 → PL, L1 → BL1, FR1 → FL1, ES1 → PD, IT1 → SA
```

**Fusion — `fusionner_sources()` :**
- Concaténation des 3 DataFrames
- Tri par priorité source : `api=0 > football_datasets=1 > transfermarkt=2`
- `drop_duplicates(subset=["match_id"], keep="first")` — en cas de doublon, la source prioritaire gagne
- Filtre qualité scores : `0 ≤ home_goals ≤ 20` et `0 ≤ away_goals ≤ 20`
- Calcule `result` et `total_goals`

**Chargement PostgreSQL :** `DROP TABLE IF EXISTS ... CASCADE` puis `to_sql(if_exists="append")`

---

### 3.3 `build_classements_unified.py`

**Entrée :** `marts_clubs.unified_matches` (via PostgreSQL)  
**Sortie :** 3 tables PostgreSQL dans `marts_clubs` :
- `classements_equipes_unified`
- `avantage_domicile`
- `matches_enrichis`

**Colonnes lues depuis `unified_matches` :**
`match_id`, `competition`, `date`, `season`, `home_team`, `away_team`, `home_goals`, `away_goals`, `result`, `total_goals`, `home_shots`, `away_shots`, `home_shots_on_target`, `away_shots_on_target`, `home_corners`, `away_corners`, `home_yellow_cards`, `away_yellow_cards`, `home_red_cards`, `away_red_cards`
— Filtre : `home_goals IS NOT NULL AND away_goals IS NOT NULL`

**`construire_classements(df)`** via DuckDB — colonnes produites :
`competition`, `competition_name`, `season`, `club_name`, `matchs_joues`, `points`, `buts_marques`, `buts_encaisses`, `victoires`, `nuls`, `defaites`, `diff_buts`, `moy_buts_par_match`, `moy_buts_encaisses_par_match`

**`construire_stats_avantage_domicile(df)`** via DuckDB — colonnes produites :
`competition`, `competition_name`, `season`, `total_matchs`, `victoires_domicile`, `victoires_exterieur`, `nuls`, `pct_victoires_domicile`, `moy_buts_par_match`, `moy_buts_domicile`, `moy_buts_exterieur`

**`construire_stats_matchs_enrichis(df)`** — retourne le DataFrame avec toutes les colonnes stats supplémentaires de football-datasets + `home_points`, `away_points`.

---

### 3.4 `build_clubs_unified.py`

**Entrée :** `marts_players.clubs` (via PostgreSQL)  
**Sortie :** Table `marts_clubs.clubs_unified` dans PostgreSQL

**Colonnes lues :** `club_id`, `name`, `domestic_competition_id`, `squad_size`, `average_age`, `foreigners_number`, `foreigners_percentage`, `national_team_players`, `stadium_name`, `stadium_seats`, `coach_name`, `last_season`

**Transformations :**
- `name_normalized` = `normaliser_nom(name)` via `team_mapping.py`
- `competition` = mapping `{"GB1": "PL", "ES1": "PD", "IT1": "SA", "L1": "BL1", "FR1": "FL1"}`
- `competition_name` = mapping lisible (`"PL"` → `"Premier League"` etc.)
- Vérification et log des clubs sans mapping normalisé

---

### 3.5 `transform_players.py`

**Entrée :** 5 fichiers CSV dans `data/brut/transfermarkt/`  
**Sortie :** Tables dans le schéma `staging` PostgreSQL

**Connexion DuckDB partagée** (`con = duckdb.connect()` au niveau module)

**`transformer_players(df)` — entrée : `players.csv`**  
Colonnes transformées via DuckDB :
- `player_id`, `first_name`, `last_name`, `name`
- `date_of_birth` → `DATE`
- `position`, `sub_position`, `foot`
- `height_in_cm` → `INTEGER`
- `country_of_birth`, `country_of_citizenship`
- `current_club_id`, `current_club_name`, `current_club_domestic_competition_id`
- `last_season` → `INTEGER`
- `market_value_in_eur` → `DOUBLE` (NULL si < 0)
- `highest_market_value_in_eur` → `DOUBLE` (NULL si < 0)
- `international_caps` → `INTEGER`
- `international_goals` → `INTEGER`
- Filtre : `player_id IS NOT NULL AND name IS NOT NULL`
- Déduplication sur `player_id`

**`transformer_clubs(df)` — entrée : `clubs.csv`**
- Colonnes : `club_id`, `name`, `domestic_competition_id`, `squad_size`, `average_age` → `FLOAT`, `foreigners_number`, `foreigners_percentage` → `FLOAT`, `national_team_players`, `stadium_name`, `stadium_seats` → `INTEGER`, `coach_name`, `last_season` → `INTEGER`
- Filtre : `club_id IS NOT NULL`
- Déduplication sur `club_id`

**`transformer_valuations(df)` — entrée : `player_valuations.csv`**
- Colonnes : `player_id`, `date` → `DATE`, `market_value_in_eur` → `DOUBLE` (NULL si < 0), `current_club_name`, `current_club_id`, `player_club_domestic_competition_id`
- Filtre : `player_id IS NOT NULL AND market_value_in_eur IS NOT NULL`

**`transformer_transfers(df)` — entrée : `transfers.csv`**
- Colonnes : `player_id`, `player_name`, `transfer_date` → `DATE` (NULL si > CURRENT_DATE), `transfer_season`, `from_club_id`, `from_club_name`, `to_club_id`, `to_club_name`, `transfer_fee` → `DOUBLE` (NULL si < 0), `market_value_in_eur` → `DOUBLE` (NULL si < 0)
- Filtre : `player_id IS NOT NULL`

**`transformer_appearances(df_app, df_players)` — entrée : `appearances.csv` + df players**
- Colonnes : `appearance_id`, `game_id`, `player_id`, `player_club_id`, `date` → `DATE`, `competition_id`
- `goals` / `assists` / `yellow_cards` / `red_cards` / `minutes_played` → `INTEGER` (0 si < 0)
- Jointure LEFT JOIN avec `df_players` pour ajouter `player_name`, `position`, `market_value_in_eur`
- Filtre : `minutes_played > 0 AND player_id IS NOT NULL`
- Déduplication sur `appearance_id`

---

### 3.6 `build_appearances_unified.py`

**Entrée :**
- `staging.stg_appearances` (PostgreSQL) — filtre `date >= '2012-01-01'`
- `data/brut/transfermarkt/games.csv` (CSV) — filtre `date >= '2012-01-01'`, filtre `competition_id IN (GB1, ES1, IT1, L1, FR1)`

**Sortie :** Table `marts_players.appearances_unified` dans PostgreSQL

**Colonnes lues depuis `stg_appearances` :**
`appearance_id`, `game_id`, `player_id`, `player_club_id`, `date`, `competition_id`, `goals`, `assists`, `minutes_played`, `yellow_cards`, `red_cards`, `player_name`

**Mécanisme de jointure :**
1. Normalise les noms d'équipes via `normaliser_nom()`
2. Génère un `match_id` MD5 pour chaque match Transfermarkt
3. Crée une table de correspondance `game_id TM → match_id unifié`
4. LEFT JOIN `appearances ↔ correspondance` sur `game_id`
5. Log la couverture : % d'apparitions avec `match_id` trouvé

**Méthode d'insertion PostgreSQL :** `COPY FROM STDIN` (méthode `psql_insert_copy` — insert ultra-rapide via `psycopg2.cursor.copy_expert`)

---

### 3.7 `build_players_enriched.py`

**Entrée :**
- `marts_players.players` (PostgreSQL) — filtre `position IS NOT NULL AND position != 'Missing'`
- `marts_players.player_performance` (PostgreSQL) — agrégat par `player_id`
- `marts_ml.predictions_market_value_temporal` (PostgreSQL) — `DISTINCT ON (player_id)` ordonné par `date_t1 DESC`

**Sortie :** Table `marts_players.players_enriched` dans PostgreSQL

**Colonnes produites :**
Toutes les colonnes de `marts_players.players` +
- `current_club_normalized` — `normaliser_nom(current_club_name)`
- `current_competition` — mapping Transfermarkt → code standard
- `current_competition_name` — libellé lisible
- `age` — calculé en années (arrondi à 1 décimale)
- `matchs_joues_total`, `total_goals`, `total_assists`, `total_minutes` (depuis `player_performance`)
- `goals_per_90`, `assists_per_90`, `goal_contributions_per_90`, `value_efficiency` (moyennes)
- `predicted_value`, `actual_value`, `difference_pct`, `evaluation` (depuis prédictions ML)

---

## 4. DAGs AIRFLOW (`dags/`)

### 4.1 `dag_pipeline_clubs.py` — `pipeline_clubs`

**`schedule_interval`** : `"0 6 * * 1"` (lundi 6h00)  
**`catchup`** : `False`  
**Tags** : `["clubs", "etl", "football"]`

**`default_args` :**
```python
{
    "owner":               "football_analytics",
    "depends_on_past":     False,
    "start_date":          datetime(2024, 1, 1),
    "email_on_failure":    True,
    "email_on_retry":      False,
    "email":               [os.getenv("AIRFLOW_ALERT_EMAIL", "hamidbelhadjkacem@gmail.com")],
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}
```

**Tâches dans l'ordre et dépendances :**

| Ordre | `task_id` | Script exécuté |
|---|---|---|
| 1 | `refresh_football_datasets_source` | `src/ingestion/refresh_football_datasets_source.py` |
| 2 | `collect_api_matches` | `src/ingestion/collect_api_matches.py` |
| 3 | `transform_matches` | `src/transformation/transform_matches.py` |
| 4 | `build_unified_matches` | `src/transformation/build_unified_matches.py` |
| 5 | `build_classements` | `src/transformation/build_classements_unified.py` |
| 6 | `build_clubs_unified` | `src/transformation/build_clubs_unified.py` |

**Graphe de dépendances :**
```
t0_refresh_fd >> t1_collect >> t2_transform >> t3_unified >> t4_classements >> t5_clubs
```

**`on_failure_callback`** : envoie un email HTML via `send_email()` incluant `dag_id`, `task_id`, `execution_date`, `exception`, `log_url`. Destinataire : `AIRFLOW_ALERT_EMAIL`.

**`run_script()` (implémentation dans ce DAG) :**
- Copie `os.environ` et force `PYTHONPATH=/opt/airflow`
- Appelle `subprocess.run(["python", script_path], capture_output=True, text=True, cwd="/opt/airflow", env=env)`
- Lève `Exception` si `returncode != 0` avec `stderr`

---

### 4.2 `dag_pipeline_players.py` — `pipeline_players`

**`schedule_interval`** : `"0 7 * * 1"` (lundi 7h00)  
**`catchup`** : `False`  
**Tags** : `["players", "etl", "football"]`

**`default_args` :** Idem `pipeline_clubs` mais `"retries": 0`

**Tâches dans l'ordre et dépendances :**

| Ordre | `task_id` | Type | Détail |
|---|---|---|---|
| 1 | `refresh_transfermarkt_source` | `PythonOperator` | `src/ingestion/refresh_transfermarkt_source.py` |
| 2 | `transform_players` | `PythonOperator` | `src/transformation/transform_players.py` |
| 3 | `wait_for_pipeline_clubs` | `ExternalTaskSensor` | Attend `pipeline_clubs.build_clubs_unified` |
| 4 | `build_appearances_unified` | `PythonOperator` | `src/transformation/build_appearances_unified.py` |
| 5 | `build_players_enriched` | `PythonOperator` | `src/transformation/build_players_enriched.py` |

**Graphe de dépendances :**
```
t0_refresh >> t1_transform >> t1b_wait_clubs >> t2_appearances >> t3_enriched
```

**`ExternalTaskSensor` — `wait_for_pipeline_clubs` :**
```python
ExternalTaskSensor(
    task_id="wait_for_pipeline_clubs",
    external_dag_id="pipeline_clubs",
    external_task_id="build_clubs_unified",
    execution_delta=timedelta(hours=1),  # pipeline_clubs démarre 1h avant
    timeout=3600,                         # 1h max d'attente
    poke_interval=60,                     # vérifie toutes les 60s
    mode="reschedule",                    # libère le slot Worker entre les sondages
)
```

**`run_script()` (implémentation dans ce DAG) :**
- Utilise `subprocess.Popen` avec `python -u` pour le streaming des logs en temps réel
- Lit `stdout` ligne par ligne avec `sys.stdout.flush()`
- Lève `Exception` si `returncode != 0`

---

### 4.3 `dag_pipeline_ml.py` — `pipeline_ml`

**`schedule_interval`** : `"0 8 * * 1"` (lundi 8h00)  
**`catchup`** : `False`  
**Tags** : `["ml", "projection", "carriere", "football"]`

**`default_args` :** Idem `pipeline_clubs` (`"retries": 1`)

**Tâches dans l'ordre et dépendances :**

| Ordre | `task_id` | Type | Détail |
|---|---|---|---|
| 1 | `wait_for_pipeline_players` | `ExternalTaskSensor` | Attend `pipeline_players.build_players_enriched` |
| 2 | `train_model_projection` | `PythonOperator` | `src/ml/train_model_projection.py` |

**Graphe de dépendances :**
```
t0_wait_players >> t1_projection
```

**`ExternalTaskSensor` — `wait_for_pipeline_players` :**
```python
ExternalTaskSensor(
    task_id="wait_for_pipeline_players",
    external_dag_id="pipeline_players",
    external_task_id="build_players_enriched",
    execution_delta=timedelta(hours=1),  # pipeline_players démarre 1h avant
    timeout=3600,                         # 1h max d'attente
    poke_interval=60,                     # vérifie toutes les 60s
    mode="reschedule",
)
```

---

## 5. BASE DE DONNÉES POSTGRESQL

### 5.1 Schémas

| Schéma | Rôle |
|---|---|
| `staging` | Tables intermédiaires brutes — résultats directs de l'ingestion |
| `marts_clubs` | Tables analytiques clubs et matchs |
| `marts_players` | Tables analytiques joueurs |
| `marts_ml` | Tables ML : features et prédictions |

### 5.2 Tables par schéma

#### Schéma `staging`

| Table | Colonnes clés | Provenance |
|---|---|---|
| `stg_matches_api` | `game_id`, `competition_id`, `competition_name`, `season` (INT), `date` (DATE), `round` (INT), `home_club_id`, `home_club_name`, `away_club_id`, `away_club_name`, `home_club_goals` (INT), `away_club_goals` (INT), `status`, `result`, `total_goals`, `home_points`, `away_points` | `transform_matches.py` |
| `stg_classements` | `competition_id`, `competition_name`, `season` (INT), `club_id`, `club_name`, `matchs_joues`, `points`, `buts_marques`, `buts_encaisses`, `victoires`, `nuls`, `defaites`, `diff_buts`, `moy_buts_par_match`, `moy_buts_encaisses_par_match` | `transform_matches.py` |
| `stg_players` | `player_id`, `first_name`, `last_name`, `name`, `date_of_birth` (DATE), `position`, `sub_position`, `foot`, `height_in_cm` (INT), `country_of_birth`, `country_of_citizenship`, `current_club_id`, `current_club_name`, `current_club_domestic_competition_id`, `last_season` (INT), `market_value_in_eur` (DOUBLE), `highest_market_value_in_eur` (DOUBLE), `international_caps` (INT), `international_goals` (INT) | `transform_players.py` |
| `stg_clubs` | `club_id`, `name`, `domestic_competition_id`, `squad_size`, `average_age` (FLOAT), `foreigners_number`, `foreigners_percentage` (FLOAT), `national_team_players`, `stadium_name`, `stadium_seats` (INT), `coach_name`, `last_season` (INT) | `transform_players.py` |
| `stg_valuations` | `player_id`, `date` (DATE), `market_value_in_eur` (DOUBLE), `current_club_name`, `current_club_id`, `player_club_domestic_competition_id` | `transform_players.py` |
| `stg_transfers` | `player_id`, `player_name`, `transfer_date` (DATE), `transfer_season`, `from_club_id`, `from_club_name`, `to_club_id`, `to_club_name`, `transfer_fee` (DOUBLE), `market_value_in_eur` (DOUBLE) | `transform_players.py` |
| `stg_appearances` | `appearance_id`, `game_id`, `player_id`, `player_club_id`, `date` (DATE), `competition_id`, `goals` (INT), `assists` (INT), `yellow_cards` (INT), `red_cards` (INT), `minutes_played` (INT), `player_name`, `position`, `market_value_in_eur` | `transform_players.py` |

#### Schéma `marts_clubs`

| Table | Colonnes | Contraintes | Provenance |
|---|---|---|---|
| `unified_matches` | `match_id` (VARCHAR 16), `source`, `competition`, `date`, `season`, `round`, `home_team`, `away_team`, `home_goals` (INT), `away_goals` (INT), `result`, `total_goals`, `home_shots`, `away_shots`, `home_shots_on_target`, `away_shots_on_target`, `home_corners`, `away_corners`, `home_yellow_cards`, `away_yellow_cards`, `home_red_cards`, `away_red_cards` | PK `match_id`; CHECK `home_goals [0,20]`, `away_goals [0,20]`; INDEX `(competition, season)`, `(date)`, `(home_team)` | `build_unified_matches.py` |
| `classements_equipes_unified` | `competition`, `competition_name`, `season`, `club_name`, `matchs_joues`, `points`, `buts_marques`, `buts_encaisses`, `victoires`, `nuls`, `defaites`, `diff_buts`, `moy_buts_par_match`, `moy_buts_encaisses_par_match` | INDEX `(competition, season)` | `build_classements_unified.py` |
| `avantage_domicile` | `competition`, `competition_name`, `season`, `total_matchs`, `victoires_domicile`, `victoires_exterieur`, `nuls`, `pct_victoires_domicile`, `moy_buts_par_match`, `moy_buts_domicile`, `moy_buts_exterieur` | — | `build_classements_unified.py` |
| `matches_enrichis` | Toutes les colonnes de `unified_matches` + `home_points`, `away_points` | — | `build_classements_unified.py` |
| `clubs_unified` | `club_id`, `name`, `domestic_competition_id`, `squad_size`, `average_age`, `foreigners_number`, `foreigners_percentage`, `national_team_players`, `stadium_name`, `stadium_seats`, `coach_name`, `last_season`, `name_normalized`, `competition`, `competition_name` | — | `build_clubs_unified.py` |

#### Schéma `marts_players`

| Table | Colonnes | Contraintes | Provenance |
|---|---|---|---|
| `players` | `player_id`, `first_name`, `last_name`, `name`, `date_of_birth`, `position`, `sub_position`, `foot`, `height_in_cm`, `country_of_birth`, `country_of_citizenship`, `current_club_id`, `current_club_name`, `current_club_domestic_competition_id`, `last_season`, `market_value_in_eur`, `highest_market_value_in_eur`, `international_caps`, `international_goals` | PK `player_id`; CHECK `market_value_in_eur >= 0` | `transform_players.py` |
| `clubs` | `club_id`, `name`, `domestic_competition_id`, `squad_size`, `average_age`, `foreigners_number`, `foreigners_percentage`, `national_team_players`, `stadium_name`, `stadium_seats`, `coach_name`, `last_season` | PK `club_id` | `transform_players.py` |
| `appearances` | `appearance_id`, `game_id`, `player_id`, `player_club_id`, `date`, `competition_id`, `goals`, `assists`, `yellow_cards`, `red_cards`, `minutes_played`, `player_name`, `position`, `market_value_in_eur` | PK `appearance_id`; FK `player_id → players(player_id) ON DELETE CASCADE`; CHECK `minutes_played [0,120]`, `goals >= 0`, `assists >= 0`; INDEX `(player_id)`, `(date)`, `(competition_id)` | `transform_players.py` |
| `player_valuations` | `player_id`, `date`, `market_value_in_eur`, `current_club_name`, `current_club_id`, `player_club_domestic_competition_id` | PK `(player_id, date)`; FK `player_id → players(player_id) ON DELETE CASCADE`; CHECK `market_value_in_eur >= 0`; INDEX `(player_id, date)` | `transform_players.py` |
| `transfers` | `player_id`, `player_name`, `transfer_date`, `transfer_season`, `from_club_id`, `from_club_name`, `to_club_id`, `to_club_name`, `transfer_fee`, `market_value_in_eur` | FK `player_id → players(player_id) ON DELETE CASCADE` | `transform_players.py` |
| `appearances_unified` | `appearance_id`, `game_id`, `player_id`, `player_club_id`, `date`, `competition_id`, `goals`, `assists`, `minutes_played`, `yellow_cards`, `red_cards`, `player_name`, `match_id`, `competition`, `home_norm`, `away_norm` | — | `build_appearances_unified.py` |
| `player_performance` | `player_id`, `matchs_joues`, `total_goals`, `total_assists`, `total_minutes`, `goals_per_90`, `assists_per_90`, `goal_contributions_per_90`, `value_efficiency`, `avg_minutes_per_match` | INDEX `(player_id)` | (inféré depuis `build_players_enriched.py`) |
| `players_enriched` | Toutes les colonnes de `players` + `current_club_normalized`, `current_competition`, `current_competition_name`, `age`, `matchs_joues_total`, `total_goals`, `total_assists`, `total_minutes`, `goals_per_90`, `assists_per_90`, `goal_contributions_per_90`, `value_efficiency`, `predicted_value`, `actual_value`, `difference_pct`, `evaluation` | — | `build_players_enriched.py` |

#### Schéma `marts_ml`

| Table | Colonnes | Contraintes | Provenance |
|---|---|---|---|
| `features_market_value` | `player_id`, `name`, `position`, `target_market_value`, `international_caps`, `international_goals`, `highest_market_value_in_eur`, `matchs_joues`, `total_goals`, `total_assists`, `total_minutes`, `goals_per_90`, `assists_per_90`, `goal_contributions_per_90`, `avg_minutes_per_match` + colonnes one-hot `pos_*`, `foot_*`, `nat_*` | — | `build_features.py` |
| `features_temporal` | `player_id`, `date_t0`, `date_t1`, `duree_jours`, `age_a_t1`, `market_value_t0`, `market_value_t1`, `delta_value`, `delta_pct`, `matchs_periode`, `buts_periode`, `assists_periode`, `minutes_periode`, `cartons_j_periode`, `cartons_r_periode`, `goals_per_90`, `assists_per_90`, `position`, `foot`, `country`, `international_caps`, `international_goals` + colonnes one-hot `pos_*`, `foot_*`, `nat_*` | PK `(player_id, date_t0, date_t1)`; FK `player_id → players(player_id) ON DELETE CASCADE` | `build_features_temporal.py` |
| `predictions_market_value_temporal` | `player_id`, `predicted_value`, `actual_value`, `difference_pct`, `evaluation`, `date_t1` | FK `player_id → players(player_id) ON DELETE CASCADE` | `train_model_temporal.py` |
| `predictions_projection_carriere` | `player_id`, `name`, `position`, `age_actuel`, `current_club_name`, `valeur_actuelle`, `valeur_projetee`, `goals_per_90_jeune`, `assists_per_90_jeune`, `matchs_jeune` | — | `train_model_projection.py` |

### 5.3 Rôles PostgreSQL

| Rôle | Login | Permissions |
|---|---|---|
| `football_reader` | Oui | `CONNECT` sur `football_db`; `USAGE` sur `marts_clubs`, `marts_players`, `marts_ml`; `SELECT` sur toutes les tables de ces schémas; `ALTER DEFAULT PRIVILEGES` pour les futures tables. **Pas d'accès à `staging`** (explicitement révoqué). Usage : Metabase |
| `football_writer` | Oui | `CONNECT` sur `football_db`; `USAGE + CREATE` sur `staging`, `marts_clubs`, `marts_players`, `marts_ml`; `SELECT + INSERT + UPDATE + DELETE + TRUNCATE` sur toutes les tables; `ALTER DEFAULT PRIVILEGES` pour les futures tables. Usage : pipelines ETL |
| `football_user` (admin) | — | Superuser du projet (mentionné dans les logs de vérification) |

**Isolation sécurisée :** `staging` est inaccessible à `football_reader` → Metabase ne peut pas lire les données brutes intermédiaires.

### 5.4 Index de performance

| Index | Table | Colonnes |
|---|---|---|
| `idx_appearances_player_id` | `marts_players.appearances` | `player_id` |
| `idx_appearances_date` | `marts_players.appearances` | `date` |
| `idx_appearances_competition` | `marts_players.appearances` | `competition_id` |
| `idx_valuations_player_date` | `marts_players.player_valuations` | `(player_id, date)` |
| `idx_matches_competition_season` | `marts_clubs.unified_matches` | `(competition, season)` |
| `idx_matches_date` | `marts_clubs.unified_matches` | `date` |
| `idx_matches_home_team` | `marts_clubs.unified_matches` | `home_team` |
| `idx_classements_competition_season` | `marts_clubs.classements_equipes_unified` | `(competition, season)` |
| `idx_player_perf_player` | `marts_players.player_performance` | `player_id` |

---

## 6. MACHINE LEARNING (`src/ml/`)

### 6.1 `train_model_projection.py` — Modèle de production

**Objectif :** Prédire la valeur marchande future d'un joueur ayant joué entre 16 et 21 ans.

**Dataset SQL — `charger_dataset()` :**
- Source principale : `marts_players.appearances_unified` jointure `marts_players.players`
- Filtre âge : `DATE_PART('year', AGE(a.date, p.date_of_birth)) BETWEEN 16 AND 21`
- Filtre activité : `SUM(a.minutes_played) >= 500`
- Variable cible (`valeur_cible`) : `COALESCE(dernière_valeur_transfermarkt, market_value_in_eur)`
- Niveau du club formateur (`valeur_moyenne_club`) : `AVG(market_value_in_eur)` par `current_club_id`
- Filtre final : `valeur_cible IS NOT NULL AND age_actuel BETWEEN 22 AND 35`
- **Taille du dataset : 3 289 joueurs**

**Features utilisées (14 au total) :**

| Feature | Description |
|---|---|
| `matchs_jeune` | Nombre de matchs joués entre 16 et 21 ans |
| `buts_jeune` | Buts totaux entre 16 et 21 ans |
| `passes_jeune` | Passes décisives totales entre 16 et 21 ans |
| `minutes_jeune` | Minutes jouées entre 16 et 21 ans |
| `goals_per_90_jeune` | Buts par 90 minutes (16–21 ans) |
| `assists_per_90_jeune` | Passes déc. par 90 minutes (16–21 ans) |
| `age_premier_match` | Âge au premier match professionnel |
| `nb_competitions_jeune` | Nombre de compétitions différentes jouées (16–21 ans) |
| `height_in_cm` | Taille en cm |
| `international_caps` | Sélections en équipe nationale |
| `international_goals` | Buts en équipe nationale |
| `valeur_moyenne_club` | Valeur marchande moyenne des joueurs du club formateur (indicateur de niveau) |
| `position_enc` | Poste encodé via `LabelEncoder` (Attack / Midfield / Defender / Goalkeeper) |
| `foot_enc` | Pied préférentiel encodé via `LabelEncoder` (right / left / both) |

**Transformation de la variable cible :**
- `y = np.log1p(valeur_cible)` — log-transformation pour normaliser la distribution asymétrique
- Reconstruction : `np.expm1(y_pred)` pour revenir en euros

**Modèle retenu : `GradientBoostingRegressor`**
```python
GradientBoostingRegressor(
    n_estimators=200,
    max_depth=4,
    learning_rate=0.05,
    random_state=42
)
```

**Split train/test :** `test_size=0.2, random_state=42`

**Métriques de performance (modèle global) :**
- **R² = 0,61**
- **MAE = 4 651 790 €**
- **R² CV 5-fold = 0,6589 ± 0,0105** (pas de surapprentissage)

**Métriques par poste (modèles spécialisés) :**

| Poste | R² | N joueurs |
|---|---|---|
| Attaquants | 0,68 | 944 |
| Défenseurs | 0,65 | 1 135 |
| Milieux | 0,64 | 1 046 |
| Gardiens | 0,53 | 164 |

**Importance des features (sans `valeur_moyenne_club`) :**
- Matchs joués (16–21 ans) : **31,4 %**
- Sélections nationales : **16,2 %**
- Buts/90 min (16–21 ans) : **~12 %**

**Avec `valeur_moyenne_club` :** variable dominante à **~85 %** — le niveau du club formateur est de loin le meilleur prédicteur.

**Sorties :**
- `models/model_projection_carriere.pkl` : dict contenant `model_global`, `modeles_poste`, `features`, `le_position`, `le_foot`, `r2`, `mae`
- `models/projection_feature_importance_globale.png` — graphique importance globale
- `models/projection_feature_importance_performance.png` — graphique sans `valeur_moyenne_club`
- Table `marts_ml.predictions_projection_carriere` : **1 147 joueurs U22**

---

### 6.2 `train_model.py` — Modèle valeur marchande statique (non retenu en production)

**Objectif :** Prédire la valeur marchande courante (pas de projection future).  
**Dataset :** `marts_ml.features_market_value` — joueurs avec `market_value_in_eur > 0`

**Features (liste fixe, 39 colonnes au total) :**
- Performance : `matchs_joues`, `total_goals`, `total_assists`, `total_minutes`, `goals_per_90`, `assists_per_90`, `goal_contributions_per_90`, `avg_minutes_per_match`
- Profil : `international_caps`, `international_goals`, `highest_market_value_in_eur`
- One-hot position : `pos_Attack`, `pos_Defender`, `pos_Goalkeeper`, `pos_Midfield`
- One-hot pied : `foot_both`, `foot_left`, `foot_right`
- One-hot nationalité (21 modalités) : `nat_Argentina`, `nat_Belgium`, `nat_Brazil`, `nat_Colombia`, `nat_Denmark`, `nat_England`, `nat_France`, `nat_Germany`, `nat_Greece`, `nat_Italy`, `nat_Japan`, `nat_Netherlands`, `nat_Portugal`, `nat_Russia`, `nat_Scotland`, `nat_Serbia`, `nat_Spain`, `nat_Sweden`, `nat_Turkey`, `nat_Ukraine`, `nat_other`

**Modèles entraînés :**
1. **Régression Linéaire** avec `StandardScaler` — R² ≈ 0,58
2. **Random Forest** : `n_estimators=200, max_depth=15, min_samples_leaf=5, random_state=42, n_jobs=-1`

**Sélection :** meilleur sur MAE → `best_model.pkl`

---

### 6.3 `build_features.py`

**Entrée :** `marts_players.players` + `marts_players.player_performance` (PostgreSQL)  
**Sortie :** `marts_ml.features_market_value`

**Encodage :**
- Position : one-hot `pd.get_dummies` → `pos_*`
- Pied : one-hot → `foot_*`
- Nationalité : top 20 pays + `"other"` → one-hot `nat_*`
- `"Türkiye"` normalisé en `"Turkey"`
- `"Missing"` en position → supprimé (dropna)

---

### 6.4 `build_features_temporal.py`

**Entrée :**
- `marts_players.player_valuations` — filtre `date >= 2017-01-01` et `market_value_in_eur > 0`
- `marts_players.players` — filtre `last_season >= 2022` et `position != 'Missing'`
- `marts_players.appearances` — filtre `date >= 2017-01-01` et `minutes_played > 0`

**Sortie :** `marts_ml.features_temporal`

**Mécanisme :** Pour chaque joueur, pour chaque paire de valuations consécutives `(t0, t1)`, calcule les performances de la période `[t0, t1[` et le profil du joueur. Produit une ligne par paire.

**Colonnes produites :** `player_id`, `date_t0`, `date_t1`, `duree_jours`, `age_a_t1`, `market_value_t0`, `market_value_t1`, `delta_value`, `delta_pct`, `matchs_periode`, `buts_periode`, `assists_periode`, `minutes_periode`, `cartons_j_periode`, `cartons_r_periode`, `goals_per_90`, `assists_per_90`, `position`, `foot`, `country`, `international_caps`, `international_goals`

---

### 6.5 `train_model_temporal.py`

**Modèles entraînés (3) :**
1. **Linear Regression** avec `StandardScaler`
2. **Random Forest** : `n_estimators=200, max_depth=15, min_samples_leaf=5, random_state=42, n_jobs=-1`
3. **Gradient Boosting** : `n_estimators=200, max_depth=5, learning_rate=0.05, random_state=42`

**Variable cible :** `market_value_t1` — log-transformée `np.log1p`

**Features fixes (13) :** `market_value_t0`, `age_a_t1`, `duree_jours`, `matchs_periode`, `buts_periode`, `assists_periode`, `minutes_periode`, `cartons_j_periode`, `cartons_r_periode`, `goals_per_90`, `assists_per_90`, `international_caps`, `international_goals` + colonnes one-hot dynamiques `pos_*`, `foot_*`, `nat_*`

**Sorties :** `models/best_model_temporal.pkl` + graphiques `feature_importance_globale.png` et `feature_importance_sans_t0.png`

---

## 7. API REST (`api/`)

### 7.1 Application

**Framework :** FastAPI + Uvicorn  
**Port Docker :** 8000  
**Version :** 1.0.0  
**Documentation Swagger :** `GET /docs` (auto-générée par FastAPI)

**Chargement au démarrage :**
- Lit `models/model_projection_carriere.pkl` → extrait `model_global`, `modeles_poste`, `features`, `le_position`, `le_foot`
- Lève `FileNotFoundError` si le fichier est absent
- Crée une connexion SQLAlchemy vers PostgreSQL (variables d'env : `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`)

---

### 7.2 Schéma d'entrée — `PlayerInput` (Pydantic)

| Champ | Type | Contraintes | Description |
|---|---|---|---|
| `name` | `str` | requis | Nom du joueur |
| `position` | `str` | requis | `Attack` / `Midfield` / `Defender` / `Goalkeeper` |
| `foot` | `str` | défaut `"right"` | `right` / `left` / `both` |
| `age_actuel` | `int` | requis, `ge=14, le=25` | Âge actuel |
| `matchs_jeune` | `int` | requis, `ge=1` | Matchs joués (16–21 ans) |
| `buts_jeune` | `int` | requis, `ge=0` | Buts totaux (16–21 ans) |
| `passes_jeune` | `int` | requis, `ge=0` | Passes déc. totales (16–21 ans) |
| `minutes_jeune` | `int` | requis, `ge=100` | Minutes jouées (16–21 ans) |
| `nb_competitions_jeune` | `int` | requis, `ge=1, le=10` | Nb compétitions différentes |
| `height_in_cm` | `int` | requis, `ge=150, le=210` | Taille en cm |
| `age_premier_match` | `int` | requis, `ge=13, le=21` | Âge au 1er match pro |
| `international_caps` | `int` | défaut `0`, `ge=0` | Sélections nationales |
| `international_goals` | `int` | défaut `0`, `ge=0` | Buts internationaux |
| `valeur_actuelle_eur` | `float` | défaut `0.0`, `ge=0` | Valeur actuelle en euros |
| `valeur_moyenne_club` | `float` | requis, `ge=0` | Valeur marchande moyenne du club formateur |

### 7.3 Schéma de sortie — `PredictionOutput` (Pydantic)

| Champ | Type | Description |
|---|---|---|
| `name` | `str` | Nom du joueur |
| `position` | `str` | Poste |
| `age_actuel` | `int` | Âge actuel |
| `valeur_actuelle_eur` | `float` | Valeur actuelle saisie |
| `valeur_projetee_eur` | `float` | Valeur marchande projetée (€) |
| `progression_pct` | `float` | Progression estimée en % |
| `age_pic_min` | `int` | Âge minimum de pic de carrière estimé |
| `age_pic_max` | `int` | Âge maximum de pic de carrière estimé |
| `modele_utilise` | `str` | `"Modèle spécialisé {position}"` ou `"Modèle global"` |
| `interpretation` | `str` | Texte d'interprétation qualitatif |

### 7.4 Endpoints

#### `GET /`
- **Tag :** Santé
- **Description :** Vérifie que l'API est opérationnelle
- **Paramètres :** Aucun
- **Réponse :**
```json
{
    "status": "ok",
    "service": "Football Analytics — API de projection de carrière",
    "version": "1.0.0",
    "modele": "Gradient Boosting · R²=0,61 · 14 features · 4 modèles par poste"
}
```

#### `POST /predict`
- **Tag :** Prédiction
- **Description :** Prédit la valeur marchande future d'un jeune joueur
- **Corps :** JSON conforme à `PlayerInput`
- **Logique interne :**
  1. Calcule `goals_per_90 = buts_jeune * 90 / max(minutes_jeune, 1)`
  2. Calcule `assists_per_90 = passes_jeune * 90 / max(minutes_jeune, 1)`
  3. Encode `position` et `foot` via les `LabelEncoder` du modèle (fallback sur `"Missing"` / `"Unknown"` si inconnu)
  4. Construit le vecteur de 14 features
  5. Si `position` est dans `modeles_poste` → utilise le modèle spécialisé, sinon → modèle global
  6. Prédit en espace log → `np.expm1()` pour revenir en euros
  7. Calcule `progression_pct` si `valeur_actuelle_eur > 0`
  8. Calcule `age_pic_min = max(age_actuel + (21 - age_actuel) + 2, 22)`, `age_pic_max = age_pic_min + 2`
- **Réponse :** JSON conforme à `PredictionOutput`

**Interprétation qualitative (`interpret()`) :**
- `≥ 50 M€` → "Talent exceptionnel — profil de joueur international de haut niveau"
- `[20, 50[ M€` → "Très grand potentiel — profil de joueur de top club européen"
- `[5, 20[ M€` → "Bon potentiel — profil de joueur professionnel de championnat majeur"
- `[1, 5[ M€` → "Potentiel correct — profil de joueur professionnel L2 ou équivalent"
- `< 1 M€` → "Potentiel limité selon les données actuelles"

#### `GET /predictions`
- **Tag :** Données
- **Description :** Retourne les prédictions U22 stockées en base
- **Paramètres query :** `limit: int = 20`, `offset: int = 0`
- **Requête SQL :**
```sql
SELECT player_id, name, position, age_actuel, current_club_name,
       valeur_actuelle, valeur_projetee, goals_per_90_jeune,
       assists_per_90_jeune, matchs_jeune
FROM marts_ml.predictions_projection_carriere
ORDER BY valeur_projetee DESC
LIMIT :limit OFFSET :offset
```
- **Réponse :** `{"total_returned": N, "limit": N, "offset": N, "predictions": [...]}`
- **Erreurs :** `HTTP 500` avec `detail: str(exception)` en cas d'erreur PostgreSQL

#### `GET /predictions/{player_id}`
- **Tag :** Données
- **Description :** Retourne la prédiction d'un joueur spécifique
- **Paramètre path :** `player_id: int`
- **Requête SQL :** Idem `/predictions` avec `WHERE player_id = :player_id`
- **Erreurs :** `HTTP 404` si `player_id` non trouvé; `HTTP 500` en cas d'erreur PostgreSQL

---

## 8. INFRASTRUCTURE

### 8.1 `docker-compose.yml` — Version 3.8

#### Anchor commun `x-airflow-common`

Image : `apache/airflow:2.8.1`

**Variables d'environnement Airflow :**

| Variable | Valeur |
|---|---|
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://airflow:airflow@airflow_postgres/airflow?keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=5` |
| `AIRFLOW__CORE__FERNET_KEY` | `''` |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `'false'` |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `'false'` |
| `AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL` | `'30'` |
| `AIRFLOW__SCHEDULER__JOB_HEARTBEAT_SEC` | `'60'` |
| `AIRFLOW__SCHEDULER__SCHEDULER_ZOMBIE_TASK_THRESHOLD` | `'3000'` |
| `AIRFLOW__SCHEDULER__PARSER_SET_CREATION_DATE` | `'true'` |
| `AIRFLOW__API__AUTH_BACKENDS` | `airflow.api.auth.backend.basic_auth` |
| `_PIP_ADDITIONAL_REQUIREMENTS` | `matplotlib duckdb==0.10.3 pandas requests python-dotenv scikit-learn sqlalchemy psycopg2-binary minio kaggle` |
| `FOOTBALL_DATA_API_TOKEN` | `${FOOTBALL_DATA_API_TOKEN}` |
| `KAGGLE_USERNAME` | `${KAGGLE_USERNAME}` |
| `KAGGLE_KEY` | `${KAGGLE_KEY}` |
| `POSTGRES_USER` | `${POSTGRES_USER}` |
| `POSTGRES_PASSWORD` | `${POSTGRES_PASSWORD}` |
| `POSTGRES_DB` | `${POSTGRES_DB}` |
| `POSTGRES_HOST` | `postgres` (nom du service Docker interne) |
| `POSTGRES_PORT` | `"5432"` (port interne) |
| `MINIO_ACCESS_KEY` | `${MINIO_ACCESS_KEY}` |
| `MINIO_SECRET_KEY` | `${MINIO_SECRET_KEY}` |
| `MINIO_ENDPOINT` | `minio:9000` |
| `AIRFLOW_ALERT_EMAIL` | `${AIRFLOW_ALERT_EMAIL}` |
| `AIRFLOW__SMTP__SMTP_HOST` | `smtp.gmail.com` |
| `AIRFLOW__SMTP__SMTP_PORT` | `"587"` |
| `AIRFLOW__SMTP__SMTP_STARTTLS` | `"True"` |
| `AIRFLOW__SMTP__SMTP_SSL` | `"False"` |
| `AIRFLOW__SMTP__SMTP_USER` | `${AIRFLOW__SMTP__SMTP_USER}` |
| `AIRFLOW__SMTP__SMTP_PASSWORD` | `${AIRFLOW__SMTP__SMTP_PASSWORD}` |
| `AIRFLOW__SMTP__SMTP_MAIL_FROM` | `${AIRFLOW__SMTP__SMTP_MAIL_FROM}` |

**Volumes montés (communs à tous les services Airflow) :**
- `./dags:/opt/airflow/dags`
- `./logs/airflow:/opt/airflow/logs`
- `./src:/opt/airflow/src`
- `./.env:/opt/airflow/.env`
- `./data:/opt/airflow/data`

**`depends_on` :**
- `airflow_postgres` : `condition: service_healthy`
- `postgres` : `condition: service_started`

---

#### Services

| Service | Conteneur | Image | Ports hôte:container | Volumes | Variables notables |
|---|---|---|---|---|---|
| `postgres` | `football_postgres` | `postgres:15` | `5433:5432` | `postgres_data:/var/lib/postgresql/data` | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` depuis `.env` |
| `minio` | `football_minio` | `minio/minio` | `9002:9000`, `9003:9001` | `minio_data:/data` | `MINIO_ROOT_USER=${MINIO_ACCESS_KEY}`, `MINIO_ROOT_PASSWORD=${MINIO_SECRET_KEY}`, commande : `server /data --console-address ":9001"` |
| `metabase` | `football_metabase` | `metabase/metabase` | `3000:3000` | — | `MB_DB_TYPE=postgres`, `MB_DB_DBNAME=${POSTGRES_DB}`, `MB_DB_PORT=5432`, `MB_DB_USER=${POSTGRES_USER}`, `MB_DB_PASS=${POSTGRES_PASSWORD}`, `MB_DB_HOST=postgres` |
| `airflow_postgres` | `airflow_postgres` | `postgres:15` | Aucun (non exposé) | `airflow_postgres_data:/var/lib/postgresql/data` | `POSTGRES_USER=airflow`, `POSTGRES_PASSWORD=airflow`, `POSTGRES_DB=airflow`; healthcheck : `pg_isready -U airflow` (interval 10s, retries 5) |
| `airflow_webserver` | `airflow_webserver` | `apache/airflow:2.8.1` | `8080:8080` | (communs) | `command: webserver`; healthcheck : `curl --fail http://localhost:8080/health` (interval 30s, timeout 10s, retries 5) |
| `airflow_scheduler` | `airflow_scheduler` | `apache/airflow:2.8.1` | Aucun | (communs) | `command: scheduler` |
| `airflow_init` | `airflow_init` | `apache/airflow:2.8.1` | Aucun | (communs) | `command: bash -c "airflow db migrate && airflow users create --username admin --firstname Admin --lastname Football --role Admin --email admin@football.com --password admin123"`; `restart: on-failure` |
| `api` | `football_api` | build `./api` | `8000:8000` | `./models:/app/models` | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST=postgres`, `POSTGRES_PORT=5432` |

**Volumes Docker nommés :** `postgres_data`, `minio_data`, `airflow_postgres_data`

**Note de sécurité :** `airflow_postgres` n'est pas exposé sur l'hôte → inaccessible depuis l'extérieur du réseau Docker.

---

### 8.2 `.github/workflows/ci.yml`

**Déclencheurs :** `push` et `pull_request` sur les branches `main` et `develop`

#### Job 1 — `code-quality` — Vérification qualité code

| Étape | Action |
|---|---|
| Checkout | `actions/checkout@v4` |
| Setup Python | `actions/setup-python@v5` — Python 3.12 |
| Installation | `flake8 pandas numpy scikit-learn sqlalchemy python-dotenv` |
| Flake8 | `flake8 src/ dags/ --max-line-length=120 --exclude=__pycache__` — `continue-on-error: true` |
| Imports critiques | Vérifie `import pandas`, `import numpy`, `import sklearn`, `import sqlalchemy` |

#### Job 2 — `security-check` — Vérification sécurité

| Étape | Vérification | Comportement si échec |
|---|---|---|
| `.env` non versionné | `if [ -f ".env" ]` | `exit 1` |
| Secrets en dur | `grep -r "POSTGRES_PASSWORD\s*=\s*['\"][^'\"]" src/ dags/` | `exit 1` |
| `.env.example` présent | `if [ -f ".env.example" ]` | Avertissement uniquement |
| `.pkl` non versionné | `find . -name "*.pkl" -not -path "./.git/*"` | `exit 1` |

#### Job 3 — `tests` — Tests unitaires

`needs: code-quality` — s'exécute seulement si le job 1 réussit.

| Test | Ce qu'il vérifie |
|---|---|
| Déduplication MD5 | Deux appels avec les mêmes données normalisées produisent le même `match_id` ; des dates différentes produisent des IDs différents |
| `log1p` / `expm1` | Reconstruction exacte des valeurs `[500000, 5000000, 50000000, 200000000]` avec tolérance `< 1` |
| Features ML | `len(features) == 14`, présence de `valeur_moyenne_club` et `position_enc` |
| Division par zéro `goals_per_90` | `minutes=0` → `NaN` (pas de division par zéro) |

#### Job 4 — `summary` — Résumé CI

`needs: [code-quality, security-check, tests]` — `if: always()` — affiche un résumé textuel final.

---

## 9. DOCUMENTATION EXISTANTE

### 9.1 `README.md`

**Objectifs du projet :**
- Analyser les performances des équipes sur 5 ligues européennes (2016–2025)
- Scouting individuel (goals/90, assists/90)
- Projection de carrière des jeunes joueurs (16–21 ans) par ML

**Architecture décrite :**
```
Sources → Data Lake (MinIO S3) → ETL (Airflow) → Data Warehouse (PostgreSQL) → ML → API REST (FastAPI) → BI (Metabase) / Clients
```

**Prérequis :** Python 3.12.x, Docker Desktop 24.x, Git 2.x, Java 11+ (PySpark), `winutils.exe` sur Windows

**Avertissements réseau :** Rafraîchissement Kaggle bloqué derrière inspection SSL d'entreprise (proxy type Zscaler).

**Sources de données :**
- API football-data.org : 3 502 matchs (2023–2024)
- Transfermarkt/Kaggle : 1,86M apparitions (2012–2026)
- football-datasets/GitHub : 15 827 matchs (2017–2025)
- **Résultat après unification : 16 802 matchs uniques, 47 702 joueurs, 616 377 valorisations**

**Variables d'environnement documentées :** `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_PORT=5433`, `FOOTBALL_DATA_API_TOKEN`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `KAGGLE_USERNAME`, `KAGGLE_KEY`, `AIRFLOW_ALERT_EMAIL`, `AIRFLOW__SMTP__SMTP_USER`, `AIRFLOW__SMTP__SMTP_PASSWORD`, `AIRFLOW__SMTP__SMTP_MAIL_FROM`

**Mécanismes de rafraîchissement :**
- Validation structure + volume avant tout remplacement (seuil alerte 90%, seuil bloquant 30%)
- Sauvegarde snapshot avant écrasement
- Alerte email même si l'appel réseau réussit mais les données sont invalides
- Testé avec : authentification invalide, schéma corrompu, volume anormal, blocage réseau

**Résultats ML documentés :**

| Modèle | Dataset | R² | MAE |
|---|---|---|---|
| Régression linéaire | 3 289 joueurs | 0,58 | — |
| **Gradient Boosting (retenu)** | **3 289 joueurs** | **0,61** | **4 651 790 €** |

**Dashboards Metabase (localhost:3000) :**
- Clubs & Ligues : classements, avantage domicile, top offensives/défenses
- Scouting Joueurs : goals/90, assists/90, joueurs sous-évalués
- Projection de Carrière : top U22, pépites <5M€, valeur projetée vs actuelle

---

### 9.2 `EXPLOITATION.md`

**Architecture en production** (tableau des 7 services avec ports et rôles).

**Maintenance hebdomadaire automatique :**
- `pipeline_clubs` : lundi 6h00 (`0 6 * * 1`)
- `pipeline_players` : lundi 7h00 (`0 7 * * 1`)
- `pipeline_ml` : lundi 8h00 (`0 8 * * 1`)
- Alertes email automatiques (`on_failure_callback`)

**Maintenance mensuelle manuelle :**
- Vérification quota API (10 req/min plan gratuit)
- Purge snapshots `transfermarkt_backup/` si volume disque élevé
- `docker system df` — purge si > 80%
- Revue logs Airflow (`localhost:8080`)
- Vérification accès MinIO (`localhost:9003`)

**Maintenance trimestrielle :** mise à jour images Docker, dépendances Python, ré-entraînement ML, sauvegarde volumes.

**Maintenance annuelle :** renouvellement token API, mot de passe Google SMTP, revue politique sécurité.

**Indicateurs de supervision :**

| Indicateur | Seuil d'alerte |
|---|---|
| Taux de succès DAGs | < 100% → alerte email |
| Espace disque volumes | > 80% → action requise |
| Quota API | > 8 req/min → risque blocage |
| Prédictions U22 | < 1 000 → vérifier pipeline ML |
| Latence requêtes PostgreSQL | > 5s → vérifier index |

**Points de vigilance clés :**
- Ne jamais faire `docker compose down -v` en production (perte volumes)
- Port 5433 : conflit potentiel avec PostgreSQL natif Windows
- Proxy SSL d'entreprise bloque Kaggle au démarrage du container
- Fichier `.env` jamais versionné (vérifié par CI/CD)

**Procédure de reprise :** 6 étapes documentées (identification, logs, restart, restauration BDD, relance ML, commit git).

---

### 9.3 `RECETTES.md`

Cahier de 19 tests avec résultats obtenus. Voir section 10 (Tests) pour le détail exhaustif.

---

### 9.4 `INCIDENT.md`

**Incident :** Échec total des 3 DAGs Airflow — `ModuleNotFoundError`  
**Date :** Juin 2026  
**Sévérité :** Critique — pipeline de données complètement arrêté (~3h de résolution)

**Cause racine (triple) :**
1. `PYTHONPATH` non défini dans `docker-compose.yml` → `src` introuvable depuis les containers
2. Dépendances Python manquantes dans l'image `apache/airflow:2.8.1` (`sqlalchemy`, `pandas`, etc.)
3. Variables d'environnement non transmises au sous-processus `subprocess.run()` dans `run_script()`

**Symptôme :**
```
ModuleNotFoundError: No module named 'src'
ModuleNotFoundError: No module named 'dotenv'
ModuleNotFoundError: No module named 'sqlalchemy'
```

**4 corrections appliquées :**

| Correction | Fichier modifié | Action |
|---|---|---|
| 1 | `docker-compose.yml` | Ajout `PYTHONPATH: /opt/airflow` dans `x-airflow-common` |
| 2 | `docker-compose.yml` | Ajout `_PIP_ADDITIONAL_REQUIREMENTS` avec toutes les dépendances |
| 3 | `dags/dag_pipeline_*.py` | `env = os.environ.copy(); env["PYTHONPATH"] = "/opt/airflow"` passé à `subprocess.run()` |
| 4 | `dags/dag_pipeline_ml.py` | `ExternalTaskSensor` : retiré temporairement pour les tests, puis rétabli avec `execution_delta` correct et validé en conditions réelles |

**Résolution :** Validation complète des 3 DAGs avec sensors en conditions réelles.

---

## 10. TESTS

### 10.1 Tests fonctionnels (RECETTES.md)

**F01 — Démarrage de l'infrastructure**
- Procédure : `docker compose up -d` puis `docker compose ps`
- Critère : 7 conteneurs en état `running` : `football_postgres`, `football_minio`, `football_metabase`, `airflow_webserver`, `airflow_scheduler`, `airflow_postgres`, `football_api`
- Résultat : ✅

**F02 — Accès interfaces web**
- Airflow `localhost:8080` → page de login ✅
- Metabase `localhost:3000` → page d'accueil ✅
- MinIO Console `localhost:9003` → page de login ✅

**F03 — Exécution DAG `pipeline_clubs`**
- Procédure : Airflow UI → Trigger DAG
- Critère : 5 tâches succeeded dans l'ordre : `collect_api_matches` → `transform_matches` → `build_unified_matches` → `build_classements` → `build_clubs_unified`
- Résultat : ✅

**F04 — Exécution DAG `pipeline_players`**
- Critère : 3 tâches succeeded : `transform_players` → `build_appearances_unified` → `build_players_enriched`
- Résultat : ✅

**F05 — Exécution DAG `pipeline_ml`**
- Critère : 1 tâche succeeded : `train_model_projection` + table `marts_ml.predictions_projection_carriere` avec 1 147 lignes
- Résultat : ✅

**F06 — Volumes de données PostgreSQL**

| Table | Volume attendu | Résultat |
|---|---|---|
| `marts_clubs.unified_matches` | 16 802 lignes | ✅ |
| `marts_players.players` | 47 702 lignes | ✅ |
| `marts_players.appearances_unified` | 1 862 208 lignes | ✅ |
| `marts_players.player_valuations` | 616 377 lignes | ✅ |
| `marts_ml.predictions_projection_carriere` | 1 147 lignes | ✅ |

```sql
SELECT schemaname, tablename, n_live_tup AS nb_lignes
FROM pg_stat_user_tables
WHERE schemaname IN ('marts_clubs', 'marts_players', 'marts_ml')
ORDER BY schemaname, tablename;
```

**F07 — Dashboards Metabase**
- Premier League saison 2024 → classement 20 clubs ✅
- Scouting joueurs sous-évalués → 20 joueurs sans doublons ✅
- Top 20 U22 → Lamine Yamal en tête ✅

**F08 — Application Streamlit**
- Procédure : `streamlit run app.py` → saisie profil → clic "Prédire"
- Critère : valeur projetée cohérente avec profils de test ✅

**F09 — API REST FastAPI**
- `GET /` → `{"status": "ok"}` ✅
- `GET /predictions` → 20 prédictions U22 ✅
- `POST /predict` → valeur projetée + interprétation ✅
- `GET /docs` → Swagger UI accessible ✅

---

### 10.2 Tests structurels (RECETTES.md)

**S01 — Contraintes d'intégrité PostgreSQL**

| Test | Requête SQL | Attendu | Résultat |
|---|---|---|---|
| Clé primaire unique players | `SELECT player_id, COUNT(*) FROM marts_players.players GROUP BY player_id HAVING COUNT(*) > 1` | 0 ligne | ✅ |
| Minutes jouées valides | `SELECT COUNT(*) FROM marts_players.appearances_unified WHERE minutes_played < 0 OR minutes_played > 150` | 0 ligne | ✅ |
| Valorisations positives | `SELECT COUNT(*) FROM staging.stg_valuations WHERE market_value_in_eur < 0` | 0 ligne | ✅ |

**S02 — Déduplication des matchs**
- Requête : `SELECT match_id, COUNT(*) FROM marts_clubs.unified_matches GROUP BY match_id HAVING COUNT(*) > 1`
- Résultat attendu : 0 doublon → ✅ déduplication MD5 validée

**S03 — Cohérence du modèle ML**

| Test | Critère | Attendu | Résultat |
|---|---|---|---|
| R² global | `>= 0.50` | Modèle significativement meilleur qu'aléatoire | ✅ R²=0,61 |
| R² attaquants | `>= 0.60` | Segmentation efficace | ✅ R²=0,68 |
| R² CV 5-fold | Absence surapprentissage | Score stable | ✅ 0,6589 ± 0,0105 |
| Nb prédictions | `>= 1000` | Volume suffisant dashboard | ✅ 1 147 |

**S04 — Idempotence du pipeline ETL**
- Procédure : déclencher `pipeline_clubs` deux fois consécutives
- Attendu : `unified_matches` contient toujours exactement 16 802 lignes (DROP TABLE ... CASCADE avant rechargement)
- Résultat : ✅

**S05 — Rafraîchissement Transfermarkt (Kaggle)**

| Test | Procédure | Attendu | Résultat |
|---|---|---|---|
| Refresh réussi | `python src/ingestion/refresh_transfermarkt_source.py` | Téléchargement, validation, backup, remplacement | ✅ |
| Rollback schéma invalide | Colonne obligatoire retirée temporairement | Snapshot local inchangé, exception levée | ✅ |
| Seuil alerte (90%) | Volume en baisse notable sur `player_valuations.csv` | Accepté avec avertissement | ✅ |
| Seuil bloquant (30%) | Volume catastrophique sur `transfers.csv` | Refresh refusé, snapshot conservé | ✅ |

**S06 — Rafraîchissement football-datasets (GitHub)**

| Test | Procédure | Attendu | Résultat |
|---|---|---|---|
| Refresh réussi | `python src/ingestion/refresh_football_datasets_source.py` | 10 fichiers vérifiés/mis à jour | ✅ |
| Idempotence ETag | 2 exécutions consécutives | 2e exécution : 0 mis à jour, 10 inchangés | ✅ |
| Rollback schéma invalide | Colonne obligatoire retirée temporairement | Fichiers locaux inchangés, exception levée | ✅ |
| Intégration Airflow | Tâche `refresh_football_datasets_source` dans `pipeline_clubs` | Tâche verte, précède `collect_api_matches` | ✅ |

---

### 10.3 Tests de sécurité (RECETTES.md)

**SEC01 — `.env` non versionné**
- Procédure : `git ls-files | grep .env`
- Attendu : seul `.env.example` apparaît, jamais `.env`
- Résultat : ✅ `.env` dans `.gitignore`

**SEC02 — Séparation des rôles PostgreSQL**

| Rôle | Test | Attendu | Résultat |
|---|---|---|---|
| `football_reader` | `INSERT INTO marts_clubs.unified_matches VALUES (...)` | Erreur : permission refusée | ✅ |
| `football_writer` | `SELECT * FROM marts_clubs.unified_matches LIMIT 1` | Résultat retourné | ✅ |
| `football_reader` | `SELECT * FROM staging.stg_matches_api LIMIT 1` | Erreur : schema staging inaccessible | ✅ |

**SEC03 — Isolation réseau Docker**
- Procédure : `psql -h localhost -p 5432 -U airflow` depuis l'hôte
- Attendu : Connection refused (port non exposé)
- Résultat : ✅

**SEC04 — Modèles `.pkl` non versionnés**
- Procédure : `git ls-files | grep .pkl`
- Attendu : aucun fichier `.pkl` listé
- Résultat : ✅ `*.pkl` dans `.gitignore`

**SEC05 — Pipeline CI/CD**

| Job | Test | Attendu | Résultat |
|---|---|---|---|
| Security check | Push avec `.env` dans le repo | Pipeline bloque, erreur retournée | ✅ |
| Security check | Push avec `.pkl` dans le repo | Pipeline bloque, erreur retournée | ✅ |
| Tests unitaires | Transformation `log1p`/`expm1` | Reconstruction exacte | ✅ |
| Tests unitaires | Déduplication MD5 | IDs identiques pour mêmes matchs normalisés | ✅ |

---

### 10.4 Tests unitaires CI/CD (`.github/workflows/ci.yml`)

**Test 1 — Déduplication MD5 :**
Vérifie que `match_id('2024-01-01', 'Arsenal FC', 'Chelsea', 'PL') == match_id('2024-01-01', 'arsenal fc', 'chelsea', 'PL')` (normalisation insensible à la casse) et que des dates différentes produisent des IDs différents.

**Test 2 — `log1p`/`expm1` :**
Pour chaque valeur dans `[500000, 5000000, 50000000, 200000000]`, vérifie que `|expm1(log1p(v)) - v| < 1`.

**Test 3 — Features ML :**
Vérifie que la liste des 14 features contient exactement 14 éléments, inclut `valeur_moyenne_club` et `position_enc`.

**Test 4 — Division par zéro `goals_per_90` :**
Vérifie que `goals_per_90` vaut `NaN` pour un joueur ayant `minutes=0` (pas de `ZeroDivisionError`).

---

### 10.5 `test_hypothese_domicile.py`

Script de test statistique indépendant testant l'hypothèse de l'avantage à domicile sur les données de `unified_matches`.

---

## RÉSUMÉ GLOBAL DES TESTS

| Catégorie | Nb tests | Réussis | Échoués |
|---|---|---|---|
| Fonctionnels (RECETTES.md) | 8 | 8 | 0 |
| Structurels (RECETTES.md) | 6 | 6 | 0 |
| Sécurité (RECETTES.md) | 5 | 5 | 0 |
| Unitaires CI/CD | 4 | 4 | 0 |
| **Total** | **23** | **23** | **0** |

---

*Document généré par audit technique automatisé — Football Analytics Platform — août 2026*
