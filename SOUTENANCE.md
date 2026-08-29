# Guide de soutenance — Football Analytics Platform
> Tout ce que tu dois savoir sur ton projet, coin par coin.
> Responsable : BELHADJ KACEM Abdelhamid

---

## 1. RÉSUMÉ DU PROJET EN 3 PHRASES

Tu as construit une **plateforme data engineering complète** pour l'analyse du football européen. Elle collecte des données de 3 sources, les nettoie, les transforme et les stocke dans un entrepôt de données. Elle expose ensuite un modèle de machine learning qui prédit la valeur marchande future des jeunes joueurs via une API REST et une application Streamlit.

**Cas d'usage ciblé** : clubs professionnels, cellules de scouting, centres de formation qui veulent identifier des talents avant leur pic de valeur.

---

## 2. ARCHITECTURE GLOBALE

```
Sources → MinIO (S3) → Staging (PG) → Transformation → Marts (PG) → ML → API + Streamlit + Metabase
```

### Les 7 services Docker
| Service | Container | Port | Rôle |
|---|---|---|---|
| PostgreSQL projet | `football_postgres` | 5433 | Entrepôt de données (football_db) |
| MinIO | `football_minio` | 9002/9003 | Data Lake S3 compatible |
| Metabase | `football_metabase` | 3000 | Dashboards BI |
| Airflow Webserver | `airflow_webserver` | 8080 | Interface orchestration |
| Airflow Scheduler | `airflow_scheduler` | — | Déclencheur des DAGs |
| PostgreSQL Airflow | `airflow_postgres` | — | Métadonnées Airflow + Metabase |
| FastAPI | `football_api` | 8000 | API REST prédictions ML |

### Les 2 bases de données PostgreSQL
- **`football_db`** sur `football_postgres` → toutes tes données (staging, marts)
- **`airflow`** + **`metabase_app`** sur `airflow_postgres` → usage interne Airflow et Metabase

---

## 3. SOURCES DE DONNÉES

### Source 1 — API football-data.org
- **Format** : JSON
- **10 fichiers** : 5 ligues × 2 saisons (2023 et 2024)
- **Tu utilises** : 10/10 (100%)
- **Contenu** : résultats de matchs, scores, équipes
- **Limite** : plan gratuit = 10 requêtes/minute max → gestion du rate limit (429) avec pause automatique dans `collect_api_matches.py`
- **Stockage** : `raw-football-api/` dans MinIO

### Source 2 — Transfermarkt (Kaggle)
- **Format** : CSV
- **12 fichiers disponibles, 6 utilisés**

| Fichier | Utilisé | Rôle |
|---|---|---|
| `players.csv` | ✅ | 80 000 joueurs — identité, valeur, poste |
| `appearances.csv` | ✅ | 1.86M lignes — stats par match |
| `clubs.csv` | ✅ | Infos clubs (stade, coach, effectif) |
| `player_valuations.csv` | ✅ | Historique valorisations |
| `transfers.csv` | ✅ | Historique transferts |
| `games.csv` | ✅ | Lien game_id → match pour apparitions |
| `club_games.csv` | ❌ | Non utilisé |
| `competitions.csv` | ❌ | Non utilisé (tu as `build_competitions.py`) |
| `countries.csv` | ❌ | Non utilisé |
| `game_events.csv` | ❌ | Non utilisé (événements détaillés) |
| `game_lineups.csv` | ❌ | Non utilisé (compositions) |
| `national_teams.csv` | ❌ | Non utilisé |

- **Stockage** : `raw-transfermarkt/` dans MinIO
- **Validation** : seuil alerte 90%, seuil bloquant 30% (ratio lignes nouveau/ancien)
- **Backup** : copie `.backup` avant remplacement

### Source 3 — football-datasets (GitHub)
- **Format** : CSV
- **165 fichiers** (33 par ligue × 5 ligues) — remontent aux années 90
- **Tu utilises** : ~50/165 (filtre `annee < 2015 → ignoré`)
- **Particularité** : contient des stats bonus (tirs, corners, cartons) que l'API n'a pas
- **Format date** : DD/MM/YYYY → parsing avec `dayfirst=True`
- **Guard important dans le code** : `if "HomeTeam" not in df_s.columns: continue` → certains vieux fichiers n'ont pas cette colonne, on les ignore silencieusement
- **Stockage** : `raw-football-datasets/` dans MinIO
- **Dette technique** : ces fichiers passent directement de MinIO à la transformation sans passer par le staging (contrairement aux 2 autres sources)

---

## 4. LA PIPELINE DE DONNÉES — ÉTAPE PAR ÉTAPE

### Étape 1 — MinIO (archive brute)
Tous les fichiers téléchargés sont stockés dans 3 buckets MinIO :
- `raw-football-api/`
- `raw-transfermarkt/`
- `raw-football-datasets/`

**Pourquoi MinIO ?** Compatible API S3 → migration vers AWS S3 = changer une URL. Pas de vendor lock-in.

### Étape 2 — Staging (copie brute dans PostgreSQL)
**7 tables staging** — copie fidèle, aucune transformation :

| Table | Source | Volume |
|---|---|---|
| `staging.stg_players` | `players.csv` | ~80 000 lignes |
| `staging.stg_appearances` | `appearances.csv` | 1.86M lignes |
| `staging.stg_valuations` | `player_valuations.csv` | ~600K lignes |
| `staging.stg_transfers` | `transfers.csv` | ~400K lignes |
| `staging.stg_clubs` | `clubs.csv` | ~500 lignes |
| `staging.stg_games` | `games.csv` | ~400K lignes |
| `staging.stg_matches_api` | JSON API | 3 502 lignes |

**Pourquoi le staging ?** Permet de rejouer une transformation sans re-télécharger depuis MinIO. Permet de débugger avec SQL. Traçabilité.

### Étape 3 — Transformation → Marts

#### `transform_players.py` — Pipeline joueurs
Lit les 6 CSV depuis MinIO, crée le staging, puis transforme :
- Cast types (texte → DATE, INTEGER, FLOAT)
- Valeurs marchandes négatives → NULL
- Déduplication sur `player_id` (doublon = supprimé)
- `minutes_played = 0` → ligne supprimée

**Tables créées :**
- `marts_players.players` — 47 702 joueurs propres
- `marts_players.clubs`
- `marts_players.player_valuations`
- `marts_players.transfers`
- `marts_players.appearances`

#### `transform_matches.py` — Pipeline matchs API
Lit `stg_matches_api`, nettoie :
- Filtre `status != "FINISHED"` → supprimé
- Normalise noms équipes via `team_mapping.py`
- Calcule `result` : `home`, `away`, `draw`

**Tables créées :**
- `marts_clubs.matches`
- `marts_clubs.classements_equipes`

#### `spark_appearances.py` — Agrégation PySpark (1.86M lignes)
Utilise PySpark `local[*]` (tous les cœurs CPU) car pandas serait trop lent sur 1.86M lignes.
- Jointure apparitions ↔ joueurs (pour le poste)
- Agrégation par joueur + saison + compétition
- Calcul buts/90, passes/90, value_efficiency

**Table créée :** `marts_players.player_performance`

#### `build_unified_matches.py` — Fusion des 2 sources de matchs
- Charge API (MinIO) + football-datasets (MinIO direct — dette technique)
- Normalise tous les noms d'équipes
- Génère un hash MD5 : `hashlib.md5(f"{date}_{competition}_{home}_{away}".encode()).hexdigest()[:16]`
- Même match dans 2 sources → même hash → déduplication garde api > football_datasets
- **Filtre scores aberrants** : `home_goals > 20` ou `away_goals > 20` → supprimé

**Table créée :** `marts_clubs.unified_matches` → **18 557 matchs uniques (2016-2025)**

#### `build_classements_unified.py` — 3 tables analytiques
Depuis `unified_matches` :

**Tables créées :**
- `marts_clubs.classements_equipes_unified` — points, buts, victoires par club par saison (2016-2025)
- `marts_clubs.avantage_domicile` — % victoires domicile, moy buts par ligue/saison
- `marts_clubs.matches_enrichis` — matchs avec stats détaillées (tirs, corners, cartons)

#### `build_clubs_unified.py` — Référentiel clubs
Prend `marts_players.clubs` (Transfermarkt) et :
- Normalise les noms (`"Manchester City FC"` → `"Man City"`)
- Traduit les codes compétition (`"GB1"` → `"PL"` → `"Premier League"`)

**Table créée :** `marts_clubs.clubs_unified`

#### `build_competitions.py` — Table de référence statique
Table fixe des 5 ligues. Écrite une fois, jamais recalculée.

**Table créée :** `marts_clubs.competitions`

#### `build_appearances_unified.py` — Lien apparitions ↔ matchs
Joint `stg_appearances` + `stg_games` pour générer le même hash MD5 sur chaque match Transfermarkt.
Résultat : chaque apparition sait dans quel match unifié elle s'est produite.

**Table créée :** `marts_players.appearances_unified` (1.86M lignes avec `match_id`)

#### `build_players_enriched.py` — Vue finale joueur
Assemble en 1 table : `players` + `player_performance` + `predictions_projection_carriere`
Ajoute normalisation club + mapping compétition lisible.

**Table créée :** `marts_players.players_enriched`

---

## 5. STRATÉGIE FULL REFRESH — POURQUOI JAMAIS DE DOUBLONS

**À chaque exécution du pipeline, toutes les tables sont supprimées et reconstruites.**

2 méthodes utilisées :
1. `if_exists="replace"` (pandas) → DROP automatique + INSERT
2. `DROP TABLE IF EXISTS ... CASCADE` + `if_exists="append"` → utilisé quand il y a des dépendances (CASCADE supprime aussi les vues liées)

**Résultat** : lancer le pipeline 2 fois → même résultat. C'est l'**idempotence** — validée dans le cahier de recettes (test S04).

---

## 6. LES 3 DAGs AIRFLOW

### Planification
```
Lundi 06h00 → pipeline_clubs   (6 tâches)
Lundi 07h00 → pipeline_players (6 tâches dont 1 sensor)
Lundi 08h00 → pipeline_ml      (2 tâches dont 1 sensor)
```

### `pipeline_clubs` — 6 tâches
```
refresh_football_datasets_source
    ↓
collect_api_matches
    ↓
transform_matches
    ↓
build_unified_matches
    ↓
build_classements
    ↓
build_clubs_unified
```

### `pipeline_players` — 6 tâches
```
refresh_transfermarkt_source
    ↓
transform_players
    ↓
spark_player_performance          ← tourne en parallèle avec pipeline_clubs
    ↓
wait_for_pipeline_clubs (SENSOR)  ← attend pipeline_clubs.build_clubs_unified
    ↓
build_appearances_unified
    ↓
build_players_enriched
```

**Pourquoi le sensor est APRÈS spark ?** Spark n'a pas besoin de clubs. Il peut tourner pendant que clubs tourne. On attend clubs seulement pour les étapes suivantes.

### `pipeline_ml` — 2 tâches
```
wait_for_pipeline_players (SENSOR) ← attend pipeline_players.build_players_enriched
    ↓
train_model_projection
```

### Mécanisme commun
- `run_script()` utilise `subprocess.Popen` avec `-u` (unbuffered) → logs en temps réel dans Airflow
- `os.environ.copy()` + `env["PYTHONPATH"] = "/opt/airflow"` → transmis au sous-process (incident résolu)
- `on_failure_callback` → email automatique avec DAG, tâche, erreur, lien logs

---

## 7. LE MODÈLE ML

### Ce qu'il fait
Prédit la valeur marchande future d'un joueur à partir de ses stats entre 16 et 21 ans.

### Population d'entraînement
Joueurs **actuellement âgés de 22 à 35 ans** dont on connaît la valeur marchande.
On apprend : "ces stats à 16-21 ans → cette valeur adulte".

### Les 14 features
1. `matchs_jeune` — nb matchs joués entre 16-21 ans
2. `buts_jeune` — buts totaux
3. `passes_jeune` — passes décisives totales
4. `minutes_jeune` — minutes jouées
5. `goals_per_90_jeune` — buts par 90 min
6. `assists_per_90_jeune` — passes déc. par 90 min
7. `age_premier_match` — âge au 1er match professionnel
8. `nb_competitions_jeune` — nb compétitions différentes
9. `height_in_cm` — taille en cm
10. `international_caps` — sélections nationales
11. `international_goals` — buts en sélection
12. `valeur_moyenne_club` — valeur marchande moyenne des joueurs du club formateur (**feature la plus importante**)
13. `position_enc` — poste encodé (LabelEncoder)
14. `foot_enc` — pied préférentiel encodé

### Architecture du modèle
- 1 modèle **global** GradientBoostingRegressor (tous postes confondus)
- 4 modèles **par poste** (Attack, Midfield, Defender, Goalkeeper) — min 100 joueurs pour entraîner
- Si la position est connue et dans les modèles → modèle spécialisé. Sinon → modèle global.

### Transformation de la cible
`y = np.log1p(valeur_cible)` — transforme les valeurs marchandes en log pour gérer la distribution très skewée (quelques joueurs à 200M€, beaucoup à 500K€)
Prédiction : `valeur = np.expm1(y_pred)`

### Performances
- R² global = **0.61** (explique 61% de la variance)
- R² CV 5-fold = **0.6589 ± 0.0105** (pas de surapprentissage)
- R² attaquants = **0.68**
- MAE = erreur moyenne en euros (variable selon le poste)

### Arbitrage (déjà documenté dans le projet)
**Première version** : prédire la valeur actuelle depuis les stats récentes → R² = 0.19
**Décision** : reformuler en prédisant la valeur future depuis les stats jeunes → R² = 0.61 (+220%)

### Résultat
- ~1 147 prédictions stockées dans `marts_ml.predictions_projection_carriere`
- Modèle sauvegardé dans `models/model_projection_carriere.pkl`

---

## 8. L'API FASTAPI

### Endpoints
| Méthode | Endpoint | Protection | Limite |
|---|---|---|---|
| `GET` | `/` | Public | — |
| `POST` | `/predict` | Clé API | 30/minute |
| `GET` | `/predictions` | Clé API | 60/minute |
| `GET` | `/predictions/{player_id}` | Clé API | 60/minute |

### Sécurité
- **Authentification** : clé API dans header `X-API-Key` → `401` si invalide
- **Rate limiting** : `slowapi` — 30 req/min sur `/predict`, 60 req/min sur les autres
- **CORS** : autorise `localhost:3000` (Metabase), `localhost:8501` (Streamlit), `localhost:8080` (Airflow)
- **Injection SQL** : requêtes paramétrées via SQLAlchemy `text()` + `:param`
- **Validation input** : Pydantic avec contraintes `ge`, `le` sur chaque champ
- **Réponses filtrées** : schéma Pydantic `PredictionOutput` → jamais de dump brut de la base

### Calcul de l'âge pic dans l'API
```python
age_pic_min = round(24 - (player.age_actuel - 14) * (2 / 7))
age_pic_max = age_pic_min + 3
```
- Joueur 14 ans → pic 24-27 ans
- Joueur 16 ans → pic 23-26 ans
- Joueur 21 ans → pic 22-25 ans

---

## 9. STREAMLIT (app.py)

Interface de démonstration pour les scouts et formateurs. Entrée manuelle des stats d'un joueur → prédiction de valeur marchande future avec :
- Indicateur de fiabilité selon l'âge (moins de données à 14 ans qu'à 18 ans)
- Indicateur de volume selon les minutes jouées
- Âge pic calculé
- Interprétation narrative (talent exceptionnel / bon potentiel / etc.)
- Résumé exportable en texte

---

## 10. SÉCURITÉ COMPLÈTE

| Mesure | Où | Ce que ça protège |
|---|---|---|
| Clé API `X-API-Key` | FastAPI | Accès aux endpoints REST |
| Rate limiting (slowapi) | FastAPI | Abus / DDoS |
| CORS (CORSMiddleware) | FastAPI | Origines non autorisées |
| Fernet Key | Airflow | Variables/connexions chiffrées en base |
| STARTTLS port 587 | SMTP Airflow | Transit emails d'alerte |
| `.env` + variables d'env | Partout | Credentials hors du code |
| Rôles PostgreSQL reader/writer | `football_db` | Séparation lecture/écriture |
| CI/CD vérifie `.env` non commité | GitHub Actions | Exposition accidentelle secrets |
| CI/CD vérifie `.pkl` non commité | GitHub Actions | Modèle ML hors dépôt |
| SCRAM-SHA-256 | PostgreSQL 15 | Authentification chiffrée (par défaut) |
| Requêtes paramétrées | SQLAlchemy | Injection SQL |

### Ce qui manque (à mentionner si demandé)
- **JWT** : clé API statique, pas de JWT avec expiration/révocation
- **SSL/TLS PostgreSQL** : connexions en clair (réseau Docker interne = acceptable en dev)

---

## 11. CI/CD — GITHUB ACTIONS

Déclenché à chaque `git push` sur `main` ou `develop`.

### Job 1 — Qualité code
`flake8` sur `src/` et `dags/` (max 120 chars/ligne) + vérification imports critiques (pandas, numpy, scikit-learn, sqlalchemy)

### Job 2 — Sécurité
- `.env` absent du repo
- Pas de mots de passe en dur dans le code
- `.env.example` présent
- Pas de `.pkl` versionné

### Job 3 — Tests unitaires (attend Job 1)
4 tests sans base de données ni Docker :
1. Hash MD5 : "Arsenal FC" = "arsenal fc" → même hash ✅
2. `log1p`/`expm1` : 180 000 000 → log → expm1 → 180 000 000 ✅
3. 14 features ML dans le bon ordre ✅
4. Division par zéro goals_per_90 : 0 minutes → NaN (pas d'erreur) ✅

### Job 4 — Résumé (toujours affiché)
Bilan visuel des 3 jobs précédents.

---

## 12. LES RÔLES POSTGRESQL

### `football_reader` (utilisé par Metabase)
- Accès lecture seule : `marts_clubs`, `marts_players`, `marts_ml`
- **Pas d'accès à `staging`** (révoqué explicitement)

### `football_writer` (utilisé par les scripts ETL)
- Lecture + écriture : `staging`, `marts_clubs`, `marts_players`, `marts_ml`
- `DEFAULT PRIVILEGES` → toute nouvelle table créée hérite automatiquement des droits

### Mécanisme de restriction
PostgreSQL : **tout est interdit par défaut**. Un rôle ne peut faire que ce qui lui est explicitement accordé.
3 niveaux nécessaires : CONNECT (base) → USAGE (schéma) → SELECT/INSERT/etc. (table)

---

## 13. ARCHITECTURE DES TABLES — SCHÉMA EN ÉTOILE

### Tables de faits (événements mesurables)
- `unified_matches` — 1 ligne = 1 match (buts, tirs, résultat)
- `appearances_unified` — 1 ligne = 1 joueur dans 1 match (goals, assists, minutes)
- `player_valuations` — 1 ligne = 1 évaluation de valeur à 1 date
- `transfers` — 1 ligne = 1 transfert

### Tables de dimensions (référentiels descriptifs)
- `players` — qui est le joueur
- `clubs` / `clubs_unified` — quel est le club
- `competitions` — quelle est la ligue

### Tables agrégées
- `player_performance` — buts/90, assists/90 par joueur/saison
- `classements_equipes_unified` — points, buts par club/saison
- `avantage_domicile` — % victoires domicile par ligue/saison
- `players_enriched` — vue finale joueur complète
- `predictions_projection_carriere` — prédictions ML

### Meilleur exemple de schéma en étoile
`appearances_unified` (fait) reliée à :
- `players` via `player_id`
- `unified_matches` via `match_id`
- `competitions` via `competition_id`

---

## 14. DÉDUPLICATION MD5 — COMMENT ÇA MARCHE

```python
import hashlib

def generer_match_id(date, home, away, competition):
    cle = f"{date}_{competition}_{home}_{away}"
    return hashlib.md5(cle.encode()).hexdigest()[:16]
```

Avant le hash : normalisation des noms via `team_mapping.py`
→ "Manchester City FC", "Man City", "manchester city" → toujours "Man City"
→ Même match dans 2 sources → même hash → déduplication garde 1 seule ligne

**Priorité** : API (0) > football_datasets (1) — en cas de doublon on garde la source la plus fiable

---

## 15. CHEMINEMENT COMPLET DES DONNÉES

```
SOURCES → MINIO → STAGING → TRANSFORMATION → MARTS → ML → SERVICE

API JSON     raw-football-api/      stg_matches_api  →  matches, unified_matches
Kaggle CSV   raw-transfermarkt/     stg_players       →  players, appearances
                                    stg_appearances   →  appearances_unified
                                    stg_valuations    →  player_valuations
                                    stg_transfers     →  transfers
                                    stg_clubs         →  clubs_unified
                                    stg_games         →  appearances_unified (lien)
GitHub CSV   raw-football-datasets/ (pas de staging)  →  unified_matches (dette)

unified_matches → classements_equipes_unified, avantage_domicile, matches_enrichis
appearances_unified + players → player_performance (Spark)
players + player_performance + appearances_unified → players_enriched

appearances_unified + players + stg_valuations → train_model_projection
→ predictions_projection_carriere + model.pkl

model.pkl + PostgreSQL → API FastAPI (port 8000)
model.pkl               → Streamlit (port 8501)
marts_* + marts_ml.*   → Metabase (port 3000)
```

---

## 16. L'INCIDENT RÉEL RÉSOLU

**Symptôme** : `ModuleNotFoundError: No module named 'src'` sur toutes les tâches Airflow au premier déploiement.

**Cause triple** :
1. `PYTHONPATH` non défini dans `docker-compose.yml`
2. Dépendances Python manquantes dans l'image Airflow (`sqlalchemy`, `pandas`, etc.)
3. `subprocess.run()` ne transmet pas automatiquement les variables d'environnement au sous-processus

**3 corrections** :
1. `PYTHONPATH: /opt/airflow` dans docker-compose
2. `_PIP_ADDITIONAL_REQUIREMENTS` dans docker-compose pour installer les packages
3. `env = os.environ.copy(); env["PYTHONPATH"] = "/opt/airflow"` passé explicitement à `subprocess.Popen`

---

## 17. POINTS DE VIGILANCE À CONNAITRE

| Point | Détail |
|---|---|
| `docker compose down -v` | **JAMAIS en prod** — supprime les volumes Docker (toutes les données) |
| `docker compose down` | ✅ OK — arrête sans supprimer les données |
| `.env` | Ne jamais commiter — CI bloque si présent dans Git |
| Port 5433 | PostgreSQL exposé en 5433 (pas 5432) pour éviter conflit avec PG natif Windows |
| Fernet Key | Doit être définie dans `.env` sinon Airflow ne chiffre pas les variables |
| JAVA_HOME | Chemin Windows hardcodé dans `spark_appearances.py` — ok sur ta machine, à corriger pour autre machine |
| `annee < 2015: continue` | football-datasets — filtre les saisons avant 2015 |
| `if "HomeTeam" not in df_s.columns: continue` | football-datasets — certains vieux CSV n'ont pas cette colonne, ignorés silencieusement |
| `execution_delta=timedelta(hours=1)` | ExternalTaskSensor — cherche le run de pipeline_clubs 1h avant (cohérent avec le scheduling) |

---

## 18. QUESTIONS JURY PROBABLES ET RÉPONSES

**Q : Pourquoi 3 sources de données ?**
→ Chaque source apporte quelque chose d'unique : l'API donne les résultats récents officiels, Transfermarkt donne les joueurs et leurs stats individuelles sur 10 ans, football-datasets donne l'historique des matchs avec les stats avancées (tirs, corners). Aucune source seule ne suffit.

**Q : Pourquoi MinIO et pas directement PostgreSQL ?**
→ MinIO est un data lake — il stocke les fichiers bruts dans leur format original (JSON, CSV) de façon pérenne. PostgreSQL est un entrepôt de données structurées. Cette séparation permet de rejouer toute la transformation depuis le début si nécessaire, sans re-télécharger les sources.

**Q : Pourquoi PySpark pour les apparitions ?**
→ 1.86M de lignes en agrégation par joueur/saison avec pandas prendrait 5-10 minutes. Spark distribue le calcul sur tous les cœurs CPU en parallèle, ce qui réduit le temps à 30-60 secondes. L'architecture `local[*]` est identique à un cluster Spark, seule l'échelle change.

**Q : Comment tu évites les doublons entre sources ?**
→ Je génère une empreinte MD5 déterministe à partir de la date, la compétition, et les deux équipes normalisées. Le même match dans 2 sources produit toujours le même hash. La déduplication garde la source la plus fiable (API > football-datasets).

**Q : Ton modèle ML, comment il fonctionne ?**
→ J'entraîne un Gradient Boosting sur les statistiques de formation (16-21 ans) de joueurs qui ont aujourd'hui entre 22 et 35 ans. Le modèle apprend la relation entre ces stats jeunes et leur valeur marchande adulte. 4 modèles spécialisés par poste + 1 global. R² = 0.61, validé en cross-validation 5-fold.

**Q : Pourquoi R² = 0.61 et pas plus ?**
→ La valeur marchande d'un joueur dépend de nombreux facteurs non quantifiables dans mes données : blessures, réputation, agent, conjoncture du marché, médiatisation. Un R² de 0.61 signifie que le modèle explique 61% de la variance — c'est significatif pour ce type de prédiction. La feature la plus importante est le niveau du club formateur.

**Q : C'est quoi le CI/CD dans ton projet ?**
→ À chaque push sur GitHub, 4 jobs automatiques tournent : qualité du code (flake8), sécurité (.env non commité, pas de secrets en dur, pas de .pkl), tests unitaires (MD5, log1p/expm1, 14 features ML), et un résumé. Ça garantit qu'aucune régression n'est introduite sans être détectée.

**Q : Est-ce que ton projet est scalable ?**
→ Oui. L'architecture est identique à ce qu'on déploierait en production cloud. MinIO → AWS S3 (changer une URL), PostgreSQL → RDS (changer un endpoint), Airflow → Cloud Composer ou MWAA, PySpark local → cluster Databricks ou EMR. Pas de vendor lock-in.

**Q : Quelles sont les limites/dettes techniques ?**
→ 3 principales : football-datasets bypass le staging (devrait avoir `stg_fd_matches`), pas de JWT sur l'API (clé API statique), pas de SSL sur les connexions PostgreSQL internes (acceptable en réseau Docker, à corriger en production cloud).

---

## 19. CHIFFRES CLÉS À RETENIR

| Métrique | Valeur |
|---|---|
| Matchs uniques (unified_matches) | **18 557** |
| Joueurs (marts_players.players) | **47 702** |
| Apparitions (appearances_unified) | **1 862 208** |
| Valorisations historiques | **616 377** |
| Prédictions ML (U22) | **1 147** |
| R² modèle global | **0.61** |
| R² attaquants | **0.68** |
| Saisons couvertes | **2016 → 2025** |
| Ligues | **5** (PL, BL1, FL1, PD, SA) |
| Services Docker | **7** |
| Tables PostgreSQL | **23** (7 staging + 7 marts_clubs + 8 marts_players + 1 marts_ml) |
| Features ML | **14** |
| DAGs Airflow | **3** |
| Jobs CI/CD | **4** |

---

*Document généré le 29 août 2026 — Football Analytics Platform*
