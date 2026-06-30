# Cahier de recettes — Football Analytics Platform

> Document de validation fonctionnelle, structurelle et sécurité.  
> Responsable : BELHADJ KACEM Abdelhamid — Version : juin 2026

---

## 1. Périmètre des tests

| Domaine | Composant testé |
|---|---|
| Infrastructure | Docker Compose · PostgreSQL · MinIO · Airflow · Metabase |
| Pipeline ETL | DAG pipeline_clubs · pipeline_players · pipeline_ml |
| Data Warehouse | Schémas · contraintes · volumes de données |
| Modèle ML | Features · prédictions · cohérence des résultats |
| Sécurité | Rôles PostgreSQL · secrets · réseau Docker |
| CI/CD | GitHub Actions · détection secrets |

---

## 2. Tests fonctionnels

### F01 — Démarrage de l'infrastructure

| Champ | Détail |
|---|---|
| **Objectif** | Vérifier que les 6 services démarrent correctement |
| **Procédure** | `docker compose up -d` puis `docker compose ps` |
| **Résultat attendu** | 6 conteneurs en état `running` : `football_postgres`, `football_minio`, `football_metabase`, `airflow_webserver`, `airflow_scheduler`, `airflow_postgres` |
| **Résultat obtenu** | ✅ 6 conteneurs running |

---

### F02 — Accès aux interfaces web

| Interface | URL | Résultat attendu | Résultat obtenu |
|---|---|---|---|
| Airflow | `localhost:8080` | Page de login Airflow | ✅ |
| Metabase | `localhost:3000` | Page d'accueil Metabase | ✅ |
| MinIO Console | `localhost:9003` | Page de login MinIO | ✅ |

---

### F03 — Exécution du DAG pipeline_clubs

| Champ | Détail |
|---|---|
| **Objectif** | Vérifier l'exécution complète du pipeline clubs |
| **Procédure** | Airflow UI → DAG `pipeline_clubs` → Trigger DAG |
| **Résultat attendu** | 5 tâches en vert : `collect_api_matches` → `transform_matches` → `build_unified_matches` → `build_classements` → `build_clubs_unified` |
| **Résultat obtenu** | ✅ 5 tâches succeeded |

---

### F04 — Exécution du DAG pipeline_players

| Champ | Détail |
|---|---|
| **Objectif** | Vérifier l'exécution complète du pipeline joueurs |
| **Procédure** | Airflow UI → DAG `pipeline_players` → Trigger DAG |
| **Résultat attendu** | 3 tâches en vert : `transform_players` → `build_appearances_unified` → `build_players_enriched` |
| **Résultat obtenu** | ✅ 3 tâches succeeded |

---

### F05 — Exécution du DAG pipeline_ml

| Champ | Détail |
|---|---|
| **Objectif** | Vérifier l'entraînement du modèle de projection de carrière |
| **Procédure** | Airflow UI → DAG `pipeline_ml` → Trigger DAG |
| **Résultat attendu** | 1 tâche en vert : `train_model_projection` — table `marts_ml.predictions_projection_carriere` créée avec 1 147 lignes |
| **Résultat obtenu** | ✅ 1 tâche succeeded · 1 147 prédictions générées |

---

### F06 — Volumes de données PostgreSQL

| Table | Volume attendu | Résultat obtenu |
|---|---|---|
| `marts_clubs.unified_matches` | 16 802 lignes | ✅ |
| `marts_players.players` | 47 702 lignes | ✅ |
| `marts_players.appearances_unified` | 1 862 208 lignes | ✅ |
| `marts_players.player_valuations` | 616 377 lignes | ✅ |
| `marts_ml.predictions_projection_carriere` | 1 147 lignes | ✅ |

```sql
-- Requête de vérification
SELECT schemaname, tablename, n_live_tup AS nb_lignes
FROM pg_stat_user_tables
WHERE schemaname IN ('marts_clubs', 'marts_players', 'marts_ml')
ORDER BY schemaname, tablename;
```

---

### F07 — Dashboards Metabase

| Dashboard | Test | Résultat attendu | Résultat obtenu |
|---|---|---|---|
| Clubs & Ligues | Filtre Compétition = Premier League · Saison = 2024 | Classement 20 clubs affiché | ✅ |
| Scouting Joueurs | Tableau joueurs sous-évalués | 20 joueurs · pas de doublons | ✅ |
| Projection de Carrière | Top 20 U22 par valeur projetée | Lamine Yamal en tête | ✅ |

---

### F08 — Application Streamlit

| Champ | Détail |
|---|---|
| **Objectif** | Vérifier que la démo de projection fonctionne |
| **Procédure** | `streamlit run app.py` → saisir profil joueur → cliquer "Prédire" |
| **Résultat attendu** | Valeur marchande projetée affichée en M€ avec interprétation |
| **Résultat obtenu** | ✅ Prédiction cohérente avec les profils de test |

---

## 3. Tests structurels

### S01 — Contraintes d'intégrité PostgreSQL

| Test | Requête | Résultat attendu | Résultat obtenu |
|---|---|---|---|
| Clé primaire unique | `SELECT player_id, COUNT(*) FROM marts_players.players GROUP BY player_id HAVING COUNT(*) > 1` | 0 ligne (pas de doublon) | ✅ |
| Minutes jouées valides | `SELECT COUNT(*) FROM marts_players.appearances_unified WHERE minutes_played < 0 OR minutes_played > 150` | 0 ligne | ✅ |
| Valorisations positives | `SELECT COUNT(*) FROM staging.stg_valuations WHERE market_value_in_eur < 0` | 0 ligne | ✅ |

---

### S02 — Déduplication des matchs

| Champ | Détail |
|---|---|
| **Objectif** | Vérifier l'absence de doublons dans `unified_matches` |
| **Requête** | `SELECT match_id, COUNT(*) FROM marts_clubs.unified_matches GROUP BY match_id HAVING COUNT(*) > 1` |
| **Résultat attendu** | 0 doublon |
| **Résultat obtenu** | ✅ 0 doublon — déduplication MD5 validée |

---

### S03 — Cohérence du modèle ML

| Test | Critère | Résultat attendu | Résultat obtenu |
|---|---|---|---|
| R² global | `>= 0.50` | Modèle significativement meilleur qu'aléatoire | ✅ R²=0,61 |
| R² par poste | Attaquants `>= 0.60` | Segmentation par poste efficace | ✅ R²=0,68 |
| Validation croisée | `R² CV 5-fold` | Absence de surapprentissage | ✅ 0,6589 ± 0,0105 |
| Nombre prédictions | `>= 1000` | Volume suffisant pour le dashboard | ✅ 1 147 |

---

### S04 — Idempotence du pipeline ETL

| Champ | Détail |
|---|---|
| **Objectif** | Vérifier qu'une double exécution ne crée pas de doublons |
| **Procédure** | Déclencher `pipeline_clubs` deux fois consécutives |
| **Résultat attendu** | `unified_matches` contient toujours 16 802 lignes (DROP TABLE ... CASCADE avant rechargement) |
| **Résultat obtenu** | ✅ Idempotence confirmée |

---

## 4. Tests de sécurité

### SEC01 — Fichier .env non versionné

| Champ | Détail |
|---|---|
| **Objectif** | Vérifier que les secrets ne sont pas exposés sur GitHub |
| **Procédure** | `git ls-files \| grep .env` |
| **Résultat attendu** | Seul `.env.example` apparaît, jamais `.env` |
| **Résultat obtenu** | ✅ Vérifié — `.env` dans `.gitignore` |

---

### SEC02 — Séparation des rôles PostgreSQL

| Rôle | Test | Résultat attendu | Résultat obtenu |
|---|---|---|---|
| `football_reader` | `INSERT INTO marts_clubs.unified_matches VALUES (...)` | Erreur : permission refusée | ✅ |
| `football_writer` | `SELECT * FROM marts_clubs.unified_matches LIMIT 1` | Résultat retourné | ✅ |
| `football_reader` | `SELECT * FROM staging.stg_matches_api LIMIT 1` | Erreur : schema staging inaccessible | ✅ |

---

### SEC03 — Isolation réseau Docker

| Champ | Détail |
|---|---|
| **Objectif** | Vérifier que `airflow_postgres` n'est pas accessible depuis l'hôte |
| **Procédure** | `psql -h localhost -p 5432 -U airflow` depuis le poste hôte |
| **Résultat attendu** | Connexion refusée (port non exposé) |
| **Résultat obtenu** | ✅ Connection refused |

---

### SEC04 — Modèles .pkl non versionnés

| Champ | Détail |
|---|---|
| **Objectif** | Vérifier que les modèles ML ne sont pas sur GitHub |
| **Procédure** | `git ls-files \| grep .pkl` |
| **Résultat attendu** | Aucun fichier `.pkl` listé |
| **Résultat obtenu** | ✅ Vérifié — `*.pkl` dans `.gitignore` |

---

### SEC05 — Pipeline CI/CD (GitHub Actions)

| Job | Test | Résultat attendu | Résultat obtenu |
|---|---|---|---|
| Security check | Push avec `.env` dans le repo | Pipeline bloque et retourne erreur | ✅ |
| Security check | Push avec `.pkl` dans le repo | Pipeline bloque et retourne erreur | ✅ |
| Tests unitaires | Transformation log1p/expm1 | Reconstruction exacte des valeurs | ✅ |
| Tests unitaires | Déduplication MD5 | IDs identiques pour mêmes matchs normalisés | ✅ |

---

## 5. Résumé des tests

| Catégorie | Nb tests | Réussis | Échoués |
|---|---|---|---|
| Fonctionnels | 8 | 8 | 0 |
| Structurels | 4 | 4 | 0 |
| Sécurité | 5 | 5 | 0 |
| **Total** | **17** | **17** | **0** |

---

*Document maintenu dans le dépôt GitHub : `Justhamid/football-analytics-platform`*
