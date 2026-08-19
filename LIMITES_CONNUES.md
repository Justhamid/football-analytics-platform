# Limites connues et axes d'amélioration

Ce document recense les limites techniques identifiées mais non corrigées
dans le cadre de ce projet, par choix assumé (risque de régression,
périmètre hors Bloc 4, ou priorité donnée à d'autres chantiers).

---

## 1. Horizon temporel de la cible du modèle ML

La cible d'entraînement du modèle de projection (`marts_ml.predictions_projection_carriere`)
utilise la dernière valorisation connue de chaque joueur, plutôt qu'une fenêtre stricte
"2 ans après la période jeune" comme le suggère la documentation initiale. Le modèle prédit
donc un potentiel de carrière à maturité plutôt qu'une échéance précise à +2 ans.

Correction identifiée : contraindre la cible à une fenêtre de valorisation entre 23 et 25 ans
(date_of_birth + 23 à 25 ans), au prix d'une réduction significative de l'échantillon
d'entraînement disponible. Non traité dans le cadre de ce projet, le module ML étant un
différenciateur du projet et non son cœur (Bloc 4 = infrastructure data).

## 2. Scripts ML prototypes non utilisés

Le dossier `src/ml/` contient 4 scripts (`build_features.py`, `build_features_temporal.py`,
`train_model.py`, `train_model_temporal.py`) correspondant à une première approche de
modélisation, explorée puis abandonnée au profit de l'approche finale implémentée dans
`train_model_projection.py` — le seul script réellement intégré au DAG `pipeline_ml`.
Ces scripts prototypes n'écrivent dans aucune table utilisée en production. Conservés
pour la traçabilité de la démarche exploratoire.

## 3. Metabase partage la base PostgreSQL du projet — RÉSOLU

Metabase utilisait initialement la même instance PostgreSQL que les
données du projet pour stocker ses propres métadonnées. Corrigé en
déplaçant la base applicative de Metabase (comptes, dashboards,
permissions) vers airflow_postgres, une instance PostgreSQL distincte
déjà présente dans l'infrastructure — via pg_dump/pg_restore du schéma
public (144 tables), avec activation préalable de l'extension citext.
Un incident sur football_postgres n'affecte désormais plus l'accès à
l'interface Metabase elle-même, seules les données affichées (marts_*)
seraient temporairement indisponibles.

## 4. Scripts de validation non intégrés aux DAGs

Sur les 4 scripts du dossier `src/validation/`, seul `valider_mapping.py` contient une
vraie logique de contrôle qualité (détection de noms d'équipes non mappés dans
`team_mapping.py`). Les 3 autres (`audit_jointure.py`, `validation_initiale.py`,
`inventaire_donnees.py`) sont des outils d'exploration ponctuels, déjà utilisés en
amont pour construire la logique de normalisation actuelle, sans réelle valeur en
exécution récurrente.

`valider_mapping.py` serait le seul candidat pertinent à intégrer comme tâche Airflow,
en amont de `build_unified_matches.py` — non fait par prudence, ce script n'ayant pas
été conçu à l'origine pour lever une exception bloquante (il faudrait ajouter un seuil
de tolérance et un `raise` explicite avant intégration).

## 5. Vendor lock-in scikit-learn / Python 3.8 (Airflow)

L'image Airflow de base (Python 3.8) plafonne certaines dépendances (pandas ≤ 2.0.3,
contrainte dure liée à Python 3.8) alors que l'API de prédiction tourne sous Python 3.12
avec des versions plus récentes. scikit-learn a été aligné (1.8.0 des deux côtés, critique
pour la désérialisation du modèle `.pkl`) ; pandas reste différent sans impact observé sur
le fonctionnement. Migrer vers une image Airflow basée sur Python 3.9+ permettrait un
alignement complet.

## 6. Authentification API REST — RÉSOLU

Les 3 endpoints sensibles (POST /predict, GET /predictions,
GET /predictions/{id}) sont désormais protégés par une clé API
transmise via l'en-tête X-API-Key, vérifiée à chaque requête. Seul
GET / reste public (simple healthcheck, aucune donnée exposée). Les
communications restent en HTTP simple sans TLS, adapté au contexte de
développement local -- à corriger avant tout déploiement public.
Aucun scan automatisé des vulnérabilités des dépendances (pip-audit,
safety) n'est encore intégré à la CI/CD.