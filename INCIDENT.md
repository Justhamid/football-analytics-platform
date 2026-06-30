# Rapport d'incident — Football Analytics Platform

> **Incident** : Échec total des 3 DAGs Airflow — modules Python introuvables  
> **Date** : Juin 2026  
> **Sévérité** : Critique (pipeline de données complètement arrêté)  
> **Responsable résolution** : BELHADJ KACEM Abdelhamid  
> **Durée de résolution** : ~3 heures  

---

## 1. Description de l'incident

### Symptôme observé

Au premier lancement des 3 DAGs Airflow après déploiement via Docker Compose,
l'ensemble des tâches échouaient immédiatement avec le message suivant dans
les logs Airflow :

```
ModuleNotFoundError: No module named 'src'
```

ou selon la tâche :

```
ModuleNotFoundError: No module named 'dotenv'
ModuleNotFoundError: No module named 'sqlalchemy'
```

**Impact** : Les 3 pipelines (`pipeline_clubs`, `pipeline_players`, `pipeline_ml`)
étaient en échec total. Aucune donnée n'était collectée ni transformée.
La plateforme était non opérationnelle.

---

## 2. Chronologie de l'investigation

### Étape 1 — Identification du problème (15 min)

Consultation des logs Airflow depuis l'interface web (`localhost:8080`) :

```
Task instance: collect_api_matches
State: FAILED
Error: ModuleNotFoundError: No module named 'src'
```

**Hypothèse initiale** : les scripts Python du projet (`src/`) ne sont pas
accessibles depuis les conteneurs Airflow.

---

### Étape 2 — Inspection de l'environnement Docker (30 min)

Connexion au conteneur Airflow pour investiguer :

```bash
docker exec -it airflow_webserver bash
python -c "import sys; print(sys.path)"
```

**Résultat** :
```
['', '/usr/local/lib/python3.11', '/usr/local/lib/python3.11/site-packages']
```

Le répertoire `/opt/airflow` (racine du projet) n'était **pas dans le
`sys.path`** Python du conteneur. Les imports `from src.utils import ...`
échouaient donc systématiquement.

**Diagnostic confirmé** : `PYTHONPATH` non défini dans l'environnement
d'exécution des conteneurs Airflow.

---

### Étape 3 — Identification du problème de dépendances (45 min)

En parallèle, une seconde erreur était identifiée sur certaines tâches :

```
ModuleNotFoundError: No module named 'sqlalchemy'
```

Vérification des packages installés dans le conteneur :

```bash
docker exec -it airflow_webserver pip list | grep sqlalchemy
```

**Résultat** : `sqlalchemy` absent. L'image `apache/airflow:2.8.1` n'embarque
pas toutes les dépendances du projet par défaut.

**Diagnostic** : les dépendances Python du projet (`pandas`, `sqlalchemy`,
`scikit-learn`, `duckdb`, `minio`, etc.) n'étaient pas installées dans
les conteneurs Airflow.

---

### Étape 4 — Identification du problème de volume (30 min)

Vérification que le dossier `src/` était bien monté dans le conteneur :

```bash
docker exec -it airflow_webserver ls /opt/airflow/src
```

**Résultat** : dossier `src/` présent mais **le `PYTHONPATH` n'était pas
propagé** à l'environnement du sous-processus `subprocess.run()` utilisé
dans les DAGs pour exécuter les scripts.

Le problème était donc triple :
1. `PYTHONPATH` non défini dans `docker-compose.yml`
2. Dépendances Python manquantes dans l'image Airflow
3. Variables d'environnement non transmises au sous-processus dans `run_script()`

---

## 3. Actions correctives

### Correction 1 — Ajout du PYTHONPATH dans docker-compose.yml

Ajout de la variable d'environnement dans la section `x-airflow-common` :

```yaml
# Avant
environment:
  AIRFLOW__CORE__EXECUTOR: LocalExecutor
  # ... autres variables ...

# Après
environment:
  AIRFLOW__CORE__EXECUTOR: LocalExecutor
  PYTHONPATH: /opt/airflow          # ← ajout
  # ... autres variables ...
```

---

### Correction 2 — Installation des dépendances via _PIP_ADDITIONAL_REQUIREMENTS

Ajout dans `docker-compose.yml` :

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: 'matplotlib duckdb==0.10.3 pandas requests
  python-dotenv scikit-learn sqlalchemy psycopg2-binary minio'
```

Ce mécanisme natif d'Airflow installe automatiquement les packages Python
listés au démarrage des conteneurs, sans nécessiter de créer une image
Docker personnalisée.

---

### Correction 3 — Propagation de l'environnement dans run_script()

Modification de la fonction `run_script()` dans les 3 DAGs :

```python
# Avant
def run_script(script_path: str) -> None:
    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True,
        cwd="/opt/airflow"
    )

# Après
def run_script(script_path: str) -> None:
    env = os.environ.copy()          # ← copie de l'environnement complet
    env["PYTHONPATH"] = "/opt/airflow"  # ← force le PYTHONPATH

    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True,
        cwd="/opt/airflow",
        env=env                      # ← transmission au sous-processus
    )
```

**Explication** : `subprocess.run()` ne transmet pas automatiquement les
variables d'environnement du processus parent. Il fallait explicitement
copier `os.environ` et le passer via le paramètre `env`.

---

### Correction 4 — Suppression de l'ExternalTaskSensor

`pipeline_ml` utilisait initialement un `ExternalTaskSensor` pour attendre
la fin de `pipeline_players`. Ce mécanisme bloquait indéfiniment lors des
exécutions manuelles (hors scheduling). Il a été remplacé par le
séquencement temporel (6h/7h/8h) déjà en place.

---

## 4. Validation de la résolution

Après application des 4 corrections et redémarrage de l'infrastructure :

```bash
docker compose down
docker compose up -d
```

Tests de validation effectués :

| Test | Commande | Résultat |
|---|---|---|
| PYTHONPATH correct | `docker exec airflow_webserver python -c "import src"` | ✅ Pas d'erreur |
| Dépendances présentes | `docker exec airflow_webserver pip list \| grep sqlalchemy` | ✅ SQLAlchemy 2.x présent |
| DAG pipeline_clubs | Trigger manuel depuis Airflow UI | ✅ 5 tâches succeeded |
| DAG pipeline_players | Trigger manuel depuis Airflow UI | ✅ 3 tâches succeeded |
| DAG pipeline_ml | Trigger manuel depuis Airflow UI | ✅ 1 tâche succeeded |

---

## 5. Commits de résolution (traçabilité Git)

```
fix(airflow): add PYTHONPATH to pipeline_players and pipeline_ml DAGs
fix(airflow): fix volume mount and PYTHONPATH for DAG execution
fix(airflow): add _PIP_ADDITIONAL_REQUIREMENTS for DAG dependencies
fix(airflow): remove ExternalTaskSensor from pipeline_ml for manual execution
```

---

## 6. Mesures préventives mises en place

| Mesure | Description |
|---|---|
| **Test CI/CD** | Le pipeline GitHub Actions vérifie les imports critiques (`pandas`, `numpy`, `sklearn`, `sqlalchemy`) à chaque push |
| **Documentation** | Le `README.md` documente explicitement le port 5433 et les variables d'environnement requises |
| **Feuille de route** | `EXPLOITATION.md` liste la vérification des logs Airflow comme tâche mensuelle |
| **Alertes email** | `on_failure_callback` configuré sur les 3 DAGs — tout échec futur déclenche une alerte immédiate |

---

## 7. Enseignements

Cet incident illustre une problématique classique en data engineering :
**l'isolation des conteneurs Docker**. Un script Python qui fonctionne
parfaitement en local peut échouer dans un conteneur si l'environnement
d'exécution n'est pas correctement configuré.

Les trois principes retenus après cet incident :

1. **Toujours propager `os.environ`** dans les sous-processus Python
2. **Tester les DAGs dans Docker** dès le début du projet, pas seulement en local
3. **Utiliser `_PIP_ADDITIONAL_REQUIREMENTS`** plutôt qu'une image custom
   pour les petits projets — plus simple à maintenir

---

*Document maintenu dans le dépôt GitHub : `Justhamid/football-analytics-platform`*
