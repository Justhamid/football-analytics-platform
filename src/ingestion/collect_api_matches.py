from pathlib import Path
from io import BytesIO
import json
import os
import time
import requests
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("FOOTBALL_DATA_API_TOKEN")

if not API_TOKEN:
    raise ValueError("FOOTBALL_DATA_API_TOKEN introuvable dans le fichier .env")

HEADERS = {"X-Auth-Token": API_TOKEN}

COMPETITIONS = ["PL", "PD", "SA", "BL1", "FL1"]
SEASONS = [2023, 2024]

# Seuils minimums plausibles par compétition (nb de matchs sur une saison complète)
MIN_MATCHES_ATTENDUS = {
    "PL": 350,
    "PD": 350,
    "SA": 350,
    "BL1": 280,
    "FL1": 280,
}

CHAMPS_OBLIGATOIRES = {"homeTeam", "awayTeam", "score", "utcDate", "status"}

BUCKET = "raw-football-api"

minio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9002"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)


def assurer_bucket() -> None:
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
        print(f"  -> Bucket '{BUCKET}' cree")


def valider_reponse(matches: list, competition_code: str, season: int) -> None:
    """
    Contrôle qualité post-collecte. Lève une exception si une anomalie
    est détectée, afin de déclencher l'alerte Airflow (on_failure_callback)
    même quand l'appel HTTP a techniquement réussi (200 OK) mais que les
    données renvoyées sont vides, incomplètes ou anormalement faibles.
    """
    if len(matches) == 0:
        raise ValueError(
            f"Collecte vide pour {competition_code}/{season} — "
            f"l'API a peut-être changé de format ou est indisponible."
        )

    seuil = MIN_MATCHES_ATTENDUS.get(competition_code, 250)
    if len(matches) < seuil:
        raise ValueError(
            f"Volume anormalement bas pour {competition_code}/{season} : "
            f"{len(matches)} matchs reçus (attendu >= {seuil})."
        )

    # Vérification structurelle sur un échantillon
    echantillon = matches[:5]
    for m in echantillon:
        champs_manquants = CHAMPS_OBLIGATOIRES - m.keys()
        if champs_manquants:
            raise ValueError(
                f"Structure JSON inattendue pour {competition_code}/{season} : "
                f"champs manquants {champs_manquants}."
            )


def Effectuer_requete_securisee(url: str, max_retries: int = 3) -> requests.Response:
    """
    Exécute une requête HTTP avec gestion automatique du Rate Limit (429).
    Si le plan gratuit (10 req/min) est dépassé, attend le temps nécessaire avant de re-tester.
    """
    for tentative in range(max_retries):
        response = requests.get(url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            return response
            
        elif response.status_code == 429:
            # Récupère le temps d'attente suggéré dans les headers ou prend 15s par défaut
            wait_time = int(response.headers.get("X-Request-Counter-Reset", 15))
            if wait_time <= 0:
                wait_time = 15
                
            print(f"  ⚠️ Rate limit (429) atteint pour l'API. Pause forcée de {wait_time}s (tentative {tentative + 1}/{max_retries})...")
            time.sleep(wait_time)
        else:
            # Pour les autres codes d'erreur (403, 500, etc.)
            return response

    return response


def collect_matches(competition_code: str, season: int) -> None:
    url = f"https://api.football-data.org/v4/competitions/{competition_code}/matches?season={season}"
    
    response = Effectuer_requete_securisee(url)

    print(f"{competition_code} {season} -> status {response.status_code}")

    if response.status_code != 200:
        raise RuntimeError(
            f"Erreur API pour {competition_code}/{season} : "
            f"status {response.status_code} — {response.text}"
        )

    data = response.json()

    if "matches" not in data:
        raise ValueError(
            f"Réponse API malformée pour {competition_code}/{season} : "
            f"clé 'matches' absente."
        )

    matches = data["matches"]
    valider_reponse(matches, competition_code, season)

    object_name = f"{competition_code}_{season}_matches.json"
    contenu = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    minio_client.put_object(
        BUCKET,
        object_name,
        data=BytesIO(contenu),
        length=len(contenu),
        content_type="application/json",
    )

    print(f"Enregistré dans MinIO : {BUCKET}/{object_name} | {len(matches)} matchs")


def main():
    assurer_bucket()
    erreurs = []

    for competition in COMPETITIONS:
        for season in SEASONS:
            try:
                collect_matches(competition, season)
            except Exception as e:
                print(f"❌ Échec {competition}/{season} : {e}")
                erreurs.append(f"{competition}/{season} : {e}")
            
            # Pause de 7 secondes entre chaque appel (seuil de sécurité pour le plan gratuit : max 10 req/min)
            time.sleep(7)

    if erreurs:
        resume = "\n".join(erreurs)
        raise RuntimeError(
            f"{len(erreurs)} collecte(s) en échec sur {len(COMPETITIONS) * len(SEASONS)} :\n{resume}"
        )

    print("✅ Toutes les collectes ont réussi.")


if __name__ == "__main__":
    main()