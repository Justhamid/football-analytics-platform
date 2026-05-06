from minio import Minio
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

client = Minio(
    "localhost:9000",
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)


def upload_folder(local_folder: Path, bucket: str, prefix: str = "") -> None:
    fichiers = sorted([f for f in local_folder.rglob("*") if f.is_file()])

    if not fichiers:
        print(f"Aucun fichier dans {local_folder}")
        return

    for fichier in fichiers:
        chemin_minio = f"{prefix}/{fichier.relative_to(local_folder)}".replace("\\", "/")
        taille = fichier.stat().st_size / (1024 * 1024)

        client.fput_object(
            bucket_name=bucket,
            object_name=chemin_minio,
            file_path=str(fichier)
        )
        print(f"{bucket}/{chemin_minio} ({taille:.1f} MB)")


def main():
    print("\n===== UPLOAD VERS MINIO =====\n")

    # API football-data
    print("--- API football-data ---")
    upload_folder(
        local_folder=Path("data/brut/api"),
        bucket="raw-football-api",
        prefix="football_api"
    )

    # Transfermarkt
    print("\n--- Transfermarkt ---")
    upload_folder(
        local_folder=Path("data/brut/transfermarkt"),
        bucket="raw-transfermarkt",
        prefix="transfermarkt"
    )

    # football-datasets
    print("\n--- football-datasets ---")
    upload_folder(
        local_folder=Path("data/brut/football_datasets"),
        bucket="raw-football-datasets",
        prefix="football_datasets"
    )

    print("\nUpload terminé.")


if __name__ == "__main__":
    main()