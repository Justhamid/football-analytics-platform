from minio import Minio
from dotenv import load_dotenv
import os

load_dotenv()

client = Minio(
    "localhost:9000",
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

BUCKETS = [
    "raw-football-api",
    "raw-transfermarkt",
    "raw-football-datasets",
]

for bucket in BUCKETS:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Bucket créé : {bucket}")
    else:
        print(f"Bucket existant : {bucket}")