import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

cur = conn.cursor()

schemas = [
    "staging",
    "marts_clubs",
    "marts_players",
    "marts_ml"
]

for schema in schemas:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    print(f"Schéma créé : {schema}")

conn.commit()
cur.close()
conn.close()
print("\nPostgreSQL prêt.")