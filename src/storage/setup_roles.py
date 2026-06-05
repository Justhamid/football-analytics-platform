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
conn.autocommit = True
cur = conn.cursor()


def executer(sql: str, description: str) -> None:
    try:
        cur.execute(sql)
        print(f"  ✅ {description}")
    except Exception as e:
        print(f"  ⚠️  {description} — {e}")


print("\n===== RÔLES ET SÉCURITÉ =====\n")

# ─── RÔLE LECTURE SEULE (Metabase) ────────────────────────────

print("Création rôle reader (Metabase)...")

executer("""
    CREATE ROLE football_reader WITH LOGIN PASSWORD 'reader_pass_2024';
""", "Rôle football_reader créé")

executer("""
    GRANT CONNECT ON DATABASE football_db TO football_reader;
""", "CONNECT accordé à football_reader")

executer("""
    GRANT USAGE ON SCHEMA marts_clubs TO football_reader;
""", "USAGE marts_clubs accordé")

executer("""
    GRANT USAGE ON SCHEMA marts_players TO football_reader;
""", "USAGE marts_players accordé")

executer("""
    GRANT USAGE ON SCHEMA marts_ml TO football_reader;
""", "USAGE marts_ml accordé")

executer("""
    GRANT SELECT ON ALL TABLES IN SCHEMA marts_clubs TO football_reader;
""", "SELECT marts_clubs accordé")

executer("""
    GRANT SELECT ON ALL TABLES IN SCHEMA marts_players TO football_reader;
""", "SELECT marts_players accordé")

executer("""
    GRANT SELECT ON ALL TABLES IN SCHEMA marts_ml TO football_reader;
""", "SELECT marts_ml accordé")

# Accès automatique aux futures tables
executer("""
    ALTER DEFAULT PRIVILEGES IN SCHEMA marts_clubs
    GRANT SELECT ON TABLES TO football_reader;
""", "DEFAULT PRIVILEGES marts_clubs")

executer("""
    ALTER DEFAULT PRIVILEGES IN SCHEMA marts_players
    GRANT SELECT ON TABLES TO football_reader;
""", "DEFAULT PRIVILEGES marts_players")

executer("""
    ALTER DEFAULT PRIVILEGES IN SCHEMA marts_ml
    GRANT SELECT ON TABLES TO football_reader;
""", "DEFAULT PRIVILEGES marts_ml")

# ─── RÔLE ÉCRITURE (ETL) ──────────────────────────────────────

print("\nCréation rôle writer (ETL pipelines)...")

executer("""
    CREATE ROLE football_writer WITH LOGIN PASSWORD 'writer_pass_2024';
""", "Rôle football_writer créé")

executer("""
    GRANT CONNECT ON DATABASE football_db TO football_writer;
""", "CONNECT accordé à football_writer")

for schema in ["staging", "marts_clubs", "marts_players", "marts_ml"]:
    executer(f"""
        GRANT USAGE, CREATE ON SCHEMA {schema} TO football_writer;
    """, f"USAGE+CREATE {schema} accordé")

    executer(f"""
        GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
        ON ALL TABLES IN SCHEMA {schema} TO football_writer;
    """, f"DML {schema} accordé")

    executer(f"""
        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema}
        GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
        ON TABLES TO football_writer;
    """, f"DEFAULT PRIVILEGES {schema}")

# ─── RÉVOQUER ACCÈS STAGING AU READER ─────────────────────────

print("\nSécurisation staging (pas accessible au reader)...")

executer("""
    REVOKE ALL ON SCHEMA staging FROM football_reader;
""", "staging révoqué pour football_reader")

# ─── VÉRIFICATION ─────────────────────────────────────────────

print("\nVérification des rôles...")
cur.execute("""
    SELECT rolname, rolcanlogin
    FROM pg_roles
    WHERE rolname IN ('football_reader', 'football_writer', 'football_user')
    ORDER BY rolname;
""")
roles = cur.fetchall()
for role, can_login in roles:
    print(f"  → {role} (login={can_login})")

print("\n✅ Rôles et sécurité configurés.")
print("\n  Résumé des accès :")
print("  football_reader → SELECT sur marts_* uniquement (Metabase)")
print("  football_writer → DML sur staging + marts_* (ETL pipelines)")
print("  football_user   → superuser du projet (admin)")

cur.close()
conn.close()