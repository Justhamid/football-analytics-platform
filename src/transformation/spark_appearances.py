"""
Traitement distribué des apparitions avec PySpark.
Justification : 1.8 million de lignes nécessitent un traitement
partitionné et parallélisable — PySpark permet de scaler
horizontalement si le volume augmente.
"""
import os

# JAVA_HOME : chemin Windows en local, sinon on garde celui deja configure
# dans l'environnement (ex : /usr/lib/jvm/java-17-openjdk-amd64 dans Docker)
if os.name == "nt":
    os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
    os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DateType
)
import os
import tempfile
from pathlib import Path
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST_JDBC = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT_JDBC = os.getenv('POSTGRES_PORT', '5433')
POSTGRES_URL  = f"jdbc:postgresql://{POSTGRES_HOST_JDBC}:{POSTGRES_PORT_JDBC}/{os.getenv('POSTGRES_DB')}"
POSTGRES_PROPS = {
    "user":     os.getenv('POSTGRES_USER'),
    "password": os.getenv('POSTGRES_PASSWORD'),
    "driver":   "org.postgresql.Driver"
}

BUCKET_TM = "raw-transfermarkt"

minio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9002"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)


def telecharger_depuis_minio(object_name: str, dest_dir: Path) -> str:
    """Telecharge un objet MinIO vers un fichier temporaire local, pour lecture par Spark."""
    dest_path = dest_dir / object_name
    minio_client.fget_object(BUCKET_TM, object_name, str(dest_path))
    return str(dest_path)


def creer_session_spark() -> SparkSession:
    print("Initialisation SparkSession...")
    chemin_jar = os.path.abspath("jars/postgresql-42.7.13.jar")
    spark = SparkSession.builder \
        .appName("FootballAnalytics_Appearances") \
        .master("local[*]") \
        .config("spark.driver.extraClassPath", chemin_jar) \
        .config("spark.executor.extraClassPath", chemin_jar) \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions",
                "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print(f"  → Spark {spark.version} initialisé")
    print(f"  → Cores disponibles : {spark.sparkContext.defaultParallelism}")
    return spark

def charger_appearances(spark: SparkSession, dest_dir: Path) -> "DataFrame":
    print("\nTelechargement appearances.csv depuis MinIO...")
    chemin_local = telecharger_depuis_minio("appearances.csv", dest_dir)

    print("Chargement appearances.csv avec Spark...")
    df = spark.read.csv(
        chemin_local,
        header=True,
        inferSchema=True,
        encoding="UTF-8"
    )

    print(f"  → {df.count()} lignes chargées")
    print(f"  → {df.rdd.getNumPartitions()} partitions")
    print(f"  → Colonnes : {df.columns}")
    return df

def charger_players(spark: SparkSession, dest_dir: Path) -> "DataFrame":
    print("\nTelechargement players.csv depuis MinIO...")
    chemin_local = telecharger_depuis_minio("players.csv", dest_dir)

    print("Chargement players.csv avec Spark...")
    df = spark.read.csv(
        chemin_local,
        header=True,
        inferSchema=True,
        encoding="UTF-8"
    ).select("player_id", "position", "market_value_in_eur")
    print(f"  → {df.count()} joueurs chargés")
    return df

def transformer_appearances(df: "DataFrame", df_players: "DataFrame") -> "DataFrame":
    print("\nTransformation avec Spark...")

    df_joined = df.join(df_players, on="player_id", how="left")

    df_clean = df_joined \
        .filter(F.col("minutes_played") > 0) \
        .filter(F.col("player_id").isNotNull()) \
        .filter(F.col("date") >= "2017-01-01") \
        .withColumn("goals",
            F.when(F.col("goals") < 0, 0).otherwise(F.col("goals").cast(IntegerType()))
        ) \
        .withColumn("assists",
            F.when(F.col("assists") < 0, 0).otherwise(F.col("assists").cast(IntegerType()))
        ) \
        .withColumn("minutes_played",
            F.col("minutes_played").cast(IntegerType())
        ) \
        .withColumn("yellow_cards",
            F.when(F.col("yellow_cards") < 0, 0).otherwise(F.col("yellow_cards").cast(IntegerType()))
        ) \
        .withColumn("red_cards",
            F.when(F.col("red_cards") < 0, 0).otherwise(F.col("red_cards").cast(IntegerType()))
        ) \
        .dropDuplicates(["appearance_id"])

    print(f"  → {df_clean.count()} lignes après nettoyage")
    return df_clean


def calculer_performance_spark(df: "DataFrame") -> "DataFrame":
    print("\nCalcul player_performance avec Spark (partitionné par player_id)...")

    # Repartitionner par player_id pour paralléliser les agrégations
    df_repartitioned = df.repartition(8, "player_id")

    df_filtered = df_repartitioned \
        .filter(F.col("market_value_in_eur").isNotNull()) \
        .filter(F.col("market_value_in_eur") > 0)

    df_perf = df_filtered \
        .groupBy("player_id", "player_name", "position", "market_value_in_eur", "competition_id") \
        .agg(
            F.count("*").alias("matchs_joues"),
            F.sum("goals").alias("total_goals"),
            F.sum("assists").alias("total_assists"),
            F.sum("minutes_played").alias("total_minutes"),
            F.sum("yellow_cards").alias("total_yellow_cards"),
            F.sum("red_cards").alias("total_red_cards"),
            F.avg("minutes_played").alias("avg_minutes_per_match"),
        ) \
        .withColumn("goals_per_90",
            F.round(F.col("total_goals") * 90.0 / F.nullif(F.col("total_minutes"), F.lit(0)), 3)
        ) \
        .withColumn("assists_per_90",
            F.round(F.col("total_assists") * 90.0 / F.nullif(F.col("total_minutes"), F.lit(0)), 3)
        ) \
        .withColumn("goal_contributions_per_90",
            F.round((F.col("total_goals") + F.col("total_assists")) * 90.0 / F.nullif(F.col("total_minutes"), F.lit(0)), 3)
        ) \
        .withColumn("value_efficiency",
            F.round(
                (F.col("total_goals") + F.col("total_assists")) * 90.0 /
                F.nullif(F.col("total_minutes"), F.lit(0)) /
                F.nullif(F.col("market_value_in_eur") / 1000000.0, F.lit(0)),
                4
            )
        ) \
        .filter(F.col("total_minutes") >= 90)

    print(f"  → {df_perf.count()} profils de performance calculés")
    return df_perf


def sauvegarder_postgres(df: "DataFrame") -> None:
    print("\nSauvegarde -> PostgreSQL marts_players.player_performance")
    df.write \
        .mode("overwrite") \
        .jdbc(
            url=POSTGRES_URL,
            table="marts_players.player_performance",
            properties=POSTGRES_PROPS
        )
    print(f"  -> {df.count()} lignes ecrites dans marts_players.player_performance")


def afficher_stats(df: "DataFrame") -> None:
    print("\nTop 10 joueurs par goals_per_90 :")
    df.filter(F.col("total_minutes") >= 500) \
      .orderBy(F.col("goals_per_90").desc()) \
      .select("player_id", "competition_id",
              "total_goals", "total_minutes", "goals_per_90") \
      .show(10)

    print("\nRépartition par compétition :")
    df.groupBy("competition_id") \
      .agg(F.count("*").alias("nb_joueurs")) \
      .orderBy(F.col("nb_joueurs").desc()) \
      .show(20)


def main():
    print("\n===== SPARK — TRAITEMENT APPEARANCES =====\n")

    spark = creer_session_spark()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        df_appearances = charger_appearances(spark, tmp_path)
        df_players_ref = charger_players(spark, tmp_path)
        df_clean       = transformer_appearances(df_appearances, df_players_ref)
        df_perf        = calculer_performance_spark(df_clean)

        afficher_stats(df_perf)

        sauvegarder_postgres(df_perf)

    print("\n✅ Traitement Spark terminé.")
    spark.stop()


if __name__ == "__main__":
    main()