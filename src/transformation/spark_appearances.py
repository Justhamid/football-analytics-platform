"""
Traitement distribué des apparitions avec PySpark.
Justification : 1.8 million de lignes nécessitent un traitement
partitionné et parallélisable — PySpark permet de scaler
horizontalement si le volume augmente.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DateType
)
import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_URL  = f"jdbc:postgresql://localhost:5432/{os.getenv('POSTGRES_DB')}"
POSTGRES_PROPS = {
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver":   "org.postgresql.Driver"
}


def creer_session_spark() -> SparkSession:
    print("Initialisation SparkSession...")
    spark = SparkSession.builder \
        .appName("FootballAnalytics_Appearances") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"  → Spark {spark.version} initialisé")
    print(f"  → Cores disponibles : {spark.sparkContext.defaultParallelism}")
    return spark


def charger_appearances(spark: SparkSession) -> "DataFrame":
    print("\nChargement appearances.csv avec Spark...")

    df = spark.read.csv(
        "data/brut/transfermarkt/appearances.csv",
        header=True,
        inferSchema=True,
        encoding="UTF-8"
    )

    print(f"  → {df.count()} lignes chargées")
    print(f"  → {df.rdd.getNumPartitions()} partitions")
    print(f"  → Colonnes : {df.columns}")
    return df


def transformer_appearances(df: "DataFrame") -> "DataFrame":
    print("\nTransformation avec Spark...")

    df_clean = df \
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

    df_perf = df_repartitioned \
        .groupBy("player_id", "competition_id") \
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
            F.round(
                F.col("total_goals") * 90.0 /
                F.nullif(F.col("total_minutes"), F.lit(0)),
                3
            )
        ) \
        .withColumn("assists_per_90",
            F.round(
                F.col("total_assists") * 90.0 /
                F.nullif(F.col("total_minutes"), F.lit(0)),
                3
            )
        ) \
        .withColumn("goal_contributions_per_90",
            F.round(
                (F.col("total_goals") + F.col("total_assists")) * 90.0 /
                F.nullif(F.col("total_minutes"), F.lit(0)),
                3
            )
        ) \
        .filter(F.col("total_minutes") >= 90)

    print(f"  → {df_perf.count()} profils de performance calculés")
    return df_perf


def sauvegarder_csv(df: "DataFrame", chemin: str) -> None:
    print(f"\nSauvegarde → {chemin}")
    df.coalesce(1).write.csv(
        chemin,
        header=True,
        mode="overwrite"
    )
    print(f"  → Sauvegardé")


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

    df_appearances = charger_appearances(spark)
    df_clean       = transformer_appearances(df_appearances)
    df_perf        = calculer_performance_spark(df_clean)

    afficher_stats(df_perf)

    # Sauvegarde CSV pour intégration dans le pipeline
    sauvegarder_csv(df_perf, "data/traite/spark_player_performance")

    print("\n✅ Traitement Spark terminé.")
    spark.stop()


if __name__ == "__main__":
    main()