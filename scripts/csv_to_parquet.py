from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, to_date, count
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType
import sys
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initialise et retourne une session Spark"""
    return SparkSession.builder \
        .appName("CSV_to_Parquet") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "2g") \
        .getOrCreate()

def define_schema():
    """Définit le schéma des données"""
    return StructType([
        StructField("country_region_code", StringType(), True),
        StructField("country_region", StringType(), True),
        StructField("place_id", StringType(), True),
        StructField("date", DateType(), True),
        StructField("retail_and_recreation_percent_change_from_baseline", IntegerType(), True),
        StructField("grocery_and_pharmacy_percent_change_from_baseline", IntegerType(), True),
        StructField("parks_percent_change_from_baseline", IntegerType(), True),
        StructField("transit_stations_percent_change_from_baseline", IntegerType(), True),
        StructField("workplaces_percent_change_from_baseline", IntegerType(), True),
        StructField("residential_percent_change_from_baseline", IntegerType(), True)
    ])

def process_data(spark, input_path, output_path):
    """Traite les données et les sauvegarde en format Parquet"""
    try:
        # Lecture du CSV avec schéma prédéfini
        logger.info("Lecture du fichier CSV...")
        df = spark.read.csv(input_path, header=True, schema=define_schema())

        # Identification et suppression des colonnes avec plus de 50% de valeurs NULL
        logger.info("Analyse des colonnes nulles...")
        total_rows = df.count()
        null_counts = df.select([count(col(c)).alias(c) for c in df.columns]).collect()[0]
        cols_to_drop = [c for c in df.columns if null_counts[c] < total_rows * 0.5]

        if cols_to_drop:
            logger.warning(f"Colonnes supprimées (>50% NULL): {cols_to_drop}")
            df = df.drop(*cols_to_drop)

        # Ajout des colonnes de partitionnement
        logger.info("Ajout des colonnes de partitionnement...")
        df = df.withColumn("année", year(col("date"))) \
               .withColumn("mois", month(col("date"))) \
               .withColumn("jour", dayofmonth(col("date")))

        # Tri des données par année, mois et jour
        df = df.orderBy("année", "mois", "jour")

        # Optimisation et cache
        df = df.repartition("année", "mois")
        df.cache()

        # Écriture en Parquet
        logger.info("Écriture des données en format Parquet...")
        df.write.mode("overwrite") \
            .partitionBy("année", "mois", "jour") \
            .parquet(output_path)

        logger.info(f"Données stockées avec succès dans {output_path}")

        # Statistiques sur les données
        logger.info("Statistiques finales:")
        logger.info(f"Nombre total d'enregistrements: {df.count()}")
        logger.info(f"Nombre de colonnes: {len(df.columns)}")

    except Exception as e:
        logger.error(f"Erreur lors du traitement: {str(e)}")
        raise

def main():
    """Fonction principale"""
    input_path = "/data/mobility/2021_2022_MA_Region_Mobility_Report.csv"
    output_path = "/data/mobility/mobility_parquet"

    spark = None
    try:
        spark = create_spark_session()
        process_data(spark, input_path, output_path)
    except Exception as e:
        logger.error(f"Erreur critique: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("Session Spark fermée")

if __name__ == "__main__":
    main()