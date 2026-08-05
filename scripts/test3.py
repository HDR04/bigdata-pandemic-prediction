#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, lit, current_timestamp, struct, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, ArrayType
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors
import logging
from datetime import datetime
from hdfs import InsecureClient
import sys
import json

# Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration PostgreSQL avec driver intégré
POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://postgres:5432/bigdata",
    "user": "spark_user",
    "password": "SecurePass123!",
    "driver": "org.postgresql.Driver",
    "predictions_table": "predictions",
    "staging_table": "predictions_staging",
    "connection_properties": {
        "ssl": "false",
        "application_name": "spark_streaming"
    }
}

HDFS_PATHS = {
    "models_path": "hdfs:///data/mobility/models",
}

# UDF pour conversion Vector -> Array
def vector_to_array(v):
    try:
        return v.toArray().tolist()
    except Exception as e:
        logger.error(f"Vector conversion error: {str(e)}")
        return None

# Initialisation Spark avec configuration du driver
def create_spark_session():
    spark = SparkSession.builder \
        .appName("Mobility_RealTime_Prediction") \
        .master("local[2]") \
        .config("spark.jars", "/root/postgresql-42.7.3.jar") \
        .config("spark.driver.extraClassPath", "/root/postgresql-42.7.3.jar") \
        .config("spark.executor.extraClassPath", "/root/postgresql-42.7.3.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    spark.udf.register("vector_to_array", vector_to_array, ArrayType(DoubleType()))
    return spark

# Vérification du driver PostgreSQL
def verify_postgres_driver():
    try:
        from py4j.java_gateway import java_import
        gw = SparkSession.builder.getOrCreate().sparkContext._gateway
        java_import(gw.jvm, "org.postgresql.Driver")
        logger.info("PostgreSQL driver verified successfully")
        return True
    except Exception as e:
        logger.error(f"Driver verification failed: {str(e)}")
        return False

# Test de connexion PostgreSQL
def test_postgres_connection(spark):
    try:
        test_df = spark.read \
            .format("jdbc") \
            .option("url", POSTGRES_CONFIG["url"]) \
            .option("query", "SELECT 1 as test") \
            .option("user", POSTGRES_CONFIG["user"]) \
            .option("password", POSTGRES_CONFIG["password"]) \
            .option("driver", POSTGRES_CONFIG["driver"]) \
            .load()
        return test_df.first()["test"] == 1
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return False

# Chargement du modèle
def load_models(spark, hdfs_client):
    logger.info("Loading model from HDFS...")
    model_path = f"{HDFS_PATHS['models_path']}/LinearRegression"
    try:
        if hdfs_client.status(model_path.replace("hdfs://", "/"), strict=False):
            model = LinearRegressionModel.load(model_path)
            logger.info(f"Model loaded from {model_path}")
            return model
    except Exception as e:
        logger.error(f"Model loading failed: {str(e)}", exc_info=True)
        raise

# Sauvegarde des données avec gestion du driver
def save_to_postgres(df, epoch_id):
    try:
        # Validation des données
        df = df.withColumn("is_valid",
            when(col("features_json").isNotNull(), True).otherwise(False))

        # Écriture dans la table staging
        (df.write
            .format("jdbc")
            .option("url", POSTGRES_CONFIG["url"])
            .option("dbtable", POSTGRES_CONFIG["staging_table"])
            .option("user", POSTGRES_CONFIG["user"])
            .option("password", POSTGRES_CONFIG["password"])
            .option("driver", POSTGRES_CONFIG["driver"])
            .options(**POSTGRES_CONFIG["connection_properties"])
            .mode("append")
            .save())

        logger.info(f"Batch {epoch_id} saved to staging table")
    except Exception as e:
        logger.error(f"PostgreSQL save failed: {str(e)}", exc_info=True)
        raise

# Traitement des données
def process_batch(batch_df, batch_id, model, spark):
    epoch_id = int(datetime.now().timestamp())
    logger.info(f"Processing batch {batch_id} (epoch_id: {epoch_id})")

    try:
        # Schéma des données Kafka
        schema = StructType([
            StructField("type", StringType()),
            StructField("timestamp", StringType()),
            StructField("data", StructType([
                StructField("date", DateType()),
                StructField("country", StringType()),
                StructField("metrics", StructType([
                    StructField("retail", DoubleType()),
                    StructField("grocery", DoubleType()),
                    StructField("parks", DoubleType()),
                    StructField("transit", DoubleType()),
                    StructField("workplaces", DoubleType()),
                    StructField("residential", DoubleType())
                ]))
            ]))
        ])

        # Parsing des données
        parsed_df = batch_df.select(
            from_json(col("value").cast("string"), schema).alias("json")
        ).select("json.data.*")

        # Extraction des métriques
        metrics_df = parsed_df.select(
            col("date"),
            col("country"),
            col("metrics.retail").alias("retail"),
            col("metrics.grocery").alias("grocery"),
            col("metrics.parks").alias("parks"),
            col("metrics.transit").alias("transit"),
            col("metrics.workplaces").alias("workplaces"),
            col("metrics.residential").alias("residential")
        )

        # Feature engineering
        feature_cols = ["retail", "grocery", "parks", "transit", "workplaces", "residential"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        processed_df = assembler.transform(metrics_df)

        # Conversion des features
        vector_to_array_udf = udf(vector_to_array, ArrayType(DoubleType()))
        processed_df = processed_df.withColumn("features_array", vector_to_array_udf(col("features")))

        # Formatage pour PostgreSQL
        predictions = processed_df.select(
            col("date"),
            col("country"),
            col("residential"),
            to_json(struct(
                lit("vector").alias("type"),
                lit(len(feature_cols)).alias("size"),
                col("features_array").alias("values")
            )).alias("features_json"),
            lit("LinearRegression").alias("model"),
            lit(epoch_id).alias("epoch_id"),
            current_timestamp().alias("created_at")
        )

        # Sauvegarde
        save_to_postgres(predictions, epoch_id)

    except Exception as e:
        logger.error(f"Batch processing failed: {str(e)}", exc_info=True)
        raise

def main():
    spark = None
    query = None
    hdfs_client = InsecureClient('http://hadoop-master:9870', user='root')

    try:
        # Initialisation
        spark = create_spark_session()

        if not verify_postgres_driver():
            logger.error("PostgreSQL driver not available")
            sys.exit(1)

        if not test_postgres_connection(spark):
            logger.error("PostgreSQL connection test failed")
            sys.exit(1)

        # Chargement modèle
        model = load_models(spark, hdfs_client)

        # Configuration du flux Kafka
        df_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "hadoop-master:9092") \
            .option("subscribe", "mobility-data") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # Démarrage du traitement
        query = df_stream.writeStream \
            .foreachBatch(lambda df, id: process_batch(df, id, model, spark)) \
            .outputMode("update") \
            .option("checkpointLocation", "hdfs://hadoop-master:9000/checkpoints/mobility_prediction") \
            .start()

        logger.info("Stream processing started successfully")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal...")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
    finally:
        try:
            if query and query.isActive:
                query.stop()
        except Exception as e:
            logger.error(f"Error stopping query: {str(e)}")

        try:
            if spark:
                spark.stop()
        except Exception as e:
            logger.error(f"Error stopping Spark: {str(e)}")

        logger.info("Application shutdown completed")

if __name__ == "__main__":
    main()