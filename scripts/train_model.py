from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from hdfs import InsecureClient
import os
import logging
from pyspark.sql import SparkSession  # Import SparkSession

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def list_parquet_files(client, directory):
    """
    Liste récursivement tous les fichiers Parquet dans le répertoire donné sur HDFS.
    """
    parquet_files = []
    try:
        items = client.list(directory, status=True)
        for item, status in items:
            path = os.path.join(directory, item)
            if status['type'] == 'FILE' and item.endswith('.parquet'):
                parquet_files.append(f"hdfs://hadoop-master:9000{path}")
            elif status['type'] == 'DIRECTORY':
                # Parcours récursif du sous-dossier
                parquet_files.extend(list_parquet_files(client, path))
        return parquet_files
    except Exception as e:
        logger.error(f"Erreur lors du listing des fichiers dans {directory}: {str(e)}")
        return []

def load_and_prepare_data(hdfs_dir, spark):  # Added spark parameter
    """
    Charge et prépare les données de mobilité depuis tous les fichiers Parquet dans HDFS.
    """
    try:
        # Configuration du client HDFS
        client = InsecureClient('http://hadoop-master:9870', user='root')  # Utilisateur root
        logger.info(f"Listing des fichiers Parquet dans {hdfs_dir}")
        parquet_files = list_parquet_files(client, hdfs_dir)
        if not parquet_files:
            logger.error(f"Aucun fichier Parquet trouvé dans : {hdfs_dir}")
            return None
        else:
            logger.info(f"{len(parquet_files)} fichier(s) Parquet trouvé(s) : {parquet_files}")

        # Charger tous les fichiers Parquet avec Spark
        df = spark.read.parquet(*parquet_files).limit(7)  # Limiter à 10 lignes pour test
        df = df.dropna(thresh=5)
        feature_cols = [c for c in df.columns
                       if c not in ["country_region_code", "place_id", "date",
                                   "country_region", "retail_and_recreation_percent_change_from_baseline"]]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        return assembler.transform(df)
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données : {str(e)}")
        return None

def train_and_evaluate_model(model, data, model_name, model_path):
    logger.info(f"Entraînement du modèle {model_name}...")
    trained_model = model.fit(data)

    # Evaluation du modèle
    predictions = trained_model.transform(data)
    evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="retail_and_recreation_percent_change_from_baseline")

    rmse = evaluator.setMetricName("rmse").evaluate(predictions)
    r2 = evaluator.setMetricName("r2").evaluate(predictions)
    mae = evaluator.setMetricName("mae").evaluate(predictions)

    # Affichage des métriques dans la console
    logger.info(f"Metrics pour le modèle {model_name}:")
    logger.info(f"  RMSE (Root Mean Squared Error) : {rmse}")
    logger.info(f"  R² (Coefficient de détermination) : {r2}")
    logger.info(f"  MAE (Mean Absolute Error) : {mae}")

    model_save_path = f"hdfs://hadoop-master:9000{model_path}/{model_name}"
    trained_model.save(model_save_path)
    logger.info(f"Modèle {model_name} sauvegardé dans {model_save_path}")

def main():
    # Créer une SparkSession avec ressources minimales
    spark = SparkSession.builder \
        .appName("MobilityModelTraining") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    config = {
        "input_path": "/data/mobility/mobility_parquet",  # Chemin HDFS sans préfixe
        "models_path": "/data/mobility/models"
    }

    try:
        # Charger et préparer les données
        prepared_data = load_and_prepare_data(config["input_path"], spark)  # Pass spark to the function
        if prepared_data is None:
            logger.error("Échec de la préparation des données. Arrêt.")
            return

        # Configurer et entraîner le modèle
        model = LinearRegression(featuresCol="features", labelCol="retail_and_recreation_percent_change_from_baseline")
        train_and_evaluate_model(model, prepared_data, "LinearRegression", config["models_path"])

        logger.info("Entraînement terminé avec succès")
    except Exception as e:
        logger.error(f"Erreur dans main : {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()