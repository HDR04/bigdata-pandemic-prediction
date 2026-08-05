import json
import time
import logging
import io
from datetime import datetime
import os

import pandas as pd
from kafka import KafkaProducer
from hdfs import InsecureClient

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration Kafka et topics
KAFKA_BOOTSTRAP_SERVERS = 'hadoop-master:9092'
TOPICS = {
    'mobility': 'mobility-data'
}

def create_producer():
    """Crée et retourne une instance de KafkaProducer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            acks='all',
            linger_ms=10,
            retry_backoff_ms=500
        )
        return producer
    except Exception as e:
        logger.error(f"Erreur création producteur Kafka: {str(e)}")
        raise

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
                parquet_files.append(path)
            elif status['type'] == 'DIRECTORY':
                # Parcours récursif du sous-dossier
                parquet_files.extend(list_parquet_files(client, path))
        return parquet_files
    except Exception as e:
        logger.error(f"Erreur lors du listing des fichiers dans {directory}: {str(e)}")
        raise

def process_mobility_data(hdfs_dir):
    """
    Traite les données de mobilité depuis tous les fichiers Parquet du répertoire HDFS.
    Pour chaque fichier trouvé, il lit le contenu avec pandas et génère un message par ligne.
    """
    try:
        # Configuration du client HDFS
        client = InsecureClient('http://hadoop-master:9870', user='user')
        logger.info(f"Listing des fichiers Parquet dans {hdfs_dir}")
        parquet_files = list_parquet_files(client, hdfs_dir)
        if not parquet_files:
            raise FileNotFoundError(f"Aucun fichier Parquet trouvé dans : {hdfs_dir}")
        else:
            logger.info(f"{len(parquet_files)} fichier(s) Parquet trouvé(s)")

        # Pour chaque fichier Parquet trouvé
        for file_path in parquet_files:
            logger.info(f"Lecture du fichier : {file_path}")
            with client.read(file_path) as reader:
                # Charger le contenu dans un DataFrame pandas
                df = pd.read_parquet(io.BytesIO(reader.read()))

            # Conversion de la colonne date
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])

            # Pour chaque ligne, construire le message à envoyer
            for _, row in df.iterrows():
                try:
                    message = {
                        "type": "mobility",
                        "timestamp": datetime.now().isoformat(),
                        "data": {
                            "date": row["date"].isoformat() if 'date' in row and pd.notnull(row["date"]) else None,
                            "country": str(row["country_region"]) if "country_region" in row else None,
                            "metrics": {
                                "retail": float(row["retail_and_recreation_percent_change_from_baseline"]) if "retail_and_recreation_percent_change_from_baseline" in row else None,
                                "grocery": float(row["grocery_and_pharmacy_percent_change_from_baseline"]) if "grocery_and_pharmacy_percent_change_from_baseline" in row else None,
                                "parks": float(row["parks_percent_change_from_baseline"]) if "parks_percent_change_from_baseline" in row else None,
                                "transit": float(row["transit_stations_percent_change_from_baseline"]) if "transit_stations_percent_change_from_baseline" in row else None,
                                "workplaces": float(row["workplaces_percent_change_from_baseline"]) if "workplaces_percent_change_from_baseline" in row else None,
                                "residential": float(row["residential_percent_change_from_baseline"]) if "residential_percent_change_from_baseline" in row else None
                            }
                        }
                    }
                    yield message
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping row due to data error: {e}")
                    continue
    except Exception as e:
        logger.error(f"Erreur traitement données mobilité: {str(e)}")
        raise

def send_message(producer, topic, data):
    """Envoie un message à Kafka"""
    try:
        future = producer.send(topic, value=data)
        result = future.get(timeout=10)
        logger.debug(f"Message sent to partition {result.partition}, offset {result.offset}")
        return True
    except Exception as e:
        logger.error(f"Erreur envoi message Kafka: {str(e)}")
        return False

def main():
    producer = None
    try:
        producer = create_producer()
        logger.info("Producteur Kafka créé avec succès")

        # Chemin du répertoire contenant les fichiers Parquet sur HDFS
        hdfs_dir = "/data/mobility/mobility_parquet"
        logger.info(f"Début du traitement des données Parquet depuis: {hdfs_dir}")

        mobility_data = process_mobility_data(hdfs_dir)
        messages_sent = 0

        for data in mobility_data:
            success = send_message(producer, TOPICS['mobility'], data)
            if success:
                messages_sent += 1
                if messages_sent % 100 == 0:
                    logger.info(f"Nombre de messages envoyés: {messages_sent}")
            time.sleep(0.5)  # Limitation de débit

        logger.info(f"Traitement terminé. Total messages envoyés: {messages_sent}")

    except Exception as e:
        logger.error(f"Erreur critique: {str(e)}")
    finally:
        if producer:
            try:
                producer.flush()
                producer.close(timeout=5)
                logger.info("Producteur Kafka fermé")
            except Exception as e:
                logger.error(f"Erreur lors de la fermeture du producteur: {str(e)}")

if __name__ == "__main__":
    main()