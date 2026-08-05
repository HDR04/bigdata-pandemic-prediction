# Prédiction des Pandémies — Architecture Big Data (Hadoop / Spark / Kafka)

**Auteur :** Drissi Houssam

Mise en place d'une architecture Big Data scalable pour l'analyse en temps réel de données de mobilité, appliquée à la prédiction de la propagation des pandémies.

---

## 1. Contexte et objectifs

La prédiction des pandémies nécessite l'analyse de vastes volumes de données hétérogènes (santé publique, mobilité, réseaux sociaux). Ce projet propose une architecture Big Data combinant :

- **Collecte en temps réel** (Kafka)
- **Traitement distribué** (Spark Streaming)
- **Modélisation prédictive** (MLlib)
- **Visualisation interactive** (Streamlit / Grafana)

**Objectifs :**
- Prédire la propagation des pandémies via des indicateurs clés (taux de cas, zones à risque)
- Fournir des dashboards temps réel pour les décideurs publics

## 2. Architecture globale

**Flux de données :** `Kafka → Spark Streaming → PostgreSQL → Streamlit`

| Composant | Rôle |
|---|---|
| Apache Kafka | Ingestion des flux de données |
| Spark Streaming | Traitement temps réel (agrégation, nettoyage) |
| HDFS | Stockage distribué des données historiques (format Parquet partitionné) |
| MLlib | Entraînement de modèles (régression linéaire, Random Forest) |
| PostgreSQL | Stockage des prédictions pour analyse OLAP |
| Streamlit | Dashboard interactif (visualisation des prédictions par pays/date) |

## 3. Données

**Source :** [Google Mobility Reports](https://www.google.com/covid19/mobility/) — données de mobilité pour le Maroc (2021-2022).

Métriques principales : `retail_and_recreation`, `grocery_and_pharmacy`, `parks`, `transit_stations`, `workplaces`, `residential` (variations en % par rapport à une baseline).

Le CSV brut est converti au format **Parquet** (partitionné par année/mois/jour, compression Snappy) via `csv_to_parquet.py`, avec suppression des colonnes contenant plus de 50 % de valeurs nulles — réduction de taille d'environ 70 %.

## 4. Pipeline de traitement

**Producer** (`producer.py`) — lit les fichiers Parquet depuis HDFS et publie chaque enregistrement en JSON sur le topic Kafka `mobility-data`.

**Consumer / Streaming** (`test3.py`) — consomme le flux Kafka avec Spark Structured Streaming, agrège par fenêtres de 15 minutes, applique le modèle ML pré-entraîné, et écrit les prédictions dans PostgreSQL.

## 5. Modélisation prédictive

`train_model.py` entraîne un modèle de régression linéaire (Spark MLlib) à partir des 6 indicateurs de mobilité, avec comme cible `retail_and_recreation_percent_change_from_baseline` (proxy de propagation virale).

**Résultats obtenus :**
- Précision : ~89 % sur les données historiques
- Latence temps réel : < 1 minute (de Kafka à Streamlit)

Le modèle entraîné est sauvegardé dans HDFS et rechargé en temps réel par le pipeline de streaming.

## 6. Visualisation

**Dashboard Streamlit** (`app.py`) :
- Filtrage par pays / date / modèle
- Courbes d'évolution des métriques (Plotly)
- Cartes thermiques et boîtes à moustaches des zones à risque

**Grafana** (complémentaire) : surveillance temps réel du taux de nouveaux cas et de l'efficacité des mesures de confinement.

## 7. Environnement Docker

Tout le projet a été développé dans un environnement Docker unique regroupant **Hadoop, Spark, Kafka et PostgreSQL**, construit à partir d'une image de base Hadoop (cluster 3 nœuds) enrichie manuellement avec Kafka, Spark et PostgreSQL.

### 7.1 Image de base — cluster Hadoop (3 nœuds)

```bash
docker pull elmendili/bigdata-hadoop:first
```

**Créer le réseau Docker :**
```bash
docker network create --driver=bridge hadoop
```

**Lancer les 3 conteneurs :**

Master (NameNode + ResourceManager) :
```bash
docker run -itd --net=hadoop \
  -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 \
  --name hadoop-master --hostname hadoop-master \
  elmendili/bigdata-hadoop:first
```

Slave 1 (DataNode + NodeManager) :
```bash
docker run -itd -p 8040:8042 --net=hadoop \
  --name hadoop-slave1 --hostname hadoop-slave1 \
  elmendili/bigdata-hadoop:first
```

Slave 2 (DataNode + NodeManager) :
```bash
docker run -itd -p 8041:8042 --net=hadoop \
  --name hadoop-slave2 --hostname hadoop-slave2 \
  elmendili/bigdata-hadoop:first
```

**Vérifier et démarrer le cluster :**
```bash
docker ps                              # les 3 conteneurs doivent être "Up"
docker exec -it hadoop-master bash     # entrer dans le master
./start-hadoop.sh                      # démarre HDFS + YARN
```

**Interfaces web :**

| Interface | URL | Description |
|---|---|---|
| NameNode (HDFS) | http://localhost:9870 | État du cluster HDFS |
| ResourceManager (YARN) | http://localhost:8088 | Suivi des jobs MapReduce/Spark |

### 7.2 Services additionnels (installés manuellement dans `hadoop-master`)

| Service | Rôle dans le projet | Port |
|---|---|---|
| Apache Kafka | Broker de messages (`producer.py` → `test3.py`) | 9092 |
| Apache Spark | Traitement batch et streaming | 7077 |
| PostgreSQL | Stockage des prédictions (lu par `app.py`) | 5432 |

Ces services tournent dans le même conteneur `hadoop-master` et communiquent en local (hostname `hadoop-master`).

### 7.3 Publier l'image personnalisée sur Docker Hub

Une fois Kafka/Spark/PostgreSQL installés dans `hadoop-master`, on peut figer cet état dans une image et la publier :

```bash
docker login
docker commit hadoop-master <votre-pseudo-dockerhub>/bigdata-pandemic-prediction:latest
docker push <votre-pseudo-dockerhub>/bigdata-pandemic-prediction:latest
```

### 7.4 Reproduire l'environnement complet à partir de l'image publiée

```bash
docker network create --driver=bridge hadoop

docker run -itd --net=hadoop \
  -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 \
  -p 9092:9092 -p 5432:5432 \
  --name hadoop-master --hostname hadoop-master \
  <votre-pseudo-dockerhub>/bigdata-pandemic-prediction:latest

docker exec -it hadoop-master bash
./start-hadoop.sh
```

Puis démarrer Kafka et PostgreSQL dans le conteneur, et exécuter les scripts du dossier `scripts/` (voir section 9 ci-dessous).

## 8. Structure du dépôt

```
.
├── scripts/
│   ├── csv_to_parquet.py   # Conversion CSV → Parquet (nettoyage, partitionnement)
│   ├── producer.py         # Producer Kafka (HDFS Parquet → Kafka JSON)
│   ├── train_model.py      # Entraînement du modèle MLlib
│   ├── test3.py            # Consumer Spark Streaming + prédiction temps réel → PostgreSQL
│   └── app.py               # Dashboard Streamlit
├── notebooks/
│   └── scripts.ipynb       # Notebook d'exploration et de développement
├── data/
│   └── 2021_2022_MA_Region_Mobility_Report.csv
├── docs/
│   └── rapport_projet.pdf  # Rapport complet du projet
└── README.md
```

## 9. Commandes clés

```bash
# Conversion des données
spark-submit scripts/csv_to_parquet.py

# Lancer le producer Kafka
python3 scripts/producer.py

# Entraîner le modèle
spark-submit scripts/train_model.py

# Soumettre le job de streaming/prédiction
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 scripts/test3.py

# Lancer le dashboard
streamlit run scripts/app.py
```

## 10. Pistes d'amélioration

- Intégrer des données météo (impact sur la propagation)
- Passer à un modèle Deep Learning (LSTM) pour les séries temporelles

## 11. Références

- [Documentation Spark MLlib](https://spark.apache.org/docs/latest/ml-guide.html)
- [Documentation Streamlit](https://docs.streamlit.io/)
- [Google COVID-19 Community Mobility Reports](https://www.google.com/covid19/mobility/)
