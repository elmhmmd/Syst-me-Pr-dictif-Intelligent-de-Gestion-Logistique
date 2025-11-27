from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, window, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.ml.feature import VectorAssembler
import os

# --- CONFIGURATION ---
POSTGRES_URL = "jdbc:postgresql://localhost:5432/logistics_db"
POSTGRES_PROPS = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}
# Note: L'URI Mongo est géré dans les options du writer plus bas
MODEL_PATH = "delivery_risk_model" 

# --- SESSION SPARK ---
# Configuration avec les packages nécessaires pour Spark 4.0.1
spark = (SparkSession.builder
    .appName("LogisticsRealTimePrediction")
    .master("local[*]")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1,org.mongodb.spark:mongo-spark-connector_2.13:10.4.1")
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017")
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# --- CHARGEMENT DU MODÈLE ---
print("Chargement du modèle...")
try:
    model = CrossValidatorModel.load(MODEL_PATH)
    print("✅ Modèle chargé avec succès.")
except Exception as e:
    print(f"❌ Erreur chargement modèle: {e}")
    exit(1)

# --- SCHÉMA DES DONNÉES ENTRANTES ---
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("Days for shipment (scheduled)", IntegerType(), True),
    StructField("Category Id", IntegerType(), True),
    StructField("Order Item Quantity", IntegerType(), True),
    StructField("Shipping Mode", StringType(), True),
    StructField("distance", DoubleType(), True),
    StructField("Market", StringType(), True),
    StructField("Sales", DoubleType(), True)
])

# --- LECTURE STREAMING (TCP) ---
print("🎧 En attente de connexion sur localhost:9999...")
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parsing JSON
json_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# --- TRANSFORMATION (Feature Engineering) ---
processed_stream = json_stream.withColumn("ShippingMode_index", 
    when(col("Shipping Mode") == "Standard Class", 0.0)
    .when(col("Shipping Mode") == "First Class", 1.0)
    .when(col("Shipping Mode") == "Second Class", 2.0)
    .when(col("Shipping Mode") == "Same Day", 3.0)
    .otherwise(0.0)
).withColumn("timestamp", col("timestamp").cast(TimestampType()))

# --- PRÉDICTION ---
predictions = model.transform(processed_stream)

# Sélection des colonnes finales
final_stream = predictions.select(
    col("timestamp"),
    col("Order Item Quantity"),
    col("distance"),
    col("Shipping Mode"),
    col("prediction").alias("late_risk_prediction"),
    col("probability")
)

# --- FONCTION DE TRAITEMENT PAR MICRO-BATCH ---
def process_batch(df, epoch_id):
    # 1. Vérifier si le batch contient des données
    if df.count() > 0:
        print(f"🔄 Traitement du Batch {epoch_id}...")
        
        # --- ECRITURE POSTGRESQL ---
        try:
            print(f"💾 Écriture dans PostgreSQL...")
            df_postgres = df.select(
                "timestamp", "Order Item Quantity", "distance", "Shipping Mode", "late_risk_prediction"
            )
            # Conversion prediction en int pour postgres
            df_postgres = df_postgres.withColumn("late_risk_prediction", col("late_risk_prediction").cast("int"))
            
            df_postgres.write.jdbc(POSTGRES_URL, "predictions_realtime", mode="append", properties=POSTGRES_PROPS)
        except Exception as e:
            print(f"⚠️ Erreur écriture Postgres: {e}")

        # --- AGREGATION ---
        df_agg = df.groupBy("Shipping Mode").agg(
            {"late_risk_prediction": "mean", "distance": "mean", "*": "count"}
        ).withColumnRenamed("avg(late_risk_prediction)", "avg_risk") \
         .withColumnRenamed("avg(distance)", "avg_distance") \
         .withColumnRenamed("count(1)", "order_count") \
         .withColumn("batch_time", current_timestamp())

        # --- ECRITURE MONGODB ---
        # Tout ce bloc est indenté pour ne s'exécuter QUE si df_agg existe
        try:
            print(f"📊 Écriture dans MongoDB...")
            df_agg.write \
                .format("mongodb") \
                .mode("append") \
                .option("connection.uri", "mongodb://localhost:27017/") \
                .option("database", "logistics_db") \
                .option("collection", "aggregated_stats") \
                .save()
            print("✅ Succès écriture MongoDB")
        except Exception as e:
            print(f"⚠️ Erreur écriture MongoDB: {e}")
            
    else:
        # Si le batch est vide, on ne fait RIEN
        print(f"💤 Batch {epoch_id} vide, en attente de données...")

# --- DÉMARRAGE DU STREAM ---
query = final_stream.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()