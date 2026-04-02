from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, when, round, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import os
import shutil

# --- CONFIGURATION DES CHEMINS ---
# On s'assure que les dossiers sont propres AVANT de démarrer Spark
CHECKPOINT_DIR = "./checkpoints_ml"
EXPORT_DIR = "./exports_ml"

if os.path.exists(CHECKPOINT_DIR):
    shutil.rmtree(CHECKPOINT_DIR)
if os.path.exists(EXPORT_DIR):
    shutil.rmtree(EXPORT_DIR)

os.makedirs(CHECKPOINT_DIR)
os.makedirs(EXPORT_DIR)

# 1. Schéma des données
schema = StructType([
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("category_code", StringType()),
    StructField("user_id", StringType()),
    StructField("user_session", StringType()),
    StructField("price", StringType())
])

# 2. Spark Session (Configurée pour ton PC local)
spark = SparkSession.builder \
    .appName("SparkEngine-Local-Host") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Lecture Kafka (ATTENTION : localhost car Spark tourne sur ton PC)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clickstream") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("event_time").cast("timestamp"))

# 4. Feature Engineering
features_df = json_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("user_id"),
        col("user_session")
    ) \
    .agg(
        sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
        sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
        sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        sum(when(col("event_type") == "cart", col("price").cast("float")).otherwise(0)).alias("total_cart_value")
    ) \
    .filter((col("cart_count") > 0) & (col("purchase_count") == 0))

# 5. Scoring
raw_score = (
    lit(40) + 
    when(col("total_cart_value") >= 1000, 35).when(col("total_cart_value") >= 500, 15).otherwise(0) +
    when(col("cart_count") > 1, 15).otherwise(0) +
    when((col("view_count") >= 5) & (col("view_count") <= 20), 10).when(col("view_count") > 20, -10).otherwise(0)
)

scored_df = features_df.withColumn(
    "propensity_score",
    when(raw_score > 99, 99).when(raw_score < 5, 5).otherwise(raw_score)
)

# 6. Export avec Timer
export_df = scored_df.select(
    col("window.start").alias("window_start"),
    col("user_id"),
    col("user_session"),
    col("cart_count"),
    col("view_count"),
    round(col("total_cart_value"), 2).alias("total_cart_value"),
    col("propensity_score"),
    current_timestamp().alias("processed_at") 
)

print("🚀 Moteur Spark LOCAL lancé sur localhost:9092")
print("📂 Nettoyage des dossiers exports_ml et checkpoints_ml terminé.")

# Écriture en CSV local
query = export_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", EXPORT_DIR) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .option("header", "true") \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()