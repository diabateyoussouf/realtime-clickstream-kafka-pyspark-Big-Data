from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, when
from pyspark.sql.types import StructType, StructField, StringType

# 1. Le schéma des données JSON
schema = StructType([
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("user_id", StringType()),
    StructField("user_session", StringType()),
    StructField("price", StringType())
])

# Démarrage de Spark
spark = SparkSession.builder \
    .appName("CartAbandonmentDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Connexion à Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clickstream") \
    .option("startingOffsets", "earliest") \
    .load()

# Nettoyage et conversion
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("event_time").cast("timestamp"))

#  L'algorithme de détection
abandoned_carts_df = json_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("user_id"),
        col("user_session")
    ) \
    .agg(
        sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
        sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count")
    ) \
    .filter((col("cart_count") > 0) & (col("purchase_count") == 0))

# On aplatit la colonne "window" car un fichier CSV n'accepte pas les objets complexes
export_df = abandoned_carts_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("user_id"),
    col("user_session"),
    col("cart_count")
)

# Sauvegarde continue dans des fichiers CSV
print("⚡ Détection activée : Export des paniers abandonnés vers le dossier './exports_abandons'")

query = export_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "./exports_abandons") \
    .option("checkpointLocation", "./checkpoints_abandons") \
    .option("header", "true") \
    .start()

query.awaitTermination()