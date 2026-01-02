import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from delta import *
from dotenv import load_dotenv

# 1. Load Secrets
load_dotenv()
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_KEY = os.getenv('KAFKA_API_KEY')
KAFKA_SECRET = os.getenv('KAFKA_API_SECRET')

if not KAFKA_KEY:
    raise ValueError("API Keys missing! Check your.env file.")

# 2. Configure Local Spark Session
print("⏳ Starting Local Spark Cluster...")

# --- THIS WAS MISSING ---
# We define the exact Maven packages for Spark 3.5 + Scala 2.12
spark_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    "io.delta:delta-spark_2.12:3.3.0"
]

builder = SparkSession.builder \
  .appName("CryptoBronzeIngestion") \
  .master("local[*]") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .config("spark.jars.packages", ",".join(spark_packages)) \
  .config("spark.sql.shuffle.partitions", "4") # Optimize for local machine

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"Spark Session Created with Jars: {','.join(spark_packages)}")

# 3. Read Stream from Confluent Kafka
jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_KEY}" password="{KAFKA_SECRET}";'

print("Connecting to Kafka...")
raw_stream = spark.readStream \
 .format("kafka") \
 .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
 .option("kafka.security.protocol", "SASL_SSL") \
 .option("kafka.sasl.mechanism", "PLAIN") \
 .option("kafka.sasl.jaas.config", jaas_config) \
 .option("subscribe", "crypto_market_data") \
 .option("startingOffsets", "earliest") \
 .option("failOnDataLoss", "false") \
 .load()

# 4. Write Stream to Local Delta Table
print("Starting Stream...")
query = raw_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json_payload", "timestamp as kafka_ts") \
 .withColumn("ingest_ts", current_timestamp()) \
 .writeStream \
 .format("delta") \
 .outputMode("append") \
 .option("checkpointLocation", "data/checkpoints/bronze") \
 .option("path", "data/delta/bronze") \
 .start()

print(f"Streaming active! Data is writing to: {os.path.abspath('data/delta/bronze')}")
print("Press Ctrl+C to stop.")
query.awaitTermination()