import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, max, min, first, last, avg, count, expr, current_timestamp
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType
from delta import *

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("GoldLayerJob")

# Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data"))
SILVER_TABLE_PATH = os.path.join(BASE_DIR, "delta/silver")
GOLD_TABLE_PATH = os.path.join(BASE_DIR, "delta/gold")
CHECKPOINT_LOCATION = os.path.join(BASE_DIR, "checkpoints/gold")

def create_spark_session():
    """Initialize Spark with Delta support."""
    # We must include the packages again for the local cluster to find them
    spark_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        "io.delta:delta-spark_2.12:3.3.0"
    ]
    
    return (SparkSession.builder
           .appName("CoinbaseGoldProcessor")
           .master("local[*]")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("spark.jars.packages", ",".join(spark_packages))
           .config("spark.sql.shuffle.partitions", "4") # Optimized for local
           .getOrCreate())

def process_gold_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Reading Silver Data from: {SILVER_TABLE_PATH}")
    
    # 1. Read Silver Stream
    silver_df = (spark.readStream
                .format("delta")
                .option("ignoreChanges", "true") 
                .load(SILVER_TABLE_PATH))

    # 2. Define Watermark & Window
    # We handle late data up to 10 minutes.
    # We group trades into 1-minute "Candles".
    windowed_df = (silver_df
                  .withWatermark("trade_timestamp", "10 minutes")
                  .groupBy(
                       window(col("trade_timestamp"), "1 minute"),
                       col("symbol")
                   )
                  .agg(
                       first("price").alias("open"),
                       max("price").alias("high"),
                       min("price").alias("low"),
                       last("price").alias("close"),
                       count("*").alias("trade_count"),
                       avg("price").alias("vwap") # Simple average for this demo (True VWAP requires volume)
                   )
                  .select(
                       col("window.start").alias("window_start"),
                       col("window.end").alias("window_end"),
                       col("symbol"),
                       col("open"),
                       col("high"),
                       col("low"),
                       col("close"),
                       col("trade_count"),
                       col("vwap"),
                       current_timestamp().alias("gold_processed_at")
                   ))

    # 3. Write to Gold Delta Table
    # OutputMode "append" means we only write the window once it is closed (after watermark passes)
    # This guarantees perfect data accuracy but introduces a slight delay.
    logger.info("Starting Gold Stream...")
    
    query = (windowed_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .option("path", GOLD_TABLE_PATH)
            .start())

    logger.info(f"Gold Stream Running! Writing to {GOLD_TABLE_PATH}")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping Gold Stream...")

if __name__ == "__main__":
    process_gold_stream()