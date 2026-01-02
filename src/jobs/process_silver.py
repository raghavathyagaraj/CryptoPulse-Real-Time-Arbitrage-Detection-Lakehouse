import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType, DecimalType
from delta import *

# ==============================================================================
# Configuration & Constants
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("SilverLayerJob")

# Paths (Dynamic based on where the script is run)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data"))
BRONZE_TABLE_PATH = os.path.join(BASE_DIR, "delta/bronze")
SILVER_TABLE_PATH = os.path.join(BASE_DIR, "delta/silver")
CHECKPOINT_LOCATION = os.path.join(BASE_DIR, "checkpoints/silver")

# ==============================================================================
# Schema Definition
# ==============================================================================

def get_coinbase_ticker_schema():
    """
    Schema for parsing the raw JSON from Coinbase.
    We read numbers as Strings first to avoid precision loss during parsing.
    """
    return (StructType()
      .add("type", StringType())
      .add("product_id", StringType())
      .add("price", StringType())
      .add("volume_24h", StringType())
      .add("time", StringType())
      .add("trade_id", StringType())
      .add("side", StringType())
    )

# ==============================================================================
# Spark Session Initialization
# ==============================================================================

def create_spark_session():
    """
    Initializes Spark with Delta Lake support.
    CRITICAL FIX: Explicitly adding the Delta Lake Maven packages.
    """
    logger.info("Initializing Spark Session...")
    
    # We only need the Delta library for this job (reading/writing Delta)
    spark_packages = [
        "io.delta:delta-spark_2.12:3.3.0"
    ]

    builder = SparkSession.builder \
     .appName("CoinbaseSilverProcessor") \
     .master("local[*]") \
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
     .config("spark.jars.packages", ",".join(spark_packages)) \
     .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
     .config("spark.sql.shuffle.partitions", "4") # Optimized for local use

    return builder.getOrCreate()

# ==============================================================================
# Main Processing Logic
# ==============================================================================

def process_silver_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # 1. READ FROM BRONZE
    logger.info(f"Reading from Bronze path: {BRONZE_TABLE_PATH}")
    
    if not os.path.exists(BRONZE_TABLE_PATH):
        logger.error(f"Bronze table not found at {BRONZE_TABLE_PATH}. Run ingest_bronze.py first!")
        sys.exit(1)

    bronze_df = spark.readStream \
     .format("delta") \
     .option("ignoreChanges", "true") \
     .load(BRONZE_TABLE_PATH)
    
    # 2. DESERIALIZE JSON
    # Use 'json_payload' to match what ingest_bronze.py produced
    input_column = "json_payload" 
    json_schema = get_coinbase_ticker_schema()
    
    logger.info(f"Parsing JSON from column: {input_column}")
    parsed_df = bronze_df.withColumn("parsed", from_json(col(input_column), json_schema))

    # 3. FILTERING
    # We allow both 'ticker' and 'ticker_batch' types
    filtered_df = parsed_df.filter(
        (col("parsed.type").isin("ticker", "ticker_batch")) & 
        (col("parsed.product_id").isNotNull())
    )

    # 4. TRANSFORMATION & TYPING
    silver_df = filtered_df.select(
        col("parsed.product_id").alias("symbol"),
        col("parsed.price").cast(DecimalType(18, 8)).alias("price"),
        col("parsed.time").cast(TimestampType()).alias("trade_timestamp"),
        col("parsed.side").alias("taker_side"),
        current_timestamp().alias("silver_processed_at")
    )
    
    # 5. WRITE TO SILVER
    logger.info("Starting WriteStream to Silver...")
    
    query = silver_df.writeStream \
     .format("delta") \
     .outputMode("append") \
     .option("checkpointLocation", CHECKPOINT_LOCATION) \
     .option("path", SILVER_TABLE_PATH) \
     .start()
    
    logger.info(f"Stream is running. Writing to {SILVER_TABLE_PATH}")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping stream...")

if __name__ == "__main__":
    process_silver_stream()