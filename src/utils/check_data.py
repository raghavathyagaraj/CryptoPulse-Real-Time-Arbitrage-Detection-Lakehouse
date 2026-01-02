from pyspark.sql import SparkSession
from delta import *
import os

# 1. Setup Spark with Delta Lake support
# We MUST include the packages again because this is a separate Spark process
builder = SparkSession.builder \
   .appName("CheckBronzeData") \
   .master("local[*]") \
   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
   .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0") \
   .config("spark.sql.shuffle.partitions", "1")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 2. Read the Bronze Table
bronze_path = "data/delta/bronze"

print(f"\n🔍 Inspecting Bronze Table at: {os.path.abspath(bronze_path)}")

if os.path.exists(bronze_path):
    df = spark.read.format("delta").load(bronze_path)
    
    # Count rows
    count = df.count()
    print(f"✅ Total Rows Found: {count}")
    
    if count > 0:
        print("\n📊 First 5 Rows (Raw JSON payload):")
        df.select("ingest_ts", "json_payload").show(5, truncate=False)
        
        print("\n🕒 Data Freshness (Latest Timestamps):")
        df.select("ingest_ts").orderBy("ingest_ts", ascending=False).show(3)
    else:
        print("⚠️ Table exists but is empty. Did you run the producer long enough?")
else:
    print("❌ Path does not exist! Run the 'ingest_bronze.py' job first.")