from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import os

spark = SparkSession.builder.master("local[*]").appName("CheckGold") \
   .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0") \
   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
   .getOrCreate()

gold_path = "data/delta/gold"

if os.path.exists(gold_path):
    print("📊 Reading Gold Data (OHLC Candles)...")
    df = spark.read.format("delta").load(gold_path)
    df.orderBy(desc("window_start")).show(20, truncate=False)
else:
    print("⏳ Gold data not ready yet. Keep the orchestrator running!")