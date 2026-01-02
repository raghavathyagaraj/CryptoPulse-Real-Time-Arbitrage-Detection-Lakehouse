import sys
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col

def create_spark_session() -> SparkSession:
    """
    Initializes a SparkSession with the specific Delta Lake configurations
    required for local execution.
    
    It leverages 'configure_spark_with_delta_pip' to automatically 
    inject the correct Maven coordinates for delta-spark 3.3.0 
    and pyspark 3.5.3.
    """
    builder = (
        SparkSession.builder
       .appName("BronzeLayerInspector")
       .master("local[*]")  # Use all available cores for batch processing
       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # UI optimization for local usage
       .config("spark.ui.enabled", "false")
        # Optimize shuffle for small local datasets (default is 200)
       .config("spark.sql.shuffle.partitions", "4")
    )
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def inspect_bronze_table(spark: SparkSession, table_path: str):
    """
    Performs a batch read of the Delta table and outputs diagnostic statistics.
    """
    # Resolve to absolute path to avoid ambiguity in Spark context
    abs_path = os.path.abspath(table_path)
    
    print(f"\n{'='*80}")
    print(f"Inspecting Delta Table (Bronze Layer)")
    print(f"Location: {abs_path}")
    print(f"{'='*80}\n")

    if not os.path.exists(abs_path):
        print(f"ERROR: Path not found: {abs_path}")
        print("Ensure the ingestion pipeline has run and the path is correct.")
        return

    try:
        # Load the data using the Delta source format
        # [10] - reading delta via format("delta")
        df = spark.read.format("delta").load(abs_path)
        
        # 1. Schema Verification
        print("--- [1] Schema Definition ---")
        df.printSchema()
        
        # 2. Volume Analysis
        # Note: In Delta, count() can often use metadata statistics for speed
        print("--- [2] Data Volume ---")
        record_count = df.count()
        print(f"Total Records: {record_count:,}")
        
        if record_count == 0:
            print("WARNING: Table initialized but contains 0 records.")
            return

        # 3. Sample Data
        print("\n--- [3] Raw Data Sample (Truncated) ---")
        # Show top 20 rows, truncate long fields for readability in console
        df.show(20, truncate=True)
        
        # 4. Transaction History (Delta Specific)
        # This inspects the _delta_log to see the latest commits
        print("\n--- [4] Transaction Log (Latest 3 Commits) ---")
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forPath(spark, abs_path)
        
        # Select key columns to keep output clean
        history_df = delta_table.history().select(
            "version", "timestamp", "operation", "operationParameters.mode", "readVersion"
        ).limit(3)
        
        history_df.show(truncate=False)

    except AnalysisException as e:
        print(f"Spark Analysis Exception: {e}")
        print("Possible cause: The directory exists but is not a valid Delta table (missing _delta_log).")
    except Exception as e:
        print(f"Unexpected Error: {e}")

if __name__ == "__main__":
    # Default path as requested
    DEFAULT_PATH = "data/delta/bronze"
    
    parser = argparse.ArgumentParser(description="Inspect a local Delta Lake table.")
    parser.add_argument("--path", default=DEFAULT_PATH, help="Path to the Delta table")
    
    args = parser.parse_args()
    
    spark_session = create_spark_session()
    try:
        inspect_bronze_table(spark_session, args.path)
    finally:
        spark_session.stop()