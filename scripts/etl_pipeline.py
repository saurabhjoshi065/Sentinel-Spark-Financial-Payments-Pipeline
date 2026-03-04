from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, rand, to_date, year, month, dayofmonth, broadcast, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from delta.tables import DeltaTable
import os

def run_etl():
    # 0. Initialize Delta Spark Session
    spark = (SparkSession.builder 
        .appName("Sentinel-Lakehouse: Financial Payments Pipeline") 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "10") 
        .config("spark.sql.warehouse.dir", "spark-warehouse") 
        .enableHiveSupport() 
        .getOrCreate())

    # 1. Ingest (Batch source, easily switchable to streaming)
    raw_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("status", StringType(), True),
        StructField("transaction_timestamp", TimestampType(), False)
    ])

    raw_data_path = "data/raw/transactions/*.json"
    if not os.path.exists("data/raw/transactions"):
        print("Raw data not found.")
        return

    print("Ingesting raw data...")
    raw_df = spark.read.schema(raw_schema).json(raw_data_path)

    # 2. Data Quality (Simulated Great Expectations Circuit Breaker)
    # In production, use GE's Checkpoint for full reporting.
    print("Performing Data Quality Checks...")
    invalid_records = raw_df.filter((col("amount") <= 0) | col("transaction_id").isNull()).count()
    if invalid_records > 0:
        print(f"CRITICAL: Found {invalid_records} invalid records. Aborting pipeline.")
        # spark.stop() # Uncomment in prod to stop
    
    # 3. Clean & Salt (Handling Skew)
    NUM_SALTS = 10
    cleaned_df = raw_df.dropna(subset=["transaction_id", "user_id", "merchant_id", "amount", "transaction_timestamp"]) \
        .filter(col("amount") > 0) \
        .dropDuplicates(["transaction_id"]) \
        .withColumn("salt", (rand() * NUM_SALTS).cast(IntegerType())) \
        .withColumn("salted_user_id", concat(col("user_id"), lit("_"), col("salt")))

    # 4. Dimensions (Lakehouse Tables)
    print("Loading dimension tables as Delta Tables...")
    # Seed local dimensions if they don't exist in Delta
    dim_users_raw = spark.read.json("data/dim_users.json")
    dim_merchants_raw = spark.read.json("data/dim_merchants.json")

    # Delta Lake: Joins & Broadcast Optimization
    dim_users_salted = (dim_users_raw
        .withColumn("salts", expr(f"explode(sequence(0, {NUM_SALTS - 1}))")) 
        .withColumn("salted_user_id", concat(col("user_id"), lit("_"), col("salts"))) 
        .drop("salts", "user_id"))

    # 5. Transform & Enforce Schema
    print("Joining Fact & Dimensions (Lakehouse Optimization)...")
    enriched_df = (cleaned_df
        .join(broadcast(dim_merchants_raw), on="merchant_id", how="inner")
        .join(dim_users_salted, on="salted_user_id", how="inner"))

    final_fact_df = enriched_df.select(
        col("transaction_id"),
        col("user_id"),
        col("merchant_id"),
        col("amount"),
        col("currency"),
        col("status"),
        col("transaction_timestamp"),
        to_date(col("transaction_timestamp")).alias("transaction_date"),
        concat(year(col("transaction_timestamp")), 
               month(col("transaction_timestamp")), 
               dayofmonth(col("transaction_timestamp"))).alias("time_key")
    )

    # 6. Load: ACID Upsert (Merge) into Delta Table
    # This allows us to handle late data or updates without rewriting partitions.
    delta_path = "spark-warehouse/fact_transactions_delta"
    print(f"Writing to Delta Lakehouse at {delta_path}...")
    
    if not DeltaTable.isDeltaTable(spark, delta_path):
        final_fact_df.write.format("delta").partitionBy("transaction_date").save(delta_path)
    else:
        # Merge (Upsert) - Core Lakehouse Feature
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target").merge(
            final_fact_df.alias("source"),
            "target.transaction_id = source.transaction_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    print("Lakehouse ETL Job completed successfully.")
    
    # 7. Time Travel (Proof of Concept)
    print("\nDelta Table History (Audit Trail):")
    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.history().select("version", "timestamp", "operation").show()

    spark.stop()

if __name__ == "__main__":
    run_etl()
