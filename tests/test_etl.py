import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, rand, to_date, year, month, dayofmonth, broadcast, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder 
        .appName("Sentinel-Test-Suite") 
        .config("spark.sql.shuffle.partitions", "1") 
        .getOrCreate())

def test_salting_logic(spark):
    # Test data: 1 skewed user_id
    data = [("user_1",)]
    schema = StructType([StructField("user_id", StringType(), False)])
    df = spark.createDataFrame(data, schema)
    
    # Apply salting (NUM_SALTS=5)
    NUM_SALTS = 5
    salted_df = df.withColumn("salt", (rand() * NUM_SALTS).cast(IntegerType())) \
        .withColumn("salted_user_id", concat(col("user_id"), lit("_"), col("salt")))
    
    # Assert
    assert "salted_user_id" in salted_df.columns
    # Check format (e.g., user_1_3)
    row = salted_df.collect()[0]
    assert row["salted_user_id"].startswith("user_1_")
    assert int(row["salted_user_id"].split("_")[-1]) < NUM_SALTS

def test_schema_enforcement(spark):
    # Test data with invalid column
    data = [("txn_1", "user_1", -10.0)] # Amount should be positive
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("amount", DoubleType(), False)
    ])
    df = spark.createDataFrame(data, schema)
    
    # Filter logic
    cleaned_df = df.filter(col("amount") > 0)
    
    # Assert
    assert cleaned_df.count() == 0 # Invalid record should be dropped
