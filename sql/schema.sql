-- SQL for Star Schema (Hive)

-- Dimension: User Profiles
CREATE TABLE IF NOT EXISTS dim_users (
    user_id STRING,
    name STRING,
    email STRING,
    region STRING,
    created_at TIMESTAMP
) STORED AS PARQUET;

-- Dimension: Merchant Details
CREATE TABLE IF NOT EXISTS dim_merchants (
    merchant_id STRING,
    merchant_name STRING,
    category STRING,
    location STRING
) STORED AS PARQUET;

-- Dimension: Time
CREATE TABLE IF NOT EXISTS dim_time (
    time_key STRING,
    date DATE,
    day INT,
    month INT,
    year INT,
    quarter INT
) STORED AS PARQUET;

-- Fact: Transactions
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id STRING,
    user_id STRING,
    merchant_id STRING,
    amount DOUBLE,
    currency STRING,
    status STRING,
    transaction_timestamp TIMESTAMP,
    time_key STRING
) 
PARTITIONED BY (transaction_date DATE)
STORED AS PARQUET;
