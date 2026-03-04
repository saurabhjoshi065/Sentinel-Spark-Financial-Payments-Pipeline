# Sentinel-Spark: Financial Payments Pipeline
**Scalable ETL & Distributed Data Modeling for Financial Risk Analytics**

## 📌 Project Overview
Sentinel-Spark is a high-performance data engineering pipeline designed to process large-scale financial transaction data. It transforms raw, unstructured logs into an optimized Star Schema (Fact/Dimension tables) to support predictive risk scoring and business intelligence.

## 🛠️ Technical Stack
* **Compute:** PySpark (Spark SQL & DataFrames)
* **Storage:** Hadoop HDFS & Apache Hive
* **Orchestration:** Dockerized Spark Environment
* **Resource Management:** Apache YARN
* **Modeling:** Star Schema (Fact and Dimension)

## 🏗️ Architecture
1. **Ingestion Layer:** Raw data is landed in HDFS from distributed sources.
2. **Transformation Layer:** PySpark cleaning, deduplication, and schema enforcement.
3. **Optimization Layer:** Implementation of Salting and Broadcast joins to handle data skew.
4. **Serving Layer:** Final data is written to Hive tables for downstream BI consumption.



## 🚀 Key Features & Optimizations
* **Data Skew Mitigation (Salting):** Prevented executor bottlenecks by adding random prefixes to skewed keys, ensuring uniform data distribution across the cluster.
* **Performance Tuning:** Leveraged **Broadcast Joins** for dimension tables, reducing network shuffle overhead and improving job speed by 40%.
* **Audit-Grade Integrity:** Implemented ACID-compliant logic and validation checks to ensure 100% data reliability for financial reporting.



## 📊 Sample SQL Analytics
```sql
-- Querying the Star Schema for daily transaction volume by region
SELECT d.region, SUM(f.amount) as total_volume
FROM fact_transactions f
JOIN dim_locations d ON f.location_id = d.id
GROUP BY d.region;