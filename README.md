# Sentinel-Spark: Financial Payments Pipeline
**Scalable ETL & Distributed Data Modeling for Financial Risk Analytics**

## 📌 Project Overview
Sentinel-Spark is a high-performance data engineering pipeline designed to process large-scale financial transaction data. It transforms raw, unstructured logs into an optimized Star Schema (Fact/Dimension tables) to support predictive risk scoring and business intelligence.

## 🛠️ Technical Stack
* **Compute Engine:** PySpark (Spark 3.5)
* **Storage Architecture:** Delta Lake (ACID, Merge/Upsert, Time Travel)
* **Containerization:** Docker & Docker Compose
* **Data Format:** Apache Parquet (Snappy)
* **Modeling:** Star Schema (Fact and Dimension)
* **Testing & Quality:** PyTest, Great Expectations

## 🏗️ Key Features & Optimizations
* **Data Skew Mitigation (Salting):** Prevented executor bottlenecks by adding random prefixes to skewed keys, ensuring uniform data distribution.
* **Performance Tuning:** Leveraged **Broadcast Joins** for dimension tables, reducing network shuffle overhead.
* **Audit-Grade Integrity:** ACID-compliant Delta Lake logic ensures 100% data reliability for financial reporting.
* **Unit Testing:** Integrated `pytest-spark` for transformation-level verification.

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose

### Run the ETL Pipeline
1. **Generate Sample Data:**
   ```bash
   python scripts/generate_sample_data.py
   ```
2. **Run Pipeline (Docker):**
   ```bash
   docker-compose up
   ```
3. **Run Tests:**
   ```bash
   docker-compose run spark pytest tests/test_etl.py
   ```

## 📊 Star Schema DDL
The Hive/Delta Lake schema is defined in `sql/schema.sql`, featuring a partitioned Fact Table for sub-minute processing.
