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
*   **Data Skew Mitigation (Salting):** Prevented executor bottlenecks by adding random prefixes to skewed keys, ensuring uniform data distribution.
*   **Performance Tuning:** Leveraged **Broadcast Joins** for dimension tables, reducing network shuffle overhead.
*   **Audit-Grade Integrity:** ACID-compliant Delta Lake logic ensures 100% data reliability for financial reporting.
*   **Orchestration (Apache Airflow):** Pipeline scheduled with a DAG managing task dependencies, retries, and monitoring.
*   **Data Quality (Great Expectations):** Automated validation suite for schema enforcement and data profiling.
*   **Unit Testing:** Integrated `pytest-spark` for transformation-level verification.

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose

### Run the Pipeline (Enterprise Flow)
1. **Build Environment:**
   ```bash
   docker-compose build
   ```
2. **Execute Full Orchestrated Flow:**
   ```bash
   docker-compose up
   ```
3. **Run Data Quality Tests:**
   ```bash
   docker-compose run spark pytest tests/test_etl.py
   ```

## 📂 Project Architecture (Lakehouse & DataOps)
```text
├── dags/            # Apache Airflow Orchestration (Task Scheduling & Retries)
├── gx/              # Great Expectations Suite (Automated Data Quality Reports)
├── scripts/         # PySpark ETL (Delta Lake + Salted/Broadcast Optimization)
├── sql/             # Star Schema Definition (Fact/Dimension DDL)
├── tests/           # PyTest-Spark (Unit Testing Suite)
└── docker-compose/  # Containerized Infrastructure
```
