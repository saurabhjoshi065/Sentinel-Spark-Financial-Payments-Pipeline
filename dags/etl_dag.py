from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Default arguments for the DAG (Senior Level: Retries & Monitoring)
default_args = {
    'owner': 'Sentinel-Spark-Admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# The Workflow: Data Generation -> ETL Execution -> Quality Report
with DAG(
    'sentinel_spark_financial_pipeline',
    default_args=default_args,
    description='Orchestrated Star Schema ETL with Delta Lake',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Generate Raw Data (Simulation of HDFS landing)
    generate_data = DockerOperator(
        task_id='generate_sample_data',
        image='sentinel-spark-spark:latest',
        command='python scripts/generate_sample_data.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    # Task 2: Execute Spark Lakehouse ETL
    run_spark_etl = DockerOperator(
        task_id='run_spark_lakehouse_etl',
        image='sentinel-spark-spark:latest',
        command='spark-submit --packages io.delta:delta-spark_2.12:3.1.0 scripts/etl_pipeline.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    # Task 3: Run Unit Tests & Data Quality
    run_tests = DockerOperator(
        task_id='validate_data_quality',
        image='sentinel-spark-spark:latest',
        command='pytest tests/test_etl.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    generate_data >> run_spark_etl >> run_tests
