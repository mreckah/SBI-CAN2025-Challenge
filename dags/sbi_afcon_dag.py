from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sbi_afcon_pipeline',
    default_args=default_args,
    description='SBI AFCON 2025 Data Pipeline DAG',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task 1: Data Ingestion (Restarting the container to run data_pipeline.py)
    # Note: On Windows with Docker Desktop, communicating with the socket from inside a container 
    # might require the docker CLI to be installed or using DockerOperator.
    # Since we want to mirror pipeline.bat which runs:
    # docker-compose restart data-pipeline
    
    ingest_data = BashOperator(
        task_id='ingest_data',
        bash_command='echo "Triggering ingestion..." && sleep 5', # Placeholder if docker CLI isn't in image
    )

    # Task 2: Spark ETL Job
    # Mirrors: docker exec spark-master /opt/spark/bin/spark-submit ...
    spark_etl = BashOperator(
        task_id='spark_etl',
        bash_command='echo "Running Spark ETL..." && sleep 5', # Placeholder
    )

    ingest_data >> spark_etl
