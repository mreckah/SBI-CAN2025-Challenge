from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Base path for host mounts
BASE_PATH = "C:\\Users\\Mreckah\\Desktop\\sbi-can2025-pipeline"

with DAG(
    'sbi_afcon_pipeline',
    default_args=default_args,
    description='SBI AFCON 2025 Data Pipeline DAG',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task 1: Data Ingestion
    ingest_data = DockerOperator(
        task_id='ingest_data',
        image='sbi-can2025-pipeline-data-pipeline:latest',
        api_version='auto',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="pipeline-net",
        mount_tmp_dir=False,
        environment={
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
            'KAFKA_TOPIC': 'can2025_data_files',
            'HDFS_URL': 'http://namenode:9870',
            'HDFS_DIR': '/data'
        },
        mounts=[
            Mount(source=f'{BASE_PATH}\\data', target='/data', type='bind', read_only=True)
        ]
    )

    # Task 2: Spark ETL Job
    spark_etl = DockerOperator(
        task_id='spark_etl',
        image='sbi-can2025-pipeline-spark-master:latest',
        api_version='auto',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="pipeline-net",
        mount_tmp_dir=False,
        command="/opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/postgresql-42.6.0.jar --driver-class-path /opt/spark/jars/postgresql-42.6.0.jar /jobs/performance_job.py",
        mounts=[
            Mount(source=f'{BASE_PATH}\\jobs', target='/jobs', type='bind', read_only=True),
            Mount(source=f'{BASE_PATH}\\postgresql-42.6.0.jar', target='/opt/spark/jars/postgresql-42.6.0.jar', type='bind', read_only=True)
        ],
        environment={
            'PG_HOST': 'postgres',
            'PG_PORT': '5432',
            'PG_DB': 'can2025_gold',
            'PG_USER': 'admin',
            'PG_PASSWORD': 'password123'
        }
    )

    ingest_data >> spark_etl
