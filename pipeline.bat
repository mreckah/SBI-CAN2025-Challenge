@echo off
setlocal enabledelayedexpansion

echo ============================================================
echo   SBI AFCON 2025 - FULL DATA PIPELINE
echo ============================================================

:: 1. Check for PostgreSQL Driver
if exist "postgresql-42.6.0.jar" goto driver_ok
echo [ERROR] PostgreSQL driver (postgresql-42.6.0.jar) not found in root directory.
echo Please run the installation step first.
exit /b 1

:driver_ok
:: 2. Start Infrastructure
echo [1/4] Starting Docker containers...
:: docker-compose down
docker-compose up -d
if errorlevel 1 (
    echo [ERROR] Failed to start docker-compose.
    exit /b 1
)

:: 3. Wait for Data Ingestion to finish
echo [2/4] Waiting for Data Pipeline (Kafka -> HDFS) to complete...
:wait_ingestion
for /f "tokens=*" %%i in ('docker inspect -f "{{.State.Status}}" data-pipeline') do set STATUS=%%i
if "!STATUS!"=="exited" goto spark_job
echo Ingestion in progress (Status: !STATUS!)...
timeout /t 5 /nobreak > nul
goto wait_ingestion

:: 4. Run Spark ETL
:spark_job
echo [3/4] Running Spark ETL Job (HDFS -> Postgres Gold)...
:: Ensure we use the correct container name or ID
docker exec sbi-can2025-pipeline-spark-master-1 /opt/spark/bin/spark-submit --master local[*] --jars /opt/spark/jars/postgresql-42.6.0.jar --driver-class-path /opt/spark/jars/postgresql-42.6.0.jar /jobs/gold_etl.py

if errorlevel 1 (
    echo [ERROR] Spark ETL job failed. Check logs in the Spark container.
    exit /b 1
)
echo [OK] Spark ETL complete.

:: 5. Final Status
echo [4/4] Pipeline finished!
echo PostgreSQL tables updated (only 8 core tables).
echo Grafana is available at http://localhost:3000 (admin/admin)
echo.
echo Tables in 'gold' schema:
docker exec postgres_db psql -U admin -d can2025_gold -t -c "SELECT tablename FROM pg_tables WHERE schemaname = 'gold' ORDER BY tablename;"

echo ============================================================
echo   PIPELINE EXECUTION SUCCESSFUL
echo ============================================================
pause
