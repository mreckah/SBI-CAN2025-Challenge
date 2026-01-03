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
echo [1/4] Starting Docker containers (Cleaning Cache)...
:: docker-compose down -v
:: docker-compose up -d --build

:: Force restart data-pipeline to ensure it runs with the latest code/requirements
docker-compose restart data-pipeline
if errorlevel 1 (
    echo [ERROR] Failed to start docker-compose.
    exit /b 1
)

:: 3. Wait for Data Ingestion to finish
echo [2/4] Waiting for Data Pipeline to complete...
:wait_ingestion
set STATUS=not_found
for /f "tokens=*" %%i in ('docker inspect -f "{{.State.Status}}" data-pipeline 2^>nul') do set STATUS=%%i
if "!STATUS!"=="exited" goto spark_job
if "!STATUS!"=="not_found" (
    echo [ERROR] data-pipeline container not found. Build might have failed.
    exit /b 1
)
echo Ingestion in progress (Status: !STATUS!)...
timeout /t 5 /nobreak > nul
goto wait_ingestion

:: 4. Run Spark ETL
:spark_job
echo [3/4] Running Spark ETL Job (Performance Job)...
:: Ensure we use the correct container name or ID
docker exec sbi-can2025-pipeline-spark-master-1 /opt/spark/bin/spark-submit --master local[*] --jars /opt/spark/jars/postgresql-42.6.0.jar --driver-class-path /opt/spark/jars/postgresql-42.6.0.jar /jobs/performance_job.py

if errorlevel 1 (
    echo [ERROR] Spark ETL job failed. Check logs in the Spark container.
    exit /b 1
)
echo [OK] Spark ETL complete.

echo [4/4] Pipeline finished!
echo PostgreSQL tables updated.
echo Grafana is available at http://localhost:3000 (admin/admin)
echo.
echo Tables in 'can2025_gold' database:
docker exec postgres_db psql -U admin -d can2025_gold -t -c "SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema') ORDER BY schemaname, tablename;"

echo ============================================================
echo   PIPELINE EXECUTION SUCCESSFUL
echo ============================================================
pause
