@echo off
setlocal enabledelayedexpansion

echo ============================================================
echo   AFCON 2025 - DASHBOARD DATA PIPELINE
echo ============================================================

:: 1. Check for PostgreSQL Driver
if exist "postgresql-42.6.0.jar" goto driver_ok
echo [ERROR] PostgreSQL driver (postgresql-42.6.0.jar) not found in root directory.
echo Please run the installation step first.
exit /b 1

:driver_ok
:: 2. Start Infrastructure
echo [1/3] Starting Docker containers...
:: docker-compose up -d
if errorlevel 1 (
    echo [ERROR] Failed to start docker-compose.
    exit /b 1
)

:: 3. Wait for PostgreSQL to be ready
echo [2/3] Waiting for PostgreSQL to be ready...
:wait_postgres
for /f "tokens=*" %%i in ('docker inspect -f "{{.State.Health.Status}}" postgres_db') do set PG_STATUS=%%i
if "!PG_STATUS!"=="healthy" goto spark_job
echo PostgreSQL not ready (Status: !PG_STATUS!)...
timeout /t 5 /nobreak > nul
goto wait_postgres

:: 4. Run Dashboard ETL
:spark_job
echo [3/3] Running Dashboard ETL Job...
docker exec sbi-can2025-pipeline-spark-master-1 /opt/spark/bin/spark-submit --master local[*] --jars /opt/spark/jars/postgresql-42.6.0.jar --driver-class-path /opt/spark/jars/postgresql-42.6.0.jar /jobs/dashboard_etl.py

if errorlevel 1 (
    echo [ERROR] Dashboard ETL job failed. Check logs in the Spark container.
    exit /b 1
)
echo [OK] Dashboard ETL complete.

:: 5. Final Status
echo Pipeline finished!
echo Dashboard tables created in PostgreSQL gold schema.
echo Grafana is available at http://localhost:3000 (admin/admin)
echo.
echo Tables created:
echo - dim_date, dim_team, dim_stadium, dim_city
echo - dim_channel, dim_campaign
echo - fact_matches, fact_attendance, fact_marketing
echo - fact_team_performance, fact_player_performance

echo ============================================================
echo   DASHBOARD PIPELINE SUCCESSFUL
echo ============================================================
pause
