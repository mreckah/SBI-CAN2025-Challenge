# --- Stage 1: Data Pipeline ---
FROM python:3.9-slim AS data-pipeline
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY data_pipeline.py .
CMD ["python", "data_pipeline.py"]

# --- Stage 2: Spark Base ---
FROM apache/spark:3.4.1 AS spark-base
USER root
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --no-cache-dir "great_expectations<1.0.0" "notebook<7" pandas
USER spark