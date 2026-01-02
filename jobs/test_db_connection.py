#!/usr/bin/env python3

import os
from pyspark.sql import SparkSession

def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

PG_HOST = env("PG_HOST", "postgres")
PG_PORT = env("PG_PORT", "5432")
PG_DB = env("PG_DB", "can2025_gold")
PG_USER = env("PG_USER", "admin")
PG_PASSWORD = env("PG_PASSWORD", "password123")
PG_SCHEMA = env("PG_SCHEMA", "gold")

print(f"Testing PostgreSQL connection:")
print(f"Host: {PG_HOST}")
print(f"Port: {PG_PORT}")
print(f"Database: {PG_DB}")
print(f"User: {PG_USER}")
print(f"Password: {'*' * len(PG_PASSWORD)}")
print(f"Schema: {PG_SCHEMA}")

# Create Spark session
spark = SparkSession.builder \
    .appName("test-db-connection") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

# Test connection
try:
    url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    print(f"JDBC URL: {url}")
    
    # Try to read a simple query
    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("query", "SELECT 1 as test_col") \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    result = df.collect()
    print(f"Connection successful! Result: {result}")
    
except Exception as e:
    print(f"Connection failed: {e}")
    import traceback
    traceback.print_exc()

spark.stop()
