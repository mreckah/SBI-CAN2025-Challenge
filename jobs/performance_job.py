import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset


def _env(name: str, default: str) -> str:
    val = os.getenv(name)
    return val if val is not None and val != "" else default


HDFS_URL = _env("HDFS_URL", "hdfs://namenode:9000")
HDFS_DATA_PATH = _env("HDFS_DATA_PATH", "/data/data.csv")
POSTGRES_HOST = _env("PG_HOST", "postgres")
POSTGRES_PORT = _env("PG_PORT", "5432")
POSTGRES_DB = _env("PG_DB", "can2025_gold")
POSTGRES_USER = _env("PG_USER", "admin")
POSTGRES_PASSWORD = _env("PG_PASSWORD", "password123")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}


def get_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("CAN2025_ETL_StarSchema") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def read_raw_data(spark: SparkSession, hdfs_path: str):
    print(f"Reading data from HDFS: {hdfs_path}")
    
    schema = StructType([
        StructField("Player_Name", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Position", StringType(), True),
        StructField("Goals", IntegerType(), True),
        StructField("Assists", IntegerType(), True),
        StructField("Minutes_Played", IntegerType(), True),
        StructField("Matches_Played", IntegerType(), True),
        StructField("Pass_Accuracy", DoubleType(), True),
        StructField("Distance_Run_km", DoubleType(), True),
        StructField("Sprint_Speed_kmh", DoubleType(), True),
        StructField("Tackles", IntegerType(), True),
        StructField("Interceptions", IntegerType(), True),
        StructField("Saves", IntegerType(), True),
        StructField("Clean_Sheets", IntegerType(), True),
        StructField("Yellow_Cards", IntegerType(), True),
        StructField("Red_Cards", IntegerType(), True),
        StructField("Performance_Rating", DoubleType(), True)
    ])
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(hdfs_path)
    
    print(f"Total records read: {df.count()}")
    return df


def validate_data(df):
    print("Validating data with Great Expectations...")
    
    ge_df = SparkDFDataset(df)
    
    # 1. Column existence
    required_columns = ["Player_Name", "Country", "Age", "Position", "Performance_Rating"]
    for col in required_columns:
        ge_df.expect_column_to_exist(col)
    
    # 2. Null checks
    ge_df.expect_column_values_to_not_be_null("Player_Name")
    ge_df.expect_column_values_to_not_be_null("Country")
    
    # 3. Value ranges
    ge_df.expect_column_values_to_be_between("Age", 15, 50)
    ge_df.expect_column_values_to_be_between("Performance_Rating", 0, 10)
    ge_df.expect_column_values_to_be_between("Minutes_Played", 0)
    ge_df.expect_column_values_to_be_between("Goals", 0)
    
    # 4. categorical checks
    valid_positions = ["Forward", "Midfielder", "Defender", "Goalkeeper", "FW", "MF", "DF", "GK"]
    ge_df.expect_column_values_to_be_in_set("Position", valid_positions)
    
    validation_results = ge_df.validate()
    
    if validation_results["success"]:
        print("Data quality validation PASSED!")
    else:
        print("Data quality validation FAILED!")
        # Print summary of failed expectations
        for result in validation_results["results"]:
            if not result["success"]:
                print(f" - FAILED: {result['expectation_config']['expectation_type']} on {result['expectation_config']['kwargs'].get('column')}")
    
    return validation_results["success"]


def clean_data(df):
    print("Cleaning data...")
    
    df_clean = df.dropDuplicates(["Player_Name", "Country"])
    
    df_clean = df_clean.filter(F.col("Player_Name").isNotNull())
    df_clean = df_clean.filter(F.col("Country").isNotNull())
    
    numeric_cols = ["Goals", "Assists", "Minutes_Played", "Matches_Played", 
                    "Tackles", "Interceptions", "Saves", "Clean_Sheets",
                    "Yellow_Cards", "Red_Cards"]
    
    for col in numeric_cols:
        df_clean = df_clean.fillna({col: 0})
    
    df_clean = df_clean.fillna({
        "Pass_Accuracy": 0.0,
        "Distance_Run_km": 0.0,
        "Sprint_Speed_kmh": 0.0,
        "Performance_Rating": 0.0
    })
    
    df_clean = df_clean.withColumn(
        "Position",
        F.when(F.col("Position").isin("Forward", "FW"), "Forward")
         .when(F.col("Position").isin("Midfielder", "MF"), "Midfielder")
         .when(F.col("Position").isin("Defender", "DF"), "Defender")
         .when(F.col("Position").isin("Goalkeeper", "GK"), "Goalkeeper")
         .otherwise(F.col("Position"))
    )
    
    df_clean = df_clean.withColumn(
        "is_active",
        F.when(F.col("Minutes_Played") > 0, True).otherwise(False)
    )
    
    print(f"Records after cleaning: {df_clean.count()}")
    return df_clean


def create_dim_player(df):
    print("Creating dim_player...")
    
    dim_player = df.select(
        F.monotonically_increasing_id().alias("player_id"),
        F.col("Player_Name").alias("player_name"),
        F.col("Age").alias("age"),
        F.col("Position").alias("position"),
        F.lit(datetime.now()).alias("created_at"),
        F.lit(datetime.now()).alias("updated_at")
    ).distinct()
    
    return dim_player


def create_dim_country(df):
    print("Creating dim_country...")
    
    dim_country = df.select("Country").distinct() \
        .withColumn("country_id", F.monotonically_increasing_id()) \
        .withColumnRenamed("Country", "country_name") \
        .withColumn("created_at", F.lit(datetime.now())) \
        .select("country_id", "country_name", "created_at")
    
    return dim_country


def create_dim_position(df):
    print("Creating dim_position...")
    
    dim_position = df.select("Position").distinct() \
        .withColumn("position_id", F.monotonically_increasing_id()) \
        .withColumnRenamed("Position", "position_name") \
        .withColumn("position_category", 
                   F.when(F.col("position_name") == "Forward", "Attack")
                    .when(F.col("position_name") == "Midfielder", "Midfield")
                    .when(F.col("position_name") == "Defender", "Defense")
                    .when(F.col("position_name") == "Goalkeeper", "Defense")
                    .otherwise("Unknown")) \
        .withColumn("created_at", F.lit(datetime.now())) \
        .select("position_id", "position_name", "position_category", "created_at")
    
    return dim_position


def create_dim_date():
    print("Creating dim_date...")
    
    spark = SparkSession.getActiveSession()
    
    dates_data = []
    from datetime import date, timedelta
    
    start_date = date(2024, 12, 21)
    end_date = date(2025, 1, 18)
    current = start_date
    
    while current <= end_date:
        dates_data.append({
            "date_id": int(current.strftime("%Y%m%d")),
            "full_date": current,
            "year": current.year,
            "month": current.month,
            "day": current.day,
            "week_of_year": current.isocalendar()[1],
            "day_of_week": current.strftime("%A"),
            "is_weekend": current.weekday() >= 5
        })
        current += timedelta(days=1)
    
    dim_date = spark.createDataFrame(dates_data)
    return dim_date


def create_fact_performance(df, dim_player, dim_country, dim_position):
    print("Creating fact_performance...")
    
    fact = df.alias("f") \
        .join(dim_player.alias("p"), 
              (F.col("f.Player_Name") == F.col("p.player_name")) & 
              (F.col("f.Age") == F.col("p.age")), 
              "inner") \
        .join(dim_country.alias("c"), 
              F.col("f.Country") == F.col("c.country_name"), 
              "inner") \
        .join(dim_position.alias("pos"), 
              F.col("f.Position") == F.col("pos.position_name"), 
              "inner")
    
    fact_performance = fact.select(
        F.monotonically_increasing_id().alias("performance_id"),
        F.col("p.player_id"),
        F.col("c.country_id"),
        F.col("pos.position_id"),
        F.lit(20250103).alias("date_id"),
        
        F.col("f.Goals").alias("goals"),
        F.col("f.Assists").alias("assists"),
        F.col("f.Pass_Accuracy").alias("pass_accuracy_pct"),
        
        F.col("f.Minutes_Played").alias("minutes_played"),
        F.col("f.Matches_Played").alias("matches_played"),
        F.col("f.Distance_Run_km").alias("distance_run_km"),
        F.col("f.Sprint_Speed_kmh").alias("sprint_speed_kmh"),
        
        F.col("f.Tackles").alias("tackles"),
        F.col("f.Interceptions").alias("interceptions"),
        
        F.col("f.Saves").alias("saves"),
        F.col("f.Clean_Sheets").alias("clean_sheets"),
        
        F.col("f.Yellow_Cards").alias("yellow_cards"),
        F.col("f.Red_Cards").alias("red_cards"),
        
        F.col("f.Performance_Rating").alias("performance_rating"),
        
        (F.col("f.Goals") + F.col("f.Assists")).alias("goal_contributions"),
        F.when(F.col("f.Matches_Played") > 0, 
               F.col("f.Minutes_Played") / F.col("f.Matches_Played"))
         .otherwise(0).alias("avg_minutes_per_match"),
        F.when(F.col("f.Matches_Played") > 0,
               F.col("f.Goals") / F.col("f.Matches_Played"))
         .otherwise(0).alias("goals_per_match"),
        
        F.col("f.is_active"),
        F.lit(datetime.now()).alias("created_at")
    )
    
    return fact_performance


def create_aggregate_tables(fact_performance, dim_player, dim_country, dim_position):
    print("Creating aggregate tables...")
    
    country_agg = fact_performance.alias("f") \
        .join(dim_country.alias("c"), "country_id") \
        .groupBy("f.country_id", "c.country_name") \
        .agg(
            F.count("f.player_id").alias("total_players"),
            F.sum("f.goals").alias("total_goals"),
            F.sum("f.assists").alias("total_assists"),
            F.avg("f.performance_rating").alias("avg_rating"),
            F.sum("f.minutes_played").alias("total_minutes"),
            F.sum("f.yellow_cards").alias("total_yellow_cards"),
            F.sum("f.red_cards").alias("total_red_cards"),
            F.avg("f.pass_accuracy_pct").alias("avg_pass_accuracy")
        ) \
        .withColumn("created_at", F.lit(datetime.now()))
    
    position_agg = fact_performance.alias("f") \
        .join(dim_position.alias("p"), "position_id") \
        .groupBy("f.position_id", "p.position_name") \
        .agg(
            F.count("f.player_id").alias("total_players"),
            F.avg("f.performance_rating").alias("avg_rating"),
            F.avg("f.goals").alias("avg_goals"),
            F.avg("f.assists").alias("avg_assists"),
            F.avg("f.sprint_speed_kmh").alias("avg_sprint_speed"),
            F.avg("f.distance_run_km").alias("avg_distance_run"),
            F.max("f.performance_rating").alias("max_rating")
        ) \
        .withColumn("created_at", F.lit(datetime.now()))
    
    window_spec = Window.orderBy(F.desc("performance_rating"))
    
    top_performers = fact_performance.alias("f") \
        .join(dim_player.alias("p"), "player_id") \
        .join(dim_country.alias("c"), "country_id") \
        .select(
            F.row_number().over(window_spec).alias("rank"),
            "p.player_name",
            "c.country_name",
            "f.performance_rating",
            "f.goals",
            "f.assists",
            "f.goal_contributions"
        ) \
        .limit(50) \
        .withColumn("created_at", F.lit(datetime.now()))
    
    return country_agg, position_agg, top_performers


def write_to_postgres(df, table_name: str, mode: str = "overwrite"):
    print(f"Writing to PostgreSQL table: {table_name}")
    
    df.write \
        .jdbc(url=JDBC_URL, table=table_name, mode=mode, properties=JDBC_PROPERTIES)
    
    print(f"Successfully wrote {df.count()} records to {table_name}")


def main():
    print("=" * 80)
    print("Starting CAN 2025 ETL Pipeline - Star Schema")
    print("=" * 80)
    
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        raw_df = read_raw_data(spark, f"{HDFS_URL}{HDFS_DATA_PATH}")
        
        # Data Quality Check
        is_valid = validate_data(raw_df)
        if not is_valid:
            print("WARNING: Data quality issues found, but proceeding anyway.")
        
        clean_df = clean_data(raw_df)
        
        dim_player = create_dim_player(clean_df)
        dim_country = create_dim_country(clean_df)
        dim_position = create_dim_position(clean_df)
        dim_date = create_dim_date()
        
        fact_performance = create_fact_performance(
            clean_df, dim_player, dim_country, dim_position
        )
        
        country_agg, position_agg, top_performers = create_aggregate_tables(
            fact_performance, dim_player, dim_country, dim_position
        )
        
        print("\n" + "=" * 80)
        print("Loading data to PostgreSQL...")
        print("=" * 80)
        
        write_to_postgres(dim_player, "dim_player")
        write_to_postgres(dim_country, "dim_country")
        write_to_postgres(dim_position, "dim_position")
        write_to_postgres(dim_date, "dim_date")
        
        write_to_postgres(fact_performance, "fact_performance")
        
        write_to_postgres(country_agg, "agg_country_performance")
        write_to_postgres(position_agg, "agg_position_performance")
        write_to_postgres(top_performers, "agg_top_performers")
        
        print("\n" + "=" * 80)
        print("ETL Pipeline completed successfully!")
        print("=" * 80)
        print(f"\nSummary:")
        print(f"  - Players: {dim_player.count()}")
        print(f"  - Countries: {dim_country.count()}")
        print(f"  - Positions: {dim_position.count()}")
        print(f"  - Performance Records: {fact_performance.count()}")
        print(f"  - Date Records: {dim_date.count()}")
        
        print("\nSample from fact_performance:")
        fact_performance.show(5, truncate=False)
        
        print("\nTop 10 Players by Rating:")
        top_performers.show(10, truncate=False)
        
    except Exception as e:
        print(f"ERROR: ETL pipeline failed - {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()