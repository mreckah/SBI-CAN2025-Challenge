import os
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


HDFS_NAMENODE = env("HDFS_NAMENODE", "namenode")
HDFS_PORT = env("HDFS_PORT", "9000")
HDFS_BASE_DIR = env("HDFS_BASE_DIR", "/data")

PG_HOST = env("PG_HOST", "postgres")
PG_PORT = env("PG_PORT", "5432")
PG_DB = env("PG_DB", env("POSTGRES_DB", "can2025_gold"))
PG_USER = env("PG_USER", env("POSTGRES_USER", "admin"))
PG_PASSWORD = env("PG_PASSWORD", env("POSTGRES_PASSWORD", "password123"))
PG_SCHEMA = env("PG_SCHEMA", "gold")


def hdfs_path(filename: str) -> str:
    return f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}{HDFS_BASE_DIR.rstrip('/')}/{filename}"


def read_csv_optional(spark: SparkSession, filename: str) -> Optional[DataFrame]:
    """Read CSV from HDFS if present. If missing/unreadable, log and return None."""
    path = hdfs_path(filename)
    try:
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path)
        )
        print(f"[READ] {filename} -> rows={df.count()} cols={len(df.columns)}")
        return df
    except Exception as e:
        print(f"[READ-SKIP] {filename} not readable at {path}: {e}")
        return None


def write_jdbc(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    print(f"[WRITE] {PG_SCHEMA}.{table} mode={mode} rows={df.count()} cols={len(df.columns)}")
    (
        df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", f"{PG_SCHEMA}.{table}")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )
    print(f"[WRITE-OK] {PG_SCHEMA}.{table}")


def main() -> None:
    spark = (
        SparkSession.builder.appName("can2025-gold-etl")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    print(
        "[CONFIG] "
        f"HDFS=hdfs://{HDFS_NAMENODE}:{HDFS_PORT}{HDFS_BASE_DIR} "
        f"PG=jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB} schema={PG_SCHEMA} user={PG_USER}"
    )

    # Smoke test: verify JDBC write works early and visibly.
    smoke = spark.createDataFrame([(1, "ok")], ["id", "status"])
    write_jdbc(smoke, "_smoke_test", mode="overwrite")

    matches = read_csv_optional(spark, "can2025_matches.csv")
    attendance = read_csv_optional(spark, "can2025_match_attendance.csv")
    team_kpis = read_csv_optional(spark, "can2025_team_kpis.csv")
    marketing = read_csv_optional(spark, "can2025_marketing_daily.csv")
    player_match = read_csv_optional(spark, "can2025_player_match_stats.csv")
    group_standings_raw = read_csv_optional(spark, "group_standings_raw.csv")
    players_snapshot = read_csv_optional(spark, "players_detailed_stats.csv")

    if matches is None or attendance is None or team_kpis is None:
        raise RuntimeError(
            "Missing required inputs in HDFS /data. Required: can2025_matches.csv, can2025_match_attendance.csv, can2025_team_kpis.csv"
        )

    matches = (
        matches.withColumn("match_ts", F.to_timestamp("match_date_utc"))
        .withColumn("match_date", F.to_date("match_ts"))
        .withColumn("home_goals", F.col("home_goals").cast("int"))
        .withColumn("away_goals", F.col("away_goals").cast("int"))
        .withColumn("home_xg", F.col("home_xg").cast("double"))
        .withColumn("away_xg", F.col("away_xg").cast("double"))
        .withColumn("attendance", F.col("attendance").cast("int"))
    )

    attendance = (
        attendance.withColumn("match_ts", F.to_timestamp("match_date_utc"))
        .withColumn("match_date", F.to_date("match_ts"))
        .withColumn("capacity", F.col("capacity").cast("int"))
        .withColumn("attendance", F.col("attendance").cast("int"))
        .withColumn("occupancy_pct", F.col("occupancy_pct").cast("double"))
        .withColumn("ticket_revenue_usd", F.col("ticket_revenue_usd").cast("double"))
        .withColumn("hospitality_revenue_usd", F.col("hospitality_revenue_usd").cast("double"))
        .withColumn("concession_revenue_usd", F.col("concession_revenue_usd").cast("double"))
        .withColumn("fan_satisfaction_score", F.col("fan_satisfaction_score").cast("double"))
    )

    team_kpis = (
        team_kpis.withColumn("asof_date", F.to_date("asof_date"))
        .withColumn("played", F.col("played").cast("int"))
        .withColumn("points", F.col("points").cast("int"))
        .withColumn("goals_for", F.col("goals_for").cast("int"))
        .withColumn("goals_against", F.col("goals_against").cast("int"))
        .withColumn("goal_diff", F.col("goal_diff").cast("int"))
        .withColumn("xg_for", F.col("xg_for").cast("double"))
        .withColumn("xg_against", F.col("xg_against").cast("double"))
        .withColumn("shots_pg", F.col("shots_pg").cast("double"))
        .withColumn("shots_on_target_pg", F.col("shots_on_target_pg").cast("double"))
        .withColumn("pass_accuracy_pct", F.col("pass_accuracy_pct").cast("double"))
        .withColumn("possession_pct", F.col("possession_pct").cast("double"))
        .withColumn("pressing_ppda", F.col("pressing_ppda").cast("double"))
        .withColumn("duels_won_pct", F.col("duels_won_pct").cast("double"))
        .withColumn("set_piece_xg", F.col("set_piece_xg").cast("double"))
        .withColumn("total_distance_km", F.col("total_distance_km").cast("double"))
        .withColumn("injuries_reported", F.col("injuries_reported").cast("int"))
    )

    if marketing is not None:
        marketing = (
            marketing.withColumn("date", F.to_date("date_utc"))
            .withColumn("value", F.col("value").cast("double"))
        )

    if player_match is not None:
        player_match = (
            player_match.withColumn("match_ts", F.to_timestamp("match_date_utc"))
            .withColumn("match_date", F.to_date("match_ts"))
            .withColumn("minutes", F.col("minutes").cast("int"))
            .withColumn("goals", F.col("goals").cast("int"))
            .withColumn("assists", F.col("assists").cast("int"))
            .withColumn("shots", F.col("shots").cast("int"))
            .withColumn("shots_on_target", F.col("shots_on_target").cast("int"))
            .withColumn("key_passes", F.col("key_passes").cast("int"))
            .withColumn("passes_completed", F.col("passes_completed").cast("int"))
            .withColumn("dribbles_completed", F.col("dribbles_completed").cast("int"))
            .withColumn("tackles", F.col("tackles").cast("int"))
            .withColumn("interceptions", F.col("interceptions").cast("int"))
            .withColumn("xg", F.col("xg").cast("double"))
            .withColumn("xa", F.col("xa").cast("double"))
        )

    dim_team = (
        team_kpis.select(
            F.col("team").alias("team_name"),
            F.col("group_id"),
        )
        .dropDuplicates(["team_name"])
        .withColumn("team_id", F.sha2(F.col("team_name"), 256))
    )

    dim_stadium = (
        matches.select(
            F.col("stadium").alias("stadium_name"),
            F.col("city").alias("city"),
        )
        .dropDuplicates(["stadium_name", "city"])
        .withColumn("stadium_id", F.sha2(F.concat_ws("|", F.col("stadium_name"), F.col("city")), 256))
    )

    dim_referee = (
        matches.select(F.col("referee").cast("string").alias("referee_name"))
        .where(F.col("referee_name").isNotNull())
        .dropDuplicates(["referee_name"])
        .withColumn("referee_id", F.sha2(F.col("referee_name"), 256))
    )

    if player_match is not None:
        dim_player = (
            player_match.select(
                F.col("player_name"),
                F.col("team").alias("team_name"),
                F.col("position"),
            )
            .dropDuplicates(["player_name", "team_name"])
            .withColumn("player_id", F.sha2(F.concat_ws("|", F.col("player_name"), F.col("team_name")), 256))
        )
    else:
        dim_player = spark.createDataFrame([], schema=T.StructType([T.StructField("player_id", T.StringType()), T.StructField("player_name", T.StringType()), T.StructField("team_name", T.StringType()), T.StructField("position", T.StringType())]))

    if marketing is not None:
        dim_campaign = (
            marketing.select(F.col("campaign"))
            .where(F.col("campaign").isNotNull())
            .dropDuplicates(["campaign"])
            .withColumn("campaign_id", F.sha2(F.col("campaign"), 256))
        )
        dim_channel = (
            marketing.select(F.col("channel"))
            .where(F.col("channel").isNotNull())
            .dropDuplicates(["channel"])
            .withColumn("channel_id", F.sha2(F.col("channel"), 256))
        )
    else:
        dim_campaign = spark.createDataFrame([], schema=T.StructType([T.StructField("campaign_id", T.StringType()), T.StructField("campaign", T.StringType())]))
        dim_channel = spark.createDataFrame([], schema=T.StructType([T.StructField("channel_id", T.StringType()), T.StructField("channel", T.StringType())]))

    dim_date_match = (
        matches.select(F.col("match_date").alias("date"))
        .dropDuplicates(["date"])
        .withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_week", F.date_format(F.col("date"), "E"))
    )

    fact_matches = (
        matches.join(dim_stadium, [matches.stadium == dim_stadium.stadium_name, matches.city == dim_stadium.city], "left")
        .join(dim_referee, [matches.referee == dim_referee.referee_name], "left")
        .join(dim_team.withColumnRenamed("team_id", "home_team_id").withColumnRenamed("team_name", "home_team_name"), matches.home_team == F.col("home_team_name"), "left")
        .join(dim_team.withColumnRenamed("team_id", "away_team_id").withColumnRenamed("team_name", "away_team_name"), matches.away_team == F.col("away_team_name"), "left")
        .join(dim_date_match.withColumnRenamed("date_id", "match_date_id"), matches.match_date == F.col("date"), "left")
        .select(
            F.col("match_id"),
            F.col("match_date_id"),
            F.col("match_ts"),
            F.col("stage"),
            matches.group_id,
            F.col("stadium_id"),
            F.col("referee_id"),
            F.col("home_team_id"),
            F.col("away_team_id"),
            F.col("home_goals"),
            F.col("away_goals"),
            F.col("home_xg"),
            F.col("away_xg"),
            F.col("possession_home_pct").cast("double"),
            F.col("shots_home").cast("int"),
            F.col("shots_away").cast("int"),
            F.col("shots_on_target_home").cast("int"),
            F.col("shots_on_target_away").cast("int"),
            F.col("cards_yellow_home").cast("int"),
            F.col("cards_yellow_away").cast("int"),
            F.col("cards_red_home").cast("int"),
            F.col("cards_red_away").cast("int"),
            F.col("attendance").cast("int"),
        )
    )

    fact_attendance = (
        attendance.join(dim_stadium, [attendance.stadium == dim_stadium.stadium_name, attendance.city == dim_stadium.city], "left")
        .join(dim_date_match.withColumnRenamed("date_id", "match_date_id"), attendance.match_date == F.col("date"), "left")
        .select(
            F.col("match_id"),
            F.col("match_date_id"),
            F.col("stadium_id"),
            F.col("capacity"),
            F.col("attendance"),
            F.col("occupancy_pct"),
            F.col("ticket_price_avg_usd").cast("double"),
            F.col("ticket_revenue_usd"),
            F.col("hospitality_revenue_usd"),
            F.col("concession_spend_per_capita_usd").cast("double"),
            F.col("concession_revenue_usd"),
            F.col("security_incidents_count").cast("int"),
            F.col("transport_disruption_minutes").cast("int"),
            F.col("fan_satisfaction_score"),
        )
    )

    fact_team_kpis = (
        team_kpis.alias("tk").join(dim_team.alias("team_dim"), F.col("tk.team") == F.col("team_dim.team_name"), "left")
        .select(
            F.date_format(F.col("tk.asof_date"), "yyyyMMdd").cast("int").alias("asof_date_id"),
            F.col("tk.asof_date"),
            F.col("team_dim.team_id"),
            F.col("tk.group_id"),
            F.col("tk.stage"),
            F.col("tk.played"),
            F.col("tk.points"),
            F.col("tk.goals_for"),
            F.col("tk.goals_against"),
            F.col("tk.goal_diff"),
            F.col("tk.xg_for"),
            F.col("tk.xg_against"),
            F.col("tk.shots_pg"),
            F.col("tk.shots_on_target_pg"),
            F.col("tk.pass_accuracy_pct"),
            F.col("tk.possession_pct"),
            F.col("tk.pressing_ppda"),
            F.col("tk.duels_won_pct"),
            F.col("tk.set_piece_xg"),
            F.col("tk.total_distance_km"),
            F.col("tk.injuries_reported"),
        )
    )

    if marketing is not None:
        dim_date_marketing = (
            marketing.select(F.col("date"))
            .dropDuplicates(["date"])
            .withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))
            .withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("day", F.dayofmonth("date"))
            .withColumn("day_of_week", F.date_format(F.col("date"), "E"))
        )

        fact_marketing = (
            marketing.join(dim_channel, "channel", "left")
            .join(dim_campaign, "campaign", "left")
            .join(dim_date_marketing, "date", "left")
            .select(
                F.col("date_id"),
                F.col("date"),
                F.col("channel_id"),
                F.col("campaign_id"),
                F.col("metric"),
                F.col("region"),
                F.col("value"),
                F.col("unit"),
            )
        )
    else:
        dim_date_marketing = spark.createDataFrame([], schema=T.StructType([T.StructField("date", T.DateType()), T.StructField("date_id", T.IntegerType())]))
        fact_marketing = spark.createDataFrame([], schema=T.StructType([]))

    if player_match is not None:
        fact_player_match = (
            player_match.alias("pm").join(dim_player.alias("p"), F.col("pm.player_name") == F.col("p.player_name"), "left")
            .join(dim_team.alias("t"), F.col("pm.team") == F.col("t.team_name"), "left")
            .join(dim_date_match.alias("dm").withColumnRenamed("date_id", "match_date_id"), F.col("pm.match_date") == F.col("dm.date"), "left")
            .select(
                F.col("pm.match_id"),
                F.col("match_date_id"),
                F.col("t.team_id").alias("team_id"),
                F.col("p.player_id"),
                F.col("p.position"),
                F.col("pm.minutes"),
                F.col("pm.goals"),
                F.col("pm.assists"),
                F.col("pm.shots"),
                F.col("pm.shots_on_target"),
                F.col("pm.key_passes"),
                F.col("pm.passes_completed"),
                F.col("pm.dribbles_completed"),
                F.col("pm.tackles"),
                F.col("pm.interceptions"),
                F.col("pm.xg"),
                F.col("pm.xa"),
            )
        )
    else:
        fact_player_match = spark.createDataFrame([], schema=T.StructType([]))

    bigtable_gold = (
        matches.join(attendance.select("match_id", "capacity", "occupancy_pct", "ticket_revenue_usd", "hospitality_revenue_usd", "concession_revenue_usd", "fan_satisfaction_score"), "match_id", "left")
        .join(team_kpis.alias("home").select(F.col("team").alias("home_team"), F.col("xg_for").alias("home_xg_for_group"), F.col("xg_against").alias("home_xg_against_group"), F.col("pass_accuracy_pct").alias("home_pass_accuracy_pct"), F.col("pressing_ppda").alias("home_pressing_ppda"), F.col("injuries_reported").alias("home_injuries_reported")), "home_team", "left")
        .join(team_kpis.alias("away").select(F.col("team").alias("away_team"), F.col("xg_for").alias("away_xg_for_group"), F.col("xg_against").alias("away_xg_against_group"), F.col("pass_accuracy_pct").alias("away_pass_accuracy_pct"), F.col("pressing_ppda").alias("away_pressing_ppda"), F.col("injuries_reported").alias("away_injuries_reported")), "away_team", "left")
        .withColumn("match_result", F.when(F.col("home_goals") > F.col("away_goals"), "HOME_WIN").when(F.col("home_goals") < F.col("away_goals"), "AWAY_WIN").otherwise("DRAW"))
        .select(
            F.col("match_id"),
            F.col("match_ts"),
            F.col("match_date"),
            F.col("stage"),
            F.col("group_id"),
            F.col("stadium"),
            F.col("city"),
            F.col("home_team"),
            F.col("away_team"),
            F.col("home_goals"),
            F.col("away_goals"),
            F.col("home_xg"),
            F.col("away_xg"),
            F.col("possession_home_pct"),
            F.col("shots_home"),
            F.col("shots_away"),
            F.col("shots_on_target_home"),
            F.col("shots_on_target_away"),
            F.col("cards_yellow_home"),
            F.col("cards_yellow_away"),
            F.col("cards_red_home"),
            F.col("cards_red_away"),
            F.col("attendance"),
            F.col("capacity"),
            F.col("occupancy_pct"),
            F.col("ticket_revenue_usd"),
            F.col("hospitality_revenue_usd"),
            F.col("concession_revenue_usd"),
            F.col("fan_satisfaction_score"),
            F.col("home_xg_for_group"),
            F.col("home_xg_against_group"),
            F.col("home_pass_accuracy_pct"),
            F.col("home_pressing_ppda"),
            F.col("home_injuries_reported"),
            F.col("away_xg_for_group"),
            F.col("away_xg_against_group"),
            F.col("away_pass_accuracy_pct"),
            F.col("away_pressing_ppda"),
            F.col("away_injuries_reported"),
            F.col("match_result"),
        )
    )

    if group_standings_raw is not None:
        group_standings = (
            group_standings_raw.join(dim_team, group_standings_raw.team == dim_team.team_name, "left")
            .select(
                F.col("team_id"),
                F.col("team").alias("team_name"),
                F.col("group_id"),
                F.col("pos").cast("int").alias("pos"),
                F.col("played").cast("int").alias("played"),
                F.col("points").cast("int").alias("points"),
            )
        )
    else:
        group_standings = spark.createDataFrame([], schema=T.StructType([]))

    if players_snapshot is not None:
        players_snapshot_gold = (
            players_snapshot.join(dim_team, players_snapshot.team == dim_team.team_name, "left")
            .select(
                F.sha2(F.concat_ws("|", F.col("player_name"), F.col("team")), 256).alias("player_id"),
                F.col("player_name"),
                F.col("team_id"),
                F.col("team").alias("team_name"),
                F.col("position"),
                F.col("goals").cast("int"),
                F.col("assists").cast("int"),
                F.col("goal_involvements").cast("int"),
                F.col("minutes_played").cast("int"),
            )
        )
    else:
        players_snapshot_gold = spark.createDataFrame([], schema=T.StructType([]))

    writes: Dict[str, DataFrame] = {
        "dim_team": dim_team,
        "dim_stadium": dim_stadium,
        "dim_referee": dim_referee,
        "dim_date": dim_date_match,
        "fact_matches": fact_matches,
        "fact_attendance": fact_attendance,
        "fact_team_kpis": fact_team_kpis,
        "bigtable_gold": bigtable_gold,
    }

    if marketing is not None:
        writes["dim_channel"] = dim_channel
        writes["dim_campaign"] = dim_campaign
        writes["dim_date_marketing"] = dim_date_marketing
        writes["fact_marketing"] = fact_marketing

    if player_match is not None:
        writes["dim_player"] = dim_player
        writes["fact_player_match"] = fact_player_match

    if group_standings_raw is not None:
        writes["group_standings"] = group_standings

    if players_snapshot is not None:
        writes["players_snapshot"] = players_snapshot_gold

    for table in sorted(writes.keys()):
        print(f"[PREPARE] Writing table: {table}, rows: {writes[table].count() if writes[table] else 0}")
        write_jdbc(writes[table], table)

    print("[DONE] Gold load complete")

    spark.stop()


if __name__ == "__main__":
    main()
