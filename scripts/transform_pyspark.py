"""
transform_pyspark.py
PySpark job for Dataproc. Cleans and transforms 4 NBA CSV datasets.

Usage:
    gcloud dataproc jobs submit pyspark gs://BUCKET/scripts/transform_pyspark.py \
        --cluster=nba-cluster --region=us-central1
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

BUCKET = "final-project-493902-nba-analytics"


def create_spark():
    return (SparkSession.builder.appName("NBA-Data-Transform")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate())


def transform_box_scores(spark, bucket):
    path = f"gs://{bucket}/raw/NBA Player Box Score Stats(1950 - 2022).csv"
    print(f"Loading box scores from {path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df = df.drop("_c0")
    df = df.select(
        F.col("Season").cast(IntegerType()).alias("season"),
        F.col("Game_ID").cast("string").alias("game_id"),
        F.col("PLAYER_NAME").alias("player_name"),
        F.col("Team").alias("team"),
        F.to_date(F.col("GAME_DATE"), "MMM dd, yyyy").alias("game_date"),
        F.col("MATCHUP").alias("matchup"),
        F.col("WL").alias("win_loss"),
        F.col("MIN").cast(FloatType()).alias("minutes"),
        F.col("FGM").cast(IntegerType()).alias("fg_made"),
        F.col("FGA").cast(IntegerType()).alias("fg_attempted"),
        F.col("FG_PCT").cast(FloatType()).alias("fg_pct"),
        F.col("FG3M").cast(IntegerType()).alias("fg3_made"),
        F.col("FG3A").cast(IntegerType()).alias("fg3_attempted"),
        F.col("FG3_PCT").cast(FloatType()).alias("fg3_pct"),
        F.col("FTM").cast(IntegerType()).alias("ft_made"),
        F.col("FTA").cast(IntegerType()).alias("ft_attempted"),
        F.col("FT_PCT").cast(FloatType()).alias("ft_pct"),
        F.col("OREB").cast(IntegerType()).alias("off_rebounds"),
        F.col("DREB").cast(IntegerType()).alias("def_rebounds"),
        F.col("REB").cast(IntegerType()).alias("rebounds"),
        F.col("AST").cast(IntegerType()).alias("assists"),
        F.col("STL").cast(IntegerType()).alias("steals"),
        F.col("BLK").cast(IntegerType()).alias("blocks"),
        F.col("TOV").cast(IntegerType()).alias("turnovers"),
        F.col("PF").cast(IntegerType()).alias("personal_fouls"),
        F.col("PTS").cast(IntegerType()).alias("points"),
        F.col("PLUS_MINUS").cast(IntegerType()).alias("plus_minus"),
    )
    df = df.filter(F.col("minutes").isNotNull() & (F.col("minutes") > 0))
    df = df.withColumn("pts_per36", F.round(F.col("points") / F.col("minutes") * 36, 1))
    df = df.withColumn("reb_per36", F.round(F.col("rebounds") / F.col("minutes") * 36, 1))
    df = df.withColumn("ast_per36", F.round(F.col("assists") / F.col("minutes") * 36, 1))
    df = df.withColumn("true_shooting_pct",
        F.when((F.col("fg_attempted") + 0.44 * F.col("ft_attempted")) > 0,
            F.round(F.col("points") / (2 * (F.col("fg_attempted") + 0.44 * F.col("ft_attempted"))), 3)))
    df = df.withColumn("is_home", F.when(F.col("matchup").contains("vs."), True).otherwise(False))
    df = df.dropDuplicates(["player_name", "game_id"])
    print(f"  Box scores transformed: {df.count()} rows")
    return df


def transform_season_stats(spark, bucket):
    path = f"gs://{bucket}/raw/NBA Player Stats(1950 - 2022).csv"
    print(f"Loading season stats from {path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df = df.drop("_c0", "Unnamed: 0")
    df = df.select(
        F.col("Season").cast(IntegerType()).alias("season"),
        F.col("Player").alias("player_name"), F.col("Pos").alias("position"),
        F.col("Age").cast(IntegerType()).alias("age"), F.col("Tm").alias("team"),
        F.col("G").cast(IntegerType()).alias("games_played"),
        F.col("GS").cast(IntegerType()).alias("games_started"),
        F.col("MP").cast(FloatType()).alias("minutes_played"),
        F.col("FG").cast(FloatType()).alias("fg_made"),
        F.col("FGA").cast(FloatType()).alias("fg_attempted"),
        F.col("`FG%`").cast(FloatType()).alias("fg_pct"),
        F.col("`3P`").cast(FloatType()).alias("fg3_made"),
        F.col("`3PA`").cast(FloatType()).alias("fg3_attempted"),
        F.col("`3P%`").cast(FloatType()).alias("fg3_pct"),
        F.col("`2P`").cast(FloatType()).alias("fg2_made"),
        F.col("`2PA`").cast(FloatType()).alias("fg2_attempted"),
        F.col("`2P%`").cast(FloatType()).alias("fg2_pct"),
        F.col("`eFG%`").cast(FloatType()).alias("efg_pct"),
        F.col("FT").cast(FloatType()).alias("ft_made"),
        F.col("FTA").cast(FloatType()).alias("ft_attempted"),
        F.col("`FT%`").cast(FloatType()).alias("ft_pct"),
        F.col("ORB").cast(FloatType()).alias("off_rebounds"),
        F.col("DRB").cast(FloatType()).alias("def_rebounds"),
        F.col("TRB").cast(FloatType()).alias("total_rebounds"),
        F.col("AST").cast(FloatType()).alias("assists"),
        F.col("STL").cast(FloatType()).alias("steals"),
        F.col("BLK").cast(FloatType()).alias("blocks"),
        F.col("TOV").cast(FloatType()).alias("turnovers"),
        F.col("PF").cast(FloatType()).alias("personal_fouls"),
        F.col("PTS").cast(FloatType()).alias("points"),
    )
    df = df.filter(F.col("team") != "TOT")
    df = df.dropDuplicates(["player_name", "season", "team"])
    print(f"  Season stats transformed: {df.count()} rows")
    return df


def transform_salaries(spark, bucket):
    path = f"gs://{bucket}/raw/NBA Salaries(1990-2023).csv"
    print(f"Loading salaries from {path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df = df.drop("_c0")
    df = df.select(
        F.col("playerName").alias("player_name"),
        F.col("seasonStartYear").cast(IntegerType()).alias("season"),
        F.regexp_replace(F.col("salary"), "[$,]", "").cast("long").alias("salary"),
        F.regexp_replace(F.col("inflationAdjSalary"), "[$,]", "").cast("long").alias("salary_inflation_adj"),
    )
    df = df.filter(F.col("salary").isNotNull())
    df = df.dropDuplicates(["player_name", "season"])
    print(f"  Salaries transformed: {df.count()} rows")
    return df


def transform_payroll(spark, bucket):
    path = f"gs://{bucket}/raw/NBA Payroll(1990-2023).csv"
    print(f"Loading payroll from {path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df = df.drop("_c0")
    df = df.select(
        F.col("team").alias("team_name"),
        F.col("seasonStartYear").cast(IntegerType()).alias("season"),
        F.regexp_replace(F.col("payroll"), "[$,]", "").cast("long").alias("payroll"),
        F.regexp_replace(F.col("inflationAdjPayroll"), "[$,]", "").cast("long").alias("payroll_inflation_adj"),
    )
    df = df.filter(F.col("payroll").isNotNull())
    print(f"  Payroll transformed: {df.count()} rows")
    return df


def write_parquet(df, bucket, name):
    path = f"gs://{bucket}/processed/{name}"
    print(f"  Writing {name} -> {path}")
    df.write.mode("overwrite").parquet(path)


def main():
    spark = create_spark()
    print("=" * 60)
    print("NBA Data Transformation Pipeline")
    print("=" * 60)
    box_scores = transform_box_scores(spark, BUCKET)
    season_stats = transform_season_stats(spark, BUCKET)
    salaries = transform_salaries(spark, BUCKET)
    payroll = transform_payroll(spark, BUCKET)
    print("\nWriting processed data to GCS as Parquet...")
    write_parquet(box_scores, BUCKET, "player_box_scores")
    write_parquet(season_stats, BUCKET, "player_season_stats")
    write_parquet(salaries, BUCKET, "player_salaries")
    write_parquet(payroll, BUCKET, "team_payroll")
    print("\nTransformation complete!")
    spark.stop()


if __name__ == "__main__":
    main()
