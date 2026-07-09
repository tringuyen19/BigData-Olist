import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.pipeline_config import BRONZE_PATHS, ETL_CONTROL_PATH, SILVER_PATHS

spark = SparkSession.builder \
    .appName("ETL_Reviews_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = \
BRONZE_PATHS["reviews"]

SILVER_PATH = \
SILVER_PATHS["reviews"]

CONTROL_PATH = \
ETL_CONTROL_PATH

# =====================================
# Read Bronze
# =====================================

reviews = spark.read \
    .parquet(BRONZE_PATH)

print("===== BRONZE =====")
print(f"Rows: {reviews.count()}")

# =====================================
# Read ETL Control
# =====================================

control = spark.read.parquet(
    CONTROL_PATH
)

last_processed_ts = (
    control
    .filter(
        F.col("table_name") == "reviews"
    )
    .select("last_processed_ts")
    .collect()[0][0]
)

print(
    f"Last Processed TS: {last_processed_ts}"
)

# =====================================
# Cleaning
# =====================================

reviews = reviews.dropDuplicates()

reviews = reviews.na.drop(
    subset=[
        "review_id",
        "order_id"
    ]
)

reviews = reviews.filter(
    F.col("review_score").between(1, 5)
)

# =====================================
# Timestamp Conversion
# =====================================

reviews = reviews.withColumn(
    "review_creation_ts",
    F.to_timestamp("review_creation_date")
)

reviews = reviews.withColumn(
    "review_answer_ts",
    F.to_timestamp("review_answer_timestamp")
)

# =====================================
# Incremental Filter
# =====================================

reviews = reviews.filter(
    F.col("review_creation_ts")
    > F.lit(last_processed_ts)
)

incremental_rows = reviews.count()

print(
    f"Incremental Rows: {incremental_rows}"
)

if incremental_rows == 0:

    print(
        "No New Records Found"
    )

    spark.stop()

    exit()

# =====================================
# Features
# =====================================

reviews = reviews.withColumn(
    "sentiment",
    F.when(F.col("review_score") >= 4, "positive")
     .when(F.col("review_score") == 3, "neutral")
     .otherwise("negative")
)

reviews = reviews.withColumn(
    "has_comment",
    F.when(
        F.length(
            F.trim(
                F.coalesce(
                    F.col("review_comment_message"),
                    F.lit("")
                )
            )
        ) > 0,
        1
    ).otherwise(0)
)

# =====================================
# Final Columns
# =====================================

reviews_silver = reviews.select(
    "review_id",
    "order_id",
    "review_score",

    "review_creation_ts",
    "review_answer_ts",

    "review_comment_title",
    "review_comment_message",

    "sentiment",
    "has_comment"
)

print("===== SILVER =====")
print(f"Rows: {reviews_silver.count()}")

reviews_silver.printSchema()

# =====================================
# Write Silver
# =====================================

reviews_silver.write \
    .mode("append") \
    .parquet(SILVER_PATH)

print("✅ Reviews Silver created successfully")

# =====================================
# Update ETL Control
# =====================================

max_ts = (
    reviews_silver
    .agg(
        F.max("review_creation_ts")
    )
    .collect()[0][0]
)

# =====================================
# Update ETL Control
# =====================================

control_rows = spark.read.parquet(
    CONTROL_PATH
).collect()

new_data = []

for row in control_rows:

    if row["table_name"] == "reviews":

        new_data.append(
            (
                "reviews",
                max_ts
            )
        )

    else:

        new_data.append(
            (
                row["table_name"],
                row["last_processed_ts"]
            )
        )

final_control = spark.createDataFrame(
    new_data,
    [
        "table_name",
        "last_processed_ts"
    ]
)

final_control.write \
    .mode("overwrite") \
    .parquet(CONTROL_PATH)

print(
    f"Reviews ETL Control Updated: {max_ts}"
)

spark.stop()
