import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[2]))

from config.pipeline_config import GOLD_DIMENSION_PATHS, SILVER_PATHS

spark = SparkSession.builder \
    .appName("Gold_Dim_Date") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================
# Paths
# =====================================

SILVER_PATH = \
SILVER_PATHS["orders"]

GOLD_PATH = \
GOLD_DIMENSION_PATHS["dim_date"]

# =====================================
# Read Silver
# =====================================

orders = spark.read.parquet(
    SILVER_PATH
)

# =====================================
# Extract Unique Dates
# =====================================

dates = orders.select(
    F.to_date("purchase_ts").alias("full_date")
).dropDuplicates()

# =====================================
# Date Attributes
# =====================================

dates = dates.withColumn(
    "date_key",
    F.date_format(
        "full_date",
        "yyyyMMdd"
    ).cast("long")
)

dates = dates.withColumn(
    "year",
    F.year("full_date")
)

dates = dates.withColumn(
    "quarter",
    F.quarter("full_date")
)

dates = dates.withColumn(
    "month",
    F.month("full_date")
)

dates = dates.withColumn(
    "day",
    F.dayofmonth("full_date")
)

dates = dates.withColumn(
    "month_name",
    F.date_format(
        "full_date",
        "MMMM"
    )
)

dates = dates.withColumn(
    "day_of_week",
    F.date_format(
        "full_date",
        "EEEE"
    )
)

# =====================================
# Weekend Flag
# =====================================

dates = dates.withColumn(
    "is_weekend",
    F.when(
        F.dayofweek("full_date").isin(1, 7),
        1
    ).otherwise(0)
)

# =====================================
# Final Columns
# =====================================

dim_date = dates.select(
    "date_key",
    "full_date",

    "year",
    "quarter",
    "month",
    "day",

    "month_name",
    "day_of_week",

    "is_weekend"
)

print("===== GOLD =====")
print(f"Rows: {dim_date.count()}")

dim_date.printSchema()

# =====================================
# Write Gold
# =====================================

dim_date.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)

print("✅ Gold Dim Date created")

spark.stop()
