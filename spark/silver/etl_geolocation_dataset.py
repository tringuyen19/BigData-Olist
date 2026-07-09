import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.pipeline_config import BRONZE_PATHS, SILVER_PATHS

spark = SparkSession.builder \
    .appName("ETL_Geolocation_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = \
BRONZE_PATHS["geolocation"]

SILVER_PATH = \
SILVER_PATHS["geolocation"]

# =====================================
# Read Bronze
# =====================================

geo = spark.read \
    .parquet(BRONZE_PATH)

print("===== BRONZE =====")
print(f"Rows: {geo.count()}")

# =====================================
# Cleaning
# =====================================

geo = geo.na.drop(
    subset=[
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng"
    ]
)

# =====================================
# Standardization
# =====================================

geo = geo.withColumn(
    "geolocation_city",
    F.lower(
        F.trim(
            F.col("geolocation_city")
        )
    )
)

geo = geo.withColumn(
    "geolocation_state",
    F.upper(
        F.trim(
            F.col("geolocation_state")
        )
    )
)

# =====================================
# Aggregate ZIP
# =====================================

geo_silver = geo.groupBy(
    "geolocation_zip_code_prefix",
    "geolocation_city",
    "geolocation_state"
).agg(
    F.avg("geolocation_lat")
        .alias("avg_lat"),
    F.avg("geolocation_lng")
        .alias("avg_lng")
)

# =====================================
# Location Key
# =====================================

geo_silver = geo_silver.withColumn(
    "location_key",
    F.concat_ws(
        "_",
        F.col("geolocation_zip_code_prefix"),
        F.col("geolocation_state")
    )
)

print("===== SILVER =====")
print(f"Rows: {geo_silver.count()}")

# =====================================
# Write Silver
# =====================================

geo_silver.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)

print("✅ Geolocation Silver created successfully")

spark.stop()
