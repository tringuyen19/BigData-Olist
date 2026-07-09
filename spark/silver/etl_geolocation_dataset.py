from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ETL_Geolocation_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

RAW_PATH = \
"hdfs://namenode:8020/raw/olist/olist_geolocation_dataset.csv"

SILVER_PATH = \
"hdfs://namenode:8020/processed/olist/geolocation"

# =====================================
# Read Bronze
# =====================================

geo = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(RAW_PATH)

print("===== RAW =====")
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