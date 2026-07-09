from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Gold_Dim_Location") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================
# Paths
# =====================================

GEO_PATH = \
"hdfs://namenode:8020/processed/olist/geolocation"

GOLD_PATH = \
"hdfs://namenode:8020/gold/dimensions/dim_location"

# =====================================
# Read Silver
# =====================================

geo = spark.read.parquet(
    GEO_PATH
)

print("===== GEO =====")
print(f"Rows: {geo.count()}")

# =====================================
# Final Dimension
# =====================================

dim_location = geo.select(
    "location_key",

    F.col(
        "geolocation_zip_code_prefix"
    ).alias("zip_code"),

    F.col(
        "geolocation_city"
    ).alias("city"),

    F.col(
        "geolocation_state"
    ).alias("state"),

    F.col(
        "avg_lat"
    ).alias("latitude"),

    F.col(
        "avg_lng"
    ).alias("longitude")
)

print("===== DIM LOCATION =====")
print(f"Rows: {dim_location.count()}")

dim_location.printSchema()

# =====================================
# Write Gold
# =====================================

dim_location.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)

print("✅ Gold Dim Location created")

spark.stop()