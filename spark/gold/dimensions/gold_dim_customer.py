import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[2]))

from config.pipeline_config import GOLD_DIMENSION_PATHS, SILVER_PATHS

spark = SparkSession.builder \
    .appName("Gold_Dim_Customer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================
# Paths
# =====================================

SILVER_PATH = \
SILVER_PATHS["customers"]

GOLD_PATH = \
GOLD_DIMENSION_PATHS["dim_customer"]

# =====================================
# Read Silver
# =====================================

customers = spark.read.parquet(
    SILVER_PATH
)

print("===== SILVER =====")
print(f"Rows: {customers.count()}")

# =====================================
# Business Attributes
# =====================================

customers = customers.withColumn(
    "customer_region",
    F.when(
        F.col("customer_state").isin(
            "SP", "RJ", "MG", "ES"
        ),
        "Southeast"
    )
    .when(
        F.col("customer_state").isin(
            "PR", "RS", "SC"
        ),
        "South"
    )
    .otherwise("Other")
)

# =====================================
# Surrogate Key
# =====================================

customers = customers.withColumn(
    "customer_key",
    F.monotonically_increasing_id()
)

# =====================================
# Final Columns
# =====================================

dim_customer = customers.select(
    "customer_key",

    "customer_id",
    "customer_unique_id",

    "customer_city",
    "customer_state",
    "customer_region"
)

print("===== GOLD =====")
print(f"Rows: {dim_customer.count()}")

dim_customer.printSchema()

# =====================================
# Write Gold
# =====================================

dim_customer.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)

print("✅ Gold Dim Customer created")

spark.stop()
