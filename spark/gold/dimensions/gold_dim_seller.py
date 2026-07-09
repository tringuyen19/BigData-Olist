import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[2]))

from config.pipeline_config import GOLD_DIMENSION_PATHS, SILVER_PATHS

spark = SparkSession.builder \
    .appName("Gold_Dim_Seller") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================
# Paths
# =====================================

SILVER_PATH = \
SILVER_PATHS["sellers"]

GOLD_PATH = \
GOLD_DIMENSION_PATHS["dim_seller"]

# =====================================
# Read Silver
# =====================================

sellers = spark.read.parquet(
    SILVER_PATH
)

print("===== SILVER =====")
print(f"Rows: {sellers.count()}")

# =====================================
# Region
# =====================================

sellers = sellers.withColumn(
    "seller_region",
    F.when(
        F.col("seller_state").isin(
            "SP", "RJ", "MG", "ES"
        ),
        "Southeast"
    )
    .when(
        F.col("seller_state").isin(
            "PR", "RS", "SC"
        ),
        "South"
    )
    .otherwise("Other")
)

# =====================================
# Surrogate Key
# =====================================

sellers = sellers.withColumn(
    "seller_key",
    F.monotonically_increasing_id()
)

# =====================================
# Final Columns
# =====================================

dim_seller = sellers.select(
    "seller_key",

    "seller_id",

    "seller_city",
    "seller_state",
    "seller_region"
)

print("===== GOLD =====")
print(f"Rows: {dim_seller.count()}")

dim_seller.printSchema()

# =====================================
# Write Gold
# =====================================

dim_seller.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)

print("✅ Gold Dim Seller created")

spark.stop()
