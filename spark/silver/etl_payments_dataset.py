import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.pipeline_config import BRONZE_PATHS, SILVER_PATHS

spark = SparkSession.builder \
    .appName("ETL_Payments_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = \
BRONZE_PATHS["payments"]

SILVER_PATH = \
SILVER_PATHS["payments"]

# =====================================
# Read Bronze
# =====================================

payments = spark.read \
    .parquet(BRONZE_PATH)

print("===== BRONZE =====")
print(f"Rows: {payments.count()}")

# =====================================
# Cleaning
# =====================================

payments = payments.dropDuplicates()

payments = payments.na.drop(
    subset=["order_id"]
)

payments = payments.filter(
    F.col("payment_value") > 0
)

# =====================================
# Standardization
# =====================================

payments = payments.withColumn(
    "payment_type",
    F.lower(
        F.trim(
            F.col("payment_type")
        )
    )
)

# =====================================
# Features
# =====================================

payments = payments.withColumn(
    "is_installment",
    F.when(
        F.col("payment_installments") > 1,
        1
    ).otherwise(0)
)

payments = payments.withColumn(
    "payment_bucket",
    F.when(F.col("payment_value") < 50, "0-50")
     .when(F.col("payment_value") < 100, "50-100")
     .when(F.col("payment_value") < 500, "100-500")
     .otherwise("500+")
)

# =====================================
# Final Columns
# =====================================

payments_silver = payments.select(
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
    "is_installment",
    "payment_bucket"
)

print("===== SILVER =====")
print(f"Rows: {payments_silver.count()}")

payments_silver.printSchema()

# =====================================
# Write Silver
# =====================================

payments_silver.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)

print("✅ Payments Silver created successfully")

spark.stop()
