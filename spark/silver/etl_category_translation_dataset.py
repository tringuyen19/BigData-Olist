import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.pipeline_config import BRONZE_PATHS, SILVER_PATHS

spark = SparkSession.builder \
    .appName("ETL_Category_Translation_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = \
BRONZE_PATHS["category_translation"]

SILVER_PATH = \
SILVER_PATHS["category_translation"]

# =====================================
# Read Bronze
# =====================================

category = spark.read \
    .parquet(BRONZE_PATH)

print("===== BRONZE =====")
print(f"Rows: {category.count()}")

# =====================================
# Cleaning
# =====================================

category = category.dropDuplicates()

category = category.na.drop(
    subset=[
        "product_category_name",
        "product_category_name_english"
    ]
)

# =====================================
# Standardization
# =====================================

category = category.withColumn(
    "product_category_name",
    F.lower(
        F.trim(
            F.col("product_category_name")
        )
    )
)

category = category.withColumn(
    "product_category_name_english",
    F.lower(
        F.trim(
            F.col("product_category_name_english")
        )
    )
)

# =====================================
# Final Columns
# =====================================

category_silver = category.select(
    "product_category_name",
    "product_category_name_english"
)

print("===== SILVER =====")
print(f"Rows: {category_silver.count()}")

category_silver.printSchema()

# =====================================
# Write Silver
# =====================================

category_silver.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)

print("✅ Category Translation Silver created successfully")

spark.stop()
