from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ETL_Category_Translation_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

RAW_PATH = \
"hdfs://namenode:8020/raw/olist/product_category_name_translation.csv"

SILVER_PATH = \
"hdfs://namenode:8020/processed/olist/category_translation"

# =====================================
# Read Bronze
# =====================================

category = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(RAW_PATH)

print("===== RAW =====")
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