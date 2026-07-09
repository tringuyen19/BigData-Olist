from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ETL_Products_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

RAW_PATH = \
"hdfs://namenode:8020/raw/olist/olist_products_dataset.csv"

SILVER_PATH = \
"hdfs://namenode:8020/processed/olist/products"

# =====================================
# Read Bronze
# =====================================

products = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(RAW_PATH)

print("===== RAW =====")
print(f"Rows: {products.count()}")

# =====================================
# Cleaning
# =====================================

products = products.dropDuplicates()

products = products.na.drop(
    subset=["product_id"]
)

# =====================================
# Standardization
# =====================================

products = products.withColumn(
    "product_category_name",
    F.lower(
        F.trim(
            F.col("product_category_name")
        )
    )
)

# =====================================
# Features
# =====================================

products = products.withColumn(
    "weight_kg",
    F.round(
        F.col("product_weight_g") / 1000,
        3
    )
)

products = products.withColumn(
    "product_volume_cm3",
    F.col("product_length_cm")
    * F.col("product_height_cm")
    * F.col("product_width_cm")
)

# =====================================
# Select Final Columns
# =====================================

products_silver = products.select(
    "product_id",
    "product_category_name",

    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",

    "product_weight_g",
    "weight_kg",

    "product_length_cm",
    "product_height_cm",
    "product_width_cm",

    "product_volume_cm3"
)

print("===== SILVER =====")
print(f"Rows: {products_silver.count()}")

products_silver.printSchema()

# =====================================
# Write Silver
# =====================================

products_silver.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)

print("✅ Products Silver created successfully")

spark.stop()