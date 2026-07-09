from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ETL_Order_Items_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

RAW_PATH = \
"hdfs://namenode:8020/raw/olist/olist_order_items_dataset.csv"

SILVER_PATH = \
"hdfs://namenode:8020/processed/olist/order_items"

# =====================================
# Read Bronze
# =====================================

items = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(RAW_PATH)

print("===== RAW =====")
print(f"Rows: {items.count()}")

# =====================================
# Cleaning
# =====================================

items = items.dropDuplicates()

items = items.na.drop(
    subset=[
        "order_id",
        "product_id",
        "seller_id"
    ]
)

items = items.filter(
    F.col("price") > 0
)

items = items.filter(
    F.col("freight_value") >= 0
)

# =====================================
# Timestamp Conversion
# =====================================

items = items.withColumn(
    "shipping_limit_ts",
    F.to_timestamp(
        "shipping_limit_date"
    )
)

# =====================================
# Features
# =====================================

items = items.withColumn(
    "total_item_value",
    F.round(
        F.col("price")
        + F.col("freight_value"),
        2
    )
)

items = items.withColumn(
    "shipping_ratio",
    F.round(
        F.col("freight_value")
        / F.col("price"),
        4
    )
)

# =====================================
# Final Columns
# =====================================

items_silver = items.select(
    "order_id",
    "order_item_id",

    "product_id",
    "seller_id",

    "shipping_limit_ts",

    "price",
    "freight_value",

    "total_item_value",
    "shipping_ratio"
)

print("===== SILVER =====")
print(f"Rows: {items_silver.count()}")

items_silver.printSchema()

# =====================================
# Write Silver
# =====================================

items_silver.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)

print("✅ Order Items Silver created successfully")

spark.stop()