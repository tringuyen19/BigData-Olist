from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =====================================
# Spark Session
# =====================================

spark = SparkSession.builder \
    .appName("ETL_Orders_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================
# Paths
# =====================================

RAW_PATH = "hdfs://namenode:8020/raw/olist/olist_orders_dataset.csv"

SILVER_PATH = "hdfs://namenode:8020/processed/olist/orders"

CONTROL_PATH = "hdfs://namenode:8020/metadata/etl_control"
# =====================================
# Read Bronze
# =====================================

orders = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(RAW_PATH)

print("===== RAW =====")
print(f"Rows: {orders.count()}")
orders.printSchema()

# =====================================
# Read ETL Control
# =====================================

control = spark.read.parquet(
    CONTROL_PATH
)

last_processed_ts = (
    control
    .filter(
        F.col("table_name") == "orders"
    )
    .select("last_processed_ts")
    .collect()[0][0]
)

print(
    f"Last Processed TS: {last_processed_ts}"
)

# =====================================
# Cleaning
# =====================================

# Remove duplicates
orders = orders.dropDuplicates()

# Remove records missing critical keys
orders = orders.na.drop(
    subset=[
        "order_id",
        "customer_id"
    ]
)

# Remove records missing status
orders = orders.filter(
    F.col("order_status").isNotNull()
)

# =====================================
# Timestamp Conversion
# =====================================

orders = orders.withColumn(
    "purchase_ts",
    F.to_timestamp("order_purchase_timestamp")
)

orders = orders.withColumn(
    "approved_ts",
    F.to_timestamp("order_approved_at")
)

orders = orders.withColumn(
    "delivered_carrier_ts",
    F.to_timestamp("order_delivered_carrier_date")
)

orders = orders.withColumn(
    "delivered_customer_ts",
    F.to_timestamp("order_delivered_customer_date")
)

orders = orders.withColumn(
    "estimated_delivery_ts",
    F.to_timestamp("order_estimated_delivery_date")
)

# =====================================
# Incremental Filter
# =====================================

orders = orders.filter(
    F.col("purchase_ts")
    > F.lit(last_processed_ts)
)

incremental_rows = orders.count()

print(
    f"Incremental Rows: {incremental_rows}"
)

if incremental_rows == 0:

    print(
        "No New Records Found"
    )

    spark.stop()

    exit()

# =====================================
# Date Features
# =====================================

orders = orders.withColumn(
    "year",
    F.year("purchase_ts")
)

orders = orders.withColumn(
    "month",
    F.month("purchase_ts")
)

orders = orders.withColumn(
    "quarter",
    F.quarter("purchase_ts")
)

orders = orders.withColumn(
    "day",
    F.dayofmonth("purchase_ts")
)

# =====================================
# Delivery Metrics
# =====================================

orders = orders.withColumn(
    "delivery_days",
    F.datediff(
        F.col("delivered_customer_ts"),
        F.col("purchase_ts")
    )
)

orders = orders.withColumn(
    "is_late",
    F.when(
        F.col("delivered_customer_ts")
        > F.col("estimated_delivery_ts"),
        1
    ).otherwise(0)
)

# Remove invalid delivery records
orders = orders.filter(
    (F.col("delivery_days").isNull()) |
    (F.col("delivery_days") >= 0)
)

# =====================================
# Select Final Columns
# =====================================

orders_silver = orders.select(
    "order_id",
    "customer_id",
    "order_status",

    "purchase_ts",
    "approved_ts",
    "delivered_carrier_ts",
    "delivered_customer_ts",
    "estimated_delivery_ts",

    "year",
    "month",
    "quarter",
    "day",

    "delivery_days",
    "is_late"
)

print("===== SILVER =====")
print(f"Rows: {orders_silver.count()}")
orders_silver.printSchema()

# =====================================
# Write Silver Layer
# =====================================

orders_silver.write \
    .mode("append")\
    .partitionBy("year", "month") \
    .parquet(SILVER_PATH)

print("✅ Orders Silver created successfully")

# =====================================
# Update ETL Control
# =====================================

max_ts = (
    orders_silver
    .agg(
        F.max("purchase_ts")
    )
    .collect()[0][0]
)

new_control = spark.createDataFrame(

    [
        ("orders", max_ts)
    ],

    [
        "table_name",
        "last_processed_ts"
    ]
)

new_control.write \
    .mode("overwrite") \
    .parquet(CONTROL_PATH)

print(
    f"ETL Control Updated: {max_ts}"
)

spark.stop()