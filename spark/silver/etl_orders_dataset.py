import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.pipeline_config import BRONZE_PATHS, ETL_CONTROL_PATH, SILVER_PATHS

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

BRONZE_PATH = BRONZE_PATHS["orders"]

SILVER_PATH = SILVER_PATHS["orders"]

CONTROL_PATH = ETL_CONTROL_PATH
# =====================================
# Read Bronze
# =====================================

orders = spark.read \
    .parquet(BRONZE_PATH)

print("===== BRONZE =====")
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

max_ts_str = max_ts.strftime("%Y-%m-%d %H:%M:%S")

control_rows = spark.read.parquet(
    CONTROL_PATH
).collect()

new_data = []
orders_control_found = False

for row in control_rows:

    if row["table_name"] == "orders":

        new_data.append(
            (
                "orders",
                max_ts_str
            )
        )

        orders_control_found = True

    else:

        new_data.append(
            (
                row["table_name"],
                row["last_processed_ts"]
            )
        )

if not orders_control_found:

    new_data.append(
        (
            "orders",
            max_ts_str
        )
    )

final_control = spark.createDataFrame(
    new_data,
    [
        "table_name",
        "last_processed_ts"
    ]
)

final_control.write \
    .mode("overwrite") \
    .parquet(CONTROL_PATH)

print(
    f"Orders ETL Control Updated: {max_ts_str}"
)

spark.stop()
