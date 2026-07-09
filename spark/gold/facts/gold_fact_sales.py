from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Gold_Fact_Sales") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================
# Paths
# =====================================

ORDERS_PATH = \
"hdfs://namenode:8020/processed/olist/orders"

ITEMS_PATH = \
"hdfs://namenode:8020/processed/olist/order_items"

PAYMENTS_PATH = \
"hdfs://namenode:8020/processed/olist/payments"

REVIEWS_PATH = \
"hdfs://namenode:8020/processed/olist/reviews"

DIM_CUSTOMER_PATH = \
"hdfs://namenode:8020/gold/dimensions/dim_customer"

DIM_PRODUCT_PATH = \
"hdfs://namenode:8020/gold/dimensions/dim_product"

DIM_SELLER_PATH = \
"hdfs://namenode:8020/gold/dimensions/dim_seller"

DIM_DATE_PATH = \
"hdfs://namenode:8020/gold/dimensions/dim_date"

FACT_PATH = \
"hdfs://namenode:8020/gold/facts/fact_sales"

# =====================================
# Read
# =====================================

orders = spark.read.parquet(ORDERS_PATH)

items = spark.read.parquet(ITEMS_PATH)

payments = spark.read.parquet(PAYMENTS_PATH)

reviews = spark.read.parquet(REVIEWS_PATH)

dim_customer = spark.read.parquet(
    DIM_CUSTOMER_PATH
)

dim_product = spark.read.parquet(
    DIM_PRODUCT_PATH
)

dim_seller = spark.read.parquet(
    DIM_SELLER_PATH
)

dim_date = spark.read.parquet(
    DIM_DATE_PATH
)

# =====================================
# Aggregate Payments
# =====================================

payments_agg = payments.groupBy(
    "order_id"
).agg(
    F.sum("payment_value")
     .alias("payment_value")
)

# =====================================
# Aggregate Reviews
# =====================================

reviews_agg = reviews.groupBy(
    "order_id"
).agg(
    F.avg("review_score")
     .alias("review_score")
)

# =====================================
# Base Fact
# =====================================

fact = items \
    .join(
        orders.select(
            "order_id",
            "customer_id",
            "purchase_ts",
            "delivery_days",
            "is_late"
        ),
        "order_id",
        "left"
    ) \
    .join(
        payments_agg,
        "order_id",
        "left"
    ) \
    .join(
        reviews_agg,
        "order_id",
        "left"
    )

# =====================================
# Date Key
# =====================================

fact = fact.withColumn(
    "full_date",
    F.to_date("purchase_ts")
)

# =====================================
# Join Dimensions
# =====================================

fact = fact.join(
    dim_customer.select(
        "customer_key",
        "customer_id"
    ),
    "customer_id",
    "left"
)

fact = fact.join(
    dim_product.select(
        "product_key",
        "product_id"
    ),
    "product_id",
    "left"
)

fact = fact.join(
    dim_seller.select(
        "seller_key",
        "seller_id"
    ),
    "seller_id",
    "left"
)

fact = fact.join(
    dim_date.select(
        "date_key",
        "full_date"
    ),
    "full_date",
    "left"
)

# =====================================
# Final Fact
# =====================================

fact_sales = fact.select(
    "customer_key",
    "product_key",
    "seller_key",
    "date_key",

    "order_id",
    "order_item_id",

    "price",
    "freight_value",

    "payment_value",

    "review_score",

    "delivery_days",
    "is_late"
)

print("===== FACT SALES =====")
print(f"Rows: {fact_sales.count()}")

fact_sales.printSchema()

# =====================================
# Write
# =====================================

fact_sales.write \
    .mode("overwrite") \
    .parquet(FACT_PATH)

print("✅ Fact Sales created")

spark.stop()