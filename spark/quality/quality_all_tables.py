from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =====================================
# Spark
# =====================================

spark = SparkSession.builder \
    .appName("Quality_All_Tables") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================
# Config
# =====================================

TABLES = {

    "orders": {
        "path":
        "hdfs://namenode:8020/raw/olist/olist_orders_dataset.csv",

        "pk": [
            "order_id"
        ],

        "critical_cols": [
            "order_id",
            "customer_id",
            "order_status"
        ]
    },

    "customers": {
        "path":
        "hdfs://namenode:8020/raw/olist/olist_customers_dataset.csv",

        "pk": [
            "customer_id"
        ],

        "critical_cols": [
            "customer_id",
            "customer_unique_id"
        ]
    },

    "products": {
        "path":
        "hdfs://namenode:8020/raw/olist/olist_products_dataset.csv",

        "pk": [
            "product_id"
        ],

        "critical_cols": [
            "product_id",
            "product_category_name"
        ]
    },

    "payments": {
        "path":
        "hdfs://namenode:8020/raw/olist/olist_order_payments_dataset.csv",

        "pk": [
            "order_id",
            "payment_sequential"
        ],

        "critical_cols": [
            "order_id",
            "payment_type",
            "payment_value"
        ]
    },

    "reviews": {
        "path":
        "hdfs://namenode:8020/raw/olist/olist_order_reviews_dataset.csv",

        "pk": [
            "review_id"
        ],

        "critical_cols": [
            "review_id",
            "order_id",
            "review_score"
        ]
    },

    "sellers": {
        "path":
        "hdfs://namenode:8020/raw/olist/olist_sellers_dataset.csv",

        "pk": [
            "seller_id"
        ],

        "critical_cols": [
            "seller_id",
            "seller_state"
        ]
    },

    "order_items": {
        "path":
        "hdfs://namenode:8020/raw/olist/olist_order_items_dataset.csv",

        "pk": [
            "order_id",
            "order_item_id"
        ],

        "critical_cols": [
            "order_id",
            "product_id",
            "seller_id"
        ]
    },

    "geolocation": {
        "path":
        "hdfs://namenode:8020/raw/olist/olist_geolocation_dataset.csv",

        "pk": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng"
        ],

        "critical_cols": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng"
        ]
    },

    "category_translation": {
        "path":
        "hdfs://namenode:8020/raw/olist/product_category_name_translation.csv",

        "pk": [
            "product_category_name"
        ],

        "critical_cols": [
            "product_category_name",
            "product_category_name_english"
        ]
    }

}
# =====================================
# Loop
# =====================================

for table_name, config in TABLES.items():

    print(f"\n===== {table_name.upper()} =====")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(config["path"])

    row_count = df.count()

    report = [

        ("row_count", row_count)

    ]

    # Null checks

    for col_name in config["critical_cols"]:

        null_count = df.filter(
            F.col(col_name).isNull()
        ).count()

        report.append(
            (f"null_{col_name}", null_count)
        )

    # Duplicate check

    pk_cols = config["pk"]

    duplicate_count = (
        df.groupBy(*pk_cols)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    report.append(
        (
            f"duplicate_{'_'.join(pk_cols)}",
            duplicate_count
        )
    )

    report_df = spark.createDataFrame(
        report,
        ["metric_name",
         "metric_value"]
    )

    report_df.show(
        truncate=False
    )

    output_path = \
        f"hdfs://namenode:8020/quality/{table_name}"

    report_df.write \
        .mode("overwrite") \
        .parquet(output_path)

    print(
        f"✅ {table_name} quality completed"
    )

spark.stop()