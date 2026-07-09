import os

HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:8020")

RAW_BASE = f"{HDFS_BASE}/raw/olist"
BRONZE_BASE = f"{HDFS_BASE}/bronze/olist"
SILVER_BASE = f"{HDFS_BASE}/processed/olist"
QUALITY_BASE = f"{HDFS_BASE}/quality"
METADATA_BASE = f"{HDFS_BASE}/metadata"
GOLD_DIMENSIONS_BASE = f"{HDFS_BASE}/gold/dimensions"
GOLD_FACTS_BASE = f"{HDFS_BASE}/gold/facts"
MOCK_GOLD_PATH = f"{HDFS_BASE}/gold/orders_mock"

RAW_PATHS = {
    "orders": f"{RAW_BASE}/olist_orders_dataset.csv",
    "customers": f"{RAW_BASE}/olist_customers_dataset.csv",
    "products": f"{RAW_BASE}/olist_products_dataset.csv",
    "payments": f"{RAW_BASE}/olist_order_payments_dataset.csv",
    "reviews": f"{RAW_BASE}/olist_order_reviews_dataset.csv",
    "sellers": f"{RAW_BASE}/olist_sellers_dataset.csv",
    "order_items": f"{RAW_BASE}/olist_order_items_dataset.csv",
    "geolocation": f"{RAW_BASE}/olist_geolocation_dataset.csv",
    "category_translation": f"{RAW_BASE}/product_category_name_translation.csv",
}

BRONZE_PATHS = {
    table_name: f"{BRONZE_BASE}/{table_name}"
    for table_name in RAW_PATHS
}

SILVER_PATHS = {
    table_name: f"{SILVER_BASE}/{table_name}"
    for table_name in RAW_PATHS
}

QUALITY_PATHS = {
    table_name: f"{QUALITY_BASE}/{table_name}"
    for table_name in RAW_PATHS
}

GOLD_DIMENSION_PATHS = {
    "dim_customer": f"{GOLD_DIMENSIONS_BASE}/dim_customer",
    "dim_product": f"{GOLD_DIMENSIONS_BASE}/dim_product",
    "dim_seller": f"{GOLD_DIMENSIONS_BASE}/dim_seller",
    "dim_date": f"{GOLD_DIMENSIONS_BASE}/dim_date",
    "dim_location": f"{GOLD_DIMENSIONS_BASE}/dim_location",
}

GOLD_FACT_PATHS = {
    "fact_sales": f"{GOLD_FACTS_BASE}/fact_sales",
}

ETL_CONTROL_PATH = f"{METADATA_BASE}/etl_control"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "analytics-postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "olist_dw")
POSTGRES_USER = os.getenv("POSTGRES_USER", "superset")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123")
POSTGRES_DRIVER = os.getenv("POSTGRES_DRIVER", "org.postgresql.Driver")

POSTGRES_JDBC_URL = os.getenv(
    "POSTGRES_JDBC_URL",
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
)

POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": POSTGRES_DRIVER,
}

POSTGRES_EXPORT_TABLES = {
    "dim_customer": GOLD_DIMENSION_PATHS["dim_customer"],
    "dim_product": GOLD_DIMENSION_PATHS["dim_product"],
    "dim_seller": GOLD_DIMENSION_PATHS["dim_seller"],
    "dim_date": GOLD_DIMENSION_PATHS["dim_date"],
    "dim_location": GOLD_DIMENSION_PATHS["dim_location"],
    "fact_sales": GOLD_FACT_PATHS["fact_sales"],
}
