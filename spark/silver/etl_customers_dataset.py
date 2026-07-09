from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ETL_Customers_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

RAW_PATH = \
"hdfs://namenode:8020/raw/olist/olist_customers_dataset.csv"

SILVER_PATH = \
"hdfs://namenode:8020/processed/olist/customers"

# =====================================
# Read Bronze
# =====================================

customers = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(RAW_PATH)

print("===== RAW =====")
print(f"Rows: {customers.count()}")

# =====================================
# Cleaning
# =====================================

customers = customers.dropDuplicates()

customers = customers.na.drop(
    subset=[
        "customer_id",
        "customer_unique_id"
    ]
)

# =====================================
# Standardization
# =====================================

customers = customers.withColumn(
    "customer_city",
    F.lower(F.trim(F.col("customer_city")))
)

customers = customers.withColumn(
    "customer_state",
    F.upper(F.trim(F.col("customer_state")))
)

# =====================================
# Select Final Columns
# =====================================

customers_silver = customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state"
)

print("===== SILVER =====")
print(f"Rows: {customers_silver.count()}")

# =====================================
# Write Silver
# =====================================

customers_silver.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)

print("✅ Customers Silver created successfully")

spark.stop()