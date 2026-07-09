from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ETL_Sellers_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

RAW_PATH = \
"hdfs://namenode:8020/raw/olist/olist_sellers_dataset.csv"

SILVER_PATH = \
"hdfs://namenode:8020/processed/olist/sellers"

# =====================================
# Read Bronze
# =====================================

sellers = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(RAW_PATH)

print("===== RAW =====")
print(f"Rows: {sellers.count()}")

# =====================================
# Cleaning
# =====================================

sellers = sellers.dropDuplicates()

sellers = sellers.na.drop(
    subset=["seller_id"]
)

# =====================================
# Standardization
# =====================================

sellers = sellers.withColumn(
    "seller_city",
    F.lower(
        F.trim(
            F.col("seller_city")
        )
    )
)

sellers = sellers.withColumn(
    "seller_state",
    F.upper(
        F.trim(
            F.col("seller_state")
        )
    )
)

# =====================================
# Final Columns
# =====================================

sellers_silver = sellers.select(
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state"
)

print("===== SILVER =====")
print(f"Rows: {sellers_silver.count()}")

sellers_silver.printSchema()

# =====================================
# Write Silver
# =====================================

sellers_silver.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)

print("✅ Sellers Silver created successfully")

spark.stop()