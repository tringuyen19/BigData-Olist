from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Gold_Dim_Product") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================
# Paths
# =====================================

PRODUCTS_PATH = \
"hdfs://namenode:8020/processed/olist/products"

CATEGORY_PATH = \
"hdfs://namenode:8020/processed/olist/category_translation"

GOLD_PATH = \
"hdfs://namenode:8020/gold/dimensions/dim_product"

# =====================================
# Read Silver
# =====================================

products = spark.read.parquet(
    PRODUCTS_PATH
)

category = spark.read.parquet(
    CATEGORY_PATH
)

# =====================================
# Join Category Translation
# =====================================

products = products.join(
    category,
    "product_category_name",
    "left"
)

# =====================================
# Weight Category
# =====================================

products = products.withColumn(
    "weight_category",
    F.when(
        F.col("weight_kg") < 1,
        "Light"
    )
    .when(
        F.col("weight_kg") < 5,
        "Medium"
    )
    .otherwise("Heavy")
)

# =====================================
# Size Category
# =====================================

products = products.withColumn(
    "product_size_category",
    F.when(
        F.col("product_volume_cm3") < 5000,
        "Small"
    )
    .when(
        F.col("product_volume_cm3") < 20000,
        "Medium"
    )
    .otherwise("Large")
)

# =====================================
# Surrogate Key
# =====================================

products = products.withColumn(
    "product_key",
    F.monotonically_increasing_id()
)

# =====================================
# Final Columns
# =====================================

dim_product = products.select(
    "product_key",

    "product_id",

    "product_category_name",
    "product_category_name_english",

    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",

    "weight_kg",
    "weight_category",

    "product_volume_cm3",
    "product_size_category"
)

print("===== GOLD =====")
print(f"Rows: {dim_product.count()}")

dim_product.printSchema()

# =====================================
# Write Gold
# =====================================

dim_product.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)

print("✅ Gold Dim Product created")

spark.stop()