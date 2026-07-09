# create_mock_data.py
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random

# Khởi động Spark local — dùng WSL Spark, không cần cluster
spark = SparkSession.builder \
    .appName("MockData") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Data giả mô phỏng Olist
CATEGORIES = ["electronics", "furniture", "toys", "fashion", "food"]
STATES = ["SP", "RJ", "MG", "RS", "PR"]

data = []
for i in range(500):
    data.append((
        f"order_{i:05d}",
        f"customer_{i % 100:04d}",
        f"product_{i % 50:03d}",
        random.choice(CATEGORIES),
        round(random.uniform(20.0, 500.0), 2),
        random.randint(1, 30),
        random.randint(1, 5),
        2018,
        random.randint(1, 12),
        random.choice(STATES),
    ))

schema = StructType([
    StructField("order_id",       StringType(),  False),
    StructField("customer_id",    StringType(),  True),
    StructField("product_id",     StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("price",          DoubleType(),  True),
    StructField("delivery_days",  IntegerType(), True),
    StructField("review_score",   IntegerType(), True),
    StructField("year",           IntegerType(), True),
    StructField("month",          IntegerType(), True),
    StructField("customer_state", StringType(),  True),
])

df = spark.createDataFrame(data, schema)

# Ghi ra thư mục mock_gold/
df.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/gold/orders_mock/"
)
print("✅ Tạo mock data xong!")
df.show(5)

spark.stop()