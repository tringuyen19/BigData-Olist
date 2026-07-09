import random
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.pipeline_config import HDFS_BASE, MOCK_GOLD_PATH

spark = SparkSession.builder \
    .appName("MockData") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", HDFS_BASE) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("delivery_days", IntegerType(), True),
    StructField("review_score", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("customer_state", StringType(), True),
])

df = spark.createDataFrame(data, schema)

df.write.mode("overwrite").parquet(MOCK_GOLD_PATH)

print("Mock data created")
df.show(5)

spark.stop()
