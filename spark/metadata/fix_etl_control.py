import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.pipeline_config import ETL_CONTROL_PATH

spark = SparkSession.builder \
    .appName("Fix_ETL_Control") \
    .getOrCreate()

data = [

    (
        "orders",
        "2018-10-17 17:30:18"
    ),

    (
        "reviews",
        "1900-01-01 00:00:00"
    ),

    (
        "order_items",
        "1900-01-01 00:00:00"
    )

]

df = spark.createDataFrame(
    data,
    [
        "table_name",
        "last_processed_ts"
    ]
)

df.write \
    .mode("overwrite") \
    .parquet(
        ETL_CONTROL_PATH
    )

print("ETL Control Fixed")

spark.stop()
