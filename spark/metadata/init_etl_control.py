from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Init_ETL_Control") \
    .getOrCreate()

data = [

    ("orders",
     "1900-01-01 00:00:00"),

    ("reviews",
     "1900-01-01 00:00:00"),

    ("order_items",
     "1900-01-01 00:00:00")

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
        "hdfs://namenode:8020/metadata/etl_control"
    )

spark.stop()