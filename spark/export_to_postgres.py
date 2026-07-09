from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Export_To_Postgres") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

jdbc_url = \
"jdbc:postgresql://analytics-postgres:5432/olist_dw"

properties = {
    "user": "superset",
    "password": "123",
    "driver": "org.postgresql.Driver"
}

tables = {

    "dim_customer":
    "hdfs://namenode:8020/gold/dimensions/dim_customer",

    "dim_product":
    "hdfs://namenode:8020/gold/dimensions/dim_product",

    "dim_seller":
    "hdfs://namenode:8020/gold/dimensions/dim_seller",

    "dim_date":
    "hdfs://namenode:8020/gold/dimensions/dim_date",

    "dim_location":
    "hdfs://namenode:8020/gold/dimensions/dim_location",

    "fact_sales":
    "hdfs://namenode:8020/gold/facts/fact_sales"
}

for table_name, path in tables.items():

    print(f"Exporting {table_name}")

    df = spark.read.parquet(path)

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", "superset") \
        .option("password", "123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print(f"Done {table_name}")

print("✅ Export completed")

spark.stop()