from pyspark.sql import SparkSession

from config.pipeline_config import (
    POSTGRES_DRIVER,
    POSTGRES_EXPORT_TABLES,
    POSTGRES_JDBC_URL,
    POSTGRES_PASSWORD,
    POSTGRES_USER,
)

spark = SparkSession.builder \
    .appName("Export_To_Postgres") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

for table_name, path in POSTGRES_EXPORT_TABLES.items():

    print(f"Exporting {table_name}")

    df = spark.read.parquet(path)

    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_JDBC_URL) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", POSTGRES_DRIVER) \
        .mode("overwrite") \
        .save()

    print(f"Done {table_name}")

print("✅ Export completed")

spark.stop()
