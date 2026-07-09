import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.pipeline_config import BRONZE_PATHS, RAW_PATHS

spark = SparkSession.builder \
    .appName("ETL_Raw_To_Bronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

for table_name, raw_path in RAW_PATHS.items():

    print(f"\n===== BRONZE {table_name.upper()} =====")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(raw_path)

    bronze_df = df.withColumn(
        "_bronze_table",
        F.lit(table_name)
    ).withColumn(
        "_bronze_source_file",
        F.input_file_name()
    ).withColumn(
        "_bronze_ingested_at",
        F.current_timestamp()
    )

    print(f"Rows: {bronze_df.count()}")
    bronze_df.printSchema()

    bronze_df.write \
        .mode("overwrite") \
        .parquet(BRONZE_PATHS[table_name])

    print(f"Bronze table written: {BRONZE_PATHS[table_name]}")

print("\nBronze layer completed")

spark.stop()
