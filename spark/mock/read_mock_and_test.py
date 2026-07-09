import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_CSV = ROOT_DIR / "monthly_revenue.csv"

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("TestRead")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/gold/orders_mock/")

df_monthly = (
    df.groupBy("year", "month")
    .agg(F.sum("price").alias("revenue"))
    .orderBy("year", "month")
)

rows = df_monthly.collect()
with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=["year", "month", "revenue"])
    writer.writeheader()
    writer.writerows(row.asDict() for row in rows)

print(f"Exported {OUTPUT_CSV}")
df_monthly.show()

spark.stop()