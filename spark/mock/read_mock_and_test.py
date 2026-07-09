import csv
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.pipeline_config import HDFS_BASE, MOCK_GOLD_PATH

ROOT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_CSV = ROOT_DIR / "monthly_revenue.csv"

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("TestRead")
    .config("spark.hadoop.fs.defaultFS", HDFS_BASE)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(MOCK_GOLD_PATH)

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
