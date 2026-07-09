import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.pipeline_config import BRONZE_PATHS, QUALITY_BASE, QUALITY_PATHS

# =====================================
# Spark
# =====================================

spark = SparkSession.builder \
    .appName("Quality_All_Tables") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================
# Config
# =====================================

TABLES = {
    "orders": {
        "path": BRONZE_PATHS["orders"],
        "pk": ["order_id"],
        "critical_cols": [
            "order_id",
            "customer_id",
            "order_status"
        ],
        "warning_cols": [],
        "rules": [
            {
                "metric_name": "invalid_order_purchase_timestamp",
                "severity": "critical",
                "condition": F.col("order_purchase_timestamp").isNull()
            }
        ]
    },
    "customers": {
        "path": BRONZE_PATHS["customers"],
        "pk": ["customer_id"],
        "critical_cols": [
            "customer_id",
            "customer_unique_id"
        ],
        "warning_cols": [
            "customer_city",
            "customer_state"
        ],
        "rules": []
    },
    "products": {
        "path": BRONZE_PATHS["products"],
        "pk": ["product_id"],
        "critical_cols": [
            "product_id"
        ],
        "warning_cols": [
            "product_category_name"
        ],
        "rules": [
            {
                "metric_name": "invalid_product_weight_g",
                "severity": "warning",
                "condition": F.col("product_weight_g") <= 0
            }
        ]
    },
    "payments": {
        "path": BRONZE_PATHS["payments"],
        "pk": [
            "order_id",
            "payment_sequential"
        ],
        "critical_cols": [
            "order_id",
            "payment_type",
            "payment_value"
        ],
        "warning_cols": [],
        "rules": [
            {
                "metric_name": "invalid_payment_value",
                "severity": "warning",
                "condition": F.col("payment_value") <= 0
            }
        ]
    },
    "reviews": {
        "path": BRONZE_PATHS["reviews"],
        "pk": [
            "review_id"
        ],
        "critical_cols": [],
        "warning_cols": [
            "review_id",
            "order_id",
            "review_score"
        ],
        "rules": [
            {
                "metric_name": "invalid_review_score",
                "severity": "warning",
                "condition": (
                    F.col("review_score").isNotNull()
                    & ~F.col("review_score").cast("int").between(1, 5)
                )
            }
        ],
        "duplicate_severity": "warning"
    },
    "sellers": {
        "path": BRONZE_PATHS["sellers"],
        "pk": [
            "seller_id"
        ],
        "critical_cols": [
            "seller_id",
            "seller_state"
        ],
        "warning_cols": [
            "seller_city"
        ],
        "rules": []
    },
    "order_items": {
        "path": BRONZE_PATHS["order_items"],
        "pk": [
            "order_id",
            "order_item_id"
        ],
        "critical_cols": [
            "order_id",
            "product_id",
            "seller_id"
        ],
        "warning_cols": [],
        "rules": [
            {
                "metric_name": "invalid_price",
                "severity": "critical",
                "condition": F.col("price") <= 0
            },
            {
                "metric_name": "invalid_freight_value",
                "severity": "critical",
                "condition": F.col("freight_value") < 0
            }
        ]
    },
    "geolocation": {
        "path": BRONZE_PATHS["geolocation"],
        "pk": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng"
        ],
        "critical_cols": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng"
        ],
        "warning_cols": [
            "geolocation_city",
            "geolocation_state"
        ],
        "rules": [],
        "duplicate_severity": "warning"
    },
    "category_translation": {
        "path": BRONZE_PATHS["category_translation"],
        "pk": [
            "product_category_name"
        ],
        "critical_cols": [
            "product_category_name",
            "product_category_name_english"
        ],
        "warning_cols": [],
        "rules": []
    }
}

checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
all_reports = []
has_critical_failure = False


def get_status(metric_value, severity):
    if severity == "info":
        return "INFO"

    if metric_value == 0:
        return "PASS"

    if severity == "critical":
        return "FAIL"

    return "WARN"


def add_report(report, table_name, metric_name, metric_value, severity):
    status = get_status(metric_value, severity)

    report.append(
        (
            table_name,
            metric_name,
            int(metric_value),
            severity,
            status,
            checked_at
        )
    )

    return status == "FAIL"


# =====================================
# Loop
# =====================================

for table_name, config in TABLES.items():

    print(f"\n===== {table_name.upper()} =====")

    df = spark.read.parquet(
        config["path"]
    )

    row_count = df.count()
    report = []

    has_critical_failure = (
        add_report(
            report,
            table_name,
            "row_count_is_zero",
            1 if row_count == 0 else 0,
            "critical"
        )
        or has_critical_failure
    )

    add_report(
        report,
        table_name,
        "row_count",
        row_count,
        "info"
    )

    for col_name in config["critical_cols"]:

        null_count = df.filter(
            F.col(col_name).isNull()
        ).count()

        has_critical_failure = (
            add_report(
                report,
                table_name,
                f"null_{col_name}",
                null_count,
                "critical"
            )
            or has_critical_failure
        )

    for col_name in config["warning_cols"]:

        null_count = df.filter(
            F.col(col_name).isNull()
        ).count()

        add_report(
            report,
            table_name,
            f"null_{col_name}",
            null_count,
            "warning"
        )

    pk_cols = config["pk"]
    duplicate_count = (
        df.groupBy(*pk_cols)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    duplicate_severity = config.get(
        "duplicate_severity",
        "critical"
    )

    has_critical_failure = (
        add_report(
            report,
            table_name,
            f"duplicate_{'_'.join(pk_cols)}",
            duplicate_count,
            duplicate_severity
        )
        or has_critical_failure
    )

    for rule in config["rules"]:

        invalid_count = df.filter(
            rule["condition"]
        ).count()

        has_critical_failure = (
            add_report(
                report,
                table_name,
                rule["metric_name"],
                invalid_count,
                rule["severity"]
            )
            or has_critical_failure
        )

    report_df = spark.createDataFrame(
        report,
        [
            "table_name",
            "metric_name",
            "metric_value",
            "severity",
            "status",
            "checked_at"
        ]
    )

    report_df.show(
        truncate=False
    )

    report_df.write \
        .mode("overwrite") \
        .parquet(QUALITY_PATHS[table_name])

    all_reports.extend(report)

    failed_metrics = [
        row for row in report
        if row[4] == "FAIL"
    ]

    if failed_metrics:
        print(f"FAIL {table_name}: {len(failed_metrics)} critical issues")
    else:
        print(f"PASS {table_name}: no critical issues")

summary_df = spark.createDataFrame(
    all_reports,
    [
        "table_name",
        "metric_name",
        "metric_value",
        "severity",
        "status",
        "checked_at"
    ]
)

summary_path = f"{QUALITY_BASE}/summary"

summary_df.write \
    .mode("overwrite") \
    .parquet(summary_path)

print(f"\nQuality summary written: {summary_path}")

if has_critical_failure:
    print("Quality gate failed: critical issues found")
    spark.stop()
    sys.exit(1)

print("Quality gate passed: no critical issues found")

spark.stop()
