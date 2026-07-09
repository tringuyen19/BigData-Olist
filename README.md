# Big Data Project - Olist Analytics Pipeline

Du an xay dung pipeline phan tich du lieu thuong mai dien tu Olist bang Hadoop HDFS, Apache Spark, PostgreSQL va Apache Superset.

## Kien truc hien tai

```text
data/olist/*.csv
  -> HDFS raw /raw/olist/...
  -> Spark Bronze /bronze/olist/...
  -> quality check /quality/...
  -> Spark Silver /processed/olist/...
  -> Spark Gold star schema /gold/...
  -> PostgreSQL olist_dw
  -> Superset dashboard
```

Bronze layer luu snapshot Parquet tu raw CSV, kem metadata ingestion nhu source file va thoi diem nap du lieu.

## Cau truc thu muc

```text
.
|-- data/
|   |-- olist/                  # Dataset Olist goc dang CSV
|   `-- online_retail.csv       # Dataset cu/bo sung
|-- hive/
|   `-- hive_schema_v1.sql      # Schema Hive tham khao
|-- spark/
|   |-- bronze/                # ETL raw CSV -> Bronze Parquet
|   |-- quality/                # Data quality checks
|   |-- metadata/               # ETL control cho incremental load
|   |-- silver/                 # ETL Bronze Parquet -> Silver Parquet
|   |-- gold/
|   |   |-- dimensions/         # Dimension tables
|   |   `-- facts/              # Fact tables
|   |-- mock/                   # Script tao va doc mock data
|   `-- export_to_postgres.py   # Export Gold layer sang PostgreSQL
|-- dashboard/
|   `-- star_schema.drawio      # Thiet ke star schema/dashboard
|-- superset/
|   `-- Dockerfile              # Superset image co PostgreSQL driver
|-- docker-compose.yml          # Hadoop, Spark, PostgreSQL, Superset
`-- postgresql-42.7.10.jar      # JDBC driver cho Spark export
```

## Thanh phan Docker

`docker-compose.yml` khoi tao cac service:

- `namenode`: Hadoop NameNode, UI tai `http://localhost:9870`
- `datanode1`, `datanode2`, `datanode3`: Hadoop DataNode
- `spark-master`: Spark 3.5.1, UI tai `http://localhost:8080`
- `postgres`: PostgreSQL 16, database `olist_dw`
- `superset`: Apache Superset, UI tai `http://localhost:8088`

Thong tin PostgreSQL hien tai:

```text
Host trong Docker network: analytics-postgres
Port tren may host: 5432
Database: olist_dw
User: superset
Password: 123
```

## Config va credential

Config dung chung nam o:

```text
spark/config/pipeline_config.py
```

File nay gom cac gia tri dung chung:

- HDFS base path.
- Raw, Bronze, Silver, Quality, Gold paths.
- ETL control path.
- PostgreSQL JDBC URL, user, password va driver.
- Danh sach bang export sang PostgreSQL.

Credential va bien moi truong mac dinh nam trong:

```text
.env
```

Gia tri hien tai:

```text
HDFS_BASE=hdfs://namenode:8020
POSTGRES_HOST=analytics-postgres
POSTGRES_DB=olist_dw
POSTGRES_USER=superset
POSTGRES_PASSWORD=123
POSTGRES_PORT=5432
```

`docker-compose.yml` doc cac bien nay tu `.env`. Neu thay doi `.env`, can recreate container de Docker Compose nap lai bien moi:

```powershell
docker compose up -d --force-recreate
```

## Chay cluster

```powershell
docker compose up -d
```

Kiem tra container:

```powershell
docker ps
```

## Nap raw data len HDFS

Tao cac thu muc HDFS cho data lake va cap quyen ghi cho Spark:

```powershell
docker exec namenode hdfs dfs -mkdir -p /raw/olist /bronze/olist /quality /processed/olist /metadata /gold/dimensions /gold/facts
docker exec namenode hdfs dfs -chmod -R 777 /raw /bronze /quality /processed /metadata /gold
```

Neu bo qua buoc nay, Spark co the gap loi `Permission denied: user=spark, access=WRITE, inode="/":root:supergroup:drwxr-xr-x` khi ghi vao `/bronze`, `/quality`, `/processed`, `/metadata` hoac `/gold`.

Copy file CSV tu may host vao container `namenode`:

```powershell
docker cp data/olist/. namenode:/tmp/olist
```

Day CSV tu container len HDFS:

```powershell
docker exec namenode bash -lc "hdfs dfs -put -f /tmp/olist/*.csv /raw/olist/"
```

Kiem tra raw data:

```powershell
docker exec namenode hdfs dfs -ls /raw/olist
```

## Khoi tao ETL control

`etl_control` luu timestamp da xu ly gan nhat cho cac bang incremental.

```powershell
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/metadata/init_etl_control.py
```

Output:

```text
/metadata/etl_control
```

## Chay Bronze layer

Bronze layer doc CSV tu `/raw/olist/...`, them metadata ingestion va ghi Parquet vao `/bronze/olist/...`.

```powershell
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/bronze/etl_raw_to_bronze.py
```

Output:

```text
/bronze/olist/orders
/bronze/olist/customers
/bronze/olist/products
/bronze/olist/payments
/bronze/olist/reviews
/bronze/olist/sellers
/bronze/olist/order_items
/bronze/olist/geolocation
/bronze/olist/category_translation
```

## Chay data quality check

```powershell
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/quality/quality_all_tables.py
```

Quality check doc du lieu tu Bronze layer. Day la quality gate nam giua Bronze va Silver:

```text
/bronze/olist/... -> quality check -> /quality/...

Neu khong co loi critical: exit code 0, tiep tuc chay Silver.
Neu co loi critical: exit code 1, dung pipeline va khong chay Silver.
```

Output:

```text
/quality/orders
/quality/customers
/quality/products
/quality/payments
/quality/reviews
/quality/sellers
/quality/order_items
/quality/geolocation
/quality/category_translation
/quality/summary
```

Quality check hien tai bao gom:

- `row_count_is_zero`: critical, fail neu bang rong.
- `row_count`: info, chi ghi thong tin.
- Null critical columns: critical, fail neu cot khoa/cot bat buoc bi null.
- Null warning columns: warning, khong fail.
- Duplicate primary key logic: critical voi bang chinh, warning voi `reviews` va `geolocation`.
- Business rules: vi du `price <= 0`, `freight_value < 0`, timestamp don hang null la critical; `payment_value <= 0`, product weight khong hop le, review score khong hop le la warning.

Moi report co schema:

```text
table_name
metric_name
metric_value
severity
status
checked_at
```

## Chay Silver layer

Silver layer doc Parquet tu `/bronze/olist/...`, lam sach du lieu va ghi Parquet vao `/processed/olist/...`.

```powershell
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/silver/etl_customers_dataset.py
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/silver/etl_sellers_dataset.py
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/silver/etl_products_dataset.py
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/silver/etl_category_translation_dataset.py
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/silver/etl_geolocation_dataset.py
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/silver/etl_order_items_dataset.py
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/silver/etl_payments_dataset.py
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/silver/etl_orders_dataset.py
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/silver/etl_reviews_dataset.py
```

Output chinh:

```text
/processed/olist/customers
/processed/olist/sellers
/processed/olist/products
/processed/olist/category_translation
/processed/olist/geolocation
/processed/olist/order_items
/processed/olist/payments
/processed/olist/orders
/processed/olist/reviews
```

## Incremental load

Hai bang dang dung incremental load:

- `orders`: loc theo `order_purchase_timestamp`, sau do tao `purchase_ts`.
- `reviews`: loc theo `review_creation_date`, sau do tao `review_creation_ts`.

Quy trinh trong hai Silver script:

```text
read Bronze Parquet
read /metadata/etl_control
lay last_processed_ts cua table_name
clean data
convert timestamp
filter timestamp > last_processed_ts
write append vao Silver layer
update /metadata/etl_control
```

Cac bang Silver con lai dang ghi theo che do `overwrite`, vi code hien tai chua co cot timestamp/logic incremental rieng cho chung.

Luu y ky thuat: `spark/silver/etl_orders_dataset.py` va `spark/silver/etl_reviews_dataset.py` deu update `etl_control` bang cach giu lai cac table control khac, chi thay timestamp cua bang dang xu ly.

## Chay Gold layer

Gold dimensions:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/gold/dimensions/gold_dim_customer.py
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/gold/dimensions/gold_dim_product.py
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/gold/dimensions/gold_dim_seller.py
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/gold/dimensions/gold_dim_location.py
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/gold/dimensions/gold_dim_date.py
```

Gold fact:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/gold/facts/gold_fact_sales.py
```

Output:

```text
/gold/dimensions/dim_customer
/gold/dimensions/dim_product
/gold/dimensions/dim_seller
/gold/dimensions/dim_location
/gold/dimensions/dim_date
/gold/facts/fact_sales
```

Star schema hien tai:

- `fact_sales`: fact table o muc order item.
- `dim_customer`: thong tin khach hang.
- `dim_product`: thong tin san pham va category.
- `dim_seller`: thong tin nguoi ban.
- `dim_location`: thong tin dia ly theo ZIP/state/city.
- `dim_date`: dimension ngay mua hang.

## Export Gold layer sang PostgreSQL

```powershell
docker exec spark-master /opt/spark/bin/spark-submit --jars /workspace/postgresql-42.7.10.jar /workspace/spark/export_to_postgres.py
```

Script export cac bang:

```text
dim_customer
dim_product
dim_seller
dim_date
dim_location
fact_sales
```

Sang PostgreSQL database:

```text
jdbc:postgresql://analytics-postgres:5432/olist_dw
```

## Superset

Mo Superset:

```text
http://localhost:8088
```

Ket noi PostgreSQL trong Superset:

```text
postgresql://superset:123@analytics-postgres:5432/olist_dw
```

Neu ket noi tu may host ben ngoai Docker network:

```text
postgresql://superset:123@localhost:5432/olist_dw
```

## Mock data

Thu muc `spark/mock/` dung cho demo nhanh voi du lieu gia lap, khong phai pipeline Olist chinh.

Tao mock data:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/mock/create_mock_data.py
```

Doc mock data va export revenue theo thang:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark/mock/read_mock_and_test.py
```

## Trang thai hien tai

Da co:

- Docker cluster cho Hadoop, Spark, PostgreSQL va Superset.
- Dataset Olist trong `data/olist`.
- Raw zone tren HDFS.
- Bronze layer dang Parquet tren HDFS.
- Data quality check co ban.
- Silver ETL cho cac bang Olist chinh.
- Gold star schema gom dimensions va fact.
- Export Gold layer sang PostgreSQL.
- Superset image co PostgreSQL driver.

Can bo sung de hoan chinh hon:

- Script hoac DAG chay end-to-end pipeline.
- Airflow/Prefect/Dagster de orchestration.
- Data quality rule co kha nang fail pipeline khi loi nghiem trong.
- Logging chuan thay cho `print`.
- Test tu dong cho ETL.
- Config rieng cho path, credential va environment.
- Export/version dashboard Superset trong repository.
