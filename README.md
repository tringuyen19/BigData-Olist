# Big Data Project

## Cấu trúc

- `data/`: dataset thô.
- `hive/`: schema Hive.
- `spark/`: script tạo mock data và ETL.
- `dashboard/`: tài liệu thiết kế dashboard/star schema.
- `mock_gold/`: output Parquet do Spark tạo ra.

## Chạy cluster

```powershell
docker compose up -d
```

## Tạo mock data

```powershell
python spark/create_mock_data.py
```

## Chạy ETL

```powershell
python spark/etl_olist.py
```


Giai đoạn 4: Star Schema
✅ Docker Cluster

✅ Hadoop HDFS

✅ Olist Dataset

✅ Bronze Layer

✅ Silver Layer

✅ Gold Layer

✅ Star Schema

⬜ PostgreSQL

⬜ Superset Dashboard