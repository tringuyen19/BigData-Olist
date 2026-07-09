# Schema Contract

Tài liệu này là hợp đồng chung giữa các thành viên trong nhóm.

## Raw Dataset

File: `data/online_retail.csv`

Các cột dự kiến:

| Column | Type | Description |
| --- | --- | --- |
| InvoiceNo | string | Mã hóa đơn |
| StockCode | string | Mã sản phẩm |
| Description | string | Tên sản phẩm |
| Quantity | int | Số lượng |
| InvoiceDate | timestamp | Thời điểm giao dịch |
| UnitPrice | double | Đơn giá |
| CustomerID | string | Mã khách hàng |
| Country | string | Quốc gia |

## Gold Layer

Output chính của Spark:

- `mock_gold/orders_mock/`

Định dạng: Parquet.
