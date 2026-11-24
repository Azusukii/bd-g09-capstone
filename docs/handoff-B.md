# Bàn giao cho B: thu bằng chứng và dựng lineage

Người giao: A. Người nhận: B. Người dùng kết quả: C.

## 1. Chuẩn bị

Làm theo `README.md`, phần Cài đặt và lệnh sinh 60 run. Xong sẽ có `runs/r0001` đến `runs/r0060`.

Đọc trước: `pipelines/orders/spec.yaml`, `samples/events.jsonl`, `samples/evidence.json`, `samples/lineage.json`.

## 2. Đầu vào của B

Với mỗi `runs/<run_id>/`:

- `events.jsonl`: mỗi bước có START và COMPLETE. COMPLETE có bảng vào/ra, rows, content_hash, schema, sql_file, sql_sha256, duration_s.
- `steps/*.sql`: câu SQL đã chạy.
- `tables/*.parquet`: bảng đầu ra. Bảng raw không có parquet, lấy từ `data/tpch_sf0.01.duckdb` nếu cần.
- `pipelines/orders/spec.yaml`: chỉ dùng để biết bảng nào cần kiểm, không dùng để dựng lineage thực tế.

Không đọc `runs/ground_truth.json`.

## 3. Việc 1: evidence.json

Theo `samples/evidence.json`. Với mỗi bước:

1. Chép giá trị pipeline báo trong event (rows, content_hash, schema).
2. Tự tính lại từ parquet, đưa vào `recomputed`. Hash tính đúng như `stats()` trong `run.py`:

```python
import duckdb
con = duckdb.connect()
path = "runs/r0101/tables/staging_orders.parquet"
cols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()]
cast_cols = ", ".join(f'CAST("{c}" AS VARCHAR)' for c in cols)
rows, total = con.execute(
    f"SELECT count(*), sum(CAST(hash(concat_ws('|', {cast_cols})) AS HUGEINT)) "
    f"FROM read_parquet('{path}')").fetchone()
content_hash = format(int(total or 0) % 2**64, "016x")
```

3. Thống kê từng cột vào `profile`: null_count, distinct_count, min, max, và sum với cột số. Tính bằng SQL trong DuckDB.
4. Tính SHA-256 theo byte của `steps/<file>.sql`, so với `sql_sha256` trong event, ghi `sql_verified`.

Chú ý:

- `content_hash` là tổng hash từng dòng, không phải SHA-256.
- Lỗi schema_change không làm đổi hash vì hash dựng từ CAST sang VARCHAR. Lỗi này chỉ thấy qua schema.
- Event không có `sql_sha256` nghĩa là run được sinh bằng code cũ. Sinh lại run.

## 4. Việc 2: lineage.json

Theo `samples/lineage.json`.

- Cạnh bảng: lấy bảng vào/ra từ event COMPLETE của từng bước, kể cả bước không có trong spec.
- Cạnh cột: dùng sqlglot (`sqlglot.lineage`) trên `steps/<file>.sql`, chỉ khi `sql_verified` là true.
- Bước không có câu SQL khớp hash thì `column_lineage` là `unknown`. Không suy lineage từ hash hay số dòng.
- Cột chỉ dùng để nối hoặc lọc (ví dụ `l_orderkey` ở bước totals) không tính là nguồn giá trị, giống cách khai báo `columns` trong spec.

## 5. Cách kiểm tra đã đúng

| Run | Phải thấy |
|---|---|
| run sạch | recomputed bằng giá trị event, sql_verified true, cạnh cột khớp `columns` trong spec |
| hash_mismatch | recomputed.content_hash của staging_orders khác content_hash trong event, rows bằng nhau |
| schema_change | schema của staging_lineitem.l_extendedprice là VARCHAR, hash vẫn khớp |
| undocumented_step | có bước adhoc_update_orders trong evidence và lineage, sql_verified true |
| duplicate_rows | staging_orders nhiều hơn 15000 dòng |
| drop_rows | order_totals ít hơn 15000 dòng |

Xem loại lỗi của từng run trong `ground_truth.json` chỉ khi tự kiểm tra như trên, code của auditor không được đọc file này.

## 6. Cần thống nhất với C

Tên trường của `lineage.json` và `evidence.json`. Nếu đổi thì sửa luôn file trong `samples/`.

Code đặt trong `auditor/`, ví dụ `auditor/collect.py` và `auditor/lineage.py`.
