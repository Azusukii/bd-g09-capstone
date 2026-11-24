# bd-g09-capstone

Nhóm 09, Đề tài 16: kiểm toán tự động và xác minh lineage dữ liệu.

Repo gồm pipeline mẫu bị kiểm toán (đã có) và bộ kiểm toán trong `auditor/` (đang làm).

## Cài đặt

Cần Python 3.11 trở lên. Lần chạy đầu cần mạng để DuckDB tải extension `tpch` và sinh dữ liệu.

```
python -m venv .venv
.venv\Scripts\activate          # Windows
source .venv/bin/activate       # Linux, macOS
pip install -r requirements.txt
```

## Chạy pipeline

Chạy từ thư mục gốc của repo.

Một run sạch:

```
python pipelines/orders/run.py --run-id r0001 --sf 0.01
```

Một run có lỗi, ví dụ mất 1% dòng ở bước totals:

```
python pipelines/orders/run.py --run-id r0002 --sf 0.01 --fault drop_rows --ratio 0.01
```

Bộ thử nghiệm 60 run (20 sạch, 40 lỗi, mỗi loại lỗi 8 run, tỉ lệ xoay vòng 0,5%, 1%, 5%):

```
python pipelines/orders/inject.py --n-clean 20 --n-fault 40 --sf 0.01 --seed 1
```

Lệnh này ghi đè các run cùng số thứ tự. Dùng `--start` để bắt đầu từ số khác.

## Kết quả của mỗi run

```
data/tpch_sf0.01.duckdb      dữ liệu nguồn, sinh một lần
runs/<run_id>/
  events.jsonl               nhật ký thực thi: START, COMPLETE của từng bước
  logs.txt                   log dạng chữ
  steps/*.sql                đúng các câu SQL đã chạy, kể cả bước không khai báo
  tables/*.parquet           bảng đầu ra của từng bước
runs/ground_truth.json       đáp án lỗi đã tiêm, auditor không được đọc
```

`data/` và `runs/` không đưa lên git. Mỗi người tự chạy để có dữ liệu.

## Năm loại lỗi

| Lỗi | Bước | Làm gì |
|---|---|---|
| drop_rows | totals | xóa một phần dòng của order_totals |
| duplicate_rows | stage_orders | chèn lặp một phần dòng của staging_orders |
| undocumented_step | stage_orders | chạy thêm `adhoc_update_orders`, tăng 10% giá khoảng 5% số đơn |
| schema_change | stage_lineitem | đổi `l_extendedprice` sang VARCHAR |
| hash_mismatch | stage_orders | tăng 1% giá sau khi bước báo COMPLETE, trước khi lưu parquet |

## Định dạng file

Xem `samples/README.md`. Khai báo pipeline nằm ở `pipelines/orders/spec.yaml`.
