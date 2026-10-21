# samples/ — định dạng file dùng chung

File mẫu viết tay, dùng để cả nhóm code song song. Khi pipeline thật chạy được thì thay bằng file thật, giữ nguyên tên trường.

| File | Ai tạo | Ai đọc | Nội dung |
|---|---|---|---|
| spec.yaml | A | B/C (dựng đồ thị khai báo, đối chiếu) | Pipeline khai báo: bước, bảng vào/ra, expect |
| events.jsonl | A (run.py ghi) | B (thu bằng chứng) | Mỗi dòng một sự kiện START/COMPLETE của bước, có rows + sha256 |
| evidence.json | B | C (dựng lineage thực tế, đối chiếu) | Gom bằng chứng của 1 run: SQL, schema, rows, hash |
| findings.json | C | D (giao diện duyệt, báo cáo) | Danh sách sai lệch, risk, bằng chứng trỏ về file |
| ground_truth.json | A (inject.py ghi) | E (đánh giá) | Lỗi đã tiêm vào từng run, dùng tính precision/recall |

Giá trị `risk`: low | medium | high. Giá trị `status`: needs_review | auto_closed | confirmed | dismissed.
Các loại lỗi (`fault` / `type`) tạm thống nhất: drop_rows, duplicate_rows, undocumented_step, schema_change, hash_mismatch.
