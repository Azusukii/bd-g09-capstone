# samples/

File mẫu để cả nhóm thống nhất định dạng. Số liệu trong mẫu là minh họa, giữ nguyên tên trường khi làm thật.

| File | Ai tạo | Ai dùng | Nội dung |
|---|---|---|---|
| spec.yaml | A | B, C | pipeline khai báo: bước, bảng vào/ra, schema, expect, nguồn của từng cột (`columns`) |
| events.jsonl | A (run.py) | B | nhật ký thực thi: START/COMPLETE từng bước, rows, content_hash, schema, sql_file, sql_sha256 |
| evidence.json | B | C | bằng chứng của một run: giá trị pipeline báo, giá trị auditor tính lại, thống kê cột, xác nhận SQL |
| lineage.json | B | C | lineage thực tế: cạnh bảng từ nhật ký, cạnh cột từ sqlglot, bước không đủ bằng chứng ghi unknown |
| findings.json | C | D | sai lệch tìm được, mức rủi ro, bằng chứng |
| ground_truth.json | A (inject.py) | E | lỗi đã tiêm vào từng run, dùng để tính precision/recall |

- `risk`: low, medium, high
- `status`: needs_review, auto_closed, confirmed, dismissed
- loại lỗi: drop_rows, duplicate_rows, undocumented_step, schema_change, hash_mismatch
- `content_hash` là tổng hash từng dòng của bảng nên không phụ thuộc thứ tự dòng, không phải SHA-256 của file
- `sql_sha256` là SHA-256 theo byte của file SQL đã chạy
- giá trị lineage của một bước: `events`, `sqlglot` hoặc `unknown`
- ground_truth.json là đáp án, auditor không được đọc
