# samples/

File mẫu để cả nhóm thống nhất định dạng. Khi pipeline thật chạy được thì thay bằng file thật, giữ nguyên tên trường.

| File | Ai tạo | Ai dùng | Nội dung |
|---|---|---|---|
| spec.yaml | A | B, C | pipeline khai báo: các bước, bảng vào/ra, schema, expect |
| events.jsonl | A (run.py) | B | sự kiện START/COMPLETE của từng bước, có rows, content_hash, schema |
| evidence.json | B | C | bằng chứng của một run: SQL, schema, số dòng, hash |
| findings.json | C | D | các sai lệch tìm được, mức rủi ro, bằng chứng |
| ground_truth.json | A (inject.py) | E | lỗi đã tiêm vào từng run, dùng để tính precision/recall |

- `risk`: low, medium, high
- `status`: needs_review, auto_closed, confirmed, dismissed
- loại lỗi tạm thống nhất: drop_rows, duplicate_rows, undocumented_step, schema_change, hash_mismatch
- `content_hash` là tổng hash từng dòng của bảng nên không phụ thuộc thứ tự dòng, không phải SHA-256 của file
- ground_truth.json là đáp án, auditor không được đọc
