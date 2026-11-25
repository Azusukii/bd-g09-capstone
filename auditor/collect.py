import json
import hashlib
import argparse
import sys
import duckdb
from pathlib import Path
from datetime import datetime

NUMERIC_AND_DATE = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "FLOAT", "DOUBLE", "DECIMAL", "DATE", "TIMESTAMP")
NUMERIC = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "FLOAT", "DOUBLE", "DECIMAL")

def resolve_sql(run_path: Path, sql_file: str) -> Path:
    p = run_path / sql_file
    return p if p.exists() else run_path / "steps" / sql_file

def recompute(con, pq: Path):
    src = f"read_parquet('{pq.as_posix()}')"
    cols = [(d[0], d[1]) for d in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
    cast_cols = ", ".join(f'CAST("{c}" AS VARCHAR)' for c, _ in cols)
    
    rows, total = con.execute(
        f"SELECT count(*), sum(CAST(hash(concat_ws('|', {cast_cols})) AS HUGEINT)) FROM {src}"
    ).fetchone()
    content_hash = format(int(total or 0) % 2**64, "016x")
    return cols, rows, content_hash

def profile(con, pq: Path, cols):
    src = f"read_parquet('{pq.as_posix()}')"
    parts = []
    
    for c, t in cols:
        q = f'"{c}"'
        parts += [
            f'count(*) - count({q}) AS "{c}|null"',
            f'count(DISTINCT {q}) AS "{c}|distinct"'
        ]
        if t.startswith(NUMERIC_AND_DATE):
            parts += [f'CAST(min({q}) AS VARCHAR) AS "{c}|min"', f'CAST(max({q}) AS VARCHAR) AS "{c}|max"']
        if t.startswith(NUMERIC):
            parts.append(f'CAST(sum({q}) AS DOUBLE) AS "{c}|sum"')
            
    cur = con.execute(f"SELECT {', '.join(parts)} FROM {src}")
    desc = [d[0] for d in cur.description]
    row = cur.fetchone()
    
    result = {}
    for c, t in cols:
        result[c] = {
            "null_count": row[desc.index(f"{c}|null")],
            "distinct_count": row[desc.index(f"{c}|distinct")]
        }
        if f"{c}|min" in desc:
            result[c]["min"] = row[desc.index(f"{c}|min")]
            result[c]["max"] = row[desc.index(f"{c}|max")]
        if f"{c}|sum" in desc:
            result[c]["sum"] = row[desc.index(f"{c}|sum")]
    return result

def verify_sql(sql_path: Path, expected_hash: str):
    if not sql_path.exists():
        return False, None
    actual = hashlib.sha256(sql_path.read_bytes()).hexdigest()
    return (expected_hash is not None and actual == expected_hash), actual

def process_run(run_path: Path, pipeline_name="orders"):
    events_file = run_path / "events.jsonl"
    if not events_file.exists():
        print(f"Lỗi: Không tìm thấy {events_file}")
        sys.exit(1)
        
    con = duckdb.connect()
    evidence = {
        "run_id": run_path.name,
        "pipeline": pipeline_name,
        "collected_at": datetime.now().isoformat(),
        "steps": []
    }

    # Đọc trước sự kiện để tìm bước cuối cùng ghi vào từng bảng (last_writer)
    with open(events_file, "r", encoding="utf-8") as f:
        events = [json.loads(l) for l in f if l.strip()]
        
    last_writer = {}
    for e in events:
        if e.get("event") == "COMPLETE":
            for o in e.get("outputs", []):
                last_writer[o["name"]] = e.get("step")

    for event in events:
        if event.get("event") == "COMPLETE":
            step = event.get("step")
            sql_file = event.get("sql_file")
            sql_sha256 = event.get("sql_sha256")
            
            sql_verified, actual_hash = False, None
            if sql_file:
                sql_verified, actual_hash = verify_sql(resolve_sql(run_path, sql_file), sql_sha256)
            
            outputs_evidence = []
            for out_table in event.get("outputs", []):
                tb_name = out_table["name"]
                pq_path = run_path / "tables" / f"{tb_name}.parquet"
                
                tb_evidence = {
                    "name": tb_name,
                    "reported": {
                        "rows": out_table.get("rows"),
                        "content_hash": out_table.get("content_hash"),
                        "schema": out_table.get("schema")
                    }
                }
                
                # Chỉ tính toán lại (recompute) nếu bước này là bước cuối cùng ghi đè lên file Parquet đó
                if pq_path.exists() and last_writer.get(tb_name) == step:
                    cols, re_rows, re_hash = recompute(con, pq_path)
                    tb_evidence["recomputed"] = {
                        "rows": re_rows,
                        "content_hash": re_hash,
                        "schema": {c: t for c, t in cols}
                    }
                    tb_evidence["profile"] = profile(con, pq_path, cols)
                else:
                    tb_evidence["recomputed"] = None
                    tb_evidence["profile"] = None
                    if last_writer.get(tb_name) != step:
                        tb_evidence["superseded_by"] = last_writer.get(tb_name)
                    
                outputs_evidence.append(tb_evidence)
                
            evidence["steps"].append({
                "step": step,
                "sql_file": sql_file,
                "sql_verified": sql_verified,
                "recomputed_sql_sha256": actual_hash,
                "duration_s": event.get("duration_s"),
                "inputs": event.get("inputs", []),
                "outputs": outputs_evidence
            })

    con.close()
    out_file = run_path / "evidence.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(evidence, f, indent=2)
    print(f"[OK] Đã ghi evidence: {out_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("run_path", type=Path)
    parser.add_argument("--pipeline", type=str, default="orders")
    args = parser.parse_args()
    process_run(args.run_path, args.pipeline)