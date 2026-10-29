# Chạy pipeline orders trên DuckDB, ghi kết quả vào runs/<run_id>/
#   python pipelines/orders/run.py --run-id r0001 --sf 0.01
#   python pipelines/orders/run.py --run-id r0002 --sf 0.01 --fault drop_rows
import argparse
import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml

import inject

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
RUNS = ROOT / "runs"
DATA = ROOT / "data"


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_source(sf):
    # sinh dữ liệu tpch một lần rồi dùng lại cho các run sau
    DATA.mkdir(exist_ok=True)
    path = DATA / f"tpch_sf{sf:g}.duckdb"
    if not path.exists():
        con = duckdb.connect(str(path))
        con.execute("INSTALL tpch")
        con.execute("LOAD tpch")
        con.execute(f"CALL dbgen(sf={sf})")
        con.close()
    return path


def stats(con, table, with_schema=False):
    # số dòng + hash nội dung (cộng hash từng dòng nên không phụ thuộc thứ tự)
    cols = con.execute(f'DESCRIBE "{table}"').fetchall()
    types = {c[0]: c[1] for c in cols}
    cast_cols = ", ".join(f'CAST("{c}" AS VARCHAR)' for c in types)
    sql = (f"SELECT count(*), sum(CAST(hash(concat_ws('|', {cast_cols})) AS HUGEINT)) "
           f'FROM "{table}"')
    rows, total = con.execute(sql).fetchone()
    result = {"name": table, "rows": rows, "content_hash": format(int(total or 0) % 2**64, "016x")}
    if with_schema:
        result["schema"] = types
    return result


def run_pipeline(run_id, sf=0.01, fault=None):
    spec = yaml.safe_load((HERE / "spec.yaml").read_text(encoding="utf-8"))

    run_dir = RUNS / run_id
    if run_dir.exists():
        shutil.rmtree(run_dir)
    (run_dir / "steps").mkdir(parents=True)
    (run_dir / "tables").mkdir()

    events = open(run_dir / "events.jsonl", "w", encoding="utf-8")
    logs = open(run_dir / "logs.txt", "w", encoding="utf-8")

    def emit(**fields):
        line = {"run_id": run_id, "ts": now(), **fields}
        events.write(json.dumps(line, ensure_ascii=False) + "\n")

    def log(msg):
        logs.write(datetime.now(timezone.utc).strftime("%H:%M:%S") + " " + msg + "\n")

    con = duckdb.connect()
    con.execute(f"ATTACH '{get_source(sf).as_posix()}' AS src (READ_ONLY)")
    con.execute("CREATE TABLE raw_orders AS SELECT * FROM src.orders")
    con.execute("CREATE TABLE raw_lineitem AS SELECT * FROM src.lineitem")
    log(f"run {run_id} start, sf={sf:g}")

    saved = []
    tamper_tables = []
    for step in spec["steps"]:
        name = step["id"]
        inputs = [stats(con, t) for t in step["inputs"]]
        emit(step=name, event="START", inputs=step["inputs"], outputs=step["outputs"])

        start = time.time()
        con.execute((HERE / step["sql"]).read_text(encoding="utf-8"))
        shutil.copy(HERE / step["sql"], run_dir / "steps" / Path(step["sql"]).name)
        if fault:
            inject.silent(con, fault, name, step["outputs"])

        outputs = [stats(con, t, with_schema=True) for t in step["outputs"]]
        emit(step=name, event="COMPLETE", inputs=inputs, outputs=outputs,
             duration_s=round(time.time() - start, 2))
        for o in outputs:
            log(f"{name}: wrote {o['rows']} rows to {o['name']}")
            saved.append(o["name"])
        if fault and fault["fault"] == "hash_mismatch" and fault["step"] == name:
            tamper_tables = step["outputs"]  # sửa sau, lúc lưu parquet

        # bước không có trong spec (chỉ khi tiêm lỗi undocumented_step)
        for op in inject.adhoc(fault, name):
            op_in = [stats(con, t) for t in op["inputs"]]
            emit(step=op["name"], event="START", inputs=op["inputs"], outputs=op["outputs"])
            start = time.time()
            con.execute(op["sql"])
            (run_dir / "steps" / (op["name"] + ".sql")).write_text(op["sql"] + "\n", encoding="utf-8")
            op_out = [stats(con, t, with_schema=True) for t in op["outputs"]]
            emit(step=op["name"], event="COMPLETE", inputs=op_in, outputs=op_out,
                 duration_s=round(time.time() - start, 2))
            log(f"{op['name']}: ran extra sql on {', '.join(op['outputs'])}")

    for t in tamper_tables:
        inject.tamper(con, fault, t)

    for t in saved:
        path = (run_dir / "tables" / (t + ".parquet")).as_posix()
        con.execute(f"COPY \"{t}\" TO '{path}' (FORMAT PARQUET)")

    emit(step="__run__", event="DONE")
    log(f"run {run_id} done")
    events.close()
    logs.close()
    con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--sf", type=float, default=0.01)
    ap.add_argument("--fault", choices=list(inject.FAULTS), help="bỏ trống = run sạch")
    ap.add_argument("--step", help="bước bị tiêm lỗi")
    ap.add_argument("--ratio", type=float, help="tỉ lệ dòng bị ảnh hưởng")
    args = ap.parse_args()

    fault = inject.build_fault(args.fault, args.step, args.ratio) if args.fault else None
    run_pipeline(args.run_id, args.sf, fault)
    inject.record_truth(args.run_id, fault)
    print("xong:", args.run_id, args.fault or "clean")


if __name__ == "__main__":
    main()
