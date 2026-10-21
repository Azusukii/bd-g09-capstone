# Tiêm lỗi vào pipeline và ghi đáp án vào runs/ground_truth.json
# Sinh nhiều run một lúc:
#   python pipelines/orders/inject.py --n-clean 20 --n-fault 40 --sf 0.01 --seed 1
# Các loại lỗi: drop_rows, duplicate_rows, undocumented_step, schema_change
import argparse
import json
import random
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
TRUTH = ROOT / "runs" / "ground_truth.json"

# bước mặc định và tham số của từng loại lỗi
FAULTS = {
    "drop_rows": {"step": "totals", "params": {"ratio": 0.01}},
    "duplicate_rows": {"step": "stage_orders", "params": {"ratio": 0.01}},
    "undocumented_step": {"step": "stage_orders", "params": {}},
    "schema_change": {"step": "stage_lineitem", "params": {"column": "l_extendedprice", "to": "VARCHAR"}},
}


def build_fault(name, step=None, ratio=None):
    params = dict(FAULTS[name]["params"])
    if ratio is not None and "ratio" in params:
        params["ratio"] = ratio
    return {"fault": name, "step": step or FAULTS[name]["step"], "params": params}


def silent(con, fault, step_id, outputs):
    # lỗi sửa thẳng bảng đầu ra của bước, không để lại event riêng
    if fault["step"] != step_id:
        return
    name = fault["fault"]
    params = fault["params"]
    table = outputs[0]

    if name in ("drop_rows", "duplicate_rows"):
        key = con.execute(f'DESCRIBE "{table}"').fetchone()[0]
        cut = int(params["ratio"] * 10000)
        where = f'hash("{key}") % 10000 < {cut}'
        if name == "drop_rows":
            con.execute(f'DELETE FROM "{table}" WHERE {where}')
        else:
            con.execute(f'INSERT INTO "{table}" SELECT * FROM "{table}" WHERE {where}')
    elif name == "schema_change":
        con.execute(f'ALTER TABLE "{table}" ALTER "{params["column"]}" TYPE {params["to"]}')


def adhoc(fault, step_id):
    # bước chạy thêm mà spec không có
    if not fault or fault["fault"] != "undocumented_step" or fault["step"] != step_id:
        return []
    if step_id != "stage_orders":
        raise SystemExit("undocumented_step chỉ dùng được với stage_orders")
    return [{
        "name": "adhoc_update_orders",
        "sql": "UPDATE staging_orders SET o_totalprice = o_totalprice * 1.10 "
               "WHERE hash(o_orderkey) % 100 < 5",
        "inputs": ["staging_orders"],
        "outputs": ["staging_orders"],
    }]


def record_truth(run_id, fault):
    TRUTH.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    if TRUTH.exists():
        rows = json.loads(TRUTH.read_text(encoding="utf-8"))
    rows = [r for r in rows if r["run_id"] != run_id]
    if fault:
        rows.append({"run_id": run_id, **fault})
    else:
        rows.append({"run_id": run_id, "fault": "none"})
    rows.sort(key=lambda r: r["run_id"])
    TRUTH.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main():
    import run  # import ở đây để khỏi bị import vòng với run.py

    ap = argparse.ArgumentParser()
    ap.add_argument("--n-clean", type=int, default=5)
    ap.add_argument("--n-fault", type=int, default=10)
    ap.add_argument("--sf", type=float, default=0.01)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--start", type=int, default=1, help="số thứ tự run đầu tiên")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    names = list(FAULTS)
    plan = [None] * args.n_clean
    for i in range(args.n_fault):
        plan.append(build_fault(names[i % len(names)], ratio=rng.choice([0.005, 0.01, 0.05])))
    rng.shuffle(plan)

    for i, fault in enumerate(plan, start=args.start):
        run_id = f"r{i:04d}"
        run.run_pipeline(run_id, args.sf, fault)
        record_truth(run_id, fault)
        print(run_id, fault["fault"] if fault else "none")


if __name__ == "__main__":
    main()
