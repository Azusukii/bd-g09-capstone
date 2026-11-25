import json
import argparse
import sys
import duckdb
from pathlib import Path
from datetime import datetime
import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage

def resolve_sql(run_path: Path, sql_file: str) -> Path:
    p = run_path / sql_file
    return p if p.exists() else run_path / "steps" / sql_file

def raw_schema(data_dir: Path):
    out = {}
    try:
        db = next(data_dir.glob("tpch_sf*.duckdb"))
        con = duckdb.connect(str(db), read_only=True)
        for (t,) in con.execute("SHOW TABLES").fetchall():
            out[t] = {r[0]: r[1] for r in con.execute(f'DESCRIBE "{t}"').fetchall()}
        con.close()
    except StopIteration:
        pass
    return out

def column_edges(sql_text, out_table, out_cols, schema):
    try:
        stmts = [s for s in sqlglot.parse(sql_text, read="duckdb") if s is not None]
    except Exception as e:
        return {}, f"parse_error: {e}"
        
    if len(stmts) != 1:
        return {}, f"expected_1_statement_got_{len(stmts)}"
        
    stmt = stmts[0]
    query = stmt.expression if isinstance(stmt, exp.Create) else stmt
    if not isinstance(query, (exp.Select, exp.Union)):
        return {}, f"unsupported_statement_{type(stmt).__name__}"
        
    result = {}
    for col in out_cols:
        try:
            root = lineage(col, query.copy(), schema=schema, dialect="duckdb")
            srcs = {(n.source.name, n.name.split(".")[-1]) 
                    for n in root.walk() if isinstance(n.source, exp.Table)}
            result[col] = (srcs, "ok" if srcs else "no_source_columns")
        except Exception as e:
            result[col] = (set(), f"error: {e}")
    return result, "parsed"

def process_run(run_path: Path, data_dir: Path):
    events_file = run_path / "events.jsonl"
    evidence_file = run_path / "evidence.json"
    steps_dir = run_path / "steps"
    
    if not events_file.exists() or not evidence_file.exists():
        print(f"Lỗi: Mất {events_file} hoặc {evidence_file}")
        sys.exit(1)
        
    with open(evidence_file, "r", encoding="utf-8") as f:
        evidence = json.load(f)
        
    full_schema = raw_schema(data_dir)
    sql_verified_map = {}
    
    for step in evidence.get("steps", []):
        sql_verified_map[step["step"]] = step.get("sql_verified", False)
        for out in step.get("outputs", []):
            if out.get("recomputed") and out["recomputed"].get("schema"):
                full_schema[out["name"]] = out["recomputed"]["schema"]
                
    lineage_data = {
        "run_id": run_path.name,
        "pipeline": evidence.get("pipeline"),
        "built_at": datetime.now().isoformat(),
        "nodes": [],
        "edges": [],
        "steps": []  # THÊM MẢNG MỚI ĐỂ CHỨA TRẠNG THÁI LINEAGE CỦA BƯỚC
    }
    nodes_dict = {}

    def add_node(node_id, n_type, table, step=None):
        if node_id not in nodes_dict:
            node = {"id": node_id, "type": n_type, "table": table, "step": step}
            nodes_dict[node_id] = node
            lineage_data["nodes"].append(node)

    with open(events_file, "r", encoding="utf-8") as f:
        events = [json.loads(l) for l in f if l.strip()]
        
    for event in events:
        if event.get("event") == "COMPLETE":
            step = event.get("step")
            sql_file = event.get("sql_file")
            inputs = [i["name"] for i in event.get("inputs", [])]
            outputs = [o["name"] for o in event.get("outputs", [])]
            
            # 1. Dựng cạnh bảng
            for in_tbl in inputs:
                add_node(in_tbl, "table", in_tbl)
                for out_tbl in outputs:
                    add_node(out_tbl, "table", out_tbl, step)
                    lineage_data["edges"].append({
                        "from": in_tbl, "to": out_tbl, "step": step, "level": "table"
                    })
            
            # 2. Dựng cạnh cột và ghi metadata cấp độ Bước
            info = {"step": step, "column_lineage": "known", "reason": None, "unknown_columns": {}}
            
            if sql_verified_map.get(step) and sql_file:
                sql_text = resolve_sql(run_path, sql_file).read_text(encoding="utf-8")
                
                for out_table in event.get("outputs", []):
                    t = out_table["name"]
                    out_cols = list(out_table.get("schema", {}).keys())
                    
                    for col in out_cols:
                        add_node(f"{t}.{col}", "column", t, step)
                        
                    col_results, status = column_edges(sql_text, t, out_cols, full_schema)
                    
                    if status != "parsed":
                        info.update(column_lineage="unknown", reason=status)
                        continue
                        
                    for col, (srcs, col_status) in col_results.items():
                        if col_status != "ok":
                            info["unknown_columns"][f"{t}.{col}"] = col_status
                            
                        for src_tbl, src_col in srcs:
                            src_node = f"{src_tbl}.{src_col}"
                            add_node(src_node, "column", src_tbl)
                            lineage_data["edges"].append({
                                "from": src_node, "to": f"{t}.{col}",
                                "step": step, "level": "column"
                            })
            else:
                info.update(column_lineage="unknown", reason="sql_not_verified")
                for out_table in event.get("outputs", []):
                    t = out_table["name"]
                    for col in out_table.get("schema", {}).keys():
                        add_node(f"{t}.{col}", "column", t, step)
            
            lineage_data["steps"].append(info)
            
    out_file = run_path / "lineage.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(lineage_data, f, indent=2)
    print(f"[OK] Đã dựng lineage: {out_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("run_path", type=Path)
    # Hỗ trợ truyền data-dir linh hoạt, mặc định lùi 2 cấp nếu chạy từ gốc
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    process_run(args.run_path, args.data_dir)