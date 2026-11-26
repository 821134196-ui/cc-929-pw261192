"""Switch sweep: replay the same seeded workload under different per-layer
retry switches and compare success rate vs downstream traffic. Finishes by
expanding the causal tree of a timed-out request from the baseline run to
verify the conclusion."""
from __future__ import annotations

import os

from .analyze import analyze_file, pick_trace, render_tree, summarize
from .runner import Bench
from .workload import generate

CONFIGS = [(2, 2), (0, 2), (2, 0), (1, 1), (0, 1), (0, 0)]


def run_experiment(seed, n, out_dir, dup_rate=0.06, configs=None):
    os.makedirs(out_dir, exist_ok=True)
    workload = generate(seed, n)
    rows = []
    for er, cr in (configs or CONFIGS):
        path = os.path.join(out_dir, f"exp-edge{er}-checkout{cr}.jsonl")
        bench = Bench(seed=seed, edge_retries=er, checkout_retries=cr,
                      out_path=path, dup_rate=dup_rate)
        # flip the switches through the real admin endpoints
        bench.set_switches(edge=er, checkout=cr)
        bench.run_workload(workload)
        bench.shutdown()
        attempts, _stats = analyze_file(path)
        rows.append({"edge": er, "checkout": cr, "path": path,
                     "m": summarize(attempts)})
    return rows


def format_table(rows):
    lines = [
        "配置                    成功率    ledger调用  总尝试   总放大   "
        "ledger放大  p99      重试(无效)",
        "-" * 96,
    ]
    for r in rows:
        m = r["m"]
        lines.append(
            f"edge={r['edge']} checkout={r['checkout']}      "
            f"{m['success_rate']:5.1f}%   "
            f"{m['by_service'].get('ledger', 0):6d}      "
            f"{m['attempts_total']:5d}   "
            f"{m['amplification']:5.2f}×  "
            f"{m['ledger_amplification']:5.2f}×     "
            f"{m['p99_ms']:4.0f}ms   "
            f"{m['retries']}({m['wasteful_retries']})")
    return "\n".join(lines)


def conclusion(rows):
    base = rows[0]["m"]
    ok_rows = [r for r in rows
               if r["m"]["success_rate"] >= base["success_rate"] - 1.0]
    best = min(ok_rows, key=lambda r: r["m"]["by_service"].get("ledger", 0))
    bm = best["m"]
    cut = (1 - bm["by_service"].get("ledger", 0)
           / max(1, base["by_service"].get("ledger", 1))) * 100
    return (f"⇒ 推荐 edge={best['edge']} checkout={best['checkout']}："
            f"成功率 {bm['success_rate']:.1f}%（基线 "
            f"{base['success_rate']:.1f}%），ledger 流量 "
            f"{bm['by_service'].get('ledger', 0)} 次，较基线 "
            f"{base['by_service'].get('ledger', 0)} 次下降 {cut:.0f}%")


def verify_with_tree(rows, trace_id=None):
    """Expand a timed-out request's causal tree from the baseline recording."""
    baseline = rows[0]
    attempts, _ = analyze_file(baseline["path"])
    tid = trace_id or pick_trace(attempts)
    return render_tree(attempts, tid)
