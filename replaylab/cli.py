"""CLI: run / analyze / tree / replay / experiment."""
from __future__ import annotations

import argparse
import os
import sys

from .analyze import analyze_file, pick_trace, render_tree, report
from .experiment import conclusion, format_table, run_experiment, verify_with_tree
from .runner import Bench
from .workload import from_recording, generate


def _ensure_parent(path):
    d = os.path.dirname(os.path.abspath(path))
    os.makedirs(d, exist_ok=True)


def cmd_run(a):
    _ensure_parent(a.out)
    bench = Bench(seed=a.seed, edge_retries=a.edge_retries,
                  checkout_retries=a.checkout_retries, out_path=a.out,
                  dup_rate=a.dup_rate)
    bench.set_switches(edge=a.edge_retries, checkout=a.checkout_retries)
    bench.run_workload(generate(a.seed, a.requests))
    bench.shutdown()
    print(f"录制完成 → {a.out}")
    print(f"下一步：python -m replaylab analyze {a.out}")


def cmd_analyze(a):
    attempts, stats = analyze_file(a.recording)
    print(report(attempts, stats))


def cmd_tree(a):
    attempts, _ = analyze_file(a.recording)
    tid = a.trace or pick_trace(attempts)
    print(render_tree(attempts, tid))


def cmd_replay(a):
    workload = from_recording(a.recording)
    if not workload:
        sys.exit("录制里没有可回放的流量")
    _ensure_parent(a.out)
    bench = Bench(seed=a.seed, edge_retries=a.edge_retries,
                  checkout_retries=a.checkout_retries, out_path=a.out,
                  dup_rate=a.dup_rate)
    bench.set_switches(edge=a.edge_retries, checkout=a.checkout_retries)
    bench.run_workload(workload)
    bench.shutdown()
    print(f"回放完成（{len(workload)} 笔，edge-retries={a.edge_retries} "
          f"checkout-retries={a.checkout_retries}）→ {a.out}")
    print(f"下一步：python -m replaylab analyze {a.out}")


def cmd_experiment(a):
    rows = run_experiment(a.seed, a.requests, a.out_dir, dup_rate=a.dup_rate)
    print(format_table(rows))
    print()
    print(conclusion(rows))
    print()
    print("== 验证：从基线录制中挑一笔超时请求展开因果树 ==")
    print(verify_with_tree(rows))


def main(argv=None):
    p = argparse.ArgumentParser(
        prog="replaylab",
        description="可回放的重试放大定位工具（edge → checkout → ledger）")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="起三个服务，跑一遍种子流量并录制")
    r.add_argument("--seed", type=int, default=7)
    r.add_argument("--requests", type=int, default=48)
    r.add_argument("--edge-retries", type=int, default=2)
    r.add_argument("--checkout-retries", type=int, default=2)
    r.add_argument("--dup-rate", type=float, default=0.06)
    r.add_argument("--out", required=True)
    r.set_defaults(fn=cmd_run)

    an = sub.add_parser("analyze", help="离线分析录制文件")
    an.add_argument("recording")
    an.set_defaults(fn=cmd_analyze)

    t = sub.add_parser("tree", help="展开一笔（默认选放大最严重的失败）请求的因果树")
    t.add_argument("recording")
    t.add_argument("--trace")
    t.set_defaults(fn=cmd_tree)

    rp = sub.add_parser("replay", help="把录制里的流量在新开关下重放")
    rp.add_argument("recording")
    rp.add_argument("--edge-retries", type=int, required=True)
    rp.add_argument("--checkout-retries", type=int, required=True)
    rp.add_argument("--seed", type=int, default=7)
    rp.add_argument("--dup-rate", type=float, default=0.06)
    rp.add_argument("--out", required=True)
    rp.set_defaults(fn=cmd_replay)

    ex = sub.add_parser("experiment",
                        help="扫各层开关组合，对比成功率与下游流量，并用因果树验证")
    ex.add_argument("--seed", type=int, default=7)
    ex.add_argument("--requests", type=int, default=48)
    ex.add_argument("--dup-rate", type=float, default=0.06)
    ex.add_argument("--out-dir", default="recordings/experiment")
    ex.set_defaults(fn=cmd_experiment)

    a = p.parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    main()
