"""Offline analysis of a recording.

Pipeline: load raw JSONL -> drop collector duplicates -> merge client/server
mirror pairs into canonical attempts -> classify each attempt as root /
first-call / normal fan-out / genuine retry (flagging wasteful retries whose
predecessor actually succeeded server-side) -> build per-trace causal trees.
"""
from __future__ import annotations

import json
import math
from collections import defaultdict

CLIENT_BUDGET_MS = 250  # mirrors services.CLIENT_TIMEOUT_S (display only)


def load_raw(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def canonicalize(raw):
    """Return (attempts, collection_stats).

    Two kinds of duplicate collection are collapsed here:
    - exact duplicates: byte-identical records re-ingested by the collector
      (same rec_id) -> dropped;
    - mirror pairs: the client and the server both recorded the same attempt
      (same attempt_id, opposite sides) -> merged into one canonical attempt.
    """
    seen, dups = set(), 0
    by_attempt = defaultdict(list)
    for r in raw:
        rid = r.get("rec_id")
        if rid in seen:
            dups += 1
            continue
        seen.add(rid)
        by_attempt[r["attempt_id"]].append(r)
    attempts, mirrors = [], 0
    client_only = server_only = 0
    for aid, recs in by_attempt.items():
        client = next((r for r in recs if r.get("side") == "client"), None)
        server = next((r for r in recs if r.get("side") == "server"), None)
        if client and server:
            mirrors += 1
        elif client:
            client_only += 1
        else:
            server_only += 1
        src = client or server
        a = {
            "attempt_id": aid,
            "trace_id": src.get("trace_id"),
            "op": src.get("op"),
            "from": (client or {}).get("from", "?"),
            "to": (server or {}).get("service") or (client or {}).get("to", "?"),
            "parent_attempt_id": src.get("parent_attempt_id"),
            "idem_key": src.get("idem_key"),
            "request_key": src.get("request_key"),
            "try_seq": src.get("try_seq", 0),
            "args": src.get("args") or {},
            "client_outcome": (client or {}).get("outcome"),
            "client_ts_start": (client or {}).get("ts_start"),
            "client_ts_end": (client or {}).get("ts_end"),
            "status": (client or {}).get("status", 0),
            "server_outcome": (server or {}).get("outcome"),
            "server_ts_start": (server or {}).get("ts_start"),
            "server_ts_end": (server or {}).get("ts_end"),
            "server_note": (server or {}).get("note", ""),
            "inflight": (server or {}).get("inflight"),
            "inflight_retries": (server or {}).get("inflight_retries"),
        }
        # the outcome that propagates upward is what the caller observed
        a["outcome"] = a["client_outcome"] or a["server_outcome"] or "?"
        a["ts_start"] = a["client_ts_start"] or a["server_ts_start"]
        attempts.append(a)
    stats = {"raw": len(raw), "exact_dups": dups, "mirror_pairs": mirrors,
             "client_only": client_only, "server_only": server_only,
             "canonical": len(attempts)}
    return attempts, stats


def classify(attempts):
    """Tag each attempt: root | first | fanout | retry.

    Siblings = same trace, same parent attempt, same op.
    Within siblings, grouping is by request_key (the semantic unit of work):
    - several request_keys under one parent  -> normal fan-out;
    - several attempts on one request_key    -> a retry chain (try_seq order).
    A retry is flagged wasteful when its predecessor's server side actually
    succeeded (the caller gave up too early).
    """
    by_trace = defaultdict(list)
    for a in attempts:
        by_trace[a["trace_id"]].append(a)
    for atts in by_trace.values():
        sibs = defaultdict(list)
        for a in atts:
            sibs[(a["parent_attempt_id"], a["op"])].append(a)
        for (parent, _op), group in sibs.items():
            if parent is None:
                for a in group:
                    a["cls"] = "root"
                continue
            chains = defaultdict(list)
            for a in group:
                chains[a["request_key"]].append(a)
            fanout = len(chains) > 1
            for chain in chains.values():
                chain.sort(key=lambda a: (a["try_seq"], a["ts_start"] or 0))
                for i, a in enumerate(chain):
                    if i == 0:
                        a["cls"] = "fanout" if fanout else "first"
                    else:
                        a["cls"] = "retry"
                        prev = chain[i - 1]
                        a["wasteful"] = prev["server_outcome"] == "ok"
    return attempts


def analyze_file(path):
    attempts, stats = canonicalize(load_raw(path))
    return classify(attempts), stats


def build_forest(attempts):
    by_id = {a["attempt_id"]: a for a in attempts}
    children, roots = defaultdict(list), []
    for a in attempts:
        p = a["parent_attempt_id"]
        if p and p in by_id:
            children[p].append(a)
        else:
            roots.append(a)
    for kids in children.values():
        kids.sort(key=lambda a: a["ts_start"] or 0)
    roots.sort(key=lambda a: a["ts_start"] or 0)
    return roots, children


def latency_ms(a):
    if a["client_ts_start"] is not None and a["client_ts_end"] is not None:
        return (a["client_ts_end"] - a["client_ts_start"]) * 1000
    return None


def server_latency_ms(a):
    if a["server_ts_start"] is not None and a["server_ts_end"] is not None:
        return (a["server_ts_end"] - a["server_ts_start"]) * 1000
    return None


def percentile(vals, q):
    if not vals:
        return 0.0
    vals = sorted(vals)
    k = max(0, min(len(vals) - 1, math.ceil(q / 100 * len(vals)) - 1))
    return vals[k]


def summarize(attempts):
    roots = [a for a in attempts if a["cls"] == "root"]
    succ = sum(1 for a in roots if a["outcome"] == "ok")
    retries = [a for a in attempts if a["cls"] == "retry"]
    by_to = defaultdict(int)
    retry_by_hop = defaultdict(int)
    for a in attempts:
        by_to[a["to"]] += 1
        if a["cls"] == "retry":
            retry_by_hop[f"{a['from']}→{a['to']}"] += 1
    lats = [latency_ms(a) for a in roots]
    lats = [x for x in lats if x is not None]
    n = len(roots) or 1
    return {
        "requests": len(roots),
        "success": succ,
        "failed": len(roots) - succ,
        "success_rate": succ / n * 100,
        "attempts_total": len(attempts),
        "by_service": dict(by_to),
        "amplification": len(attempts) / n,
        "ledger_amplification": by_to.get("ledger", 0) / n,
        "retries": len(retries),
        "wasteful_retries": sum(1 for a in retries if a.get("wasteful")),
        "retry_by_hop": dict(retry_by_hop),
        "fanout": sum(1 for a in attempts if a["cls"] == "fanout"),
        "first": sum(1 for a in attempts if a["cls"] == "first"),
        "idempotent_replays": sum(1 for a in attempts
                                  if a["server_note"] == "idempotent_replay"),
        "p50_ms": percentile(lats, 50),
        "p99_ms": percentile(lats, 99),
    }


def root_causes(a, children):
    """Failed leaves whose failure is not explained by a failed child."""
    if a["outcome"] == "ok":
        return []
    failed_kids = [k for k in children.get(a["attempt_id"], [])
                   if k["outcome"] != "ok"]
    if not failed_kids:
        return [a]
    out = []
    for k in failed_kids:
        out.extend(root_causes(k, children))
    return out


def _fmt_attempt(a):
    mark = "✓" if a["outcome"] == "ok" else "✗"
    lat = latency_ms(a)
    status = a.get("status") or 0
    s = f"{mark} {a['from']}→{a['to']} {a['op']} try#{a['try_seq']} → {a['outcome'].upper()}"
    if a["outcome"] != "ok" and status:
        s += f"({status})"
    if lat is not None:
        s += f" {lat:.0f}ms"
    tags = []
    if a["cls"] == "retry":
        tags.append("重试·无效" if a.get("wasteful") else "重试")
    elif a["cls"] == "fanout":
        tags.append("扇出")
    if (a["server_outcome"] and a["client_outcome"]
            and a["server_outcome"] != a["client_outcome"]):
        slat = server_latency_ms(a)
        extra = f" {slat:.0f}ms" if slat is not None else ""
        tags.append(f"服务端实际{a['server_outcome']}{extra}")
    if a["server_note"] == "idempotent_replay":
        tags.append("幂等重放")
    if a["inflight"]:
        tags.append(f"inflight={a['inflight']}(其中重试{a['inflight_retries']})")
    if tags:
        s += "  [" + ", ".join(tags) + "]"
    return s


def render_tree(attempts, trace_id):
    roots, children = build_forest(attempts)
    root = next((r for r in roots if r["trace_id"] == trace_id), None)
    if root is None:
        return f"trace {trace_id} 不存在"
    n_in_trace = sum(1 for a in attempts if a["trace_id"] == trace_id)
    lat = latency_ms(root)
    head = (f"== 因果树 {trace_id}（用户{root['outcome']}"
            + (f" {lat:.0f}ms" if lat else "")
            + f"，共 {n_in_trace} 次尝试）==")
    lines = [head, _fmt_attempt(root)]

    def walk(a, prefix):
        kids = children.get(a["attempt_id"], [])
        for i, k in enumerate(kids):
            last = i == len(kids) - 1
            lines.append(prefix + ("└─ " if last else "├─ ") + _fmt_attempt(k))
            walk(k, prefix + ("   " if last else "│  "))

    walk(root, "")
    causes = root_causes(root, children)
    if causes:
        lines.append("")
        lines.append(f"根因叶子（{len(causes)} 个）：")
        for i, c in enumerate(causes, 1):
            lines.append(f"  {i}. {_verdict(c)}")
    return "\n".join(lines)


def _verdict(a):
    bits = [f"{a['from']}→{a['to']} {a['op']} try#{a['try_seq']}"]
    if a["client_outcome"] == "timeout" and a["server_outcome"] == "ok":
        slat = server_latency_ms(a)
        bits.append(f"客户端 {CLIENT_BUDGET_MS}ms 预算超时，但服务端其实成功了"
                    + (f"（耗时 {slat:.0f}ms）" if slat else ""))
    elif a["client_outcome"] == "timeout":
        bits.append(f"客户端 {CLIENT_BUDGET_MS}ms 预算超时")
    elif a["outcome"] != "ok":
        bits.append(f"返回 {a['outcome']}")
    if a["inflight"]:
        bits.append(f"当时 ledger inflight={a['inflight']}，"
                    f"其中 {a['inflight_retries']} 个是重试")
    return "；".join(bits)


def pick_trace(attempts):
    """The most interesting trace: a failed root with the most attempts."""
    roots = [a for a in attempts if a["cls"] == "root"]
    counts = defaultdict(int)
    for a in attempts:
        counts[a["trace_id"]] += 1
    failed = [r for r in roots if r["outcome"] != "ok"]
    pool = failed or roots
    return max(pool, key=lambda r: counts[r["trace_id"]])["trace_id"]


def report(attempts, stats):
    m = summarize(attempts)
    lines = [
        "== 采集层（重复采集识别）==",
        f"原始记录 {stats['raw']} 条",
        f"├─ 精确重复（采集器重放，同 rec_id） {stats['exact_dups']} 条 → 丢弃",
        f"└─ 双端镜像（client+server 双采集，同 attempt_id） "
        f"{stats['mirror_pairs']} 对 → 合并",
        f"   单侧记录 {stats['client_only'] + stats['server_only']} 条"
        f"（对端缺失：对端超时发生在连接被接受之前，或记录丢失）",
        f"⇒ 真实调用 {stats['canonical']} 次",
        "",
        "== 调用性质分类 ==",
        f"首次调用 {m['first']} · 正常扇出 {m['fanout']} · "
        f"真实重试 {m['retries']}（其中无效重试 {m['wasteful_retries']}："
        f"前一次服务端已成功）",
        f"幂等重放 {m['idempotent_replays']} 次"
        f"（上游重试造成的重复执行，被 ledger 幂等键拦截）",
        "",
        "== 分层重试 ==",
    ]
    if m["retry_by_hop"]:
        for hop, n in sorted(m["retry_by_hop"].items()):
            lines.append(f"  {hop}: {n} 次")
    else:
        lines.append("  （无重试）")
    lines += [
        "",
        "== 用户视角 ==",
        f"请求 {m['requests']} 笔：成功 {m['success']} / 失败 {m['failed']}"
        f"（成功率 {m['success_rate']:.1f}%）",
        f"延迟 p50 {m['p50_ms']:.0f}ms / p99 {m['p99_ms']:.0f}ms",
        f"放大系数 {m['amplification']:.2f}×（总尝试 {m['attempts_total']} / "
        f"用户 {m['requests']}）；ledger 放大 "
        f"{m['ledger_amplification']:.2f}×（{m['by_service'].get('ledger', 0)} 次）",
        f"分层调用：edge {m['by_service'].get('edge', 0)} · "
        f"checkout {m['by_service'].get('checkout', 0)} · "
        f"ledger {m['by_service'].get('ledger', 0)}",
    ]
    failed = [a for a in attempts
              if a["cls"] == "root" and a["outcome"] != "ok"]
    if failed:
        counts = defaultdict(int)
        for a in attempts:
            counts[a["trace_id"]] += 1
        failed.sort(key=lambda r: -counts[r["trace_id"]])
        lines += ["", "== 失败 trace（按放大程度排序，可用 tree 子命令展开）=="]
        for r in failed[:5]:
            lines.append(f"  {r['trace_id']}  {r['outcome']}  "
                         f"{counts[r['trace_id']]} 次尝试")
    return "\n".join(lines)
