"""Seeded user-traffic generator, plus workload extraction from a recording
(so a recorded run can be replayed under different switches)."""
from __future__ import annotations

import json
import random
from dataclasses import dataclass


@dataclass
class UserRequest:
    trace_id: str
    offset: float   # seconds after workload start
    items: list


def generate(seed, n):
    rng = random.Random(seed)
    skus = [f"sku-{i}" for i in range(5)]
    reqs, t = [], 0.0
    for i in range(n):
        t += 0.03 + rng.random() * 0.07
        k = rng.choices([1, 2, 3], weights=[0.4, 0.4, 0.2])[0]
        reqs.append(UserRequest(f"t-{i:04d}", round(t, 4), rng.sample(skus, k)))
    return reqs


def from_recording(path):
    """Rebuild the exact workload (arrival schedule + items) from a recording."""
    roots, seen = [], set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if r.get("rec_id") in seen:
                continue
            seen.add(r.get("rec_id"))
            if r.get("side") == "client" and r.get("from") == "user":
                roots.append(r)
    roots.sort(key=lambda r: r["ts_start"])
    if not roots:
        return []
    t0 = roots[0]["ts_start"]
    return [UserRequest(r["trace_id"], round(r["ts_start"] - t0, 4),
                        list(r.get("args", {}).get("items", [])))
            for r in roots]
