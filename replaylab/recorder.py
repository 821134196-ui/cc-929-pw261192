"""Flight recorder: appends attempt records as JSONL.

Optionally simulates a buggy collector that re-ingests some records
(byte-identical duplicates) — the "duplicate collection" the analyzer
must later recognize and drop.
"""
from __future__ import annotations

import json
import random
import threading


class FlightRecorder:
    def __init__(self, path, dup_rate=0.0, seed=0):
        self.path = path
        self._f = open(path, "w", encoding="utf-8")
        self._lock = threading.Lock()
        self._rng = random.Random(seed ^ 0x5EED)
        self.dup_rate = dup_rate
        self.count = 0

    def emit(self, rec):
        line = json.dumps(rec, sort_keys=True)
        with self._lock:
            self._f.write(line + "\n")
            if self._rng.random() < self.dup_rate:
                # collector re-ingest: the exact same record lands twice
                self._f.write(line + "\n")
            self.count += 1
            self._f.flush()

    def close(self):
        with self._lock:
            self._f.close()
