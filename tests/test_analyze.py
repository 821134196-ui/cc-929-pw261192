"""Unit tests for the analysis pipeline on synthetic records."""
import unittest

from replaylab.analyze import (build_forest, canonicalize, classify,
                               root_causes, summarize)


def crec(aid, trace, parent, frm, to, op, rk, try_seq, outcome, t0, t1,
         rec_id=None):
    return {"rec_id": rec_id or f"r-{aid}-c", "side": "client",
            "attempt_id": aid, "trace_id": trace, "from": frm, "to": to,
            "op": op, "parent_attempt_id": parent, "idem_key": f"i-{rk}",
            "request_key": rk, "try_seq": try_seq, "ts_start": t0,
            "ts_end": t1, "outcome": outcome,
            "status": 200 if outcome == "ok" else 0, "detail": "", "args": {}}


def srec(aid, trace, svc, op, outcome, t0, t1, **extra):
    rec = {"rec_id": f"r-{aid}-s", "side": "server", "service": svc,
           "attempt_id": aid, "trace_id": trace, "parent_attempt_id": None,
           "idem_key": "i", "request_key": "k", "try_seq": 0,
           "ts_start": t0, "ts_end": t1, "op": op, "outcome": outcome,
           "status": 200 if outcome == "ok" else 500}
    rec.update(extra)
    return rec


def scenario():
    """One trace: user -> edge (timeout); edge order try#0 (timeout) with
    fan-out to sku-a (timeout, but server ok) and sku-b (ok); sku-a retried
    (ok via idempotent replay)."""
    raw = [
        crec("U", "t1", None, "user", "edge", "POST /checkout", "t1", 0,
             "timeout", 0.0, 0.9),
        srec("U", "t1", "edge", "POST /checkout", "error", 0.01, 0.85),
        crec("E1", "t1", "U", "edge", "checkout", "POST /order", "o1", 0,
             "timeout", 0.02, 0.27),
        srec("E1", "t1", "checkout", "POST /order", "error", 0.02, 0.30),
        crec("R1", "t1", "E1", "checkout", "ledger", "POST /reserve", "sku-a",
             0, "timeout", 0.03, 0.28),
        srec("R1", "t1", "ledger", "POST /reserve", "ok", 0.03, 0.44,
             inflight=9, inflight_retries=6, note=""),
        crec("R2", "t1", "E1", "checkout", "ledger", "POST /reserve", "sku-a",
             1, "ok", 0.30, 0.31),
        srec("R2", "t1", "ledger", "POST /reserve", "ok", 0.30, 0.31,
             inflight=3, inflight_retries=1, note="idempotent_replay"),
        crec("R3", "t1", "E1", "checkout", "ledger", "POST /reserve", "sku-b",
             0, "ok", 0.03, 0.09),
        srec("R3", "t1", "ledger", "POST /reserve", "ok", 0.03, 0.09,
             inflight=2, inflight_retries=0, note=""),
    ]
    return raw


class TestCanonicalize(unittest.TestCase):
    def test_exact_dups_dropped_and_mirrors_merged(self):
        raw = scenario()
        raw.append(dict(raw[0]))          # collector re-ingest of one record
        raw.append(dict(raw[0]))          # ... twice
        attempts, stats = canonicalize(raw)
        self.assertEqual(stats["raw"], 12)
        self.assertEqual(stats["exact_dups"], 2)
        self.assertEqual(stats["canonical"], 5)
        self.assertEqual(stats["mirror_pairs"], 5)
        by_id = {a["attempt_id"]: a for a in attempts}
        # merged view keeps both sides
        self.assertEqual(by_id["R1"]["client_outcome"], "timeout")
        self.assertEqual(by_id["R1"]["server_outcome"], "ok")
        self.assertEqual(by_id["R1"]["inflight"], 9)


class TestClassify(unittest.TestCase):
    def setUp(self):
        attempts, _ = canonicalize(scenario())
        self.by_id = {a["attempt_id"]: a for a in classify(attempts)}

    def test_root_first_fanout_retry(self):
        self.assertEqual(self.by_id["U"]["cls"], "root")
        self.assertEqual(self.by_id["E1"]["cls"], "first")
        # sku-a and sku-b under one parent => fan-out group
        self.assertEqual(self.by_id["R1"]["cls"], "fanout")
        self.assertEqual(self.by_id["R3"]["cls"], "fanout")
        # R2 retries R1 (same request_key, try_seq 1)
        self.assertEqual(self.by_id["R2"]["cls"], "retry")

    def test_wasteful_retry_detection(self):
        # R1's server side actually succeeded -> retrying it was wasteful
        self.assertTrue(self.by_id["R2"]["wasteful"])

    def test_root_cause_is_ledger_leaf(self):
        attempts = list(self.by_id.values())
        _, children = build_forest(attempts)
        causes = root_causes(self.by_id["U"], children)
        self.assertEqual([c["attempt_id"] for c in causes], ["R1"])

    def test_summary_counts(self):
        m = summarize(list(self.by_id.values()))
        self.assertEqual(m["requests"], 1)
        self.assertEqual(m["failed"], 1)
        self.assertEqual(m["retries"], 1)
        self.assertEqual(m["wasteful_retries"], 1)
        self.assertEqual(m["idempotent_replays"], 1)
        self.assertEqual(m["by_service"]["ledger"], 3)


if __name__ == "__main__":
    unittest.main()
