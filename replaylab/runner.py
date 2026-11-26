"""Bench: boots the three services on ephemeral ports and drives a workload
against them, recording every attempt to a flight recorder."""
from __future__ import annotations

import http.client
import socket
import time
from concurrent.futures import ThreadPoolExecutor

from .recorder import FlightRecorder
from .services import (USER_TIMEOUT_S, Checkout, Edge, Ledger, make_server,
                       new_id, post_json, set_switch)


class Bench:
    def __init__(self, *, seed, edge_retries, checkout_retries, out_path,
                 dup_rate=0.0):
        self.recorder = FlightRecorder(out_path, dup_rate=dup_rate, seed=seed)
        self.ledger = Ledger(self.recorder, seed)
        self._ledger_srv, self.ledger_port = make_server(self.ledger)
        self.checkout = Checkout(self.recorder, self.ledger_port, seed,
                                 checkout_retries)
        self._checkout_srv, self.checkout_port = make_server(self.checkout)
        self.edge = Edge(self.recorder, self.checkout_port, seed, edge_retries)
        self._edge_srv, self.edge_port = make_server(self.edge)

    def set_switches(self, *, edge=None, checkout=None):
        """Flip layer switches through the real HTTP admin endpoints."""
        if edge is not None:
            set_switch(self.edge_port, edge)
        if checkout is not None:
            set_switch(self.checkout_port, checkout)

    def _fire(self, req):
        attempt_id = new_id("att")
        t0 = time.monotonic()
        outcome, status, detail = "ok", 200, ""
        try:
            status, _ = post_json(
                self.edge_port, "/checkout", {"items": req.items},
                headers={"X-Trace-Id": req.trace_id,
                         "X-Attempt-Id": attempt_id,
                         "X-Parent-Attempt-Id": "",
                         "X-Idem-Key": f"user-{req.trace_id}",
                         "X-Try-Seq": "0",
                         "X-Request-Key": req.trace_id},
                timeout=USER_TIMEOUT_S)
            if status >= 500:
                outcome = "error"
        except (TimeoutError, socket.timeout):
            outcome, status, detail = "timeout", 0, "budget_exceeded"
        except (ConnectionError, http.client.HTTPException) as exc:
            outcome, status, detail = "error", 0, type(exc).__name__
        t1 = time.monotonic()
        self.recorder.emit({
            "rec_id": new_id("rec"), "side": "client",
            "attempt_id": attempt_id, "trace_id": req.trace_id,
            "from": "user", "to": "edge", "op": "POST /checkout",
            "parent_attempt_id": None, "idem_key": f"user-{req.trace_id}",
            "request_key": req.trace_id, "try_seq": 0,
            "ts_start": round(t0, 6), "ts_end": round(t1, 6),
            "outcome": outcome, "status": status, "detail": detail,
            "args": {"items": req.items},
        })

    def run_workload(self, workload, workers=8):
        t0 = time.monotonic()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for req in workload:
                lag = req.offset - (time.monotonic() - t0)
                if lag > 0:
                    time.sleep(lag)
                pool.submit(self._fire, req)
        self.wait_quiet()

    def wait_quiet(self, timeout=20.0):
        """Block until in-flight work drains (no new records for 150ms and
        ledger idle), so abandoned retries don't leak into the next run."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.ledger.lock:
                busy = self.ledger.inflight
            count = self.recorder.count
            time.sleep(0.15)
            if busy == 0 and self.recorder.count == count:
                return

    def shutdown(self):
        for srv in (self._edge_srv, self._checkout_srv, self._ledger_srv):
            srv.shutdown()
            srv.server_close()
        self.recorder.close()
