"""Three local services wired edge -> checkout -> ledger.

Each layer owns a retry switch exposed at POST /admin/switches, so retry
behaviour can be toggled per layer at runtime. Every HTTP call produces two
attempt records — one from the caller's client interceptor, one from the
callee's server handler — sharing one attempt_id (normal dual instrumentation).

ledger has limited capacity: past LEDGER_CAP concurrent requests its latency
grows, and ~8% of requests hit a "flake window" (GC / lock contention). That
is what turns retry amplification into a self-reinforcing timeout storm.
"""
from __future__ import annotations

import http.client
import json
import random
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CLIENT_TIMEOUT_S = 0.25   # per-attempt budget at every internal hop
USER_TIMEOUT_S = 0.9      # load generator's budget for a whole user request
LEDGER_CAP = 4            # concurrent requests ledger handles comfortably


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class Switches:
    """Per-layer runtime toggles (one instance per service)."""

    def __init__(self, retry_max: int):
        self._retry_max = retry_max
        self._lock = threading.Lock()

    def get(self) -> int:
        with self._lock:
            return self._retry_max

    def set(self, v: int):
        with self._lock:
            self._retry_max = int(v)


def post_json(port, path, body, headers=None, timeout=5.0):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    conn.request("POST", path, json.dumps(body),
                 {"Content-Type": "application/json", **(headers or {})})
    resp = conn.getresponse()
    raw = resp.read()
    conn.close()
    return resp.status, json.loads(raw or b"{}")


def set_switch(port, retry_max):
    """Flip a layer's retry switch over HTTP (the real admin endpoint)."""
    return post_json(port, "/admin/switches", {"retry_max": retry_max})


def call_with_retries(*, recorder, switches, timeout_s, from_svc, to_svc, port,
                      path, op, trace_id, parent_attempt_id, idem_key,
                      request_key, body, args=None, rng):
    """Call `to_svc`, honoring this layer's retry switch.

    Emits exactly one client-side attempt record per try. Returns True iff
    some attempt got a 2xx back within the budget.
    """
    try_seq = 0
    while True:
        attempt_id = new_id("att")
        headers = {
            "X-Trace-Id": trace_id,
            "X-Attempt-Id": attempt_id,
            "X-Parent-Attempt-Id": parent_attempt_id or "",
            "X-Idem-Key": idem_key,
            "X-Try-Seq": str(try_seq),
            "X-Request-Key": request_key,
        }
        t0 = time.monotonic()
        outcome, status, detail = "ok", 200, ""
        try:
            status, _ = post_json(port, path, body, headers, timeout=timeout_s)
            if status >= 500:
                outcome = "error"
        except (TimeoutError, socket.timeout):
            outcome, status, detail = "timeout", 0, "budget_exceeded"
        except (ConnectionError, http.client.HTTPException) as exc:
            outcome, status, detail = "error", 0, type(exc).__name__
        t1 = time.monotonic()
        recorder.emit({
            "rec_id": new_id("rec"), "side": "client", "attempt_id": attempt_id,
            "trace_id": trace_id, "from": from_svc, "to": to_svc, "op": op,
            "parent_attempt_id": parent_attempt_id, "idem_key": idem_key,
            "request_key": request_key, "try_seq": try_seq,
            "ts_start": round(t0, 6), "ts_end": round(t1, 6),
            "outcome": outcome, "status": status, "detail": detail,
            "args": args or {},
        })
        if outcome == "ok" or try_seq >= switches.get():
            return outcome == "ok"
        try_seq += 1
        time.sleep(0.02 * try_seq + rng.random() * 0.02)  # small backoff


def server_rec(service, headers, t0, t1, status, extra=None):
    return {
        "rec_id": new_id("rec"), "side": "server", "service": service.name,
        "attempt_id": headers.get("X-Attempt-Id", "?"),
        "trace_id": headers.get("X-Trace-Id", "?"),
        "parent_attempt_id": headers.get("X-Parent-Attempt-Id") or None,
        "idem_key": headers.get("X-Idem-Key", "?"),
        "request_key": headers.get("X-Request-Key", "?"),
        "try_seq": int(headers.get("X-Try-Seq", "0")),
        "ts_start": round(t0, 6), "ts_end": round(t1, 6),
        "outcome": "ok" if status < 500 else "error", "status": status,
        **(extra or {}),
    }


class _Handler(BaseHTTPRequestHandler):
    service = None  # bound via make_server

    def log_message(self, *args):
        pass

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        if self.path == "/admin/switches":
            self._json(200, {"service": self.service.name,
                             "retry_max": self.service.switches.get()})
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/admin/switches":
            self.service.switches.set(self._read()["retry_max"])
            self._json(200, {"service": self.service.name,
                             "retry_max": self.service.switches.get()})
            return
        try:
            self.service.dispatch(self, self.path, self._read(), self.headers)
        except Exception as exc:  # keep the bench alive
            try:
                self._json(500, {"error": f"handler: {exc!r}"})
            except OSError:
                pass  # caller already gave up


class _QuietServer(ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        pass  # a client that hit its budget is gone; nothing to report


def make_server(service):
    handler = type("BoundHandler", (_Handler,), {"service": service})
    srv = _QuietServer(("127.0.0.1", 0), handler)
    srv.request_queue_size = 128
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, srv.server_address[1]


class Ledger:
    """Leaf service. Capacity-limited; never calls anyone else."""
    name = "ledger"

    def __init__(self, recorder, seed, cap=LEDGER_CAP):
        self.recorder = recorder
        self.cap = cap
        self.switches = Switches(0)  # leaf: no outbound retries
        self.rng = random.Random(seed)
        self.lock = threading.Lock()
        self.inflight = 0
        self.inflight_retries = 0
        self.ok_keys = set()

    def dispatch(self, h, path, body, headers):
        if path != "/reserve":
            return h._json(404, {"error": "not found"})
        try_seq = int(headers.get("X-Try-Seq", "0"))
        idem = headers.get("X-Idem-Key", "?")
        t0 = time.monotonic()
        with self.lock:
            self.inflight += 1
            if try_seq > 0:
                self.inflight_retries += 1
            inflight, inflight_retries = self.inflight, self.inflight_retries
        if idem in self.ok_keys:
            # idempotent replay: the logical op already succeeded once
            latency, status, note = 0.004, 200, "idempotent_replay"
        else:
            # bounded queueing delay: past capacity, each extra in-flight
            # request adds latency, up to a 300ms ceiling
            latency = (0.04 + self.rng.random() * 0.03
                       + min(0.30, max(0, inflight - self.cap) * 0.04))
            if self.rng.random() < 0.06:
                latency += 0.35  # flake window
            hard_fail = self.rng.random() < 0.02
            status, note = (500, "transient_fault") if hard_fail else (200, "")
        time.sleep(latency)
        t1 = time.monotonic()
        if status == 200 and note != "idempotent_replay":
            with self.lock:
                self.ok_keys.add(idem)
        self.recorder.emit(server_rec(self, headers, t0, t1, status, {
            "op": "POST /reserve", "inflight": inflight,
            "inflight_retries": inflight_retries, "note": note,
            "args": {"sku": body.get("sku")}}))
        with self.lock:
            self.inflight -= 1
            if try_seq > 0:
                self.inflight_retries -= 1
        h._json(status, {"reserved": status == 200, "note": note})


class Checkout:
    """Middle service. Fans out one /reserve per item (normal fan-out),
    each with its own retry switch."""
    name = "checkout"

    def __init__(self, recorder, ledger_port, seed, retry_max):
        self.recorder = recorder
        self.ledger_port = ledger_port
        self.switches = Switches(retry_max)
        self.rng = random.Random(seed + 1)

    def dispatch(self, h, path, body, headers):
        if path != "/order":
            return h._json(404, {"error": "not found"})
        t0 = time.monotonic()
        trace_id = headers.get("X-Trace-Id", "?")
        incoming = headers.get("X-Attempt-Id", "?")
        order_idem = headers.get("X-Idem-Key", "?")
        items = body.get("items", [])
        results = {}
        with ThreadPoolExecutor(max_workers=max(1, len(items))) as pool:
            futs = {pool.submit(self._reserve, trace_id, incoming, order_idem, sku): sku
                    for sku in items}
            for fut in as_completed(futs):
                results[futs[fut]] = fut.result()
        ok = all(results.values()) if results else True
        status = 200 if ok else 500
        t1 = time.monotonic()
        self.recorder.emit(server_rec(self, headers, t0, t1, status,
                                      {"op": "POST /order",
                                       "args": {"items": items}}))
        h._json(status, {"order": ok, "items": results})

    def _reserve(self, trace_id, parent_attempt, order_idem, sku):
        return call_with_retries(
            recorder=self.recorder, switches=self.switches,
            timeout_s=CLIENT_TIMEOUT_S, from_svc="checkout", to_svc="ledger",
            port=self.ledger_port, path="/reserve", op="POST /reserve",
            trace_id=trace_id, parent_attempt_id=parent_attempt,
            idem_key=f"{order_idem}:{sku}", request_key=sku,
            body={"sku": sku}, args={"sku": sku}, rng=self.rng)


class Edge:
    """Entry service. One /order call per user request, with its own
    retry switch."""
    name = "edge"

    def __init__(self, recorder, checkout_port, seed, retry_max):
        self.recorder = recorder
        self.checkout_port = checkout_port
        self.switches = Switches(retry_max)
        self.rng = random.Random(seed + 2)

    def dispatch(self, h, path, body, headers):
        if path != "/checkout":
            return h._json(404, {"error": "not found"})
        t0 = time.monotonic()
        trace_id = headers.get("X-Trace-Id", "?")
        incoming = headers.get("X-Attempt-Id", "?")
        items = body.get("items", [])
        ok = call_with_retries(
            recorder=self.recorder, switches=self.switches,
            timeout_s=CLIENT_TIMEOUT_S, from_svc="edge", to_svc="checkout",
            port=self.checkout_port, path="/order", op="POST /order",
            trace_id=trace_id, parent_attempt_id=incoming,
            idem_key=f"order-{trace_id}", request_key=f"order-{trace_id}",
            body={"items": items}, args={"items": items}, rng=self.rng)
        t1 = time.monotonic()
        self.recorder.emit(server_rec(self, headers, t0, t1,
                                      200 if ok else 504,
                                      {"op": "POST /checkout",
                                       "args": {"items": items}}))
        h._json(200 if ok else 504, {"ok": ok})
