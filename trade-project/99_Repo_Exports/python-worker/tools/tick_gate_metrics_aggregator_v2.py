from __future__ import annotations

"""Tick Gate Metrics Aggregator (v2)

Consumes Redis Stream entries produced by the tick-quality gate wrapper (Step 19)
and exposes live Prometheus metrics at:
  /metrics
  /health

Key features:
  - consumer group based (exactly-once per group with ACK)
  - self-diagnostics: group pending (PEL), consumer idle time, stream lag
  - cardinality guard for fail reasons

Env:
  REDIS_URL
  TICK_GATE_REDIS_STREAM (default: ops:tick_quality_gate)
  TICK_GATE_CONSUMER_GROUP (default: tick_gate_agg)
  TICK_GATE_CONSUMER_NAME (default: agg_1)
  TICK_GATE_BLOCK_MS (default: 2000)
  TICK_GATE_COUNT (default: 200)
  TICK_GATE_SELF_DIAG_INTERVAL_S (default: 15)

  TICK_GATE_METRICS_ADDR (default: 0.0.0.0)
  TICK_GATE_METRICS_PORT (default: 9112)

  TICK_GATE_REASON_LABEL_MODE: collapse|skip|allow (default: collapse)
  TICK_GATE_REASON_ALLOWLIST: comma-separated (default: see compose)
"""


import os
import time
from typing import Any
from wsgiref.simple_server import WSGIRequestHandler, make_server

from prometheus_client import Counter, Gauge
from prometheus_client.exposition import make_wsgi_app

try:
    import redis  # type: ignore
except Exception:
    redis = None


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


TICK_GATE_REDIS_STREAM = os.getenv("TICK_GATE_REDIS_STREAM", "ops:tick_quality_gate")
TICK_GATE_CONSUMER_GROUP = os.getenv("TICK_GATE_CONSUMER_GROUP", "tick_gate_agg")
TICK_GATE_CONSUMER_NAME = os.getenv("TICK_GATE_CONSUMER_NAME", "agg_1")
TICK_GATE_BLOCK_MS = _env_int("TICK_GATE_BLOCK_MS", 2000)
TICK_GATE_COUNT = _env_int("TICK_GATE_COUNT", 200)
TICK_GATE_SELF_DIAG_INTERVAL_S = _env_int("TICK_GATE_SELF_DIAG_INTERVAL_S", 15)

TICK_GATE_METRICS_ADDR = os.getenv("TICK_GATE_METRICS_ADDR", "0.0.0.0")
TICK_GATE_METRICS_PORT = _env_int("TICK_GATE_METRICS_PORT", 9112)

REASON_LABEL_MODE = (os.getenv("TICK_GATE_REASON_LABEL_MODE", "collapse") or "collapse").strip().lower()
REASON_ALLOWLIST = set(
    [x.strip() for x in (os.getenv("TICK_GATE_REASON_ALLOWLIST", "") or "").split(",") if x.strip()]
)


def _get_redis() -> redis.Redis:
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(url, decode_responses=True)


from prometheus_client import REGISTRY
import contextlib


def _get_or_create_metric(collector_type, name, documentation, labelnames=()):
    # Check for name or name_total (Prometheus appends _total for Counters)
    for n in [name, name + "_total"]:
        if n in REGISTRY._names_to_collectors:
            return REGISTRY._names_to_collectors[n]
    return collector_type(name, documentation, labelnames=labelnames)

# Metrics (public)
tick_gate_events_total = _get_or_create_metric(
    Counter,
    "tick_gate_events_total",
    "Total tick-gate events by status",
    ["status"],
)
tick_gate_fail_reasons_total = _get_or_create_metric(
    Counter,
    "tick_gate_fail_reasons_total",
    "Total tick-gate failures by reason (guarded)",
    ["reason"],
)
tick_gate_last_run_ts_seconds = _get_or_create_metric(
    Gauge,
    "tick_gate_last_run_ts_seconds",
    "Unix timestamp of last tick-gate event processed",
)
tick_gate_stream_lag_ms = _get_or_create_metric(
    Gauge,
    "tick_gate_stream_lag_ms",
    "Approx stream lag in ms for the gate stream consumer",
)
tick_gate_group_pending = _get_or_create_metric(
    Gauge,
    "tick_gate_group_pending",
    "Pending entries count for the consumer group (PEL size)",
)
tick_gate_consumer_idle_ms = _get_or_create_metric(
    Gauge,
    "tick_gate_consumer_idle_ms",
    "Idle time (ms) of this consumer in the group",
)
tick_gate_redis_errors_total = _get_or_create_metric(
    Counter,
    "tick_gate_redis_errors_total",
    "Redis errors in tick gate aggregator",
    ["op"],
)
tick_gate_health_ok = _get_or_create_metric(
    Gauge,
    "tick_gate_health_ok",
    "1 if aggregator can read Redis and process stream recently, else 0",
)
tick_gate_last_diag_ts_seconds = _get_or_create_metric(
    Gauge,
    "tick_gate_last_diag_ts_seconds",
    "Unix timestamp of last successful self-diagnostics",
)


def _extract_reason(fields: dict[str, Any]) -> str | None:
    raw = (str(fields.get("fail_reason") or fields.get("reason") or "") or "").strip()
    if not raw:
        return None
    # Collapse high-cardinality strings: keep prefix before ':' or the whole token.
    if ":" in raw:
        raw = raw.split(":", 1)[0]
    raw = raw.strip()[:64]
    return raw or None


def _guard_reason(reason: str | None) -> str | None:
    if not reason:
        return None
    r = reason.strip()
    if not r:
        return None
    if REASON_LABEL_MODE == "allow":
        return r
    if REASON_LABEL_MODE == "skip":
        return r if (r in REASON_ALLOWLIST) else None
    # collapse (default)
    if REASON_ALLOWLIST and r in REASON_ALLOWLIST:
        return r
    return "__other__"


def _ensure_group(r: redis.Redis) -> None:
    with contextlib.suppress(Exception):
        r.xgroup_create(TICK_GATE_REDIS_STREAM, TICK_GATE_CONSUMER_GROUP, id="0-0", mkstream=True)


def _self_diag(r: redis.Redis) -> None:
    # Export group pending + consumer idle (best-effort)
    ok = False
    try:
        pending = r.xpending(TICK_GATE_REDIS_STREAM, TICK_GATE_CONSUMER_GROUP)
        if isinstance(pending, dict):
            tick_gate_group_pending.set(_safe_int(pending.get("pending"), 0))
            ok = True
    except Exception:
        tick_gate_redis_errors_total.labels(op="xpending").inc()

    try:
        consumers = r.xinfo_consumers(TICK_GATE_REDIS_STREAM, TICK_GATE_CONSUMER_GROUP)
        if isinstance(consumers, list):
            for c in consumers:
                if (c.get("name")) == TICK_GATE_CONSUMER_NAME:
                    tick_gate_consumer_idle_ms.set(_safe_int(c.get("idle"), 0))
                    ok = True
                    break
    except Exception:
        tick_gate_redis_errors_total.labels(op="xinfo_consumers").inc()

    if ok:
        tick_gate_last_diag_ts_seconds.set(time.time())


def _msg_id_to_ms(msg_id: str) -> int | None:
    try:
        return int(str(msg_id).split("-", 1)[0])
    except Exception:
        return None


def _consume_loop() -> None:
    if redis is None:
        raise RuntimeError("redis library is not available")

    r = _get_redis()
    _ensure_group(r)
    tick_gate_health_ok.set(0)
    last_diag = 0.0

    while True:
        now = time.time()
        try:
            resp = r.xreadgroup(
                TICK_GATE_CONSUMER_GROUP,
                TICK_GATE_CONSUMER_NAME,
                {TICK_GATE_REDIS_STREAM: ">"},
                count=TICK_GATE_COUNT,
                block=TICK_GATE_BLOCK_MS,
            )

            if not resp:
                # keep self-diagnostics going even when idle
                if now - last_diag >= TICK_GATE_SELF_DIAG_INTERVAL_S:
                    _self_diag(r)
                    last_diag = now
                continue

            tick_gate_health_ok.set(1)

            for _stream_name, entries in resp:
                for msg_id, fields in entries:
                    status = ((fields.get("status") or "") or "unknown").strip().lower()
                    tick_gate_events_total.labels(status=status).inc()
                    tick_gate_last_run_ts_seconds.set(now)

                    if status == "fail":
                        rr = _extract_reason(fields)
                        rr = _guard_reason(rr)
                        if rr:
                            tick_gate_fail_reasons_total.labels(reason=rr).inc()

                    msg_ms = _msg_id_to_ms(str(msg_id))
                    if msg_ms is not None:
                        tick_gate_stream_lag_ms.set(max(0, int(now * 1000) - msg_ms))

                    # ACK
                    r.xack(TICK_GATE_REDIS_STREAM, TICK_GATE_CONSUMER_GROUP, msg_id)

            if now - last_diag >= TICK_GATE_SELF_DIAG_INTERVAL_S:
                _self_diag(r)
                last_diag = now

        except Exception:
            tick_gate_redis_errors_total.labels(op="xreadgroup").inc()
            tick_gate_health_ok.set(0)
            time.sleep(1.0)


def _wsgi_app():
    metrics_app = make_wsgi_app()

    def app(environ, start_response):
        path = environ.get("PATH_INFO", "") or ""
        if path == "/health":
            # health: ok if we recently had successful redis reads
            ok = 1.0
            try:
                ok = float(tick_gate_health_ok._value.get())  # type: ignore[attr-defined]
            except Exception:
                ok = 0.0
            status = "200 OK" if ok >= 1.0 else "503 Service Unavailable"
            body = ("ok\n" if ok >= 1.0 else "unhealthy\n").encode("utf-8")
            start_response(status, [("Content-Type", "text/plain"), ("Content-Length", str(len(body)))])
            return [body]

        if path == "/":
            body = b"/metrics /health\n"
            start_response("200 OK", [("Content-Type", "text/plain"), ("Content-Length", str(len(body)))])
            return [body]

        # default: prometheus /metrics
        return metrics_app(environ, start_response)

    return app

class SilentRequestHandler(WSGIRequestHandler):
    """Suppresses access log messages (e.g. GET /metrics) to keep logs clean."""
    def log_message(self, format: str, *args: Any) -> None:
        return


def main() -> None:
    # Start WSGI server (serves /metrics and /health) in the main thread
    httpd = make_server(TICK_GATE_METRICS_ADDR, TICK_GATE_METRICS_PORT, _wsgi_app(), handler_class=SilentRequestHandler)

    # Run consumer loop in a background green thread substitute: just fork into a daemon thread.
    import threading

    t = threading.Thread(target=_consume_loop, name="tick-gate-consumer", daemon=True)
    t.start()

    httpd.serve_forever()


if __name__ == "__main__":
    main()
