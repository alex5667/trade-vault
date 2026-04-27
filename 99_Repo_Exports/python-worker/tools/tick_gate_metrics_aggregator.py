#!/usr/bin/env python3
"""
Tick Gate Metrics Aggregator (Step 22)

Reads gate outcomes from Redis Stream (ops:tick_quality_gate) via consumer group
and exports Prometheus /metrics on port 9112 (default).

Metrics:
  - tick_gate_events_total{status}
  - tick_gate_fail_reasons_total{reason}
  - tick_gate_last_run_ts_seconds
  - tick_gate_last_status{status}
  - tick_gate_stream_lag_ms

Alert rules: python-worker/infra/observability/tick_gate_aggregator_alerts.yml
Env template: python-worker/infra/ops/tick_gate_aggregator.env.example
systemd unit: python-worker/infra/systemd/tick-gate-aggregator.service
Runbook: python-worker/infra/ops/STEP22.md

Usage (manual):
  export REDIS_URL=redis://redis-worker-1:6379/0
  python3 -m tools.tick_gate_metrics_aggregator --metrics-port 9112

Usage (systemd):
  See python-worker/infra/ops/STEP22.md
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

# Prometheus exporter
try:
    from prometheus_client import Counter, Gauge, MetricsHandler, start_http_server  # type: ignore
    # Silence access logs (e.g. GET /metrics) to reduce log noise
    MetricsHandler.log_message = lambda *args, **kwargs: None
except ImportError:
    sys.stderr.write("ERROR: prometheus_client is required. Install via: pip install prometheus-client\n")
    raise


###############################################################################
# Label Limiting (prevent cardinality explosion)
###############################################################################
class LabelLimiter:
    """Limits label cardinality. mode='skip' → skip non-allowlist, mode='collapse' → map to __other__."""

    def __init__(self, mode: str, allowlist: Tuple[str, ...]):
        self.mode = mode.lower()
        self.allowlist = set(allowlist)

    def label(self, raw_value: str) -> Optional[str]:
        # Extract first token from multi-reason strings (e.g. "skew|p99" → "skew")
        token = _first_token(raw_value)
        if token in self.allowlist:
            return token
        if self.mode == "skip":
            return None
        return "__other__"


def _first_token(s: str) -> str:
    """Return first token before |, or the whole string."""
    if not s:
        return ""
    parts = s.split("|", 1)
    if not parts:
        return ""
    return parts[0].split(",", 1)[0].strip()


###############################################################################
# Prometheus metrics
###############################################################################
from prometheus_client import REGISTRY

def _get_or_create_metric(collector_type, name, documentation, labelnames):
    # Use internal registry mapping for fast lookup
    for n in [name, name + "_total"]:
        if n in REGISTRY._names_to_collectors:
            return REGISTRY._names_to_collectors[n]
    return collector_type(name, documentation, labelnames=labelnames)

_events_counter = _get_or_create_metric(Counter, "tick_gate_events_total", "Total tick gate events by status", ["status"])
_fail_reasons_counter = _get_or_create_metric(Counter, "tick_gate_fail_reasons_total", "Fail reasons (label-limited)", ["reason"])
_last_run_ts = _get_or_create_metric(Gauge, "tick_gate_last_run_ts_seconds", "Unix timestamp (epoch s) of last gate run", [])
_last_status = _get_or_create_metric(Gauge, "tick_gate_last_status", "One-hot encoding of last status", ["status"])
_stream_lag_ms = _get_or_create_metric(Gauge, "tick_gate_stream_lag_ms", "Consumer lag: stream head - latest consumed (ms)", [])


###############################################################################
# Helpers
###############################################################################
def _norm_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            return default
    try:
        s = str(v).strip()
    except Exception:
        return default
    return s if s else default


def _safe_int(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            return default
    try:
        return int(float(str(v).strip()))
    except Exception:
        return default


def _loads_maybe_json(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list, int, float, bool)):
        return v
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            return None
    if not isinstance(v, str):
        try:
            v = str(v)
        except Exception:
            return None
    s = v.strip()
    if not s:
        return None
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            return json.loads(s)
        except Exception:
            return v
    return v


def _parse_msg_id_ms(msg_id: str) -> int:
    # Redis stream id: "<ms>-<seq>"
    try:
        return int(msg_id.split("-", 1)[0])
    except Exception:
        return 0


def _norm_status(s: str) -> str:
    """Normalize status variants to: pass|fail|insufficient|error"""
    s = s.upper()
    if s in ("OK", "PASSING", "SUCCESS", "PASS"):
        return "pass"
    if s in ("FAILED", "FAILURE", "FAIL", "HALT_RAMP", "HALTRAMP"):
        return "fail"
    if s in ("INSUFFICIENT", "NO_DATA", "INSUFFICIENTDATA", "INSUFFICIENT_DATA"):
        return "insufficient"
    if s in ("ERROR", "ERR"):
        return "error"
    return "error"  # default unknown → error


def _extract_event(merged: Dict[str, Any]) -> Tuple[str, str, str, int]:
    """Extract (status, reason, symbol, ts_ms) from gate event payload."""
    status = _norm_str(merged.get("status") or merged.get("state"), "UNKNOWN")
    status = _norm_status(status)

    reason_raw = merged.get("reason") or merged.get("failure") or merged.get("metric") or ""
    reason = _norm_str(reason_raw)

    symbol = _norm_str(merged.get("symbol") or merged.get("sym"), "")
    ts_ms = _safe_int(merged.get("ts_ms") or merged.get("time_ms") or merged.get("timestamp_ms"), 0)
    return (status, reason, symbol, ts_ms)


###############################################################################
# Aggregator
###############################################################################
class TickGateAggregator:
    """Consumes ops:tick_quality_gate via XREADGROUP and updates metrics."""

    def __init__(
        self,
        *,
        redis_url: str,
        stream: str,
        group: str,
        consumer: str,
        start_id: str = "$",
        reason_label_mode: str = "collapse",
        reason_allowlist: Tuple[str, ...] = ("skew", "unknown_side", "process_p99", "e2e_p99", "ts_now", "ts_stream"),
        symbol_label_mode: str = "collapse",
        symbol_allowlist: Tuple[str, ...] = (),
    ):
        try:
            import redis  # type: ignore
        except ImportError:
            raise RuntimeError("redis-py is required. Install via: pip install redis")

        self.redis_url = redis_url
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.start_id = start_id

        self.r = redis.Redis.from_url(redis_url, decode_responses=False)

        # Create consumer group (idempotent: OK if exists)
        try:
            self.r.xgroup_create(self.stream, self.group, id=self.start_id, mkstream=True)
        except Exception as e:
            err = str(e).lower()
            if "busygroup" not in err and "exists" not in err:
                sys.stderr.write(f"WARN: xgroup_create failed (non-fatal): {e}\n")

        self.reason_limiter = LabelLimiter(mode=reason_label_mode, allowlist=reason_allowlist)
        self.symbol_limiter = LabelLimiter(mode=symbol_label_mode, allowlist=symbol_allowlist)

        # Track last seen
        self.last_status_str: Optional[str] = None

    def run_forever(self, *, block_ms: int = 5000, count: int = 100):
        """Infinite loop: XREADGROUP → update metrics → loop."""
        sys.stderr.write(
            f"[tick-gate-aggregator] consumer_group={self.group} consumer={self.consumer} stream={self.stream}\n"
        )
        while True:
            try:
                self._poll_once(block_ms=block_ms, count=count)
            except KeyboardInterrupt:
                sys.stderr.write("[tick-gate-aggregator] interrupted\n")
                break
            except Exception as e:
                sys.stderr.write(f"[tick-gate-aggregator] ERROR in poll: {e}\n")
                time.sleep(5)

    def _poll_once(self, *, block_ms: int, count: int):
        # XREADGROUP GROUP <group> <consumer> COUNT <count> BLOCK <block_ms> STREAMS <stream> >
        try:
            resp = self.r.xreadgroup(
                groupname=self.group,
                consumername=self.consumer,
                streams={self.stream: ">"},
                count=count,
                block=block_ms,
            )
        except Exception as e:
            sys.stderr.write(f"WARN: xreadgroup failed: {e}\n")
            return

        if not resp:
            # No new messages
            return

        # resp = [ (stream_name, [(msg_id, fields), ...]) ]
        for stream_name, messages in resp:
            for msg_id_b, fields in messages:
                msg_id = msg_id_b.decode("utf-8", errors="replace") if isinstance(msg_id_b, (bytes, bytearray)) else str(msg_id_b)
                self._process_message(msg_id, fields)
                # ACK message
                try:
                    self.r.xack(self.stream, self.group, msg_id)
                except Exception as e:
                    sys.stderr.write(f"WARN: xack failed for {msg_id}: {e}\n")

        # Update stream lag
        self._update_lag()

    def _process_message(self, msg_id: str, fields: Dict[bytes, bytes]):
        # Decode fields
        f2: Dict[str, Any] = {}
        for k, v in (fields or {}).items():
            kk = k.decode("utf-8", errors="replace") if isinstance(k, (bytes, bytearray)) else str(k)
            f2[kk] = v

        # Try to extract JSON payload
        payload = None
        for key in ("json", "payload", "report", "gate", "result"):
            if key in f2:
                payload = _loads_maybe_json(f2.get(key))
                if isinstance(payload, dict):
                    break
        if not isinstance(payload, dict):
            payload = {}

        merged = dict(payload)
        merged.update(f2)

        status, reason, symbol, ts_ms = _extract_event(merged)

        # Update counters
        _events_counter.labels(status=status).inc()

        if status == "fail" and reason:
            r_label = self.reason_limiter.label(reason)
            if r_label:
                _fail_reasons_counter.labels(reason=r_label).inc()

        # Update last run timestamp
        if ts_ms > 0:
            _last_run_ts.set(ts_ms / 1000.0)
        else:
            # Fallback to message ID timestamp
            ts_ms = _parse_msg_id_ms(msg_id)
            if ts_ms > 0:
                _last_run_ts.set(ts_ms / 1000.0)

        # Update one-hot last status
        if status != self.last_status_str:
            # Reset old
            if self.last_status_str:
                _last_status.labels(status=self.last_status_str).set(0)
            # Set new
            _last_status.labels(status=status).set(1)
            self.last_status_str = status

    def _update_lag(self):
        """Compute lag = (stream head) - (latest consumed ts) in ms."""
        try:
            # Get stream info to find latest message timestamp
            info = self.r.xinfo_stream(self.stream)
            last_entry = info.get(b"last-entry") or info.get("last-entry")
            if last_entry:
                # last_entry = [msg_id, fields]
                msg_id_b = last_entry[0]
                msg_id = msg_id_b.decode("utf-8", errors="replace") if isinstance(msg_id_b, (bytes, bytearray)) else str(msg_id_b)
                head_ms = _parse_msg_id_ms(msg_id)
                # Get current last_run_ts (in seconds)
                last_run_s = _last_run_ts._value.get() if hasattr(_last_run_ts, "_value") else 0
                last_run_ms = int(last_run_s * 1000)
                lag_ms = max(0, head_ms - last_run_ms)
                _stream_lag_ms.set(lag_ms)
        except Exception as e:
            sys.stderr.write(f"WARN: failed to compute lag: {e}\n")


###############################################################################
# Main
###############################################################################
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Tick Gate Aggregator (Step 22)")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--stream", default=os.getenv("TICK_GATE_REDIS_STREAM", "ops:tick_quality_gate"))
    ap.add_argument("--group", default=os.getenv("TICK_GATE_AGG_GROUP", "tick_gate_agg"))
    ap.add_argument("--consumer", default=os.getenv("TICK_GATE_AGG_CONSUMER", f"c-{os.getpid()}"))
    ap.add_argument("--start-id", default=os.getenv("TICK_GATE_AGG_START_ID", "$"))
    ap.add_argument("--metrics-port", type=int, default=int(os.getenv("TICK_GATE_METRICS_PORT", "9112")))
    ap.add_argument("--block-ms", type=int, default=5000)
    ap.add_argument("--count", type=int, default=100)

    # Label limiting
    reason_mode = os.getenv("TICK_GATE_REASON_LABEL_MODE", "collapse")
    reason_allow_raw = os.getenv("TICK_GATE_REASON_ALLOWLIST", "skew,unknown_side,process_p99,e2e_p99,ts_now,ts_stream")
    reason_allowlist = tuple(x.strip() for x in reason_allow_raw.split(",") if x.strip())

    symbol_mode = os.getenv("TICK_GATE_SYMBOL_LABEL_MODE", "collapse")
    symbol_allow_raw = os.getenv("TICK_GATE_SYMBOL_ALLOWLIST", "")
    symbol_allowlist = tuple(x.strip() for x in symbol_allow_raw.split(",") if x.strip())

    args = ap.parse_args(list(argv) if argv is not None else None)

    start_http_server(args.metrics_port)
    agg = TickGateAggregator(
        redis_url=args.redis_url,
        stream=args.stream,
        group=args.group,
        consumer=args.consumer,
        start_id=args.start_id,
        reason_label_mode=reason_mode,
        reason_allowlist=reason_allowlist,
        symbol_label_mode=symbol_mode,
        symbol_allowlist=symbol_allowlist,
    )
    agg.run_forever(block_ms=args.block_ms, count=args.count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
