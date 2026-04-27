"""Auto-apply blocker driven by tick-quality gate.

Goal
-----
When tick-quality gate FAILs, we *publish* a blocking flag into Redis config keys
so any auto-apply / ApplyRunner can stop applying AB winners (or other proposals).

This tool can run as a long-lived daemon:
  - periodically runs: `python -m tools.tick_quality_gate_check --json`
  - writes Redis keys under `cfg:suggestions:entry_policy:auto_apply_block:*`
  - publishes a compact audit event into `ops:auto_apply_tick_gate`
  - exposes Prometheus `/metrics` for alerting (port 9114 by default)

Design notes
------------
* Deterministic, observable: every decision is written to Redis + ops stream.
* Anti-flap: uses "hold" seconds so brief FAILs don't immediately unblock.
* Safe defaults: fail_open on INSUFFICIENT_DATA (configurable).

Env
---
REDIS_URL=redis://redis-worker-1:6379/0
TICK_GATE_METRICS_URL=http://localhost:8000/metrics
TICK_GATE_WINDOW_S=60

AUTO_APPLY_BLOCKER_INTERVAL_S=15
AUTO_APPLY_BLOCKER_HOLD_S=120
AUTO_APPLY_BLOCKER_FAIL_MODE=fail_open   # fail_open | fail_closed

AUTO_APPLY_BLOCK_PREFIX=cfg:suggestions:entry_policy:auto_apply_block
AUTO_APPLY_BLOCK_TTL_S=600
AUTO_APPLY_BLOCK_OPS_STREAM=ops:auto_apply_tick_gate
AUTO_APPLY_BLOCK_METRICS_PORT=9114

Reason label control (cardinality):
AUTO_APPLY_BLOCK_REASON_LABEL_MODE=collapse  # collapse | allow
AUTO_APPLY_BLOCK_REASON_ALLOWLIST=unknown_side,process_p99,e2e_p99,skew,age,ts_now,ts_stream
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    from prometheus_client import Counter, Gauge, start_http_server  # type: ignore
except Exception:  # pragma: no cover
    Counter = None  # type: ignore
    Gauge = None  # type: ignore
    start_http_server = None  # type: ignore


def _now_ms() -> int:
    return get_ny_time_millis()


def _getenv_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _getenv_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _getenv_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def _split_csv(v: str) -> List[str]:
    out: List[str] = []
    for part in (v or "").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


@dataclass
class GateResult:
    rc: int  # 0 pass, 2 fail, 1 insufficient, other error
    status: str  # pass|fail|insufficient|error
    reasons: List[str]
    payload: Dict[str, Any]


def _normalize_fail_mode(v: str) -> str:
    x = (v or "fail_open").strip().lower()
    if x not in ("fail_open", "fail_closed"):
        return "fail_open"
    return x


def _run_tick_gate(metrics_url: str, window_s: int, symbol: str = "") -> GateResult:
    """Runs gate tool as subprocess and parses JSON output."""
    cmd = [sys.executable, "-m", "tools.tick_quality_gate_check",
           "--metrics-url", metrics_url,
           "--window-s", str(window_s),
           "--json"]
    if symbol:
        cmd += ["--symbol", symbol]
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        rc = int(p.returncode)
        out = (p.stdout or "").strip()
        payload: Dict[str, Any] = {}
        if out:
            try:
                payload = json.loads(out)
            except Exception:
                payload = {"raw_stdout": out}
        else:
            payload = {"raw_stdout": "", "raw_stderr": (p.stderr or "")[:2000]}

        if rc == 0:
            status = "pass"
        elif rc == 2:
            status = "fail"
        elif rc == 1:
            status = "insufficient"
        else:
            status = "error"

        reasons = []
        try:
            rs = payload.get("fail_reasons") or payload.get("reasons") or []
            if isinstance(rs, list):
                reasons = [str(x) for x in rs if str(x)]
            elif isinstance(rs, str) and rs:
                reasons = [rs]
        except Exception:
            reasons = []
        return GateResult(rc=rc, status=status, reasons=reasons, payload=payload)
    except Exception as e:
        return GateResult(rc=22, status="error", reasons=["exception"], payload={"exc": str(e)})


def _label_limiter(reason: str, mode: str, allow: List[str]) -> str:
    r = (reason or "").strip()
    if not r:
        return "unknown"
    if mode == "allow":
        return r
    # collapse
    for a in allow:
        if a and a in r:
            return a
    return "__other__"


class AutoApplyBlocker:
    def __init__(self) -> None:
        self.redis_url = _getenv_str("REDIS_URL", "redis://localhost:6379/0")
        self.metrics_url = _getenv_str("TICK_GATE_METRICS_URL", "http://localhost:8000/metrics")
        self.window_s = _getenv_int("TICK_GATE_WINDOW_S", 60)
        self.symbol = _getenv_str("TICK_GATE_SYMBOL", "").strip().upper()

        self.interval_s = _getenv_int("AUTO_APPLY_BLOCKER_INTERVAL_S", 15)
        self.hold_s = _getenv_int("AUTO_APPLY_BLOCKER_HOLD_S", 120)
        self.fail_mode = _normalize_fail_mode(_getenv_str("AUTO_APPLY_BLOCKER_FAIL_MODE", "fail_open"))

        self.prefix = _getenv_str("AUTO_APPLY_BLOCK_PREFIX", "cfg:suggestions:entry_policy:auto_apply_block").strip()
        self.ttl_s = _getenv_int("AUTO_APPLY_BLOCK_TTL_S", 600)
        self.ops_stream = _getenv_str("AUTO_APPLY_BLOCK_OPS_STREAM", "ops:auto_apply_tick_gate")
        self.metrics_port = _getenv_int("AUTO_APPLY_BLOCK_METRICS_PORT", 9114)

        self.reason_label_mode = _getenv_str("AUTO_APPLY_BLOCK_REASON_LABEL_MODE", "collapse").strip().lower()
        if self.reason_label_mode not in ("collapse", "allow"):
            self.reason_label_mode = "collapse"
        self.reason_allow = _split_csv(_getenv_str(
            "AUTO_APPLY_BLOCK_REASON_ALLOWLIST",
            "unknown_side,process_p99,e2e_p99,skew,age,ts_now,ts_stream",
        ))

        self._last_fail_ms: Optional[int] = None
        self._last_status: str = "unknown"
        self._redis = None
        self._init_metrics()

    def _init_metrics(self) -> None:
        if Gauge is None or Counter is None:
            self.m_blocked = None
            self.m_last_rc = None
            self.m_last_run_ts = None
            self.m_reason_total = None
            self.m_events_total = None
            return
        self.m_blocked = Gauge("auto_apply_tick_gate_blocked", "1 if auto-apply blocked by tick gate, else 0")
        self.m_last_rc = Gauge("auto_apply_tick_gate_last_rc", "Last tick gate return code")
        self.m_last_run_ts = Gauge("auto_apply_tick_gate_last_run_ts_seconds", "Last tick gate evaluation time")
        self.m_events_total = Counter("auto_apply_tick_gate_events_total", "Gate evaluations by status", ["status"])
        self.m_reason_total = Counter("auto_apply_tick_gate_fail_reasons_total", "Fail reasons (limited labels)", ["reason"])

    def _redis_client(self):
        if self._redis is not None:
            return self._redis
        if redis is None:
            raise RuntimeError("redis-py not available")
        self._redis = redis.Redis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    def _key(self, suffix: str) -> str:
        return f"{self.prefix}:{suffix}"

    def _set_block(self, gate: GateResult, decided_block: bool, meta: Dict[str, Any]) -> None:
        r = self._redis_client()
        block_key = self._key("tick_gate")
        meta_key = self._key("tick_gate:meta")
        ts_key = self._key("tick_gate:ts_ms")

        pipe = r.pipeline()
        if decided_block:
            pipe.set(block_key, "1", ex=int(self.ttl_s))
            pipe.set(ts_key, str(_now_ms()), ex=int(self.ttl_s))
            pipe.set(meta_key, json.dumps(meta, ensure_ascii=False, sort_keys=True), ex=int(self.ttl_s))
        else:
            pipe.delete(block_key)
            pipe.delete(ts_key)
            pipe.delete(meta_key)
        pipe.execute()

    def _publish_ops(self, gate: GateResult, decided_block: bool, meta: Dict[str, Any]) -> None:
        try:
            r = self._redis_client()
            payload = {
                "ts_ms": str(_now_ms()),
                "status": gate.status,
                "rc": str(gate.rc),
                "decided_block": "1" if decided_block else "0",
                "symbol": self.symbol or "",
                "reasons": ",".join(gate.reasons[:16]),
            }
            # compact meta to keep stream light
            try:
                payload["meta"] = json.dumps(
                    {k: meta.get(k) for k in ("hold_active", "fail_mode", "ttl_s")},
                    ensure_ascii=False,
                    sort_keys=True,
                )
            except Exception:
                pass
            r.xadd(self.ops_stream, payload, maxlen=20000, approximate=True)
        except Exception:
            return

    def _decide_block(self, gate: GateResult) -> Tuple[bool, Dict[str, Any]]:
        now = _now_ms()
        hold_active = False

        # Determine "block" intent from gate status
        if gate.status == "fail":
            self._last_fail_ms = now
            block_intent = True
        elif gate.status == "pass":
            block_intent = False
        elif gate.status == "insufficient":
            block_intent = (self.fail_mode == "fail_closed")
        else:
            # error
            block_intent = True if (self.fail_mode == "fail_closed") else False

        # Hold: once fail observed, keep blocked for hold_s even if pass afterwards (anti-flap)
        if self._last_fail_ms is not None and (now - self._last_fail_ms) < int(self.hold_s * 1000):
            if gate.status != "fail":
                hold_active = True
            block_final = True
        else:
            block_final = block_intent

        meta = {
            "ts_ms": now,
            "gate_status": gate.status,
            "gate_rc": gate.rc,
            "reasons": gate.reasons[:32],
            "fail_mode": self.fail_mode,
            "hold_s": self.hold_s,
            "hold_active": hold_active,
            "ttl_s": self.ttl_s,
            "metrics_url": self.metrics_url,
            "window_s": self.window_s,
            "symbol": self.symbol or "",
        }
        # Attach small gate payload for diagnostics (bounded)
        try:
            meta["gate_payload"] = gate.payload if len(json.dumps(gate.payload)) < 4000 else {"truncated": True}
        except Exception:
            pass

        return block_final, meta

    def _update_prom(self, gate: GateResult, decided_block: bool) -> None:
        if self.m_events_total is not None:
            try:
                self.m_events_total.labels(status=gate.status).inc()
            except Exception:
                pass
        if self.m_last_rc is not None:
            try:
                self.m_last_rc.set(float(gate.rc))
            except Exception:
                pass
        if self.m_last_run_ts is not None:
            try:
                self.m_last_run_ts.set(float(time.time()))
            except Exception:
                pass
        if self.m_blocked is not None:
            try:
                self.m_blocked.set(1.0 if decided_block else 0.0)
            except Exception:
                pass
        if gate.status == "fail" and self.m_reason_total is not None:
            for rr in gate.reasons[:16]:
                lab = _label_limiter(rr, self.reason_label_mode, self.reason_allow)
                try:
                    self.m_reason_total.labels(reason=lab).inc()
                except Exception:
                    pass

    def run_once(self) -> Tuple[GateResult, bool]:
        gate = _run_tick_gate(self.metrics_url, self.window_s, self.symbol)
        decided_block, meta = self._decide_block(gate)
        try:
            self._set_block(gate, decided_block, meta)
        except Exception:
            # If Redis is down, we still expose metrics + keep looping
            pass
        self._publish_ops(gate, decided_block, meta)
        self._update_prom(gate, decided_block)
        self._last_status = gate.status
        return gate, decided_block

    def serve(self) -> None:
        if start_http_server is not None:
            try:
                start_http_server(self.metrics_port)
            except Exception:
                # If port busy, still continue without metrics
                pass
        while True:
            t0 = time.time()
            try:
                self.run_once()
            except KeyboardInterrupt:
                raise
            except Exception:
                # never crash: this is a guard daemon
                pass
            dt = time.time() - t0
            sleep_s = max(0.1, float(self.interval_s) - dt)
            time.sleep(sleep_s)


def main(argv: Optional[List[str]] = None) -> int:
    argv = argv or sys.argv[1:]
    if "--once" in argv:
        b = AutoApplyBlocker()
        gate, decided = b.run_once()
        # machine-friendly single line
        out = {"status": gate.status, "rc": gate.rc, "blocked": decided, "reasons": gate.reasons[:16]}
        print(json.dumps(out, ensure_ascii=False, sort_keys=True))
        return 0 if gate.status == "pass" else 2
    AutoApplyBlocker().serve()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
