#!/usr/bin/env python3
"""meta_enforce_guard_exporter_v1.py

P31: Prometheus exporter for meta ENFORCE guardrails state.

This exporter does NOT scan streams; it exposes the latest decision written by
meta_enforce_guardrails_v1.py from cfg2 keys.

Run:
  python3 -m tools.meta_enforce_guard_exporter_v1

ENV:
  REDIS_URL (default redis://redis-worker-1:6379/0)
  DYN_CFG_KEY (default settings:dynamic_cfg)
  META_GUARD_EXPORTER_PORT (default 9133)
  META_GUARD_EXPORTER_REFRESH_SEC (default 5)
"""

from __future__ import annotations

import json
import os
import signal
import time
from typing import Any, Dict

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore

from prometheus_client import Gauge, start_http_server


def _loads_maybe_json(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", "replace")
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                return v
        return v
    return v


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


class Exporter:
    def __init__(self, r: Any, cfg2_key: str):
        self.r = r
        self.cfg2_key = cfg2_key
        self.running = True
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, signum, frame):
        self.running = False

    def _load_cfg2(self) -> Dict[str, Any]:
        d = self.r.hgetall(self.cfg2_key) or {}
        out: Dict[str, Any] = {}
        for k, v in d.items():
            out[str(k)] = _loads_maybe_json(v)
        return out

    def step(self) -> None:
        cfg2 = self._load_cfg2()
        meta_mode = str(cfg2.get("meta_model_mode", "SHADOW") or "SHADOW").upper()
        meta_enable = _i(cfg2.get("meta_model_enable", 0), 0)
        meta_freeze = _i(cfg2.get("meta_model_freeze", 0), 0)

        GAUGE_META_ENABLE.set(float(meta_enable))
        GAUGE_META_MODE_ENFORCE.set(1.0 if meta_mode == "ENFORCE" else 0.0)
        GAUGE_META_FREEZE.set(float(meta_freeze))

        dec = cfg2.get("meta_enforce_guard_last_decision", None)
        if not isinstance(dec, dict):
            try:
                dec = _loads_maybe_json(dec)
            except Exception:
                dec = None

        if isinstance(dec, dict):
            GAUGE_GUARD_TRIGGER.set(1.0 if bool(dec.get("trigger", False)) else 0.0)
            GAUGE_GUARD_BLOCK_RATE.set(_f(dec.get("block_rate", 0.0), 0.0))
            GAUGE_GUARD_COV_BAD_RATE.set(_f(dec.get("cov_bad_rate", 0.0), 0.0))
            GAUGE_GUARD_N.set(float(_i(dec.get("n", 0), 0)))
            GAUGE_GUARD_CANARY_N.set(float(_i(dec.get("canary_n", 0), 0)))
            GAUGE_GUARD_BLOCKED_N.set(float(_i(dec.get("blocked_n", 0), 0)))
            GAUGE_GUARD_TS_MS.set(float(_i(dec.get("ts_ms", 0), 0)))


GAUGE_META_ENABLE = Gauge("meta_model_enable", "cfg2 meta_model_enable")
GAUGE_META_MODE_ENFORCE = Gauge("meta_model_mode_enforce", "cfg2 meta_model_mode == ENFORCE")
GAUGE_META_FREEZE = Gauge("meta_model_freeze", "cfg2 meta_model_freeze (latch)")

GAUGE_GUARD_TRIGGER = Gauge("meta_enforce_guard_trigger", "last guard decision trigger flag")
GAUGE_GUARD_BLOCK_RATE = Gauge("meta_enforce_guard_block_rate", "blocked/canary in last decision")
GAUGE_GUARD_COV_BAD_RATE = Gauge("meta_enforce_guard_cov_bad_rate", "coverage-bad/canary in last decision")
GAUGE_GUARD_N = Gauge("meta_enforce_guard_n", "events considered in last decision")
GAUGE_GUARD_CANARY_N = Gauge("meta_enforce_guard_canary_n", "canary applied count in last decision")
GAUGE_GUARD_BLOCKED_N = Gauge("meta_enforce_guard_blocked_n", "blocked-by-meta count in last decision")
GAUGE_GUARD_TS_MS = Gauge("meta_enforce_guard_ts_ms", "timestamp of last decision")


def main() -> int:
    if redis is None:
        print(json.dumps({"ok": False, "reason": "redis_python_not_installed"}, ensure_ascii=False))
        return 2

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    cfg2_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")
    port = int(os.getenv("META_GUARD_EXPORTER_PORT", "9133") or 9133)
    refresh_sec = float(os.getenv("META_GUARD_EXPORTER_REFRESH_SEC", "5") or 5)

    _redis_delay = 1.0
    for _attempt in range(3):
        try:
            r = redis.Redis.from_url(redis_url, decode_responses=False)
            r.ping()
            break
        except Exception as _e:
            if _attempt == 2:
                raise
            print(f"⚠️ Redis not ready (attempt {_attempt + 1}/3): {_e}. Retry in {_redis_delay:.0f}s...")
            time.sleep(_redis_delay)
            _redis_delay = min(_redis_delay * 2, 10.0)
    exp = Exporter(r, cfg2_key)

    start_http_server(port)
    print(json.dumps({"ok": True, "port": port}, ensure_ascii=False))

    while exp.running:
        try:
            exp.step()
        except Exception:
            pass
        time.sleep(max(0.5, refresh_sec))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
