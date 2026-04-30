#!/usr/bin/env python3
"""
Prometheus exporter for decision coverage KPIs (P66).

Reads Redis hash `metrics:decision_coverage:state` (written by decision_coverage_kpi_worker_v1)
and exposes:
  - decision_last_ts_ms           — ms timestamp of last observed decision
  - decision_n_24h                — total decisions in rolling 24h window
  - decision_regime_n_24h{regime} — counts per regime (ok|warn|block|unknown)
  - decision_regime_share_24h{regime} — regime share [0..1]
  - decision_last_age_seconds     — seconds since last decision (freshness probe)
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict

from prometheus_client import Gauge, start_http_server


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _now_s() -> float:
    return time.time()


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


@dataclass
class Cfg:
    redis_url: str
    state_key: str
    port: int
    interval_s: float


def load_cfg() -> Cfg:
    return Cfg(
        redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0")
        # State key must match DECISION_COVERAGE_STATE_KEY used by the worker
        state_key=_env("DECISION_COVERAGE_STATE_KEY", "metrics:decision_coverage:state")
        port=_i(_env("DECISION_COVERAGE_EXPORTER_PORT", "9816"), 9816)
        interval_s=float(_env("DECISION_COVERAGE_EXPORTER_INTERVAL_S", "5") or 5)
    )


# Prometheus metrics — low cardinality, no symbol labels
LAST_TS = Gauge("decision_last_ts_ms", "Last decision timestamp (ms)")
LAST_AGE = Gauge("decision_last_age_seconds", "Age of last decision in seconds (freshness probe)")

N24 = Gauge("decision_n_24h", "Number of decisions in rolling 24h window")
N24_REG = Gauge("decision_regime_n_24h", "Decisions per regime in rolling 24h window", ["regime"])
SHARE24 = Gauge("decision_regime_share_24h", "Regime share of rolling 24h decisions [0..1]", ["regime"])


def _read_state(r, key: str) -> Dict[str, str]:
    """Read state hash from Redis; return empty dict on any error."""
    try:
        return r.hgetall(key) or {}
    except Exception:
        return {}


def _set_metrics(d: Dict[str, str]) -> None:
    """Translate Redis state hash into Prometheus gauge values."""
    last_ts = _i(d.get("last_ts_ms"), 0)
    LAST_TS.set(float(last_ts))

    # Compute age in seconds; treat 0 (never seen) as 0 age to avoid spurious alerts on startup
    age = 0.0
    if last_ts > 0:
        age = max(0.0, _now_s() - (float(last_ts) / 1000.0))
    LAST_AGE.set(age)

    ok = _i(d.get("rolling_ok"), 0)
    warn = _i(d.get("rolling_warn"), 0)
    block = _i(d.get("rolling_block"), 0)
    unk = _i(d.get("rolling_unknown"), 0)
    total = _i(d.get("rolling_total"), ok + warn + block + unk)
    if total < 0:
        total = 0

    N24.set(float(total))
    for reg, n in (("ok", ok), ("warn", warn), ("block", block), ("unknown", unk)):
        N24_REG.labels(regime=reg).set(float(max(0, n)))
        share = (float(n) / float(total)) if total > 0 else 0.0
        SHARE24.labels(regime=reg).set(share)


def main() -> int:
    cfg = load_cfg()
    import redis  # type: ignore

    r = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    start_http_server(cfg.port)

    while True:
        d = _read_state(r, cfg.state_key)
        _set_metrics(d)
        time.sleep(cfg.interval_s)


if __name__ == "__main__":
    raise SystemExit(main())
