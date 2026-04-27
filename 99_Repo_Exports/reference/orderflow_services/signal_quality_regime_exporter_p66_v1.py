#!/usr/bin/env python3
"""
P66: Signal Quality exporter (by regime)

Reads Redis hash `settings:dynamic_cfg` (or DYN_CFG_KEY) populated by the existing
signal-quality KPI worker and exposes low-cardinality Prometheus metrics:

  signal_quality_expectancy_r_24h_by_regime{regime="ok|warn|block"}
  signal_quality_precision_top5p_24h_by_regime{regime="ok|warn|block"}
  signal_quality_ece_24h_by_regime{regime="ok|warn|block"}
  signal_quality_n_24h_by_regime{regime="ok|warn|block"}
  signal_quality_last_ts_ms
  signal_quality_last_age_seconds

Design goals:
  - no symbol labels (bounded cardinality)
  - deterministic and observable (age/ts)

Keys expected in settings:dynamic_cfg (written by signal_quality_kpi_worker_v1):
  signal_quality_expectancy_r_24h_regime_ok
  signal_quality_expectancy_r_24h_regime_warn
  signal_quality_expectancy_r_24h_regime_block
  signal_quality_precision_top5p_24h_regime_ok  (etc.)
  signal_quality_ece_24h_regime_ok              (etc.)
  signal_quality_n_24h_regime_ok                (etc.)
  signal_quality_last_ts_ms
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict

from prometheus_client import Gauge, start_http_server


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _now_s() -> float:
    return time.time()


def _f(v: Any, d: float = 0.0) -> float:
    """Safe float cast, returns default on any failure."""
    try:
        if v is None:
            return d
        return float(v)
    except Exception:
        try:
            return float(str(v).strip())
        except Exception:
            return d


def _i(v: Any, d: int = 0) -> int:
    """Safe int cast via float, returns default on any failure."""
    try:
        return int(float(v))
    except Exception:
        return d


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class Cfg:
    redis_url: str
    dyn_cfg_key: str
    port: int
    interval_s: float


def load_cfg() -> Cfg:
    return Cfg(
        redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        dyn_cfg_key=_env("DYN_CFG_KEY", "settings:dynamic_cfg"),
        port=_i(_env("SIGNAL_QUALITY_REGIME_EXPORTER_PORT", "9817"), 9817),
        interval_s=float(_env("SIGNAL_QUALITY_REGIME_EXPORTER_INTERVAL_S", "5") or 5),
    )


# ---------------------------------------------------------------------------
# Prometheus gauges
# ---------------------------------------------------------------------------

# Mean R (expectancy) per regime — primary signal quality indicator
G_EXPECT = Gauge(
    "signal_quality_expectancy_r_24h_by_regime",
    "Mean R over last 24h by dq/drift regime",
    ["regime"],
)
# Win rate in top 5% highest-scored signals per regime
G_PREC = Gauge(
    "signal_quality_precision_top5p_24h_by_regime",
    "Win rate in top 5% by score over last 24h by dq/drift regime",
    ["regime"],
)
# Expected Calibration Error per regime (lower is better)
G_ECE = Gauge(
    "signal_quality_ece_24h_by_regime",
    "ECE over last 24h by dq/drift regime",
    ["regime"],
)
# Sample count per regime — used to gate alert significance
G_N = Gauge(
    "signal_quality_n_24h_by_regime",
    "N over last 24h by dq/drift regime",
    ["regime"],
)
# Timestamps for staleness detection
G_LAST_TS = Gauge("signal_quality_last_ts_ms", "Timestamp of last signal-quality calc (ms)")
G_LAST_AGE = Gauge("signal_quality_last_age_seconds", "Age of last signal-quality calc (seconds)")


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def _read_hash(r: Any, key: str) -> Dict[str, Any]:
    """Read entire Redis hash, return empty dict on any failure."""
    try:
        return r.hgetall(key) or {}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Metrics update
# ---------------------------------------------------------------------------

def _set_metrics(cfg2: Dict[str, Any]) -> None:
    """Push all gauges from the configuration hash snapshot."""
    # Staleness tracking
    last_ts_ms = _i(cfg2.get("signal_quality_last_ts_ms"), 0)
    G_LAST_TS.set(float(last_ts_ms))
    age_s = 0.0
    if last_ts_ms > 0:
        age_s = max(0.0, _now_s() - (float(last_ts_ms) / 1000.0))
    G_LAST_AGE.set(age_s)

    # Per-regime metrics (ok, warn, block)
    for regime in ("ok", "warn", "block"):
        G_EXPECT.labels(regime=regime).set(
            _f(cfg2.get(f"signal_quality_expectancy_r_24h_regime_{regime}"), 0.0)
        )
        G_PREC.labels(regime=regime).set(
            _f(cfg2.get(f"signal_quality_precision_top5p_24h_regime_{regime}"), 0.0)
        )
        G_ECE.labels(regime=regime).set(
            _f(cfg2.get(f"signal_quality_ece_24h_regime_{regime}"), 0.0)
        )
        G_N.labels(regime=regime).set(
            float(_i(cfg2.get(f"signal_quality_n_24h_regime_{regime}"), 0))
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    cfg = load_cfg()
    import redis  # type: ignore

    r = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    start_http_server(cfg.port)
    print(
        f"[signal_quality_regime_exporter_p66] "
        f"serving on :{cfg.port}, key={cfg.dyn_cfg_key}, interval={cfg.interval_s}s"
    )

    while True:
        cfg2 = _read_hash(r, cfg.dyn_cfg_key)
        _set_metrics(cfg2)
        time.sleep(cfg.interval_s)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
