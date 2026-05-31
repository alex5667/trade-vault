#!/usr/bin/env python3
"""
P66: Policy mode exporter

Reads Redis hash `metrics:policy_mode:state` (written by policy_mode_kpi_worker_p66_v1)
and exposes Prometheus metrics on :9818/metrics:

  policy_mode_last_ts_ms            — epoch ms of most recent processed decision
  policy_mode_last_age_seconds      — seconds since last decision (staleness)
  policy_mode_n_24h_total           — total decisions in rolling 24h window
  policy_mode_n_24h{regime,effective_mode}   — count per (regime, effective_mode) cell
  policy_mode_share_24h{regime,effective_mode} — fraction of total per cell
  policy_mode_mismatch_share_24h{kind}  — mismatch rate (fraction of total)

Design:
  - No per-symbol labels: cardinality is 4*4 = 16 cells max + 4 more gauges
  - Scrape interval 15s, polling 5s — exporter is stateless, reads from Redis
  - Separate from the KPI worker for independent fault isolation
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
    """Safe int cast via float."""
    try:
        return int(float(v))
    except Exception:
        return d


@dataclass
class Cfg:
    redis_url: str
    state_key: str      # Redis hash written by KPI worker
    port: int           # Prometheus /metrics HTTP port
    interval_s: float   # polling interval between Redis reads


def load_cfg() -> Cfg:
    return Cfg(
        redis_url=_env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        state_key=_env("POLICY_MODE_STATE_KEY", "metrics:policy_mode:state"),
        port=_i(_env("POLICY_MODE_EXPORTER_PORT", "9818"), 9818),
        interval_s=float(_env("POLICY_MODE_EXPORTER_INTERVAL_S", "5") or 5),
    )


# ── Prometheus gauges ─────────────────────────────────────────────────────────

# Timestamp of most recent processed decision (ms epoch)
LAST_TS = Gauge("policy_mode_last_ts_ms", "Last policy mode decision timestamp (ms)")
# Age of most recent decision in seconds (staleness signal)
LAST_AGE = Gauge("policy_mode_last_age_seconds", "Age of last policy mode decision (seconds)")
# Total decisions processed in rolling 24h window
TOTAL = Gauge("policy_mode_n_24h_total", "Total decisions observed (24h)")
# Per-cell count: (regime x effective_mode) cross-section
N = Gauge(
    "policy_mode_n_24h",
    "Decisions count (24h) by regime and effective mode",
    ["regime", "effective_mode"],
)
# Per-cell share: fraction of total
SHARE = Gauge(
    "policy_mode_share_24h",
    "Decisions share (24h) by regime and effective mode",
    ["regime", "effective_mode"],
)
# Mismatch rates: how often policy enforcement deviates from expected
MISM = Gauge(
    "policy_mode_mismatch_share_24h",
    "Mismatch share (24h) by kind",
    ["kind"],
)

# All valid regime and mode dimensions
REGIMES = ("ok", "warn", "block", "unknown")
MODES = ("active", "shadow", "block", "unknown")


def _read_hash(r, key: str) -> Dict[str, str]:
    """Read entire Redis hash, return empty dict on error."""
    try:
        return r.hgetall(key) or {}
    except Exception:
        return {}


def _cell_key(reg: str, mode: str) -> str:
    """Build rolling state field name for a (regime, mode) cell."""
    return f"rolling_{reg}_{mode}"


def main() -> int:
    cfg = load_cfg()
    import redis  # type: ignore

    r = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    start_http_server(cfg.port)
    print(
        f"[policy_mode_exporter_p66] serving on :{cfg.port}, "
        f"key={cfg.state_key}, interval={cfg.interval_s}s"
    )

    while True:
        d = _read_hash(r, cfg.state_key)

        # ── Staleness gauges ─────────────────────────────────────────────────
        last_ts = _i(d.get("last_ts_ms"), 0)
        LAST_TS.set(float(last_ts))
        age = 0.0
        if last_ts > 0:
            age = max(0.0, _now_s() - (float(last_ts) / 1000.0))
        LAST_AGE.set(age)

        # ── Volume gauges ────────────────────────────────────────────────────
        total = _i(d.get("rolling_total"), 0)
        TOTAL.set(float(max(0, total)))

        # ── Per-cell count and share ─────────────────────────────────────────
        for reg in REGIMES:
            for mode in MODES:
                n = _i(d.get(_cell_key(reg, mode)), 0)
                N.labels(regime=reg, effective_mode=mode).set(float(max(0, n)))
                # share = n / total (or 0 if no data yet)
                share = (float(n) / float(total)) if total > 0 else 0.0
                SHARE.labels(regime=reg, effective_mode=mode).set(share)

        # ── Mismatch shares ──────────────────────────────────────────────────
        # block_regime_effective_not_block: block regime must enforce block mode
        mism1 = _i(d.get("rolling_mismatch_block_regime_effective_not_block"), 0)
        # warn_regime_effective_active: warn regime with active mode = elevated risk
        mism2 = _i(d.get("rolling_mismatch_warn_regime_effective_active"), 0)
        # use total or 1.0 to avoid division-by-zero when no data yet
        denom = float(total) if total > 0 else 1.0
        MISM.labels(kind="block_regime_effective_not_block").set(float(mism1) / denom)
        MISM.labels(kind="warn_regime_effective_active").set(float(mism2) / denom)

        time.sleep(cfg.interval_s)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
