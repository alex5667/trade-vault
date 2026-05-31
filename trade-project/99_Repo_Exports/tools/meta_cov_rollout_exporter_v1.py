#!/usr/bin/env python3
"""meta_cov_rollout_exporter_v1.py

P30: Prometheus exporter for meta coverage + canary rollout config.
P34: Adds quarantine metrics.
P37: Adds decision-log ops metrics (preflight, steps, ok).

Exports:
- cfg2 toggles/shares (meta_enforce_per_cov, meta_enforce_share_cov_*)
- coverage distribution (p10/p50) and bucket rates.
- quarantine status and recovery targets.
- ops last run status.

Run:
  python3 -m tools.meta_cov_rollout_exporter_v1

ENV:
  REDIS_URL (default redis://redis-worker-1:6379/0)
  DYN_CFG_KEY (default settings:dynamic_cfg)
  META_COV_SOURCE_STREAM (default metrics:of_gate)
  META_COV_EXPORTER_PORT (default 9132)
  META_COV_EXPORTER_LOOKBACK_MIN (default 60)
  META_COV_EXPORTER_MAX_SCAN (default 50000)
"""

import json
import os
import time
from typing import Any, Dict, List

from prometheus_client import Gauge, start_http_server  # type: ignore

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore

# Metrics
GAUGE_COV_ENABLED = Gauge("meta_enforce_per_cov", "1 if coverage-based canary is enabled")
GAUGE_COV_A_GE = Gauge("meta_cov_bucket_a_ge", "Threshold for bucket A (excellent coverage)")
GAUGE_COV_B_GE = Gauge("meta_cov_bucket_b_ge", "Threshold for bucket B (good coverage)")
GAUGE_COV_C_GE = Gauge("meta_cov_bucket_c_ge", "Threshold for bucket C (minimal coverage)")

GAUGE_SHARE_COV = Gauge("meta_enforce_share_cov", "Canary share per coverage bucket", ["bucket"])

# Quarantine Metrics (P34)
# Ensure no duplicates here
GAUGE_COV_QUARANTINE_ACTIVE = Gauge("meta_cov_quarantine_active", "1 if bucket is quarantined (cfg2)", ["bucket"])
GAUGE_COV_QUARANTINE_TTL_SEC = Gauge("meta_cov_quarantine_ttl_sec", "remaining quarantine ttl seconds (cfg2)", ["bucket"])
GAUGE_COV_QUARANTINE_TTL_MS = Gauge("meta_cov_quarantine_ttl_ms", "remaining quarantine ttl ms (cfg2)", ["bucket"])
GAUGE_COV_RECOVERY_TARGET_SHARE = Gauge("meta_cov_recovery_target_share", "recovery target share after quarantine (cfg2)", ["bucket"])

# Outcome Freshness (P35)
GAUGE_COV_OUTCOME_LAST_APPLY = Gauge("meta_cov_outcome_last_apply_ms", "last successful outcome application ms")

# Distribution Metrics (P30)
GAUGE_SAMPLES = Gauge("meta_cov_samples", "Number of samples in lookback window")
GAUGE_COV_P10 = Gauge("meta_feature_coverage_p10", "10th percentile of feature coverage")
GAUGE_COV_P50 = Gauge("meta_feature_coverage_p50", "Median feature coverage")
GAUGE_COV_BUCKET_RATE = Gauge("meta_cov_bucket_rate", "Fraction of events in each coverage bucket", ["bucket"])

# Ops Metrics (P37)
GAUGE_OPS_LAST_TS = Gauge("meta_cov_ops_last_ts_ms", "Timestamp of last ops bundle run")
GAUGE_OPS_LAST_OK = Gauge("meta_cov_ops_last_ok", "1 if last run was fully OK")
GAUGE_OPS_LAST_EXIT = Gauge("meta_cov_ops_last_exit_code", "Exit code of last run")
GAUGE_OPS_APPLY_EFFECTIVE = Gauge("meta_cov_ops_apply_effective", "1 if last run effectively applied changes")
GAUGE_OPS_PREFLIGHT_RC = Gauge("meta_cov_ops_preflight_rc", "Return code of preflight check")
GAUGE_OPS_STEP_RC = Gauge("meta_cov_ops_step_rc", "Return code of individual steps", ["step"])


def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


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


def pctl(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    xs2 = sorted(xs)
    n = len(xs2)
    pos = (n - 1) * q
    lo = int(pos)
    hi = min(n - 1, lo + 1)
    frac = pos - lo
    return xs2[lo] * (1.0 - frac) + xs2[hi] * frac


def now_ms() -> int:
    return int(time.time() * 1000)


class Exporter:
    def __init__(self) -> None:
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.cfg2_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")
        # Support fallback to legacy env vars if needed, but P31+ suggests META_COV_SOURCE_STREAM
        self.stream = os.getenv("META_COV_SOURCE_STREAM", os.getenv("ML_CONFIRM_METRICS_STREAM", os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")))
        self.lookback_min = int(os.getenv("META_COV_EXPORTER_LOOKBACK_MIN", "60") or 60)
        self.max_scan = int(os.getenv("META_COV_EXPORTER_MAX_SCAN", "50000") or 50000)
        self.r = self._connect()

    def _connect(self):
        if not redis:
            return None
        delay = 1.0
        for attempt in range(3):
            try:
                r = redis.Redis.from_url(self.redis_url, decode_responses=False)
                r.ping()
                return r
            except Exception as e:
                if attempt == 2:
                    print(f"⚠️ meta_cov_rollout_exporter: Redis unavailable after 3 attempts: {e}")
                    return None
                print(f"⚠️ Redis not ready (attempt {attempt + 1}/3): {e}. Retry in {delay:.0f}s...")
                time.sleep(delay)
                delay = min(delay * 2, 10.0)
        return None

    def _load_cfg2(self) -> Dict[str, Any]:
        if not self.r:
            return {}
        try:
            d = self.r.hgetall(self.cfg2_key) or {}
            return {k.decode() if isinstance(k, bytes) else str(k): _loads_maybe_json(v) for k, v in d.items()}
        except Exception:
            return {}

    def _read_recent(self) -> List[Dict[str, Any]]:
        if not self.r:
            return []
        rows: List[Dict[str, Any]] = []
        since_ms = int(time.time() * 1000) - self.lookback_min * 60 * 1000
        try:
            batch = self.r.xrevrange(self.stream, max="+", min=str(since_ms), count=self.max_scan)
            for _, fields in batch:
                d: Dict[str, Any] = {}
                for k, v in fields.items():
                    ks = k.decode() if isinstance(k, bytes) else str(k)
                    d[ks] = _loads_maybe_json(v)
                # Check for nested payload/json
                p = d.get("payload") or d.get("json")
                if isinstance(p, dict):
                    d.update(p)
                rows.append(d)
        except Exception:
            pass
        return rows

    def step(self) -> None:
        cfg2 = self._load_cfg2()

        # cfg2 gauges P30
        GAUGE_COV_ENABLED.set(_f(cfg2.get("meta_enforce_per_cov"), 0.0))
        GAUGE_COV_A_GE.set(_f(cfg2.get("meta_cov_bucket_a_ge"), 0.98))
        GAUGE_COV_B_GE.set(_f(cfg2.get("meta_cov_bucket_b_ge"), 0.95))
        GAUGE_COV_C_GE.set(_f(cfg2.get("meta_cov_bucket_c_ge"), 0.90))

        for b in ["a", "b", "c", "d"]:
            v = cfg2.get(f"meta_enforce_share_cov_{b}")
            if v is None:
                v = cfg2.get("meta_enforce_share", 1.0)
            GAUGE_SHARE_COV.labels(bucket=b).set(float(v))

        # P34: quarantine + recovery gauges (cfg2)
        _now = now_ms()
        for b in ("a", "b", "c", "d"):
            q_active = _i(cfg2.get(f"meta_cov_quarantine_{b}"), 0)
            q_until = _i(cfg2.get(f"meta_cov_quarantine_until_ms_{b}"), 0)
            ttl_ms = (max(0, q_until - _now) if (q_active == 1 and q_until > 0) else 0)
            ttl_sec = ttl_ms / 1000.0
            
            GAUGE_COV_QUARANTINE_ACTIVE.labels(bucket=b).set(float(1 if q_active == 1 else 0))
            GAUGE_COV_QUARANTINE_TTL_SEC.labels(bucket=b).set(float(ttl_sec))
            GAUGE_COV_QUARANTINE_TTL_MS.labels(bucket=b).set(float(ttl_ms))
            GAUGE_COV_RECOVERY_TARGET_SHARE.labels(bucket=b).set(float(_f(cfg2.get(f"meta_cov_recovery_target_share_{b}"), 0.0)))

        # P35: Outcome freshness
        last_apply = _i(cfg2.get("meta_cov_outcome_last_apply_ms"), 0)
        GAUGE_COV_OUTCOME_LAST_APPLY.set(float(last_apply))

        # P37: Ops Metrics
        GAUGE_OPS_LAST_TS.set(float(_i(cfg2.get("meta_cov_ops_last_ts_ms"), 0)))
        GAUGE_OPS_LAST_OK.set(float(_i(cfg2.get("meta_cov_ops_last_ok"), 0)))
        GAUGE_OPS_LAST_EXIT.set(float(_i(cfg2.get("meta_cov_ops_last_exit_code"), 0)))
        GAUGE_OPS_APPLY_EFFECTIVE.set(float(_i(cfg2.get("meta_cov_ops_last_apply_effective"), 0)))
        GAUGE_OPS_PREFLIGHT_RC.set(float(_i(cfg2.get("meta_cov_ops_last_preflight_rc"), -1)))
        
        for s in ["validate", "rollout", "outcome", "monitor"]:
            rc = _i(cfg2.get(f"meta_cov_ops_last_step_rc_{s}"), -1)
            GAUGE_OPS_STEP_RC.labels(step=s).set(float(rc))

        # stream stats
        rows = self._read_recent()
        covs: List[float] = []
        counts = {"a": 0, "b": 0, "c": 0, "d": 0}
        
        a_ge = _f(cfg2.get("meta_cov_bucket_a_ge"), 0.98)
        b_ge = _f(cfg2.get("meta_cov_bucket_b_ge"), 0.95)
        c_ge = _f(cfg2.get("meta_cov_bucket_c_ge"), 0.90)

        for r_row in rows:
            c = _f(r_row.get("meta_feature_coverage"), -1.0)
            if c < 0:
                tot = _i(r_row.get("meta_model_feature_total"), 0)
                mis = _i(r_row.get("meta_model_feature_missing"), 0)
                if tot > 0:
                    c = 1.0 - (mis / float(tot))
            
            if c >= 0:
                covs.append(c)
                if c >= a_ge:
                    counts["a"] += 1
                elif c >= b_ge:
                    counts["b"] += 1
                elif c >= c_ge:
                    counts["c"] += 1
                else:
                    counts["d"] += 1

        GAUGE_SAMPLES.set(float(len(covs)))
        if covs:
            GAUGE_COV_P10.set(pctl(covs, 0.10))
            GAUGE_COV_P50.set(pctl(covs, 0.50))
            for b, n in counts.items():
                rate = n / float(len(covs))
                GAUGE_COV_BUCKET_RATE.labels(bucket=b).set(float(rate))
        else:
            GAUGE_COV_P10.set(0.0)
            GAUGE_COV_P50.set(0.0)
            for b in ["a", "b", "c", "d"]:
                GAUGE_COV_BUCKET_RATE.labels(bucket=b).set(0.0)


def main() -> None:
    port = int(os.getenv("META_COV_EXPORTER_PORT", "9132") or 9132)
    print(f"Starting meta_cov_rollout_exporter_v1 on port {port}...")
    start_http_server(port)
    
    exp = Exporter()
    while True:
        try:
            exp.step()
        except Exception as e:
            print(f"Exporter error: {e}")
        time.sleep(15)


if __name__ == "__main__":
    main()
