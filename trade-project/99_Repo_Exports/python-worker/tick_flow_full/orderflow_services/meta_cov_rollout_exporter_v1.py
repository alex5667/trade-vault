from domain.evidence_keys import MetaKeys
from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

#!/usr/bin/env python3
"""meta_cov_rollout_exporter_v1.py

P30: Prometheus exporter for meta coverage + canary rollout config.
P34: Adds quarantine metrics.
P37: Adds decision-log ops metrics (preflight, steps, ok).
P43: Adds guard decision metrics and auto-apply block status.

Exports:
- cfg2 toggles/shares (meta_enforce_per_cov, meta_enforce_share_cov_*)
- coverage distribution (p10/p50) and bucket rates.
- quarantine status and recovery targets.
- ops last run status.
- auto-apply block status.

Run:
  python3 orderflow_services/meta_cov_rollout_exporter_v1.py

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
from typing import Any

from prometheus_client import Gauge, start_http_server  # type: ignore

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore

# Try to import guard for block checks
try:
    import auto_apply_guard
except ImportError:
    try:
        from orderflow_services import auto_apply_guard
    except ImportError:
        auto_apply_guard = None

# Metrics
GAUGE_COV_ENABLED = Gauge("meta_enforce_per_cov", "1 if coverage-based canary is enabled")
GAUGE_COV_A_GE = Gauge("meta_cov_bucket_a_ge", "Threshold for bucket A (excellent coverage)")
GAUGE_COV_B_GE = Gauge("meta_cov_bucket_b_ge", "Threshold for bucket B (good coverage)")
GAUGE_COV_C_GE = Gauge("meta_cov_bucket_c_ge", "Threshold for bucket C (minimal coverage)")

GAUGE_SHARE_COV = Gauge("meta_enforce_share_cov", "Canary share per coverage bucket", ["bucket"])

# Quarantine Metrics (P34)
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

# P43: Decision and Block Metrics
GAUGE_OPS_LAST_DECISION = Gauge("meta_cov_ops_last_decision", "Last decision code (1 for current code)", ["code"])
GAUGE_OPS_LAST_DECISION_AGE = Gauge("meta_cov_ops_last_decision_age_s", "Age of last decision in seconds")

GAUGE_AUTO_APPLY_BLOCK = Gauge("auto_apply_block", "1 if auto-apply is blocked by reason", ["reason"])
GAUGE_AUTO_APPLY_BLOCK_TS = Gauge("auto_apply_block_ts_ms", "Timestamp of block by reason", ["reason"])
GAUGE_AUTO_APPLY_BLOCK_AGE = Gauge("auto_apply_block_age_s", "Age of block in seconds by reason", ["reason"])

# P47: Signal Quality KPIs
GAUGE_SQ_EXPECTANCY = Gauge("signal_quality_expectancy_r_24h", "Signal quality Expectancy R (24h)")
GAUGE_SQ_PRECISION = Gauge("signal_quality_precision_top5p_24h", "Signal quality Precision @ Top 5% (24h)")
GAUGE_SQ_ECE = Gauge("signal_quality_ece_24h", "Signal quality ECE (24h)")
GAUGE_SQ_N = Gauge("signal_quality_n_24h", "Signal quality N trades (24h)")
GAUGE_SQ_LAST_TS_MS = Gauge("signal_quality_last_ts_ms", "Last KPI compute timestamp (ms)")
GAUGE_SQ_STALENESS_SEC = Gauge("signal_quality_staleness_sec", "How old the KPI snapshot is (seconds)")

# P64: export per-regime breakdown (low cardinality)
GAUGE_SQ_EXPECTANCY_R_24H_BY_REGIME = Gauge("signal_quality_expectancy_r_24h_by_regime", "Mean R over last 24h by regime", ["regime"])
GAUGE_SQ_PRECISION_TOP5P_24H_BY_REGIME = Gauge("signal_quality_precision_top5p_24h_by_regime", "Win rate in top 5%% by score over last 24h by regime", ["regime"])
GAUGE_SQ_ECE_24H_BY_REGIME = Gauge("signal_quality_ece_24h_by_regime", "ECE over last 24h by regime", ["regime"])
GAUGE_SQ_N_24H_BY_REGIME = Gauge("signal_quality_n_24h_by_regime", "N over last 24h by regime", ["regime"])


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


def pctl(xs: list[float], q: float) -> float:
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
    return get_ny_time_millis()


class Exporter:
    def __init__(self) -> None:
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.cfg2_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")
        self.stream = os.getenv("META_COV_SOURCE_STREAM", os.getenv("ML_CONFIRM_METRICS_STREAM", os.getenv("OF_GATE_METRICS_STREAM", RS.OF_GATE_METRICS)))
        self.lookback_min = int(os.getenv("META_COV_EXPORTER_LOOKBACK_MIN", "60") or 60)
        self.max_scan = int(os.getenv("META_COV_EXPORTER_MAX_SCAN", "50000") or 50000)
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=False) if redis else None

    def _load_cfg2(self) -> dict[str, Any]:
        if not self.r:
            return {}
        try:
            d = self.r.hgetall(self.cfg2_key) or {}
            return {k.decode() if isinstance(k, bytes) else str(k): _loads_maybe_json(v) for k, v in d.items()}
        except Exception:
            return {}

    def _read_recent(self) -> list[dict[str, Any]]:
        if not self.r:
            return []
        rows: list[dict[str, Any]] = []
        since_ms = get_ny_time_millis() - self.lookback_min * 60 * 1000
        try:
            batch = self.r.xrevrange(self.stream, max="+", min=str(since_ms), count=self.max_scan)
            for _, fields in batch:
                d: dict[str, Any] = {}
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

    def _check_blocks(self):
        if not auto_apply_guard:
            return

        try:
            # We want to check detailed status for all reasons
            # Reuse get_block_state but we need the components.
            # actually get_block_state returns a combined meta.
            # We can also just iterate the REASONS env var and check manually if we want granularity.
            # But auto_apply_guard.get_block_state does that and populates the meta structure?
            # It returns the FIRST blocking reason as primary reason, but we might want all.
            # Let's peek into Redis manually for all reasons to be comprehensive, OR rely on auto_apply_guard if possible.
            # auto_apply_guard iterates reasons.
            # Impl below duplicates logic slightly to be robust.

            pfx = os.getenv("AUTO_APPLY_BLOCK_PREFIX", "cfg:suggestions:entry_policy:auto_apply_block")
            reasons_str = os.getenv("AUTO_APPLY_BLOCK_REASONS", "tick_gate,meta_cov")
            reasons = [r.strip() for r in reasons_str.split(",") if r.strip()]

            # Reset all reasons to 0 first? Or just set known ones.
            # Prometheus gauges persist.

            now = now_ms()

            for rsn in reasons:
                try:
                    # Check existence
                    block_val = self.r.get(f"{pfx}:{rsn}")
                    meta_val = self.r.get(f"{pfx}:{rsn}:meta")
                    ts_val = self.r.get(f"{pfx}:{rsn}:ts_ms")

                    is_blocked = False
                    block_ts = 0

                    if block_val is not None:
                        is_blocked = True
                        block_ts = now # Treat hard block as fresh? Or try to find ts.
                        if ts_val:
                            block_ts = int(ts_val)
                    elif ts_val:
                         # Soft block check?
                         # guard logic: if ts_ms safe age ...
                         # But exporter just wants to report "Is it blocked now?"
                         # We should use get_block_state logic basically.
                         pass

                    # If we use guard's logic:
                    # But calling guard for each reason is tricky if guard function only returns first one.
                    # We will rely on simple existence + soft block logic for monitoring.

                    # Re-implement simplified check:
                    if block_val is not None:
                         is_blocked = True
                         if ts_val: block_ts = int(ts_val)
                    else:
                        # Soft check
                         if ts_val:
                             ts = int(ts_val)
                             # If meta says blocked=true and age is valid
                             meta = _loads_maybe_json(meta_val)
                             if isinstance(meta, dict) and (meta.get("blocked") or "0") in ("1", "true", "True"):
                                 # age check
                                 if (now - ts) < 15 * 60 * 1000: # 15 min default
                                     is_blocked = True
                                     block_ts = ts

                    GAUGE_AUTO_APPLY_BLOCK.labels(reason=rsn).set(1 if is_blocked else 0)
                    if block_ts > 0 and is_blocked:
                         GAUGE_AUTO_APPLY_BLOCK_TS.labels(reason=rsn).set(float(block_ts))
                         GAUGE_AUTO_APPLY_BLOCK_AGE.labels(reason=rsn).set((now - block_ts) / 1000.0)
                    else:
                         GAUGE_AUTO_APPLY_BLOCK_TS.labels(reason=rsn).set(0)
                         GAUGE_AUTO_APPLY_BLOCK_AGE.labels(reason=rsn).set(0)

                except Exception:
                    pass

        except Exception as e:
            print(f"Block check error: {e}")

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
                v = cfg2.get(MetaKeys.ENFORCE_SHARE, 1.0)
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

        for s in ["validate", "rollout", "guard", "outcome", "monitor"]:
            rc = _i(cfg2.get(f"meta_cov_ops_last_step_rc_{s}"), -1)
            GAUGE_OPS_STEP_RC.labels(step=s).set(float(rc))

        # P43: Decision Code
        dec_code = (cfg2.get("meta_cov_ops_last_decision_code") or "unknown")
        # Reset all codes to 0, set active to 1.
        # This is tricky with Prometheus without knowing all possible codes.
        # We'll just set the current one to 1. Using a set of known codes helps.
        # Known: ok, guard_block, guard_error, preflight_fail, unknown.
        for c in ["ok", "guard_block", "guard_error", "preflight_fail", "unknown"]:
            GAUGE_OPS_LAST_DECISION.labels(code=c).set(1.0 if c == dec_code else 0.0)

        dec_ts = _i(cfg2.get("meta_cov_ops_last_decision_ts_ms"), 0)
        if dec_ts > 0:
            GAUGE_OPS_LAST_DECISION_AGE.set((_now - dec_ts) / 1000.0)
        else:
            GAUGE_OPS_LAST_DECISION_AGE.set(0.0)

        # P47: Signal Quality KPIs
        GAUGE_SQ_EXPECTANCY.set(_f(cfg2.get("signal_quality_expectancy_r_24h"), 0.0))
        GAUGE_SQ_PRECISION.set(_f(cfg2.get("signal_quality_precision_top5p_24h"), 0.0))
        GAUGE_SQ_ECE.set(_f(cfg2.get("signal_quality_ece_24h"), 0.0))
        GAUGE_SQ_N_24H.set(float(_i(cfg2.get("signal_quality_n_24h"), 0)))

        # P64: per-regime gauges (ok/warn/block)
        for _regime in ("ok", "warn", "block"):
            GAUGE_SQ_EXPECTANCY_R_24H_BY_REGIME.labels(regime=_regime).set(_f(cfg2.get(f"signal_quality_expectancy_r_24h_regime_{_regime}"), 0.0))
            GAUGE_SQ_PRECISION_TOP5P_24H_BY_REGIME.labels(regime=_regime).set(_f(cfg2.get(f"signal_quality_precision_top5p_24h_regime_{_regime}"), 0.0))
            GAUGE_SQ_ECE_24H_BY_REGIME.labels(regime=_regime).set(_f(cfg2.get(f"signal_quality_ece_24h_regime_{_regime}"), 0.0))
            GAUGE_SQ_N_24H_BY_REGIME.labels(regime=_regime).set(float(_i(cfg2.get(f"signal_quality_n_24h_regime_{_regime}"), 0)))

        sq_ts = _i(cfg2.get("signal_quality_last_ts_ms"), 0)
        GAUGE_SQ_LAST_TS.set(float(sq_ts))
        if sq_ts > 0:
            GAUGE_SQ_STALENESS.set((_now - sq_ts) / 1000.0)
        else:
            GAUGE_SQ_STALENESS.set(0.0)

        # Check Blocks
        self._check_blocks()

        # stream stats
        rows = self._read_recent()
        covs: list[float] = []
        counts = {"a": 0, "b": 0, "c": 0, "d": 0}

        a_ge = _f(cfg2.get("meta_cov_bucket_a_ge"), 0.98)
        b_ge = _f(cfg2.get("meta_cov_bucket_b_ge"), 0.95)
        c_ge = _f(cfg2.get("meta_cov_bucket_c_ge"), 0.90)

        for r_row in rows:
            c = _f(r_row.get(MetaKeys.FEATURE_COVERAGE), -1.0)
            if c < 0:
                tot = _i(r_row.get(MetaKeys.MODEL_FEATURE_TOTAL), 0)
                mis = _i(r_row.get(MetaKeys.MODEL_FEATURE_MISSING), 0)
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
