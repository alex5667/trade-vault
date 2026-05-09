from __future__ import annotations

#!/usr/bin/env python3
"""enforce_bucket_state_exporter_v1.py

Prometheus exporter for:
- current enforce bucket allowlists (slippage_decomp / taker_flow_gate)
- slippage decomp impact coefficient per sym,bucket (optional)
- last promoter report (proposal/apply, bucket health residual p95/p99)
- auto-apply block state
- exec_slip_stats refresher status
- slo-freezer status

Designed for low cardinality.

ENV
  ENFORCE_STATE_EXPORTER_PORT (default 9142)
  ENFORCE_STATE_EXPORTER_REFRESH_SEC (default 10)
  ENFORCE_STATE_EXPORTER_SYMBOLS (default "") comma-separated
  ENFORCE_STATE_EXPORTER_BUCKETS (default "NORMAL,LOW_LIQ,HIGH_VOL,HIGH_VOL_LOW_LIQ")

  REDIS_URL or CRYPTO_NOTIFY_REDIS_URL

DB (optional, for residual validation time-series):
  ENFORCE_STATE_EXPORTER_DB_STATS=0|1 (default 0)
  ANALYTICS_DB_DSN or DATABASE_URL
  ENFORCE_STATE_EXPORTER_DB_LOOKBACK_H (default 4)
  ENFORCE_STATE_EXPORTER_DB_MV (default mv_exec_slippage_eval_1h_stats)
  ENFORCE_STATE_EXPORTER_DB_VIEW (fallback v_exec_slippage_eval)


  ENFORCE_PROMOTER_STATUS_PATH (default /var/lib/trade/of_reports/out/enforce/promoter/enforce_bucket_promoter_status.json)
  ENFORCE_PROMOTER_REPORT_KEY (default proposal:enforce_bucket_promotion_report)

  EXEC_SLIP_STATS_STATUS_PATH (default /var/lib/trade/of_reports/out/enforce/stats/exec_slip_stats_refresh_status.json)

  SLIPPAGE_CAL_STATUS_PATH (default /var/lib/trade/of_reports/out/enforce/stats/slippage_calibrator_status.json)
  ENFORCE_FREEZER_STATUS_PATH (default /var/lib/trade/of_reports/out/enforce/freezer/enforce_bucket_slo_freezer_status.json)

  PROM_RULES_BUNDLE_STATE_PREFIX (default state:prom_rules_bundle)

  AUTO_APPLY_BLOCK_PREFIX (default cfg:suggestions:entry_policy:auto_apply_block)
  ENFORCE_AUTO_APPLY_BLOCK_REASON (default enforce_bucket_promoter)

Keys read:
  cfg:slippage_decomp_enforce_buckets
  cfg:taker_flow_gate_enforce_buckets
  cfg:slippage_decomp_enforce_buckets:{sym} (optional)
  cfg:taker_flow_gate_enforce_buckets:{sym} (optional)
  cfg:slippage_decomp_impact_coeff_bps:{sym}:{bucket} (optional)

  proposal:enforce_bucket_promotion_report (fallback if status file missing)

  state:exec_slip_stats_refresher:last_ok_ts_ms (fallback if status file missing)
  state:exec_slip_stats_refresher:last_dur_ms
  state:exec_slip_stats_refresher:last_ok

  state:slippage_calibrator:last_ok_ts_ms
  state:slippage_calibrator:last_dur_ms
  state:slippage_calibrator:last_ok

Outputs:
  of_enforce_bucket_flag{component,sym,bucket} = 0/1
  of_slippage_decomp_impact_coeff_bps{sym,bucket} = coeff (only if present)

  of_enforce_promoter_report_age_sec
  of_enforce_promoter_apply_enabled
  of_enforce_promoter_last_ok{component}
  of_enforce_promoter_last_added_bucket{component,bucket} = 1 for last added bucket else 0
  of_enforce_promoter_bucket_resid_p95_bps{bucket}
  of_enforce_promoter_bucket_resid_p99_bps{bucket}
  of_enforce_promoter_bucket_db_n{bucket}
  of_enforce_promoter_bucket_gate_n{bucket}
  of_enforce_promoter_bucket_ok_soft_rate{bucket}

  of_exec_slip_stats_refresh_last_ok_ts_ms
  of_exec_slip_stats_refresh_last_ok_age_sec
  of_exec_slip_stats_refresh_last_dur_ms
  of_exec_slip_stats_refresh_last_ok

  of_prom_rules_bundle_last_ok
  of_prom_rules_bundle_last_ok_age_sec
  of_prom_rules_bundle_last_files_checked
  of_prom_rules_bundle_last_error_n

  of_slippage_calibrator_last_ok_ts_ms
  of_slippage_calibrator_last_ok_age_sec
  of_slippage_calibrator_last_dur_ms
  of_slippage_calibrator_last_ok

  of_exec_slip_stats_db_up
  of_exec_slip_stats_db_query_dur_ms
  of_exec_slip_resid_p95_bps{sym,bucket}
  of_exec_slip_resid_p99_bps{sym,bucket}
  of_exec_slip_edge_neg_share{sym,bucket}
  of_exec_slip_db_n{sym,bucket}

  of_auto_apply_block_active{source,cause}
  of_auto_apply_block_age_sec{source}

  of_enforce_freezer_block_active{sym,bucket}
  of_enforce_freezer_last_block_age_sec{sym},
""",
import json
import os
import signal
import time
from typing import Any

from prometheus_client import Gauge, start_http_server  # type: ignore

from utils.time_utils import get_ny_time_millis
import contextlib


def _now_ms() -> int:
    return get_ny_time_millis()


def _as_str(x: Any, default: str = "") -> str:
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            return x.decode("utf-8", "ignore")
        return str(x)
    except Exception:
        return default


def _as_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or isinstance(x, bool):
            return default
        if isinstance(x, (int, float)):
            return int(x)
        s = _as_str(x).strip()
        return int(float(s)) if s else default
    except Exception:
        return default


def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or isinstance(x, bool):
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = _as_str(x).strip()
        return float(s) if s else default
    except Exception:
        return default


def _parse_list(raw: str) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    xs: list[str] = []
    for p in raw.replace(";", ",").split(","):
        s = p.strip().upper()
        if s and s not in xs:
            xs.append(s)
    return xs


def _load_json(path: str) -> dict[str, Any] | None:
    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _connect_redis():
    rurl = os.getenv("REDIS_URL") or os.getenv("CRYPTO_NOTIFY_REDIS_URL") or ""
    if not str(rurl).strip():
        return None
    try:
        import redis  # type: ignore

        return redis.Redis.from_url(rurl, decode_responses=True)
    except Exception:
        return None


def _safe_ident(x: str, default: str = "") -> str:
    """Sanitize a string to be safe as a Prometheus label value (uppercase alphanumeric+underscore).""",
    s = (x or "").strip().upper()
    if not s:
        return default
    s2 = "".join(
        ch for ch in s if ("A" <= ch <= "Z") or ("0" <= ch <= "9") or ch == "_"
    )
    return s2 or default


def _norm_cause(x: str) -> str:
    s = (x or "").strip().lower()
    if s in ("slo_freeze", "rollback", "manual", "rules_bundle_invalid", "stale_snapshot", "last_run_failed", "preflight_soft_block", "quarantine"):
        return s
    return "unknown" if s else "unknown"


# Gauges — low cardinality by design
of_enforce_state_exporter_up = Gauge("of_enforce_state_exporter_up", "exporter loop running (1/0)")

# P90: Exporter self-health — loop liveness timestamp and cumulative error counter.
# of_enforce_state_exporter_poll_ts_ms is updated every loop iteration (even on error)
# so a Prometheus staleness check detects if the whole exporter is frozen/dead.
of_enforce_state_exporter_poll_ts_ms = Gauge(
    "of_enforce_state_exporter_poll_ts_ms",
    "timestamp (ms) of last exporter loop tick",
)
of_enforce_state_exporter_errors_total = Gauge(
    "of_enforce_state_exporter_errors_total",
    "cumulative exporter loop errors (monotonic-ish)",
)

of_enforce_bucket_flag = Gauge(
    "of_enforce_bucket_flag",
    "enforce bucket allowlist flag (1/0)",
    ["component", "sym", "bucket"],
)

of_slippage_decomp_impact_coeff_bps = Gauge(
    "of_slippage_decomp_impact_coeff_bps",
    "slippage decomp impact coefficient (bps per impact_proxy unit)",
    ["sym", "bucket"],
)

# V8: age of each coeff computed from companion timestamp key written by calibrator.
# Key pattern: cfg:slippage_decomp_impact_coeff_bps_ts_ms:{sym}:{bucket}
of_slippage_decomp_impact_coeff_age_sec = Gauge(
    "of_slippage_decomp_impact_coeff_age_sec",
    "age of slippage decomp impact coefficient in seconds (from ts key)",
    ["sym", "bucket"],
)

# V8: last successful nightly slippage calibrator run age (from state:slippage_calib:last_ok_ts_ms)
of_slippage_calib_last_ok_age_sec = Gauge(
    "of_slippage_calib_last_ok_age_sec",
    "age of last successful nightly slippage calibrator run in seconds"
)

# V8: number of (sym,bucket) groups updated in last calibrator run
of_slippage_calib_last_updated_groups = Gauge(
    "of_slippage_calib_last_updated_groups",
    "number of (sym,bucket) groups updated in last slippage calibrator run"
)

# V10: total (sym,bucket) groups discovered in last calibrator run
of_slippage_calib_last_groups_total = Gauge(
    "of_slippage_calib_last_groups_total",
    "number of (sym,bucket) groups discovered in last slippage calibrator run",
)

# V10: exec slippage eval rowcount probe (P77) — written by exec_slippage_eval_rowcount_probe_p77_v1
of_exec_slippage_eval_rows_24h = Gauge(
    "of_exec_slippage_eval_rows_24h",
    "rowcount of v_exec_slippage_eval in last 24 hours by exec_regime_bucket",
    ["bucket"],
)

of_exec_slippage_eval_rows_24h_age_sec = Gauge(
    "of_exec_slippage_eval_rows_24h_age_sec",
    "age seconds since exec slippage eval rowcount probe last updated",
)

of_enforce_promoter_report_age_sec = Gauge(
    "of_enforce_promoter_report_age_sec", "age of last promoter report in seconds"
)
of_enforce_promoter_apply_enabled = Gauge(
    "of_enforce_promoter_apply_enabled", "PROMOTE_APPLY from last report (1/0)"
)

of_enforce_promoter_last_ok = Gauge(
    "of_enforce_promoter_last_ok", "promotion decision ok flag (1/0)", ["component"]
)

of_enforce_promoter_last_added_bucket = Gauge(
    "of_enforce_promoter_last_added_bucket",
    "last added bucket marker (1/0)",
    ["component", "bucket"],
)

of_enforce_promoter_bucket_resid_p95_bps = Gauge(
    "of_enforce_promoter_bucket_resid_p95_bps", "residual p95 bps by bucket", ["bucket"]
)
of_enforce_promoter_bucket_resid_p99_bps = Gauge(
    "of_enforce_promoter_bucket_resid_p99_bps", "residual p99 bps by bucket", ["bucket"]
)
of_enforce_promoter_bucket_db_n = Gauge(
    "of_enforce_promoter_bucket_db_n", "db samples by bucket", ["bucket"]
)
of_enforce_promoter_bucket_gate_n = Gauge(
    "of_enforce_promoter_bucket_gate_n", "eligible gate samples by bucket", ["bucket"]
)
of_enforce_promoter_bucket_ok_soft_rate = Gauge(
    "of_enforce_promoter_bucket_ok_soft_rate", "ok_soft rate by bucket", ["bucket"]
)

# P89: per-bucket DB edge_minus_expected_bps < 0 share (reads from promoter bucket_health status)
# Used by OF_EnforcedBucketEdgeNegShareHigh_Warn / Crit alerts
of_enforce_promoter_bucket_edge_neg_share = Gauge(
    "of_enforce_promoter_bucket_edge_neg_share",
    "DB edge_minus_expected_bps < 0 share for bucket (lookback window)",
    ["bucket"],
)

of_enforce_promoter_last_apply_ts_ms = Gauge(
    "of_enforce_promoter_last_apply_ts_ms", "last apply timestamp ms (state key)"
)

aof = of_enforce_promoter_last_apply_ts_ms

of_enforce_promoter_last_apply_age_sec = Gauge(
    "of_enforce_promoter_last_apply_age_sec", "age seconds since last apply (state key)"
)

of_enforce_promoter_last_rollback_ts_ms = Gauge(
    "of_enforce_promoter_last_rollback_ts_ms", "last rollback timestamp ms (state key)"
)

of_enforce_promoter_last_rollback_age_sec = Gauge(
    "of_enforce_promoter_last_rollback_age_sec", "age seconds since last rollback (state key)"
)

of_enforce_promoter_rollback_active = Gauge(
    "of_enforce_promoter_rollback_active", "1 if rollback happened after last apply"
)

# Stats refresher
of_exec_slip_stats_refresh_last_ok_ts_ms = Gauge(
    "of_exec_slip_stats_refresh_last_ok_ts_ms",
    "exec slip stats MV refresher last ok ts ms",
)

of_exec_slip_stats_refresh_last_ok_age_sec = Gauge(
    "of_exec_slip_stats_refresh_last_ok_age_sec",
    "age seconds since exec slip stats refresher last ok",
)

of_exec_slip_stats_refresh_last_dur_ms = Gauge(
    "of_exec_slip_stats_refresh_last_dur_ms",
    "duration ms of last exec slip stats refresh",
)

of_exec_slip_stats_refresh_last_ok = Gauge(
    "of_exec_slip_stats_refresh_last_ok",
    "1 if last refresh ok",
)


# Prometheus rules bundle validation state (nightly smoke-check)
of_prom_rules_bundle_last_ok = Gauge(
    "of_prom_rules_bundle_last_ok",
    "1 if last rules bundle validation succeeded (state:prom_rules_bundle:last_ok)",
)

of_prom_rules_bundle_last_ok_age_sec = Gauge(
    "of_prom_rules_bundle_last_ok_age_sec",
    "age seconds since last successful rules bundle validation (state:prom_rules_bundle:last_ok_ts_ms)",
)

of_prom_rules_bundle_last_files_checked = Gauge(
    "of_prom_rules_bundle_last_files_checked",
    "number of rules files discovered in last validation run (state:prom_rules_bundle:last_files_checked)",
)

of_prom_rules_bundle_last_error_n = Gauge(
    "of_prom_rules_bundle_last_error_n",
    "number of validation errors in last run (state:prom_rules_bundle:last_error_n)",
)

# Prometheus rules loaded probe (runtime include-list correctness)
# Written by prom_rules_loaded_probe_v1.py (hourly at :10 via of_timers_worker)
rules_loaded_probe_last_ok = Gauge(
    "rules_loaded_probe_last_ok",
    "1 if last runtime rules-loaded probe succeeded (state:prom_rules_loaded:last_ok)",
)

rules_loaded_probe_last_ok_age_sec = Gauge(
    "rules_loaded_probe_last_ok_age_sec",
    "age seconds since last successful rules-loaded probe (state:prom_rules_loaded:last_ok_ts_ms)",
)

rules_loaded_probe_last_run_age_sec = Gauge(
    "rules_loaded_probe_last_run_age_sec",
    "age seconds since last probe run (state:prom_rules_loaded:last_run_ts_ms)",
)

rules_files_expected = Gauge(
    "rules_files_expected",
    "expected rules file count from manifest (state:prom_rules_loaded:files_expected)",
)

rules_files_loaded = Gauge(
    "rules_files_loaded",
    "number of expected rule files observed as loaded in Prometheus (state:prom_rules_loaded:files_loaded)",
)

rules_files_missing = Gauge(
    "rules_files_missing",
    "expected - loaded (state:prom_rules_loaded:missing_n)",
)



# Slippage calibrator staleness metrics
of_slippage_calibrator_last_ok_ts_ms = Gauge(
    "of_slippage_calibrator_last_ok_ts_ms",
    "slippage calibrator last ok timestamp (ms)",
)

of_slippage_calibrator_last_ok_age_sec = Gauge(
    "of_slippage_calibrator_last_ok_age_sec",
    "age seconds since last slippage calibrator ok",
)

of_slippage_calibrator_last_dur_ms = Gauge(
    "of_slippage_calibrator_last_dur_ms",
    "slippage calibrator last duration (ms)",
)

of_slippage_calibrator_last_ok = Gauge(
    "of_slippage_calibrator_last_ok",
    "1 if last slippage calibrator run ok",
)


# P86: DB-based residual/edge validation metrics (low cardinality: sym x bucket)
of_exec_slip_stats_db_up = Gauge(
    "of_exec_slip_stats_db_up",
    "1 if last DB residual stats query succeeded, 0 if failed",
)

of_exec_slip_stats_db_query_dur_ms = Gauge(
    "of_exec_slip_stats_db_query_dur_ms",
    "duration ms of last DB residual stats query",
)

of_exec_slip_resid_p95_bps = Gauge(
    "of_exec_slip_resid_p95_bps",
    "residual P95 bps per sym/bucket (max over lookback from DB)",
    ["sym", "bucket"],
)

of_exec_slip_resid_p99_bps = Gauge(
    "of_exec_slip_resid_p99_bps",
    "residual P99 bps per sym/bucket (max over lookback from DB)",
    ["sym", "bucket"],
)

of_exec_slip_edge_neg_share = Gauge(
    "of_exec_slip_edge_neg_share",
    "share of trades with edge_minus_expected_bps < 0 per sym/bucket (max over lookback)",
    ["sym", "bucket"],
)

# P90: Optional model-vs-expected validation gauges (same cardinality: sym x bucket)
of_exec_slip_model_resid_p95_bps = Gauge(
    "of_exec_slip_model_resid_p95_bps",
    "model residual P95 bps per sym/bucket (max over lookback from DB)",
    ["sym", "bucket"],
)

of_exec_slip_model_resid_p99_bps = Gauge(
    "of_exec_slip_model_resid_p99_bps",
    "model residual P99 bps per sym/bucket (max over lookback from DB)",
    ["sym", "bucket"],
)

of_exec_slip_model_edge_neg_share = Gauge(
    "of_exec_slip_model_edge_neg_share",
    "share of trades with edge_minus_expected_model_bps < 0 per sym/bucket (max over lookback)",
    ["sym", "bucket"],
)


of_exec_slip_db_n = Gauge(
    "of_exec_slip_db_n",
    "number of DB samples per sym/bucket (sum over lookback)",
    ["sym", "bucket"],
)


# Freezer
of_enforce_freezer_block_active = Gauge(
    "of_enforce_freezer_block_active",
    "1 if slo freezer blocked auto-apply (from status)",
    ["sym", "bucket"],
)

of_enforce_freezer_last_block_age_sec = Gauge(
    "of_enforce_freezer_last_block_age_sec",
    "age seconds since last freezer block action",
    ["sym"],
)


class Exporter:
    def __init__(self) -> None:
        self.running = True
        self.redis = _connect_redis()

        self.symbols = _parse_list(os.getenv("ENFORCE_STATE_EXPORTER_SYMBOLS", "") or "")
        self.buckets = _parse_list(
            os.getenv("ENFORCE_STATE_EXPORTER_BUCKETS", "NORMAL,LOW_LIQ,HIGH_VOL,HIGH_VOL_LOW_LIQ")
            or "NORMAL,LOW_LIQ,HIGH_VOL,HIGH_VOL_LOW_LIQ"
        )
        if not self.buckets:
            self.buckets = ["NORMAL", "LOW_LIQ", "HIGH_VOL", "HIGH_VOL_LOW_LIQ"]

        self.promoter_status_path = os.getenv(
            "ENFORCE_PROMOTER_STATUS_PATH",
            "/var/lib/trade/of_reports/out/enforce/promoter/enforce_bucket_promoter_status.json",
        )
        self.promoter_report_key = os.getenv(
            "ENFORCE_PROMOTER_REPORT_KEY", "proposal:enforce_bucket_promotion_report"
        )

        self.exec_slip_stats_status_path = os.getenv(
            "EXEC_SLIP_STATS_STATUS_PATH",
            "/var/lib/trade/of_reports/out/enforce/stats/exec_slip_stats_refresh_status.json",
        )

        self.slippage_cal_status_path = os.getenv(
            "SLIPPAGE_CAL_STATUS_PATH",
            "/var/lib/trade/of_reports/out/enforce/stats/slippage_calibrator_status.json",
        )

        self.freezer_status_path = os.getenv(
            "ENFORCE_FREEZER_STATUS_PATH",
            "/var/lib/trade/of_reports/out/enforce/freezer/enforce_bucket_slo_freezer_status.json",
        )

        self.auto_apply_block_prefix = os.getenv(
            "AUTO_APPLY_BLOCK_PREFIX", "cfg:suggestions:entry_policy:auto_apply_block"
        )
        self.auto_apply_block_reason = os.getenv(
            "ENFORCE_AUTO_APPLY_BLOCK_REASON", "enforce_bucket_promoter"
        ).strip() or "enforce_bucket_promoter"

        # P86: DB-based residual validation (disabled by default, requires ANALYTICS_DB_DSN)
        self.db_stats_enabled = _as_int(os.getenv("ENFORCE_STATE_EXPORTER_DB_STATS", "0"), 0) == 1
        self.db_dsn = os.getenv("ANALYTICS_DB_DSN") or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL")) or ""
        self.db_lookback_h = _as_int(os.getenv("ENFORCE_STATE_EXPORTER_DB_LOOKBACK_H", "4"), 4)
        self.db_mv = (os.getenv("ENFORCE_STATE_EXPORTER_DB_MV", "mv_exec_slippage_eval_1h_stats") or "mv_exec_slippage_eval_1h_stats").strip()
        self.db_view = (os.getenv("ENFORCE_STATE_EXPORTER_DB_VIEW", "v_exec_slippage_eval") or "v_exec_slippage_eval").strip()

        # Export multiple block sources (low-cardinality)
        # Priority: AUTO_APPLY_BLOCK_REASONS_EXPORT > AUTO_APPLY_BLOCK_REASONS > single reason
        reasons_csv = (os.getenv("AUTO_APPLY_BLOCK_REASONS_EXPORT")
                       or os.getenv("AUTO_APPLY_BLOCK_REASONS")
                       or self.auto_apply_block_reason)
        self.auto_apply_block_reasons = _parse_list(reasons_csv) or [self.auto_apply_block_reason]

        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, signum, frame) -> None:
        self.running = False

    def _read_allowlist(self, key: str) -> str:
        if not self.redis:
            return ""
        try:
            v = self.redis.get(key)
            return (v or "")
        except Exception:
            return ""

    def _set_allow_flags(self, *, component: str, sym: str, allow: str) -> None:
        xs = set(_parse_list(allow))
        for b in self.buckets:
            of_enforce_bucket_flag.labels(component=component, sym=sym, bucket=b).set(1.0 if b in xs else 0.0)

    def _export_coeffs(self) -> None:
        if not self.redis or not self.symbols:
            return
        for sym in self.symbols:
            for b in self.buckets:
                k = f"cfg:slippage_decomp_impact_coeff_bps:{sym}:{b}"
                try:
                    v = self.redis.get(k)
                    if v is None or str(v).strip() == "":
                        continue
                    of_slippage_decomp_impact_coeff_bps.labels(sym=sym, bucket=b).set(_as_float(v, 0.0))

                    # V8: age via companion timestamp key written by nightly_slippage_calibrator_v1
                    # Key: cfg:slippage_decomp_impact_coeff_bps_ts_ms:{sym}:{bucket}
                    try:
                        ts_key = f"cfg:slippage_decomp_impact_coeff_bps_ts_ms:{sym}:{b}"
                        ts_ms = _as_int(self.redis.get(ts_key), 0)
                        if ts_ms > 0:
                            of_slippage_decomp_impact_coeff_age_sec.labels(sym=sym, bucket=b).set((_now_ms() - ts_ms) / 1000.0)
                    except Exception:
                        pass
                except Exception:
                    continue

    def _read_promoter_report(self) -> dict[str, Any] | None:
        # Prefer file (cheap), fallback to redis report key
        obj = _load_json(self.promoter_status_path)
        if isinstance(obj, dict):
            return obj
        if not self.redis:
            return None
        try:
            raw = self.redis.get(self.promoter_report_key)
            if not raw:
                return None
            rr = json.loads(raw)
            return rr if isinstance(rr, dict) else None
        except Exception:
            return None

    def _export_promoter(self) -> None:
        rep = self._read_promoter_report()
        if not rep:
            return
        ts_ms = _as_int(rep.get("ts_ms", 0), 0)
        age_sec = float((_now_ms() - ts_ms) / 1000.0) if ts_ms > 0 else float("nan")
        of_enforce_promoter_report_age_sec.set(age_sec)
        of_enforce_promoter_apply_enabled.set(1.0 if (rep.get("apply") or "0") in ("1", "true", "True") else 0.0)

        decisions = rep.get("decisions") if isinstance(rep.get("decisions"), dict) else {}
        for comp in ("slippage", "taker"):
            d = decisions.get(comp) if isinstance(decisions, dict) else None
            ok = 1.0 if isinstance(d, dict) and bool(d.get("ok")) else 0.0
            of_enforce_promoter_last_ok.labels(component=comp).set(ok)

            added = ""
            if isinstance(d, dict):
                added = _as_str(d.get("added"), "").strip().upper()
            for b in self.buckets:
                of_enforce_promoter_last_added_bucket.labels(component=comp, bucket=b).set(1.0 if (added and b == added) else 0.0)

        bh = rep.get("bucket_health") if isinstance(rep.get("bucket_health"), dict) else {}
        for b in self.buckets:
            h = bh.get(b) if isinstance(bh, dict) else None
            if not isinstance(h, dict):
                continue
            of_enforce_promoter_bucket_db_n.labels(bucket=b).set(_as_float(h.get("db_n"), 0.0))
            of_enforce_promoter_bucket_resid_p95_bps.labels(bucket=b).set(_as_float(h.get("resid_p95"), 0.0))
            of_enforce_promoter_bucket_resid_p99_bps.labels(bucket=b).set(_as_float(h.get("resid_p99"), 0.0))
            # P89: read edge_neg_share from promoter bucket_health status JSON
            of_enforce_promoter_bucket_edge_neg_share.labels(bucket=b).set(_as_float(h.get("edge_neg_share"), 0.0))
            of_enforce_promoter_bucket_gate_n.labels(bucket=b).set(_as_float(h.get("gate_n"), 0.0))
            of_enforce_promoter_bucket_ok_soft_rate.labels(bucket=b).set(_as_float(h.get("ok_soft_rate"), 0.0))

        # State keys (apply/rollback)
        if self.redis:
            try:
                apply_ts = _as_int(self.redis.get("state:enforce_bucket_promoter:last_apply_ts_ms"), 0)
                rb_ts = _as_int(self.redis.get("state:enforce_bucket_promoter:last_rollback_ts_ms"), 0)
                of_enforce_promoter_last_apply_ts_ms.set(float(apply_ts))
                of_enforce_promoter_last_rollback_ts_ms.set(float(rb_ts))
                now_ms = _now_ms()
                of_enforce_promoter_last_apply_age_sec.set((now_ms - apply_ts) / 1000.0 if apply_ts > 0 else float("nan"))
                of_enforce_promoter_last_rollback_age_sec.set((now_ms - rb_ts) / 1000.0 if rb_ts > 0 else float("nan"))
                of_enforce_promoter_rollback_active.set(1.0 if (rb_ts > 0 and apply_ts > 0 and rb_ts > apply_ts) else 0.0)
            except Exception:
                pass

    def _export_exec_slip_stats_refresh(self) -> None:
        obj = _load_json(self.exec_slip_stats_status_path)
        if isinstance(obj, dict):
            ts_ms = _as_int(obj.get("ts_ms", 0), 0)
            dur_ms = _as_int(obj.get("dur_ms", 0), 0)
            ok = bool(obj.get("ok", False))
            of_exec_slip_stats_refresh_last_ok_ts_ms.set(float(ts_ms))
            of_exec_slip_stats_refresh_last_dur_ms.set(float(dur_ms))
            of_exec_slip_stats_refresh_last_ok.set(1.0 if ok else 0.0)
            if ts_ms > 0:
                of_exec_slip_stats_refresh_last_ok_age_sec.set((_now_ms() - ts_ms) / 1000.0)
            return

        # fallback redis state
        if not self.redis:
            return
        try:
            ts_ms = _as_int(self.redis.get("state:exec_slip_stats_refresher:last_ok_ts_ms"), 0)
            dur_ms = _as_int(self.redis.get("state:exec_slip_stats_refresher:last_dur_ms"), 0)
            ok = _as_int(self.redis.get("state:exec_slip_stats_refresher:last_ok"), 0) == 1
            of_exec_slip_stats_refresh_last_ok_ts_ms.set(float(ts_ms))
            of_exec_slip_stats_refresh_last_dur_ms.set(float(dur_ms))
            of_exec_slip_stats_refresh_last_ok.set(1.0 if ok else 0.0)
            if ts_ms > 0:
                of_exec_slip_stats_refresh_last_ok_age_sec.set((_now_ms() - ts_ms) / 1000.0)
        except Exception:
            return

    def _export_slippage_calib_state(self) -> None:
        """Export nightly slippage calibrator state keys (low cardinality, V8/V10).

        Keys written by tick_flow_full.tools.nightly_slippage_calibrator_v1:
          - state:slippage_calib:last_ok_ts_ms      -> of_slippage_calib_last_ok_age_sec
          - state:slippage_calib:last_updated_groups -> of_slippage_calib_last_updated_groups
          - state:slippage_calib:last_groups_total   -> of_slippage_calib_last_groups_total (V10)

        Alert rules SlippageCalibStale / SlippageCalibNoUpdates / SlippageCoeffStaleHVLL consume these.
        """,
        if not self.redis:
            return
        try:
            now_ms = _now_ms()
            ok_ts = _as_int(self.redis.get('state:slippage_calib:last_ok_ts_ms'), 0)
            upd = _as_int(self.redis.get('state:slippage_calib:last_updated_groups'), 0)
            total = _as_int(self.redis.get('state:slippage_calib:last_groups_total'), 0)
            of_slippage_calib_last_updated_groups.set(float(upd))
            of_slippage_calib_last_groups_total.set(float(total))
            of_slippage_calib_last_ok_age_sec.set((now_ms - ok_ts) / 1000.0 if ok_ts > 0 else float("nan"))
        except Exception:
            return

    def _export_exec_slippage_eval_rows_probe(self) -> None:
        """V10: Export exec slippage eval rowcount probe state keys.

        Keys written by exec_slippage_eval_rowcount_probe_p77_v1 (hourly at :13 via of_timers_worker):
          - state:exec_slippage_eval:rows_24h_ts_ms -> of_exec_slippage_eval_rows_24h_age_sec
          - state:exec_slippage_eval:rows_24h (hash: bucket->n) -> of_exec_slippage_eval_rows_24h{bucket}

        Alert rules OF_ExecSlippageEvalRowcountProbeStale_Crit / OF_ExecSlippageEvalRowsLow_Warn fire on these.
        """,
        if not self.redis:
            return
        try:
            now_ms = _now_ms()
            ts_ms = _as_int(self.redis.get("state:exec_slippage_eval:rows_24h_ts_ms"), 0)
            of_exec_slippage_eval_rows_24h_age_sec.set(
                (now_ms - ts_ms) / 1000.0 if ts_ms > 0 else float("nan")
            )
            raw = self.redis.hgetall("state:exec_slippage_eval:rows_24h")
            if isinstance(raw, dict):
                for b in self.buckets:
                    of_exec_slippage_eval_rows_24h.labels(bucket=b).set(
                        float(_as_int(raw.get(b, 0), 0))
                    )
        except Exception:
            return

    def _export_slippage_calibrator_status(self) -> None:
        """Export slippage calibrator status (file preferred, fallback to Redis state keys).""",
        obj = _load_json(self.slippage_cal_status_path)
        if isinstance(obj, dict):
            ts_ms = _as_int(obj.get("ts_ms", 0), 0)
            dur_ms = _as_int(obj.get("dur_ms", 0), 0)
            ok = bool(obj.get("ok", False))
            of_slippage_calibrator_last_ok_ts_ms.set(float(ts_ms))
            of_slippage_calibrator_last_dur_ms.set(float(dur_ms))
            of_slippage_calibrator_last_ok.set(1.0 if ok else 0.0)
            if ts_ms > 0:
                of_slippage_calibrator_last_ok_age_sec.set((_now_ms() - ts_ms) / 1000.0)
            else:
                of_slippage_calibrator_last_ok_age_sec.set(float("nan"))
            return

        if not self.redis:
            return
        try:
            ts_ms = _as_int(self.redis.get("state:slippage_calibrator:last_ok_ts_ms"), 0)
            dur_ms = _as_int(self.redis.get("state:slippage_calibrator:last_dur_ms"), 0)
            ok = _as_int(self.redis.get("state:slippage_calibrator:last_ok"), 0) == 1
            of_slippage_calibrator_last_ok_ts_ms.set(float(ts_ms))
            of_slippage_calibrator_last_dur_ms.set(float(dur_ms))
            of_slippage_calibrator_last_ok.set(1.0 if ok else 0.0)
            if ts_ms > 0:
                of_slippage_calibrator_last_ok_age_sec.set((_now_ms() - ts_ms) / 1000.0)
            else:
                of_slippage_calibrator_last_ok_age_sec.set(float("nan"))
        except Exception:
            return

    def _export_exec_slip_residual_stats(self) -> None:
        """P86: Query DB for slippage residual/edge distribution per regime bucket.

        Prefer materialized view mv_exec_slippage_eval_1h_stats (pre-aggregated hourly),
        fallback to raw view v_exec_slippage_eval.
        Disabled by default; enable via ENFORCE_STATE_EXPORTER_DB_STATS=1.
        """,
        if not self.db_stats_enabled:
            return
        if not self.db_dsn:
            return
        if not self.symbols:
            return

        # Import lazily to keep exporter usable without DB deps.
        try:
            import psycopg2  # type: ignore
        except Exception:
            of_exec_slip_stats_db_up.set(0.0)
            return

        syms = [_safe_ident(x, "") for x in self.symbols]
        syms = [x for x in syms if x]
        if not syms:
            return

        mv = self.db_mv or "mv_exec_slippage_eval_1h_stats"
        view = self.db_view or "v_exec_slippage_eval"
        lookback_h = max(1, int(self.db_lookback_h))

        t0 = _now_ms()
        ok = False
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(self.db_dsn)
            cur = conn.cursor()

            rows = []
            has_model = False

            # Prefer pre-aggregated MV (fast, uses t column).
            # Backward compatible: if MV lacks model columns, fall back to legacy query,
            # then to raw view queries.
            try:
                cur.execute(
                    f""",
                    select sym,
                           exec_regime_bucket,
                           sum(n)::bigint as n,
                           max(resid_p95_bps) as resid_p95_bps,
                           max(resid_p99_bps) as resid_p99_bps,
                           max(edge_neg_share) as edge_neg_share,
                           max(resid_model_p95_bps) as resid_model_p95_bps,
                           max(resid_model_p99_bps) as resid_model_p99_bps,
                           max(edge_neg_share_model) as edge_neg_share_model
                    from {mv}
                    where sym = any(%s) and t >= now() - (%s || ' hours')::interval
                    group by 1,2,
                    """
                    (syms, int(lookback_h)),
                )
                rows = cur.fetchall()
                has_model = True
            except Exception:
                try:
                    cur.execute(
                        f""",
                        select sym,
                               exec_regime_bucket,
                               sum(n)::bigint as n,
                               max(resid_p95_bps) as resid_p95_bps,
                               max(resid_p99_bps) as resid_p99_bps,
                               max(edge_neg_share) as edge_neg_share
                        from {mv}
                        where sym = any(%s) and t >= now() - (%s || ' hours')::interval
                        group by 1,2,
                        """
                        (syms, int(lookback_h)),
                    )
                    rows = cur.fetchall()
                    has_model = False
                except Exception:
                    try:
                        cur.execute(
                            f""",
                            select sym,
                                   exec_regime_bucket,
                                   count(*)::bigint as n,
                                   percentile_cont(0.95) within group (order by slippage_residual_bps) as resid_p95_bps,
                                   percentile_cont(0.99) within group (order by slippage_residual_bps) as resid_p99_bps,
                                   avg(case when edge_minus_expected_bps < 0 then 1 else 0 end) as edge_neg_share,
                                   percentile_cont(0.95) within group (order by slippage_residual_model_bps) as resid_model_p95_bps,
                                   percentile_cont(0.99) within group (order by slippage_residual_model_bps) as resid_model_p99_bps,
                                   avg(case when edge_minus_expected_model_bps < 0 then 1 else 0 end) as edge_neg_share_model
                            from {view}
                            where sym = any(%s) and ts >= now() - (%s || ' hours')::interval
                            group by 1,2,
                            """
                            (syms, int(lookback_h)),
                        )
                        rows = cur.fetchall()
                        has_model = True
                    except Exception:
                        cur.execute(
                            f""",
                            select sym,
                                   exec_regime_bucket,
                                   count(*)::bigint as n,
                                   percentile_cont(0.95) within group (order by slippage_residual_bps) as resid_p95_bps,
                                   percentile_cont(0.99) within group (order by slippage_residual_bps) as resid_p99_bps,
                                   avg(case when edge_minus_expected_bps < 0 then 1 else 0 end) as edge_neg_share
                            from {view}
                            where sym = any(%s) and ts >= now() - (%s || ' hours')::interval
                            group by 1,2,
                            """
                            (syms, int(lookback_h)),
                        )
                        rows = cur.fetchall()
                        has_model = False

            # Reset series within configured label space to prevent stale values.
            for sym in syms:
                for b in self.buckets:
                    of_exec_slip_db_n.labels(sym=sym, bucket=b).set(float("nan"))
                    of_exec_slip_resid_p95_bps.labels(sym=sym, bucket=b).set(float("nan"))
                    of_exec_slip_resid_p99_bps.labels(sym=sym, bucket=b).set(float("nan"))
                    of_exec_slip_edge_neg_share.labels(sym=sym, bucket=b).set(float("nan"))
                    of_exec_slip_model_resid_p95_bps.labels(sym=sym, bucket=b).set(float("nan"))
                    of_exec_slip_model_resid_p99_bps.labels(sym=sym, bucket=b).set(float("nan"))
                    of_exec_slip_model_edge_neg_share.labels(sym=sym, bucket=b).set(float("nan"))

            for row in rows:
                # row can be legacy (6 cols) or extended (9 cols)
                if row is None:
                    continue
                if len(row) >= 9:
                    sym, bucket, n, p95, p99, neg, mp95, mp99, mneg = row[:9]
                    has_model_row = True
                else:
                    sym, bucket, n, p95, p99, neg = row[:6]
                    mp95 = mp99 = mneg = None
                    has_model_row = False
                s_sym = _safe_ident(sym, "")
                s_bucket = _safe_ident(bucket, "NORMAL") or "NORMAL"
                if not s_sym or s_sym not in syms:
                    continue
                if s_bucket not in self.buckets:
                    continue
                of_exec_slip_db_n.labels(sym=s_sym, bucket=s_bucket).set(float(n or 0))
                of_exec_slip_resid_p95_bps.labels(sym=s_sym, bucket=s_bucket).set(_as_float(p95, 0.0))
                of_exec_slip_resid_p99_bps.labels(sym=s_sym, bucket=s_bucket).set(_as_float(p99, 0.0))
                of_exec_slip_edge_neg_share.labels(sym=s_sym, bucket=s_bucket).set(_as_float(neg, 0.0))
                # Optional model columns
                if has_model and has_model_row:
                    of_exec_slip_model_resid_p95_bps.labels(sym=s_sym, bucket=s_bucket).set(_as_float(mp95, 0.0))
                    of_exec_slip_model_resid_p99_bps.labels(sym=s_sym, bucket=s_bucket).set(_as_float(mp99, 0.0))
                    of_exec_slip_model_edge_neg_share.labels(sym=s_sym, bucket=s_bucket).set(_as_float(mneg, 0.0))

            ok = True
        except Exception:
            ok = False
        finally:
            of_exec_slip_stats_db_up.set(1.0 if ok else 0.0)
            of_exec_slip_stats_db_query_dur_ms.set(float(max(0, _now_ms() - t0)))
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
            try:
                if conn is not None:
                    with contextlib.suppress(Exception):
                        conn.rollback()
                    conn.close()
            except Exception:
                pass

    def _export_auto_apply_block(self) -> None:
        if not self.redis:
            return
        try:
            # Loop over all configured sources (multi-source, low-cardinality)
            reasons = getattr(self, "auto_apply_block_reasons", None) or [self.auto_apply_block_reason]
            for src in reasons:
                block_key = f"{self.auto_apply_block_prefix}:{src}"
                meta_key = f"{self.auto_apply_block_prefix}:{src}:meta"
                ts_key = f"{self.auto_apply_block_prefix}:{src}:ts_ms"

                v = self.redis.get(block_key)
                active = 1 if (v is not None and str(v).strip() not in ("", "0")) else 0
                ts_ms = _as_int(self.redis.get(ts_key), 0)

                cause = "unknown"
                raw = self.redis.get(meta_key)
                if raw:
                    try:
                        obj = json.loads(raw)
                        if isinstance(obj, dict):
                            cause = _norm_cause(obj.get("reason", ""))
                    except Exception:
                        pass

                auto_apply_block_active.labels(source=src, cause=cause).set(float(active))
                auto_apply_block_age_sec.labels(source=src).set(
                    (_now_ms() - ts_ms) / 1000.0 if (active and ts_ms > 0) else float("nan")
                )
        except Exception:
            return

    def _export_prom_rules_bundle_health(self) -> None:
        """Export Prometheus rules bundle validation state keys (nightly smoke-check).

        Keys written by prom_rules_bundle_health_check_v1 (hourly at :09 via of_timers_worker):
          - state:prom_rules_bundle:last_ok      -> of_prom_rules_bundle_last_ok
          - state:prom_rules_bundle:last_ok_ts_ms -> of_prom_rules_bundle_last_ok_age_sec
          - state:prom_rules_bundle:last_files_checked -> of_prom_rules_bundle_last_files_checked
          - state:prom_rules_bundle:last_error_n -> of_prom_rules_bundle_last_error_n,
        """,
        if not self.redis:
            return
        prefix = (os.getenv("PROM_RULES_BUNDLE_STATE_PREFIX") or "state:prom_rules_bundle").strip() or "state:prom_rules_bundle"
        try:
            ok = _as_int(self.redis.get(f"{prefix}:last_ok"), 0)
            ok_ts_ms = _as_int(self.redis.get(f"{prefix}:last_ok_ts_ms"), 0)
            run_ts_ms = _as_int(self.redis.get(f"{prefix}:last_run_ts_ms"), 0)
            files_checked = _as_int(self.redis.get(f"{prefix}:last_files_checked"), 0)
            err_n = _as_int(self.redis.get(f"{prefix}:last_error_n"), 0)

            of_prom_rules_bundle_last_ok.set(float(1 if ok else 0))
            of_prom_rules_bundle_last_files_checked.set(float(files_checked))
            of_prom_rules_bundle_last_error_n.set(float(err_n))
            if ok_ts_ms > 0:
                of_prom_rules_bundle_last_ok_age_sec.set((_now_ms() - ok_ts_ms) / 1000.0)
            else:
                of_prom_rules_bundle_last_ok_age_sec.set(float("nan"))
        except Exception:
            return


    def _export_prom_rules_loaded_probe_health(self) -> None:
        """Export runtime rules-loaded probe state keys.

        Keys written by prom_rules_loaded_probe_v1 (hourly at :10 via of_timers_worker):
          - state:prom_rules_loaded:last_ok        -> rules_loaded_probe_last_ok
          - state:prom_rules_loaded:last_ok_ts_ms  -> rules_loaded_probe_last_ok_age_sec
          - state:prom_rules_loaded:files_expected -> rules_files_expected
          - state:prom_rules_loaded:files_loaded   -> rules_files_loaded
          - state:prom_rules_loaded:missing_n      -> rules_files_missing

        This detects a different failure mode than promtool/validator:
          - promtool validates syntax
          - this probe validates Prometheus include-list correctness ("file not picked up")
        """,
        if not self.redis:
            return
        prefix = (os.getenv("PROM_RULES_LOADED_STATE_PREFIX") or "state:prom_rules_loaded").strip() or "state:prom_rules_loaded"
        try:
            ok = _as_int(self.redis.get(f"{prefix}:last_ok"), 0)
            ok_ts_ms = _as_int(self.redis.get(f"{prefix}:last_ok_ts_ms"), 0)
            run_ts_ms = _as_int(self.redis.get(f"{prefix}:last_run_ts_ms"), 0)
            exp_n = _as_int(self.redis.get(f"{prefix}:files_expected"), 0)
            loaded_n = _as_int(self.redis.get(f"{prefix}:files_loaded"), 0)
            miss_n = _as_int(self.redis.get(f"{prefix}:missing_n"), 0)

            rules_loaded_probe_last_ok.set(float(1 if ok else 0))

            if run_ts_ms > 0:
                rules_loaded_probe_last_run_age_sec.set((_now_ms() - run_ts_ms) / 1000.0)
            else:
                rules_loaded_probe_last_run_age_sec.set(float("nan"))

            rules_files_expected.set(float(exp_n))
            rules_files_loaded.set(float(loaded_n))
            rules_files_missing.set(float(miss_n))

            if ok_ts_ms > 0:
                rules_loaded_probe_last_ok_age_sec.set((_now_ms() - ok_ts_ms) / 1000.0)
            else:
                rules_loaded_probe_last_ok_age_sec.set(float("nan"))
        except Exception:
            return


    def _export_freezer_status(self) -> None:
        obj = _load_json(self.freezer_status_path)
        if not isinstance(obj, dict):
            return
        if not bool(obj.get("enabled", True)):
            return
        ts_ms = _as_int(obj.get("ts_ms", 0), 0)
        blocked = bool(obj.get("blocked", False))
        sym = _as_str(obj.get("sym", ""), "").strip().upper()
        bucket = _as_str(obj.get("bucket", ""), "").strip().upper()
        if not sym:
            return
        if not bucket:
            bucket = "NORMAL"
        of_enforce_freezer_block_active.labels(sym=sym, bucket=bucket).set(1.0 if blocked else 0.0)
        if ts_ms > 0:
            of_enforce_freezer_last_block_age_sec.labels(sym=sym).set((_now_ms() - ts_ms) / 1000.0)


    def tick(self) -> None:
        # export enforce buckets (global + optional per-symbol overrides)
        slip_glob = self._read_allowlist("cfg:slippage_decomp_enforce_buckets")
        taker_glob = self._read_allowlist("cfg:taker_flow_gate_enforce_buckets")
        self._set_allow_flags(component="slippage", sym="global", allow=slip_glob)
        self._set_allow_flags(component="taker_flow", sym="global", allow=taker_glob)

        for sym in self.symbols:
            s = self._read_allowlist(f"cfg:slippage_decomp_enforce_buckets:{sym}")
            t = self._read_allowlist(f"cfg:taker_flow_gate_enforce_buckets:{sym}")
            if s:
                self._set_allow_flags(component="slippage", sym=sym, allow=s)
            if t:
                self._set_allow_flags(component="taker_flow", sym=sym, allow=t)

        self._export_coeffs()
        # V8/V10: export calibrator state keys (staleness age + groups-updated counters)
        self._export_slippage_calib_state()
        # V10: export exec slippage eval rowcount probe state (hourly probe results)
        self._export_exec_slippage_eval_rows_probe()
        self._export_promoter()
        self._export_exec_slip_stats_refresh()
        self._export_exec_slip_residual_stats()
        self._export_slippage_calibrator_status()
        self._export_prom_rules_bundle_health()
        self._export_prom_rules_loaded_probe_health()
        self._export_auto_apply_block()
        self._export_freezer_status()


def main() -> None:
    port = _as_int(os.getenv("ENFORCE_STATE_EXPORTER_PORT", "9142"), 9142)
    refresh = _as_int(os.getenv("ENFORCE_STATE_EXPORTER_REFRESH_SEC", "10"), 10)

    start_http_server(port)
    ex = Exporter()

    of_enforce_state_exporter_up.set(1.0)
    # P90: cumulative error counter — monotonically increases so Grafana can detect flapping.
    err_total = 0
    while ex.running:
        try:
            ex.tick()
            of_enforce_state_exporter_up.set(1.0)
        except Exception:
            # fail-open: exporter keeps running even on unhandled tick error
            err_total += 1
            of_enforce_state_exporter_up.set(0.0)
        # P90: update liveness ts even if tick failed — lets Prometheus staleness alert
        # detect a fully frozen exporter (process alive but loop blocked).
        of_enforce_state_exporter_poll_ts_ms.set(float(_now_ms()))
        of_enforce_state_exporter_errors_total.set(float(err_total))
        time.sleep(max(1, refresh))


if __name__ == "__main__":
    main()
