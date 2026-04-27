"""
Signal Quality Exporter (v3)

Exports:
  - Global KPIs from cfg2 (settings:dynamic_cfg)
  - Breakdown by (drift_mode,dq_state) from hash
  - Breakdown by (meta_enforce_cov_bucket, meta_enforce_applied) from hash

Does NOT export reason breakdown to Prometheus (cardinality).
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict

import redis
from prometheus_client import Gauge, start_http_server

def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else default

def _env_int(name: str, default: str) -> int:
    try:
        return int(_env(name, default))
    except Exception:
        return int(default)

def _as_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0

def _loads(s: Any) -> Dict[str, Any]:
    if not s:
        return {}
    if isinstance(s, dict):
        return s
    try:
        d = json.loads(s)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}

# Global gauges
g_expect = Gauge("signal_quality_expectancy_r_24h", "Expectancy R over 24h")
g_prec  = Gauge("signal_quality_precision_top5p_24h", "Precision@top5% over 24h")
g_ece   = Gauge("signal_quality_ece_24h", "ECE over 24h")
g_n     = Gauge("signal_quality_n_24h", "N closed trades over 24h")
g_last  = Gauge("signal_quality_last_ts_ms", "Last close ts used in KPIs (ms)")
g_stale = Gauge("signal_quality_staleness_sec", "Staleness of KPIs (sec)")

# By-mode gauges
gm_expect = Gauge("signal_quality_expectancy_r_24h_by_mode", "Expectancy R by drift_mode,dq_state", ["drift_mode","dq_state"])
gm_prec   = Gauge("signal_quality_precision_top5p_24h_by_mode", "Precision@top5% by drift_mode,dq_state", ["drift_mode","dq_state"])
gm_ece    = Gauge("signal_quality_ece_24h_by_mode", "ECE by drift_mode,dq_state", ["drift_mode","dq_state"])
gm_n      = Gauge("signal_quality_n_24h_by_mode", "N by drift_mode,dq_state", ["drift_mode","dq_state"])

# By-bucket gauges
gb_expect = Gauge("signal_quality_expectancy_r_24h_by_bucket", "Expectancy R by cov_bucket,applied", ["cov_bucket","applied"])
gb_prec   = Gauge("signal_quality_precision_top5p_24h_by_bucket", "Precision@top5% by cov_bucket,applied", ["cov_bucket","applied"])
gb_ece    = Gauge("signal_quality_ece_24h_by_bucket", "ECE by cov_bucket,applied", ["cov_bucket","applied"])
gb_n      = Gauge("signal_quality_n_24h_by_bucket", "N by cov_bucket,applied", ["cov_bucket","applied"])

# Policy effectiveness (P71): computed by orderflow_services/policy_effectiveness_report_worker_v1.py
# and stored in settings:dynamic_cfg (dyn_cfg_key).
g_pe_last_ts_ms = Gauge("policy_effectiveness_last_ts_ms", "Last policy effectiveness report timestamp (epoch ms)")
g_pe_last_age_seconds = Gauge("policy_effectiveness_last_age_seconds", "Age of policy effectiveness report (seconds)")
g_pe_input_last_ts_ms = Gauge("policy_effectiveness_input_last_ts_ms", "Last input timestamp used by report (epoch ms)")
g_pe_input_age_seconds = Gauge("policy_effectiveness_input_age_seconds", "Age of last input timestamp used by report (seconds)")
g_pe_total_n_24h = Gauge("policy_effectiveness_total_n_24h", "Total decisions in last 24h used for policy effectiveness report")
g_pe_baseline_ok_present = Gauge("policy_effectiveness_baseline_ok_present", "Whether OK baseline was present (1/0)")
g_pe_share_24h = Gauge("policy_effectiveness_share_24h", "Share of effective_mode in last 24h", ["mode"])
g_pe_expectancy_r_delta_24h = Gauge("policy_effectiveness_expectancy_r_delta_24h", "Expectancy(R) delta vs OK baseline in last 24h", ["mode"])
g_pe_precision_top5p_delta_24h = Gauge("policy_effectiveness_precision_top5p_delta_24h", "Precision@top5% delta vs OK baseline in last 24h", ["mode"])
g_pe_ece_delta_24h = Gauge("policy_effectiveness_ece_delta_24h", "ECE delta vs OK baseline in last 24h (positive = worse calibration)", ["mode"])

def _parse_kv(key: str) -> Dict[str,str]:
    out: Dict[str,str] = {}
    for part in (key or "").split("|"):
        if "=" in part:
            k,v = part.split("=",1)
            out[k.strip()] = v.strip()
    return out

def main() -> None:
    redis_url = _env("REDIS_URL", "redis://localhost:6379/0")
    dyn_cfg_key = _env("DYN_CFG_KEY", "settings:dynamic_cfg")

    by_mode_key = _env("SIGNAL_QUALITY_BY_MODE_HASH", "metrics:signal_quality:24h:by_mode")
    by_bucket_key = _env("SIGNAL_QUALITY_BY_BUCKET_HASH", "metrics:signal_quality:24h:by_bucket")

    port = _env_int("SIGNAL_QUALITY_EXPORTER_PORT", "9135")
    interval_s = _env_int("SIGNAL_QUALITY_EXPORTER_INTERVAL_S", "15")
    min_n = _env_int("SIGNAL_QUALITY_EXPORTER_MIN_N", "30")

    cli = redis.Redis.from_url(redis_url, decode_responses=True)
    start_http_server(port)

    while True:
        try:
            cfg = cli.hgetall(dyn_cfg_key)
            now_ms = get_ny_time_millis()

            n = int(float(cfg.get("signal_quality_n_24h") or 0))
            last_ts = int(float(cfg.get("signal_quality_last_ts_ms") or 0))
            expect = _as_float(cfg.get("signal_quality_expectancy_r_24h"))
            prec = _as_float(cfg.get("signal_quality_precision_top5p_24h"))
            ece = cfg.get("signal_quality_ece_24h")
            ece_v = _as_float(ece) if ece is not None else 0.0

            g_n.set(n)
            g_last.set(last_ts)
            g_stale.set(max(0.0, (now_ms - last_ts)/1000.0) if last_ts > 0 else 0.0)

            if n >= min_n:
                g_expect.set(expect)
                g_prec.set(prec)
                if ece is not None:
                    g_ece.set(ece_v)

            # by_mode hash
            hm = cli.hgetall(by_mode_key)
            for k, v in hm.items():
                kv = _parse_kv(k)
                dm = kv.get("mode","unknown")
                dq = kv.get("dq","unknown")
                obj = _loads(v)
                if int(obj.get("n") or 0) < min_n:
                    continue
                gm_n.labels(dm,dq).set(int(obj.get("n") or 0))
                if obj.get("expectancy_r") is not None:
                    gm_expect.labels(dm,dq).set(float(obj["expectancy_r"]))
                if obj.get("precision_top5p") is not None:
                    gm_prec.labels(dm,dq).set(float(obj["precision_top5p"]))
                if obj.get("ece") is not None:
                    gm_ece.labels(dm,dq).set(float(obj["ece"]))

            # by_bucket hash
            hb = cli.hgetall(by_bucket_key)
            for k, v in hb.items():
                kv = _parse_kv(k)
                b = kv.get("bucket","na")
                a = kv.get("applied","na")
                obj = _loads(v)
                if int(obj.get("n") or 0) < min_n:
                    continue
                gb_n.labels(b,a).set(int(obj.get("n") or 0))
                if obj.get("expectancy_r") is not None:
                    gb_expect.labels(b,a).set(float(obj["expectancy_r"]))
                if obj.get("precision_top5p") is not None:
                    gb_prec.labels(b,a).set(float(obj["precision_top5p"]))
                if obj.get("ece") is not None:
                    gb_ece.labels(b,a).set(float(obj["ece"]))

            # --- Policy effectiveness (P71) ---
            pe_last = int(float(cfg.get("policy_effectiveness_last_ts_ms") or 0))
            g_pe_last_ts_ms.set(pe_last)
            if pe_last > 0:
                g_pe_last_age_seconds.set(max(0.0, (now_ms - pe_last) / 1000.0))
            else:
                g_pe_last_age_seconds.set(0.0)

            pe_in_last = int(float(cfg.get("policy_effectiveness_input_last_ts_ms") or 0))
            g_pe_input_last_ts_ms.set(pe_in_last)
            if pe_in_last > 0:
                g_pe_input_age_seconds.set(max(0.0, (now_ms - pe_in_last) / 1000.0))
            else:
                g_pe_input_age_seconds.set(0.0)

            g_pe_total_n_24h.set(int(float(cfg.get("policy_effectiveness_total_n_24h") or 0)))
            ok_present = str(cfg.get("policy_effectiveness_baseline_ok_present") or "0").lower().strip()
            g_pe_baseline_ok_present.set(1.0 if ok_present in ("1", "true", "yes", "y") else 0.0)

            for mode in ("ok", "warn", "block", "unknown"):
                g_pe_share_24h.labels(mode=mode).set(float(cfg.get(f"policy_effectiveness_share_24h_{mode}") or 0.0))
                g_pe_expectancy_r_delta_24h.labels(mode=mode).set(float(cfg.get(f"policy_effectiveness_expectancy_r_delta_24h_{mode}") or 0.0))
                g_pe_precision_top5p_delta_24h.labels(mode=mode).set(float(cfg.get(f"policy_effectiveness_precision_top5p_delta_24h_{mode}") or 0.0))
                g_pe_ece_delta_24h.labels(mode=mode).set(float(cfg.get(f"policy_effectiveness_ece_delta_24h_{mode}") or 0.0))

        except Exception:
            pass
        time.sleep(interval_s)

if __name__ == "__main__":
    main()
