#!/usr/bin/env python3
from __future__ import annotations

"""Prometheus exporter for nightly feature drift batch report (PSI/KS).

Source of truth:
- Redis hash `metrics:feature_drift_batch:last`
- report JSON path recorded in Redis field `report_json`

Low-cardinality summary metrics are always exported. Per-feature metrics are
exported only for the features present in the report JSON (expected Tier-1 set).
"""

import json
import os
import time
from typing import Any, Dict, Iterable, List, Mapping

import redis  # type: ignore
from prometheus_client import Gauge, start_http_server


def _now_s() -> float:
    return time.time()


def _as_float(v: Any, d: float = 0.0) -> float:
    try:
        if v is None:
            return float(d)
        return float(v)
    except Exception:
        return float(d)


def _as_int(v: Any, d: int = 0) -> int:
    try:
        if v is None:
            return int(d)
        return int(float(v))
    except Exception:
        return int(d)


def _read_report(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


UP = Gauge("feature_drift_batch_exporter_up", "1 if exporter can read Redis summary")
LAST_SUCCESS = Gauge("feature_drift_batch_last_success", "1 if latest drift batch status is ok")
LAST_UPDATED_MS = Gauge("feature_drift_batch_last_updated_ts_ms", "updated_ts_ms from Redis hash")
AGE_S = Gauge("feature_drift_batch_last_age_seconds", "Age of latest drift-batch summary")
FEATURES_TOTAL = Gauge("feature_drift_batch_features_total", "Total features considered")
FEATURES_EVAL = Gauge("feature_drift_batch_features_evaluated", "Features evaluated")
WARN_N = Gauge("feature_drift_batch_warn_n", "Warn-level feature drift count")
CRIT_N = Gauge("feature_drift_batch_crit_n", "Crit-level feature drift count")
DENY_N = Gauge("feature_drift_batch_denylist_suggest_n", "Features suggested for denylist AB")
SHADOW_N = Gauge("feature_drift_batch_shadow_disable_suggest_n", "Features suggested for shadow disable")
WORST_PSI = Gauge("feature_drift_batch_worst_psi", "Worst PSI in latest report")
WORST_KS = Gauge("feature_drift_batch_worst_ks_stat", "Worst KS stat in latest report")

FEATURE_PSI = Gauge("feature_drift_batch_feature_psi", "Per-feature PSI", ["feature"])
FEATURE_KS = Gauge("feature_drift_batch_feature_ks_stat", "Per-feature KS statistic", ["feature"])
FEATURE_KS_P = Gauge("feature_drift_batch_feature_ks_pvalue", "Per-feature KS p-value", ["feature"])
FEATURE_FLAG = Gauge("feature_drift_batch_feature_flag", "Per-feature drift flags", ["feature", "kind"])
FEATURE_DELTA = Gauge("feature_drift_batch_feature_delta", "Per-feature missing/zero/clip deltas", ["feature", "kind"])


def _clear_features(features: Iterable[str]) -> None:
    for f in features:
        for kind in ("warn", "crit", "denylist", "shadow_disable"):
            FEATURE_FLAG.labels(feature=f, kind=kind).set(0)
        for kind in ("missing_rate", "zero_rate", "clip_rate"):
            FEATURE_DELTA.labels(feature=f, kind=kind).set(0.0)
        FEATURE_PSI.labels(feature=f).set(0.0)
        FEATURE_KS.labels(feature=f).set(0.0)
        FEATURE_KS_P.labels(feature=f).set(1.0)


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    metrics_key = os.getenv("FEATURE_DRIFT_BATCH_METRICS_KEY", "metrics:feature_drift_batch:last")
    port = int(os.getenv("FEATURE_DRIFT_BATCH_EXPORTER_PORT", "9832"))
    interval_s = float(os.getenv("FEATURE_DRIFT_BATCH_EXPORTER_INTERVAL_S", "10"))
    stale_s = float(os.getenv("FEATURE_DRIFT_BATCH_EXPORTER_STALE_S", str(36 * 3600)))

    r = redis.Redis.from_url(redis_url, decode_responses=True)
    start_http_server(port)

    seen_features: List[str] = []
    while True:
        try:
            m: Dict[str, Any] = r.hgetall(metrics_key) or {}
            UP.set(1)

            status = str(m.get("status", "") or "")
            LAST_SUCCESS.set(1.0 if status == "ok" else 0.0)
            updated_ms = _as_int(m.get("updated_ts_ms", 0), 0)
            LAST_UPDATED_MS.set(float(updated_ms))
            age = max(0.0, _now_s() - (float(updated_ms) / 1000.0 if updated_ms > 0 else 0.0))
            AGE_S.set(age)
            if age > stale_s:
                LAST_SUCCESS.set(0.0)

            FEATURES_TOTAL.set(float(_as_int(m.get("features_total", 0), 0)))
            FEATURES_EVAL.set(float(_as_int(m.get("features_evaluated", 0), 0)))
            WARN_N.set(float(_as_int(m.get("warn_n", 0), 0)))
            CRIT_N.set(float(_as_int(m.get("crit_n", 0), 0)))
            DENY_N.set(float(_as_int(m.get("denylist_suggest_n", 0), 0)))
            SHADOW_N.set(float(_as_int(m.get("shadow_disable_suggest_n", 0), 0)))
            WORST_PSI.set(float(_as_float(m.get("worst_psi", 0.0), 0.0)))
            WORST_KS.set(float(_as_float(m.get("worst_ks_stat", 0.0), 0.0)))

            rep = _read_report(str(m.get("report_json", "") or ""))
            rows = rep.get("features") if isinstance(rep.get("features"), list) else []
            current_features: List[str] = []
            _clear_features(seen_features)
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                f = str(row.get("feature", "") or "")
                if not f:
                    continue
                current_features.append(f)
                FEATURE_PSI.labels(feature=f).set(float(_as_float(row.get("psi", 0.0), 0.0)))
                FEATURE_KS.labels(feature=f).set(float(_as_float(row.get("ks_stat", 0.0), 0.0)))
                FEATURE_KS_P.labels(feature=f).set(float(_as_float(row.get("ks_pvalue", 1.0), 1.0)))
                FEATURE_FLAG.labels(feature=f, kind="warn").set(float(_as_int(row.get("flag_warn", 0), 0)))
                FEATURE_FLAG.labels(feature=f, kind="crit").set(float(_as_int(row.get("flag_crit", 0), 0)))
                FEATURE_FLAG.labels(feature=f, kind="denylist").set(float(_as_int(row.get("denylist_suggested", 0), 0)))
                FEATURE_FLAG.labels(feature=f, kind="shadow_disable").set(float(_as_int(row.get("shadow_disable_suggested", 0), 0)))
                FEATURE_DELTA.labels(feature=f, kind="missing_rate").set(float(_as_float(row.get("missing_rate_delta", 0.0), 0.0)))
                FEATURE_DELTA.labels(feature=f, kind="zero_rate").set(float(_as_float(row.get("zero_rate_delta", 0.0), 0.0)))
                FEATURE_DELTA.labels(feature=f, kind="clip_rate").set(float(_as_float(row.get("clip_rate_delta", 0.0), 0.0)))
            seen_features = current_features
        except Exception:
            UP.set(0)
            LAST_SUCCESS.set(0)
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
