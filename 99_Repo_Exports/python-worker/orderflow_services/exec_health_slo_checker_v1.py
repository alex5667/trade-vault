#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis
"""ExecHealth rollout SLO-checker.

Reads compact per-process scope-state hashes written by:
  - EdgeCostGate
  - SignalPipeline
  - EntryPolicyService

Produces a low-cardinality summary hash suitable for a dedicated exporter.
""",
import argparse
import json
import logging
import os
import sys
import time
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

logger = logging.getLogger("exec_health_slo_checker_v1")

SCOPES: Sequence[str] = ("edge", "pipeline", "entry_policy")
THR_METRICS: Sequence[str] = (
    "threshold_is_p95_bps",
    "threshold_perm_impact_p95_bps",
    "threshold_realized_spread_p50_bps",
)


def _now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


def _s(x: Any, d: str = "") -> str:
    try:
        if x is None:
            return str(d)
        return str(x)
    except Exception:
        return str(d)


def _get_keys(r, prefix: str) -> List[str]:
    registry_key = f"{prefix}:registry"
    try:
        keys = r.smembers(registry_key) or []
        return [str(k) for k in keys]
    except Exception as e:
        logger.warning("Failed to read registry %s: %s", registry_key, e)
        return []


def _read_rows(r, keys: Sequence[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for key in keys:
        try:
            d = r.hgetall(key) or {}
        except Exception as e:
            exc_name = type(e).__name__
            if "BusyLoadingError" in exc_name or "ConnectionError" in exc_name or "TimeoutError" in exc_name:
                raise
            logger.warning("Failed to read row %s: %s", key, e)
            continue
        if not isinstance(d, dict):
            continue
        d = {str(k): v for k, v in d.items()}
        d["_key"] = key
        rows.append(d)
    return rows


def _norm_thr(v: Any) -> str:
    return f"{_f(v, 0.0):.6f}"


def summarize_scope_rows(rows: Sequence[Mapping[str, Any]], *, now_ms: int, stale_ms: int) -> Dict[str, Any]:
    active = [dict(r) for r in rows if max(0, now_ms - _i(r.get("updated_ts_ms"), 0)) <= stale_ms]
    stale = len(rows) - len(active)

    out: Dict[str, Any] = {
        "active_instances": int(len(active)),
        "stale_instances": int(max(0, stale)),
        "total_n": 0,
        "apply_n": 0,
        "veto_n": 0,
        "pass_n": 0,
        "reader_error_n": 0,
        "mode_distinct": 0,
        "deploy_distinct": 0,
        "rollout_drift_instances": 0,
        "top_modes_json": "[]",
        "top_deploys_json": "[]",
    }
    for metric in THR_METRICS:
        out[f"distinct_{metric}"] = 0

    if not active:
        return out

    mode_counter: Counter[str] = Counter(_s(r.get("last_mode"), "unknown") for r in active)
    deploy_counter: Counter[str] = Counter(_s(r.get("deploy_id"), "unknown") for r in active)
    out["mode_distinct"] = len(mode_counter)
    out["deploy_distinct"] = len(deploy_counter)
    out["top_modes_json"] = json.dumps(mode_counter.most_common(5), ensure_ascii=False)
    out["top_deploys_json"] = json.dumps(deploy_counter.most_common(5), ensure_ascii=False)

    modal_mode = mode_counter.most_common(1)[0][0]
    modal_thr: Dict[str, str] = {}
    for metric in THR_METRICS:
        thr_counter = Counter(_norm_thr(r.get(metric)) for r in active)
        out[f"distinct_{metric}"] = len(thr_counter)
        modal_thr[metric] = thr_counter.most_common(1)[0][0]

    drift_n = 0
    for r in active:
        if _s(r.get("last_mode"), "unknown") != modal_mode:
            drift_n += 1
            continue
        if any(_norm_thr(r.get(metric)) != modal_thr[metric] for metric in THR_METRICS):
            drift_n += 1
    out["rollout_drift_instances"] = int(drift_n)

    for row in active:
        out["total_n"] += _i(row.get("total_n"), 0)
        out["apply_n"] += _i(row.get("apply_n"), 0)
        out["veto_n"] += _i(row.get("veto_n"), 0)
        out["pass_n"] += _i(row.get("pass_n"), 0)
        out["reader_error_n"] += _i(row.get("reader_error_n"), 0)

    return out


def build_summary(rows: Sequence[Mapping[str, Any]], *, now_ms: int, stale_ms: int) -> Dict[str, str]:
    by_scope: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_scope[_s(row.get("scope"), "unknown")].append(row)

    out: Dict[str, str] = {
        "schema_name": "exec_health_slo_summary",
        "schema_version": "1",
        "updated_ts_ms": str(int(now_ms)),
        "stale_ms": str(int(stale_ms)),
        "active_instances_total": "0",
        "stale_instances_total": "0",
        "rollout_drift_instances_total": "0",
        "cross_scope_mode_distinct": "0",
    }
    for metric in THR_METRICS:
        out[f"cross_scope_distinct_{metric}"] = "0"

    scope_modal_mode: Dict[str, str] = {}
    scope_modal_thr: Dict[str, str] = {m: "" for m in THR_METRICS}

    active_total = 0
    stale_total = 0
    drift_total = 0
    for scope in SCOPES:
        ss = summarize_scope_rows(by_scope.get(scope, []), now_ms=now_ms, stale_ms=stale_ms)
        active_total += int(ss["active_instances"])
        stale_total += int(ss["stale_instances"])
        drift_total += int(ss["rollout_drift_instances"])
        total_n = max(0, int(ss["total_n"]))
        apply_n = max(0, int(ss["apply_n"]))
        veto_n = max(0, int(ss["veto_n"]))
        pass_n = max(0, int(ss["pass_n"]))
        denom = float(total_n) if total_n > 0 else 1.0

        out[f"active_instances_{scope}"] = str(int(ss["active_instances"]))
        out[f"stale_instances_{scope}"] = str(int(ss["stale_instances"]))
        out[f"reader_error_n_{scope}"] = str(int(ss["reader_error_n"]))
        out[f"share_apply_{scope}"] = f"{(float(apply_n) / denom):.9f}"
        out[f"share_veto_{scope}"] = f"{(float(veto_n) / denom):.9f}"
        out[f"share_pass_{scope}"] = f"{(float(pass_n) / denom):.9f}"
        out[f"mode_distinct_{scope}"] = str(int(ss["mode_distinct"]))
        out[f"deploy_distinct_{scope}"] = str(int(ss["deploy_distinct"]))
        out[f"rollout_drift_instances_{scope}"] = str(int(ss["rollout_drift_instances"]))
        out[f"top_modes_json_{scope}"] = str(ss["top_modes_json"])
        out[f"top_deploys_json_{scope}"] = str(ss["top_deploys_json"])
        if by_scope.get(scope):
            active = [dict(r) for r in by_scope[scope] if max(0, now_ms - _i(r.get("updated_ts_ms"), 0)) <= stale_ms]
            if active:
                scope_modal_mode[scope] = Counter(_s(r.get("last_mode"), "unknown") for r in active).most_common(1)[0][0]
                for metric in THR_METRICS:
                    thr = Counter(_norm_thr(r.get(metric)) for r in active).most_common(1)[0][0]
                    scope_modal_thr[metric] = json.dumps(scope_modal_thr.get(metric)) if False else scope_modal_thr.get(metric, "")
                    out[f"threshold_distinct_{scope}_{metric}"] = str(int(ss[f"distinct_{metric}"]))
                    out[f"threshold_modal_{scope}_{metric}"] = thr
            else:
                out[f"threshold_distinct_{scope}_{THR_METRICS[0]}"] = str(int(ss[f"distinct_{THR_METRICS[0]}"]))
                out[f"threshold_distinct_{scope}_{THR_METRICS[1]}"] = str(int(ss[f"distinct_{THR_METRICS[1]}"]))
                out[f"threshold_distinct_{scope}_{THR_METRICS[2]}"] = str(int(ss[f"distinct_{THR_METRICS[2]}"]))
        else:
            for metric in THR_METRICS:
                out[f"threshold_distinct_{scope}_{metric}"] = "0"
                out[f"threshold_modal_{scope}_{metric}"] = ""

    # cross-scope modal config drift
    mode_values = {v for v in scope_modal_mode.values() if v}
    out["cross_scope_mode_distinct"] = str(len(mode_values))
    for metric in THR_METRICS:
        modal_vals: set[str] = set()
        for scope in SCOPES:
            modal_val = out.get(f"threshold_modal_{scope}_{metric}", "")
            if modal_val != "":
                modal_vals.add(modal_val)
        out[f"cross_scope_distinct_{metric}"] = str(len(modal_vals))

    out["active_instances_total"] = str(int(active_total))
    out["stale_instances_total"] = str(int(stale_total))
    out["rollout_drift_instances_total"] = str(int(drift_total))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="ExecHealth rollout SLO checker")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--state-prefix", default=os.getenv("EXEC_HEALTH_SCOPE_STATE_PREFIX", "metrics:exec_health:scope_state"))
    ap.add_argument("--out-key", default=os.getenv("EXEC_HEALTH_SLO_SUMMARY_KEY", "metrics:exec_health:slo:last"))
    ap.add_argument("--stale-ms", type=int, default=300000)
    ap.add_argument("--notify", action="store_true")
    ap.add_argument("--notify-stream", default=os.getenv("EXEC_HEALTH_SLO_NOTIFY_STREAM", "notify:telegram"))
    args = ap.parse_args()

    if redis is None:
        logger.error("redis dependency missing")
        return 1

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    try:
        registry_key = f"{args.state_prefix}:registry"
        all_keys = _get_keys(r, args.state_prefix)
        rows = _read_rows(r, all_keys)

        # Cleanup registry: remove keys that no longer exist in Redis
        found_keys = {row["_key"] for row in rows}
        stale_keys = [k for k in all_keys if k not in found_keys]
        if stale_keys:
            logger.info("Pruning %d stale keys from registry %s", len(stale_keys), registry_key)
            r.srem(registry_key, *stale_keys)

        summary = build_summary(rows, now_ms=_now_ms(), stale_ms=max(1000, int(args.stale_ms)))
        r.hset(args.out_key, mapping=summary)
        r.expire(args.out_key, max(60, int(args.stale_ms / 1000) * 3))
        return 0
    except Exception as exc:
        exc_name = type(exc).__name__
        if "BusyLoadingError" in exc_name or "ConnectionError" in exc_name or "TimeoutError" in exc_name:
            logger.warning("ExecHealth SLO checker skipping run: Redis is busy/unavailable (%s: %s)", exc_name, exc)
            return 0

        logger.exception("exec health slo checker failed: %s", exc)
        if args.notify:
            try:
                r.xadd(args.notify_stream, {"ts_ms": str(_now_ms()), "source": "exec_health_slo_checker_v1", "text": f"ExecHealth SLO checker ERROR: {exc}"}, maxlen=5000, approximate=True)
            except Exception:
                pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
