# python-worker/tools/meta_auto_ramp_v2.py
"""
Regime-aware auto-ramp for meta-model.

Decision metrics can be taken from:
- global metrics (report["metrics"])
- worst-group metrics (report["worst"]) over selected group buckets

Per-schema gates:
- thresholds can be overridden by cfg keys with suffix: __<schema_name>
  e.g. ramp_pr_auc_min__meta_feat_v4

Guardrails latch (P11):
- if meta_guard_freeze == 1 in dynamic cfg, force share=0 + SHADOW unless --ignore-guard.

Writes dynamic cfg keys:
- meta_enforce_share (float in [0,1])
- meta_model_mode ("SHADOW"|"ENFORCE")
- meta_ramp_last_decision (json str)
- meta_ramp_good_streak, meta_ramp_bad_streak (ints)
"""
from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, Optional, Tuple
import sys
from pathlib import Path

# P16: DQ-aware freeze latch
try:
    from tools.meta_dq_rules_v1 import dq_freeze_decision
except Exception:
    dq_freeze_decision = None

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def _try_load_cfg2(redis_url: str) -> Dict[str, Any]:
    """Best-effort load of merged config knobs from Redis dynamic cfg hash."""
    if not redis_url:
        return {}
    try:
        import redis  # type: ignore
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        # dynamic cfg is a hash (confirmed: settings:dynamic_cfg)
        d = r.hgetall("settings:dynamic_cfg") or {}
        # normalize empty strings
        return {k: v for k, v in d.items() if k}
    except Exception:
        return {}


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _f(x: Any, d: float) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _get(cfg: Dict[str, Any], key: str, default: Any = None) -> Any:
    return cfg.get(key, default)


def _get_schema_override(cfg: Dict[str, Any], key: str, schema: str, default: Any) -> Any:
    if schema:
        k2 = f"{key}__{schema}"
        if k2 in cfg:
            return cfg.get(k2)
    return cfg.get(key, default)


def _parse_cfg_value(v: Any) -> Any:
    # dynamic cfg often stores strings
    if isinstance(v, (int, float)):
        return v
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    # bool
    if s.lower() in ("true", "false"):
        return s.lower() == "true"
    # int/float
    try:
        if "." in s or "e" in s.lower():
            return float(s)
        return int(s)
    except Exception:
        return s


def _load_dyn_cfg() -> Dict[str, Any]:
    # expects Redis hash key in env
    if redis is None:
        return {}
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    db = int(os.environ.get("REDIS_DB", "0"))
    password = os.environ.get("REDIS_PASSWORD") or None
    key = os.environ.get("DYN_CFG_KEY", "settings:dynamic_cfg")
    r = redis.Redis(host=host, port=port, db=db, password=password, decode_responses=True)
    d = r.hgetall(key) or {}
    out: Dict[str, Any] = {}
    for k, v in d.items():
        out[str(k)] = _parse_cfg_value(v)
    out["_redis_key"] = key
    out["_redis"] = r
    return out


def _write_dyn_cfg(cfg: Dict[str, Any], patch: Dict[str, Any]) -> None:
    r = cfg.get("_redis")
    key = cfg.get("_redis_key")
    if r is None or key is None:
        return
    m: Dict[str, str] = {}
    for k, v in patch.items():
        if isinstance(v, (dict, list)):
            m[k] = json.dumps(v, ensure_ascii=False)
        else:
            m[k] = str(v)
    r.hset(key, mapping=m)


def _extract_report_metrics(report: Dict[str, Any]) -> Dict[str, float]:
    m = report.get("metrics") or {}
    return {
        "ece": _f(m.get("ece"), 0.0),
        "brier": _f(m.get("brier"), 0.0),
        "pr_auc": _f(m.get("pr_auc"), 0.0),
        "precision_top5p": _f(m.get("precision_top5p"), 0.0),
        "precision_topk": _f(m.get("precision_topk"), 0.0),
    }


def _extract_worst(report: Dict[str, Any]) -> Dict[str, Any]:
    w = report.get("worst") or {}
    return {
        "coverage_groups": int(_f(w.get("coverage_groups"), 0)),
        "worst_ece": w.get("worst_ece"),
        "worst_pr_auc": w.get("worst_pr_auc"),
        "worst_precision_top5p": w.get("worst_precision_top5p"),
        "worst_ece_group": w.get("worst_ece_group"),
        "worst_pr_auc_group": w.get("worst_pr_auc_group"),
        "worst_precision_top5p_group": w.get("worst_precision_top5p_group"),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-json", required=True)
    ap.add_argument("--apply", type=int, default=int(os.environ.get("META_RAMP_APPLY", "0")))
    ap.add_argument("--ignore-guard", type=int, default=int(os.environ.get("META_RAMP_IGNORE_GUARD", "0")))
    ap.add_argument("--state-key-prefix", default=os.environ.get("META_RAMP_STATE_PREFIX", "meta_ramp_"))
    ap.add_argument("--schema", default=os.getenv("META_SCHEMA", ""), help="Optional schema name override")
    ap.add_argument("--ignore-dq", action="store_true", help="Ignore DQ latch (emergency)")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    args = ap.parse_args()

    report = json.loads(open(args.report_json, "r", encoding="utf-8").read())
    schema = ""
    if isinstance(report.get("schema"), dict):
        schema = str(report["schema"].get("name") or "")
    schema = schema or str(report.get("schema_name") or "")

    dyn = _load_dyn_cfg()
    cfg2: Dict[str, Any] = _try_load_cfg2(getattr(args, "redis_url", ""))
    schema_name = args.schema or report.get("schema_name") or report.get("model", {}).get("schema_name") or ""

    # --- P16: DQ latch ---
    if (not args.ignore_dq) and dq_freeze_decision is not None:
        try:
            freeze, reason, details = dq_freeze_decision(report, cfg2=cfg2, schema_name=schema_name)
        except Exception:
            freeze, reason, details = (False, "dq_eval_error", {})
        if freeze:
            decision = {
                "action": "freeze",
                "apply": bool(int(args.apply)),
                "schema_name": schema_name,
                "reason": f"dq_latch:{reason}",
                "dq": details,
            }
            sys.stdout.write(json.dumps(decision, ensure_ascii=False) + "\n")
            return

    # Guard latch (P11)
    guard_freeze = int(_f(dyn.get("meta_guard_freeze"), 0))
    guard_reason = str(dyn.get("meta_guard_reason") or "")
    if guard_freeze == 1 and int(args.ignore_guard) != 1:
        decision = {
            "ts": _now_iso(),
            "schema": schema,
            "action": "FORCE_FREEZE_BY_GUARD",
            "reason": guard_reason,
        }
        if int(args.apply) == 1:
            _write_dyn_cfg(dyn, {
                "meta_enforce_share": 0.0,
                "meta_model_mode": "SHADOW",
                f"{args.state_key_prefix}last_decision": decision,
            })
        else:
            print(json.dumps(decision, ensure_ascii=False))
        return

    # Thresholds (with per-schema overrides)
    thr_ece = float(_get_schema_override(dyn, "ramp_ece_max", schema, float(os.environ.get("RAMP_ECE_MAX", "0.08"))))
    thr_pr = float(_get_schema_override(dyn, "ramp_pr_auc_min", schema, float(os.environ.get("RAMP_PR_AUC_MIN", "0.08"))))
    thr_prec = float(_get_schema_override(dyn, "ramp_precision_top5p_min", schema, float(os.environ.get("RAMP_PREC_TOP5P_MIN", "0.12"))))
    # group thresholds (worst-case)
    g_thr_ece = float(_get_schema_override(dyn, "ramp_group_ece_max", schema, float(os.environ.get("RAMP_GROUP_ECE_MAX", str(thr_ece)))))
    g_thr_pr = float(_get_schema_override(dyn, "ramp_group_pr_auc_min", schema, float(os.environ.get("RAMP_GROUP_PR_AUC_MIN", str(thr_pr)))))
    g_thr_prec = float(_get_schema_override(dyn, "ramp_group_precision_top5p_min", schema, float(os.environ.get("RAMP_GROUP_PREC_TOP5P_MIN", str(thr_prec)))))

    policy = str(dyn.get("meta_ramp_group_policy") or os.environ.get("META_RAMP_GROUP_POLICY", "hybrid")).lower()
    min_groups = int(_get(dyn, "meta_ramp_group_min_coverage", int(os.environ.get("META_RAMP_GROUP_MIN_COVERAGE", "4"))))

    base = _extract_report_metrics(report)
    worst = _extract_worst(report)

    used = dict(base)
    used["policy"] = policy
    used["coverage_groups"] = worst["coverage_groups"]

    if policy in ("worst", "hybrid") and worst["coverage_groups"] >= min_groups:
        # take worst-case for group-sensitive metrics
        we = worst["worst_ece"]
        wp = worst["worst_pr_auc"]
        wprec = worst["worst_precision_top5p"]
        if we is not None:
            used["ece"] = max(used["ece"], float(we)) if policy == "hybrid" else float(we)
        if wp is not None:
            used["pr_auc"] = min(used["pr_auc"], float(wp)) if policy == "hybrid" else float(wp)
        if wprec is not None:
            used["precision_top5p"] = min(used["precision_top5p"], float(wprec)) if policy == "hybrid" else float(wprec)
        used["worst_group_ece"] = we
        used["worst_group_pr_auc"] = wp
        used["worst_group_precision_top5p"] = wprec
        used["worst_group_ece_group"] = worst.get("worst_ece_group")
        used["worst_group_pr_auc_group"] = worst.get("worst_pr_auc_group")
        used["worst_group_precision_top5p_group"] = worst.get("worst_precision_top5p_group")

    # Decision: PASS if all metrics pass + enough coverage (if policy demands it)
    coverage_ok = True
    if policy in ("worst", "hybrid"):
        coverage_ok = worst["coverage_groups"] >= min_groups
    pass_ok = (
        coverage_ok
        and float(used["ece"]) <= float(g_thr_ece if policy in ("worst", "hybrid") else thr_ece)
        and float(used["pr_auc"]) >= float(g_thr_pr if policy in ("worst", "hybrid") else thr_pr)
        and float(used["precision_top5p"]) >= float(g_thr_prec if policy in ("worst", "hybrid") else thr_prec)
    )

    # Ramp state
    prefix = str(args.state_key_prefix)
    good_streak = int(_f(dyn.get(prefix + "good_streak"), 0))
    bad_streak = int(_f(dyn.get(prefix + "bad_streak"), 0))
    share = float(_f(dyn.get("meta_enforce_share"), float(os.environ.get("META_ENFORCE_SHARE_DEFAULT", "0.0"))))

    up_after = int(_get(dyn, "meta_ramp_up_after", int(os.environ.get("META_RAMP_UP_AFTER", "3"))))
    down_after = int(_get(dyn, "meta_ramp_down_after", int(os.environ.get("META_RAMP_DOWN_AFTER", "2"))))
    step_up = float(_get(dyn, "meta_ramp_step_up", float(os.environ.get("META_RAMP_STEP_UP", "0.10"))))
    step_down = float(_get(dyn, "meta_ramp_step_down", float(os.environ.get("META_RAMP_STEP_DOWN", "0.20"))))
    max_share = float(_get(dyn, "meta_ramp_max_share", float(os.environ.get("META_RAMP_MAX_SHARE", "1.0"))))
    min_share = float(_get(dyn, "meta_ramp_min_share", float(os.environ.get("META_RAMP_MIN_SHARE", "0.0"))))

    action = "HOLD"
    if pass_ok:
        good_streak += 1
        bad_streak = 0
        if good_streak >= up_after:
            share = min(max_share, share + step_up)
            action = "RAMP_UP"
    else:
        bad_streak += 1
        good_streak = 0
        if bad_streak >= down_after:
            share = max(min_share, share - step_down)
            action = "RAMP_DOWN"

    mode = "ENFORCE" if share > 0.0 else "SHADOW"

    decision = {
        "ts": _now_iso(),
        "schema": schema,
        "pass": bool(pass_ok),
        "coverage_ok": bool(coverage_ok),
        "coverage_groups": int(worst["coverage_groups"]),
        "action": action,
        "share": float(share),
        "mode": mode,
        "used_metrics": {
            "ece": float(used["ece"]),
            "pr_auc": float(used["pr_auc"]),
            "precision_top5p": float(used["precision_top5p"]),
        },
        "thresholds": {
            "ece_max": float(g_thr_ece if policy in ("worst", "hybrid") else thr_ece),
            "pr_auc_min": float(g_thr_pr if policy in ("worst", "hybrid") else thr_pr),
            "precision_top5p_min": float(g_thr_prec if policy in ("worst", "hybrid") else thr_prec),
            "policy": policy,
            "min_groups": int(min_groups),
        },
        "streaks": {"good": int(good_streak), "bad": int(bad_streak)},
    }

    if int(args.apply) == 1:
        _write_dyn_cfg(dyn, {
            "meta_enforce_share": float(share),
            "meta_model_mode": mode,
            prefix + "good_streak": int(good_streak),
            prefix + "bad_streak": int(bad_streak),
            prefix + "last_decision": decision,
        })
    else:
        print(json.dumps(decision, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
