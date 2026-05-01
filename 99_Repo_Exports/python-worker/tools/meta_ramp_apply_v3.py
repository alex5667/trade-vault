#!/usr/bin/env python3
# python-worker/tools/meta_ramp_apply_v3.py
"""
P20 Controlled Ramp Applier for meta_feat_v5.

Specialized logic:
- Reads report.json (+ optional model.json)
- Checks guard freeze latch (meta_guard_freeze / meta_model_freeze)
- Checks DQ latch (via P16 dq_rules)
- Implements step-based ramp: default +0.05 up, -0.10 down
- Supports per-schema thresholds: ramp_<metric>__<schema>
- Anti-flap: blocks INCREASE during min_hold or cooldown windows
- Trend gate: blocks INCREASE if current hybrid/worst metrics degrade vs baseline
- Writes to Redis dynamic config: settings:dynamic_cfg
"""

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:
    import redis
except ImportError:
    redis = None

# DQ-aware latch
try:
    from tools.meta_dq_rules_v1 import dq_freeze_decision
except ImportError:
    dq_freeze_decision = None


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _as_float(x: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if x is None or str(x).strip() == "": return default
        return float(x)
    except Exception:
        return default


def _as_int(x: Any, default: Optional[int] = 0) -> Optional[int]:
    try:
        if x is None or str(x).strip() == "": return default
        return int(float(x))
    except Exception:
        return default


def _cfg_get(cfg: Dict[str, Any], schema: str, key: str, default: Any) -> Any:
    if schema:
        k_over = f"{key}__{schema}"
        if k_over in cfg:
            val = cfg[k_over]
            if isinstance(default, float) and isinstance(val, str):
                try: return float(val)
                except: pass
            if isinstance(default, int) and isinstance(val, str):
                try: return int(float(val))
                except: pass
            return val
    return cfg.get(key, default)


def _state_get(cfg2: Dict[str, Any], schema: str, key: str, default: Any = None) -> Any:
    # Like _cfg_get(), but for stateful keys we write back into cfg2. Uses per-schema suffix __<schema>.
    sk = f"{key}__{schema}"
    if sk in cfg2 and str(cfg2.get(sk)).strip() != "":
        return cfg2.get(sk)
    if key in cfg2 and str(cfg2.get(key)).strip() != "":
        return cfg2.get(key)
    return default


def _state_key(schema: str, key: str) -> str:
    return f"{key}__{schema}"


def _min_hold_active(now_ts: int, last_change_ts: int, min_hold_s: int) -> bool:
    if last_change_ts <= 0 or min_hold_s <= 0:
        return False
    return (now_ts - last_change_ts) < min_hold_s


@dataclass
class Quality:
    pr_auc: Optional[float] = None
    ece: Optional[float] = None
    precision_top5p: Optional[float] = None
    worst_pr_auc: Optional[float] = None
    worst_ece: Optional[float] = None
    worst_precision_top5p: Optional[float] = None
    dq_health_mean: Optional[float] = None
    worst_dq_pr_auc: Optional[float] = None
    worst_dq_ece: Optional[float] = None


def _extract_quality(report: Dict[str, Any]) -> Quality:
    m = report.get("metrics") or {}
    # report v1/v2 might have flat metrics
    if "global" in m: m = m["global"]
    
    worst = report.get("worst") or {}
    wdq = report.get("metrics", {}).get("worst_dq_bucket") or {}

    return Quality(
        pr_auc=_as_float(m.get("pr_auc")),
        ece=_as_float(m.get("ece")),
        precision_top5p=_as_float(m.get("precision_top5p")),
        worst_pr_auc=_as_float(worst.get("worst_pr_auc")),
        worst_ece=_as_float(worst.get("worst_ece")),
        worst_precision_top5p=_as_float(worst.get("worst_precision_top5p")),
        dq_health_mean=_as_float(m.get("dq_health_mean")),
        worst_dq_pr_auc=_as_float(wdq.get("pr_auc")),
        worst_dq_ece=_as_float(wdq.get("ece")),
    )


def _hybrid_quality(q: Quality) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    # Use global metrics, but worsen them if worst-group is provided and worse.
    pr = q.pr_auc
    if q.worst_pr_auc is not None:
        if pr is None: pr = q.worst_pr_auc
        else: pr = min(pr, q.worst_pr_auc)
    
    ece = q.ece
    if q.worst_ece is not None:
        if ece is None: ece = q.worst_ece
        else: ece = max(ece, q.worst_ece)
        
    p5 = q.precision_top5p
    if q.worst_precision_top5p is not None:
        if p5 is None: p5 = q.worst_precision_top5p
        else: p5 = min(p5, q.worst_precision_top5p)
    return pr, ece, p5


def _trend_gate(q: Quality, cfg2: Dict[str, Any], schema: str) -> Tuple[bool, str, Dict[str, Any], str, str]:
    """
    Gate for *increasing* share based on *trend* vs the last stored baseline in cfg2.
    """
    pr, ece, p5 = _hybrid_quality(q)

    # Baselines are per-schema to avoid cross-schema contamination.
    b_pr = _as_float(_state_get(cfg2, schema, "meta_ramp_baseline_pr_auc", None), None)
    b_ece = _as_float(_state_get(cfg2, schema, "meta_ramp_baseline_ece", None), None)
    b_worst_pr = _as_float(_state_get(cfg2, schema, "meta_ramp_baseline_worst_pr_auc", None), None)
    b_worst_ece = _as_float(_state_get(cfg2, schema, "meta_ramp_baseline_worst_ece", None), None)
    b_dq_health = _as_float(_state_get(cfg2, schema, "meta_ramp_baseline_dq_health_mean", None), None)
    b_worst_dq_pr = _as_float(_state_get(cfg2, schema, "meta_ramp_baseline_worst_dq_pr_auc", None), None)
    b_worst_dq_ece = _as_float(_state_get(cfg2, schema, "meta_ramp_baseline_worst_dq_ece", None), None)

    # If no baseline yet, allow (bootstrap).
    if b_pr is None and b_ece is None and b_worst_pr is None and b_worst_ece is None and b_dq_health is None:
        return True, "", {"bootstrap": True}, "none", "hold"

    pr_drop_max = float(_cfg_get(cfg2, schema, "ramp_trend_pr_auc_drop_max", 0.02) or 0.02)
    ece_rise_max = float(_cfg_get(cfg2, schema, "ramp_trend_ece_rise_max", 0.02) or 0.02)
    worst_pr_drop_max = float(_cfg_get(cfg2, schema, "ramp_trend_worst_pr_auc_drop_max", 0.03) or 0.03)
    worst_ece_rise_max = float(_cfg_get(cfg2, schema, "ramp_trend_worst_ece_rise_max", 0.03) or 0.03)
    dq_health_drop_max = float(_cfg_get(cfg2, schema, "ramp_trend_dq_health_drop_max", 0.05) or 0.05)
    severe_mul = float(_cfg_get(cfg2, schema, "ramp_trend_severe_mul", 2.0) or 2.0)

    failures = []
    severity = "none"

    def _mark(name: str, is_severe: bool) -> None:
        nonlocal severity
        failures.append(name)
        if is_severe:
            severity = "severe"
        elif severity == "none":
            severity = "mild"

    # Higher is better
    if pr is not None and b_pr is not None and pr_drop_max > 0:
        d = pr - b_pr
        if d < -pr_drop_max:
            _mark("pr_auc_drop", d < -(pr_drop_max * severe_mul))
    # Lower is better
    if ece is not None and b_ece is not None and ece_rise_max > 0:
        d = ece - b_ece
        if d > ece_rise_max:
            _mark("ece_rise", d > (ece_rise_max * severe_mul))

    if q.worst_pr_auc is not None and b_worst_pr is not None and worst_pr_drop_max > 0:
        d = float(q.worst_pr_auc) - b_worst_pr
        if d < -worst_pr_drop_max:
            _mark("worst_pr_auc_drop", d < -(worst_pr_drop_max * severe_mul))
    if q.worst_ece is not None and b_worst_ece is not None and worst_ece_rise_max > 0:
        d = float(q.worst_ece) - b_worst_ece
        if d > worst_ece_rise_max:
            _mark("worst_ece_rise", d > (worst_ece_rise_max * severe_mul))

    if q.dq_health_mean is not None and b_dq_health is not None and dq_health_drop_max > 0:
        d = float(q.dq_health_mean) - b_dq_health
        if d < -dq_health_drop_max:
            _mark("dq_health_drop", d < -(dq_health_drop_max * severe_mul))

    if q.worst_dq_pr_auc is not None and b_worst_dq_pr is not None and worst_pr_drop_max > 0:
        d = float(q.worst_dq_pr_auc) - b_worst_dq_pr
        if d < -worst_pr_drop_max:
            _mark("worst_dq_pr_auc_drop", d < -(worst_pr_drop_max * severe_mul))
    if q.worst_dq_ece is not None and b_worst_dq_ece is not None and worst_ece_rise_max > 0:
        d = float(q.worst_dq_ece) - b_worst_dq_ece
        if d > worst_ece_rise_max:
            _mark("worst_dq_ece_rise", d > (worst_ece_rise_max * severe_mul))

    suggested_action = str(_cfg_get(cfg2, schema, "ramp_trend_action", "hold") or "hold").strip().lower()
    if suggested_action not in ("hold", "decrease"):
        suggested_action = "hold"
    if severity == "severe":
        suggested_action = "decrease"

    details = {
        "current": {"hybrid_pr_auc": pr, "hybrid_ece": ece, "precision_top5p": p5},
        "baseline": {
            "hybrid_pr_auc": b_pr,
            "hybrid_ece": b_ece,
            "worst_pr_auc": b_worst_pr,
            "worst_ece": b_worst_ece,
            "dq_health_mean": b_dq_health,
            "worst_dq_pr_auc": b_worst_dq_pr,
            "worst_dq_ece": b_worst_dq_ece,
        },
        "thresholds": {
            "pr_auc_drop_max": pr_drop_max,
            "ece_rise_max": ece_rise_max,
            "worst_pr_auc_drop_max": worst_pr_drop_max,
            "worst_ece_rise_max": worst_ece_rise_max,
            "dq_health_drop_max": dq_health_drop_max,
            "severe_mul": severe_mul,
        },
        "failures": failures,
    }

    if not failures:
        return True, "", details, "none", "hold"
    reason = "trend_degrade:" + ",".join(failures)
    return False, reason, details, severity, suggested_action


def _baseline_kv(schema: str, q: Quality, now_ts: int) -> Dict[str, str]:
    pr, ece, _p5 = _hybrid_quality(q)
    kv: Dict[str, str] = {}
    kv[_state_key(schema, "meta_ramp_baseline_ts")] = str(int(now_ts))
    if pr is not None:
        kv[_state_key(schema, "meta_ramp_baseline_pr_auc")] = str(float(pr))
    if ece is not None:
        kv[_state_key(schema, "meta_ramp_baseline_ece")] = str(float(ece))
    if q.worst_pr_auc is not None:
        kv[_state_key(schema, "meta_ramp_baseline_worst_pr_auc")] = str(float(q.worst_pr_auc))
    if q.worst_ece is not None:
        kv[_state_key(schema, "meta_ramp_baseline_worst_ece")] = str(float(q.worst_ece))
    if q.dq_health_mean is not None:
        kv[_state_key(schema, "meta_ramp_baseline_dq_health_mean")] = str(float(q.dq_health_mean))
    if q.worst_dq_pr_auc is not None:
        kv[_state_key(schema, "meta_ramp_baseline_worst_dq_pr_auc")] = str(float(q.worst_dq_pr_auc))
    if q.worst_dq_ece is not None:
        kv[_state_key(schema, "meta_ramp_baseline_worst_dq_ece")] = str(float(q.worst_dq_ece))
    return kv


def _quality_gate(q: Quality, cfg2: Dict[str, Any], schema: str) -> Tuple[bool, str, Dict[str, Any]]:
    pass_ok = True
    reason = "quality_ok"
    
    thr_pr = _cfg_get(cfg2, schema, "ramp_pr_auc_min", 0.55)
    thr_ece = _cfg_get(cfg2, schema, "ramp_ece_max", 0.10)
    g_thr_pr = _cfg_get(cfg2, schema, "ramp_group_pr_auc_min", thr_pr)
    g_thr_ece = _cfg_get(cfg2, schema, "ramp_group_ece_max", thr_ece)
    
    curr_pr = q.pr_auc if q.pr_auc is not None else 0.0
    curr_ece = q.ece if q.ece is not None else 1.0
    w_pr = q.worst_pr_auc if q.worst_pr_auc is not None else curr_pr
    w_ece = q.worst_ece if q.worst_ece is not None else curr_ece

    details = {
        "pr_auc": curr_pr, "ece": curr_ece,
        "worst_pr": w_pr, "worst_ece": w_ece,
        "thrs": {"pr": thr_pr, "ece": thr_ece, "g_pr": g_thr_pr, "g_ece": g_thr_ece}
    }

    if curr_pr < thr_pr or curr_ece > thr_ece:
        pass_ok = False
        reason = f"global_fail:pr={curr_pr:.3f}<{thr_pr},ece={curr_ece:.3f}>{thr_ece}"
    elif w_pr < g_thr_pr or w_ece > g_thr_ece:
        pass_ok = False
        reason = f"worst_group_fail:pr={w_pr:.3f}<{g_thr_pr},ece={w_ece:.3f}>{g_thr_ece}"
        
    return pass_ok, reason, details


def _load_dyn_cfg(redis_url: str) -> Dict[str, Any]:
    if not redis or not redis_url:
        return {}
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        d = r.hgetall("settings:dynamic_cfg") or {}
        out = {k: v for k, v in d.items()}
        out["_redis"] = r
        return out
    except Exception as e:
        print(f"DEBUG: Redis load error: {e}")
        return {}


def _write_dyn_cfg(cfg: Dict[str, Any], patch: Dict[str, Any]) -> None:
    r = cfg.get("_redis")
    if not r:
        return
    mapping = {}
    for k, v in patch.items():
        if isinstance(v, (dict, list)):
            mapping[k] = json.dumps(v, ensure_ascii=False)
        else:
            mapping[k] = str(v)
    r.hset("settings:dynamic_cfg", mapping=mapping)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-json", required=True)
    ap.add_argument("--model-json", default="")
    ap.add_argument("--apply", type=int, default=int(os.environ.get("APPLY", "0")))
    ap.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--schema-override", default="")
    ap.add_argument("--ignore-guard", action="store_true")
    ap.add_argument("--ignore-dq", action="store_true")
    args = ap.parse_args()

    # Load report
    report_path = Path(args.report_json)
    if not report_path.exists():
        print(f"FATAL: Report not found: {args.report_json}")
        sys.exit(1)
    
    report = json.loads(report_path.read_text(encoding="utf-8"))
    
    # Identify schema
    schema_name = args.schema_override or report.get("schema_name") or ""
    if not schema_name and isinstance(report.get("schema"), dict):
        schema_name = report["schema"].get("name") or ""
        
    if not schema_name and args.model_json:
        m_path = Path(args.model_json)
        if m_path.exists():
            m_meta = json.loads(m_path.read_text(encoding="utf-8"))
            schema_name = m_meta.get("schema_name") or m_meta.get("schema") or ""
    
    # Load dynamic config
    dyn = _load_dyn_cfg(args.redis_url)
    
    # 1. Guard Latch Check
    guard_freeze = int(_as_float(dyn.get("meta_guard_freeze"), 0))
    model_freeze = int(_as_float(dyn.get("meta_model_freeze"), 0))
    
    if (guard_freeze or model_freeze) and not args.ignore_guard:
        reason = dyn.get("meta_guard_reason") or "forced_freeze"
        decision = {
            "ts": _now_iso(),
            "schema": schema_name,
            "action": "FREEZE",
            "reason": f"guard_latch:{reason}",
            "share": 0.0,
            "mode": "SHADOW"
        }
        if args.apply:
            _write_dyn_cfg(dyn, {
                "meta_enforce_share": 0.0,
                "meta_model_mode": "SHADOW",
                "meta_ramp_last_decision": decision,
            })
        print(json.dumps(decision, ensure_ascii=False))
        return

    # Anti-flap state
    now_ts = int(time.time())
    min_hold_s = int(float(_cfg_get(dyn, schema_name, "ramp_min_hold_s", 72000) or 72000))
    cooldown_after_decrease_s = int(float(_cfg_get(dyn, schema_name, "ramp_cooldown_after_decrease_s", 129600) or 129600))
    last_change_ts = int(_as_int(_state_get(dyn, schema_name, "meta_ramp_last_change_ts", 0)) or 0)
    last_action = str(_state_get(dyn, schema_name, "meta_ramp_last_action", "") or "").strip().lower()
    min_hold_active = _min_hold_active(now_ts, last_change_ts, min_hold_s)
    cooldown_active = bool(last_action.startswith("decrease") and _min_hold_active(now_ts, last_change_ts, cooldown_after_decrease_s))

    q = _extract_quality(report)

    # 2. DQ Latch Check
    dq_freeze = False
    reason_qual = "quality_ok"
    dq_details = {}
    
    if not args.ignore_dq and dq_freeze_decision is not None:
        dq_freeze, reason_dq, dq_details = dq_freeze_decision(report, cfg2=dyn, schema_name=schema_name)
        if dq_freeze:
            reason_qual = f"dq_latch:{reason_dq}"

    # 3. Quality Thresholds
    pass_ok = True
    quality_details = {}
    if not dq_freeze:
        pass_ok, reason_qual, quality_details = _quality_gate(q, dyn, schema_name)

    # 4. Ramp Calculation
    curr_share = _as_float(dyn.get("meta_enforce_share"), 0.0)
    cur_mode = dyn.get("meta_model_mode", "SHADOW")
    step_up = _cfg_get(dyn, schema_name, "ramp_share_step_up", 0.05)
    step_down = _cfg_get(dyn, schema_name, "ramp_share_step_down", 0.10)
    max_share = _cfg_get(dyn, schema_name, "ramp_max_share", 1.0)
    share_min = _cfg_get(dyn, schema_name, "ramp_share_min", 0.0)
    
    action = "HOLD"
    next_share = curr_share

    if not dq_freeze and pass_ok:
        cand_share = min(max_share, max(share_min, curr_share + step_up))
        cand_action = "INCREASE" if cand_share > curr_share else "HOLD"

        if cand_action == "INCREASE" and min_hold_active:
            action = "HOLD_MIN_HOLD"
            reason_qual = "antiflap_min_hold_active"
        elif cand_action == "INCREASE" and cooldown_active:
            action = "HOLD_COOLDOWN"
            reason_qual = "antiflap_cooldown_after_decrease"
        else:
            if cand_action == "INCREASE":
                trend_ok, trend_reason, trend_details, trend_sev, trend_suggest = _trend_gate(q, dyn, schema_name)
                if not trend_ok:
                    reason_qual = trend_reason
                    if trend_suggest == "decrease":
                        next_share = max(share_min, curr_share - step_down)
                        action = "DECREASE_TREND"
                    else:
                        action = "HOLD_TREND"
                else:
                    next_share = cand_share
                    action = cand_action
            else:
                next_share = cand_share
                action = cand_action
    else:
        next_share = max(share_min, curr_share - step_down)
        action = "DECREASE" if next_share < curr_share else "HOLD"

    next_mode = "ENFORCE" if next_share > 0 else "SHADOW"
    
    decision = {
        "ts": now_ts,
        "ts_iso": _now_iso(),
        "schema": schema_name,
        "pass": pass_ok and not dq_freeze,
        "action": action,
        "reason": reason_qual,
        "share_prev": curr_share,
        "share_next": next_share,
        "mode_prev": cur_mode,
        "mode_next": next_mode,
        "quality": quality_details,
        "antiflap": {
            "min_hold_active": min_hold_active,
            "cooldown_active": cooldown_active,
            "last_change_ts": last_change_ts
        }
    }
    if dq_freeze: decision["dq_details"] = dq_details

    if args.apply:
        kv = {
            "meta_enforce_share": next_share,
            "meta_model_mode": next_mode,
            "meta_ramp_last_decision": decision,
            # Persist ramp state (per-schema) for observability / alerts
            _state_key(schema_name, "meta_ramp_last_eval_ts"): str(int(now_ts)),
            _state_key(schema_name, "meta_ramp_last_decision"): str(decision.get("action") or ""),
            _state_key(schema_name, "meta_ramp_last_decision_reason"): str(decision.get("reason") or ""),
            # NOTE: last_action/last_reason are kept for backward compatibility
            _state_key(schema_name, "meta_ramp_last_action"): action,
            _state_key(schema_name, "meta_ramp_last_reason"): reason_qual,
        }

        # Block reason is set only for holds that prevent INCREASE
        act = str(decision.get("action") or "")
        block_reason = ""
        if act in ("HOLD_MIN_HOLD", "HOLD_COOLDOWN"):
            block_reason = str((decision.get("antiflap") or {}).get("blocked") or "")
        elif act == "HOLD_TREND":
            block_reason = "trend"
        elif act.startswith("FREEZE") or act == "FREEZE":
            block_reason = "freeze"
        kv[_state_key(schema_name, "meta_status_ramp_block_reason")] = block_reason
        
        # Update last_change_ts IF share or mode changed
        changed = (abs(float(curr_share) - float(next_share)) > 1e-9) or (cur_mode != next_mode)
        if changed:
            kv[_state_key(schema_name, "meta_ramp_last_change_ts")] = str(now_ts)

        # Update baseline IF quality is OK and no decrease
        should_baseline = (pass_ok and not dq_freeze and not action.startswith("DECREASE"))
        if should_baseline:
            kv.update(_baseline_kv(schema_name, q, now_ts))

        if args.model_json and pass_ok and not dq_freeze:
            kv["meta_model_path"] = str(Path(args.model_json).absolute())
            
        _write_dyn_cfg(dyn, kv)
        print(f"APPLIED: {action} {curr_share} -> {next_share} ({next_mode})")
    
    print(json.dumps(decision, ensure_ascii=False))


if __name__ == "__main__":
    main()
