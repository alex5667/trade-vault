#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
tm_policy_tuner.py

Consumes NDJSON POSITION_CLOSED exports and produces:
  - per (symbol, regime, scenario, group, arm) stats
  - winner selection using LCB(mean R) + min samples + min edge
  - proposals written to cfg:suggestions:entry_policy:* as overrides_v1 (optional)
  - human-readable markdown report (stdout)

Usage:
  cd python-worker
  PYTHONPATH=".:.." python tools/tm_policy_tuner.py --input /tmp/closed_7d.ndjson --window-days 7
"""

from utils.time_utils import get_ny_time_millis

import argparse
import hashlib
import json
import os
import time
from typing import Any, Dict, List, Tuple

import redis

try:
    from core.entry_policy_overrides_v1 import EntryPolicyOverridesV1
except Exception:
    EntryPolicyOverridesV1 = None  # type: ignore

try:
    from services.ab_winner_evaluator_lcb import LCBEvaluatorPerRegime
except ImportError:
    # Fallback or error if missing? We should have it.
    LCBEvaluatorPerRegime = None


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def load_rows(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            if not isinstance(d, dict):
                continue
            rows.append(d)
    return rows


def load_ndjson(path: str) -> List[Dict[str, Any]]:
    """Alias for load_rows to match autopilot_tm_reports_service import."""
    return load_rows(path)


def group_rows_by_context(rows: List[Dict[str, Any]], *, window_days: float) -> Dict[Tuple[str, str, str, str], List[Dict[str, Any]]]:
    """
    Returns rows keyed by (symbol, regime, scenario, group).
    """
    now_ms = _now_ms()
    min_ts = now_ms - int(window_days * 24 * 3600 * 1000)
    out: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]] = {}
    
    for r in rows:
        ts = _safe_int(r.get("ts_ms") or 0)
        if ts <= 0 or ts < min_ts:
            continue
        sym = str(r.get("symbol") or "").upper()
        rg = str(r.get("regime") or "na").lower()
        scn = str(r.get("scenario") or "").lower()
        grp = str(r.get("ab_group") or "default").lower()
        arm = str(r.get("ab_arm") or "A").upper()
        
        if scn not in ("continuation", "reversal"):
            continue
        if arm not in ("A", "B", "C"):
            continue # skip unknown arms
            
        k = (sym, rg, scn, grp)
        if k not in out:
            out[k] = []
        out[k].append(r)
    return out


def pick_winners(
    grouped_rows: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]],
    *,
    min_samples_default: int,
    min_edge_r: float,
    min_samples_by_regime: Dict[str, int],
    lcb_z: float = None,
) -> List[Dict[str, Any]]:
    """
    Winner per (symbol, regime, scenario, group).
    Uses LCBEvaluatorPerRegime + min_edge_r check.
    """
    out: List[Dict[str, Any]] = []
    
    # Configure evaluator
    lcb_z_def = lcb_z if lcb_z is not None else float(os.getenv("LCB_Z_DEFAULT", "1.28"))
    
    cfg = {
        "min_n_default": min_samples_default,
        "min_n_thin": min_samples_by_regime.get("thin", min_samples_default),
        # Pass other defaults if needed, e.g. from env
        "lcb_z_default": lcb_z_def,
        "lcb_z_thin": float(os.getenv("LCB_Z_THIN", "1.64")),
        "min_lcb_r_default": float(os.getenv("LCB_MIN_R_DEFAULT", "0.05")),
        "min_lcb_r_thin": float(os.getenv("LCB_MIN_R_THIN", "0.10")),
    }
    
    if LCBEvaluatorPerRegime:
        evaluator = LCBEvaluatorPerRegime(cfg=cfg)
    else:
        # Fallback if not available (should not happen in this env)
        return []

    for (sym, rg, scn, grp), rows in grouped_rows.items():
        res = evaluator.pick_winner(rows)
        if not res:
            continue
            
        winner_arm = res.get("winner_arm", "A")
        
        # Extract stats for A and Winner to check edge
        arms_stats = {a["arm"]: a for a in res.get("arms", [])}
        stat_a = arms_stats.get("A")
        stat_w = arms_stats.get(winner_arm)
        
        n_a = int(stat_a.get("n", 0)) if stat_a else 0
        n_w = int(stat_w.get("n", 0)) if stat_w else 0
        lcb_a = float(stat_a.get("lcb_r", -999.0)) if stat_a else -999.0
        lcb_w = float(stat_w.get("lcb_r", -999.0)) if stat_w else -999.0
        
        # Edge check
        edge = lcb_w - lcb_a
        
        # If winner is not A, enforce min_edge_r
        if winner_arm != "A":
             if edge < min_edge_r:
                 # Revert to A
                 winner_arm = "A"
                 # but we still want to report what happened? 
                 # Or just skip reporting as "winner"?
                 # The tuner usually reports "eligible winners". 
                 # If it reverts to A, it's effectively "no change".
                 continue
        
        # If winner is A, we generally don't propose it unless we want to "reset" to A?
        # Typically autopilot proposes *changes* or confirms winners.
        # If A is winner, we skip creating a proposal usually, UNLESS we are currently on B/C?
        # But we don't know current state here (offline tool).
        # We'll skip A winners to reduce noise/writes, assuming A is default.
        if winner_arm == "A":
            continue

        out.append({
            "symbol": sym,
            "regime": rg,
            "scenario": scn,
            "group": grp,
            "winner_arm": winner_arm,
            "edge_lcb_r": edge,
            "a_lcb_r": lcb_a,
            "winner_lcb_r": lcb_w,
            "n_a": n_a,
            "n_w": n_w,
        })
            
    return out


def tune(
    input_path: str,
    *,
    window_days: float,
    min_n: int,
    min_edge_r: float = None,
) -> Dict[str, Any]:
    """
    Main tuning function that processes NDJSON and returns winners.
    Returns dict with "winners" key.
    """
    if min_edge_r is None:
        min_edge_r = float(os.getenv("LCB_MIN_EDGE_R", "0.05"))
    
    rows = load_rows(input_path)
    grouped = group_rows_by_context(rows, window_days=window_days)
    
    min_samples_by_regime = {
        "thin": int(os.getenv("LCB_MIN_SAMPLES_THIN", str(min_n))),
        "news": int(os.getenv("LCB_MIN_SAMPLES_THIN", str(min_n))),
        "illiquid": int(os.getenv("LCB_MIN_SAMPLES_THIN", str(min_n))),
    }
    
    winners = pick_winners(
        grouped,
        min_samples_default=min_n,
        min_edge_r=min_edge_r,
        min_samples_by_regime=min_samples_by_regime,
    )
    
    return {"winners": winners, "window_days": window_days, "min_n": min_n}


def build_overrides_v1_proposal(tuner_out: Dict[str, Any]) -> Dict[str, Any]:
    """
    Builds overrides_v1 proposal dict from tuner output without writing to Redis.
    Returns dict with proposal data.
    """
    winners = tuner_out.get("winners", [])
    if not winners:
        return {}
    
    proposals = []
    for w in winners:
        sym = str(w["symbol"]).upper()
        rg = str(w["regime"]).lower()
        scn = str(w["scenario"]).lower()
        grp = str(w.get("group") or "default").lower()
        arm = str(w["winner_arm"]).upper()
        
        if EntryPolicyOverridesV1 is None:
            # Fallback: build minimal dict
            proposals.append({
                "symbol": sym,
                "regime": rg,
                "scenario": scn,
                "group": grp,
                "force_active_arm": arm,
                "edge_lcb_r": float(w.get("edge_lcb_r") or 0.0),
            })
            continue
        
        o = EntryPolicyOverridesV1(
            updated_ts_ms=_now_ms(),
            enabled=1,
            symbol=sym,
            regime=rg,
            scenario=scn,
            group=grp,
            force_active_arm=arm,
            freeze_active=0,
            overrides_hold_down_ms=int(os.getenv("OVR_HOLD_MS", "600000")),
            ab_split_b=int(os.getenv("AB_SPLIT_B", "10")),
            ab_split_c=int(os.getenv("AB_SPLIT_C", "10")),
            ab_salt=str(os.getenv("AB_SALT", "v1")),
            extra={
                "source": "tm_policy_tuner",
                "method": "lcb_mean_r",
                "edge_lcb_r": float(w.get("edge_lcb_r") or 0.0),
                "a_lcb_r": float(w.get("a_lcb_r") or 0.0),
                "winner_lcb_r": float(w.get("winner_lcb_r") or 0.0),
                "n_a": int(w.get("n_a") or 0),
                "n_w": int(w.get("n_w") or 0),
            },
        )
        ok, why = o.validate()
        if not ok:
            continue
        
        sid = _sha1(f"overrides_v1|{sym}|{rg}|{scn}|{grp}|{arm}|{int(o.updated_ts_ms)}")
        
        proposals.append({
            "sid": sid,
            "overrides_v1_json": o.to_json(),
            "meta_key": f"cfg:suggestions:entry_policy:meta:{sid}",
            "latest_key": f"cfg:suggestions:entry_policy:latest:overrides_v1:{sym}:{rg}:{grp}:{scn}",
            "symbol": sym,
            "regime": rg,
            "group": grp,
            "winner_arm": arm,
        })
    
    return {"proposals": proposals, "count": len(proposals)}


def write_proposals_overrides_v1(
    *,
    r: redis.Redis,
    winners: List[Dict[str, Any]],
    approvals_required: int = 2,
    suggest_prefix: str = "cfg:suggestions:entry_policy",
) -> int:
    """
    Writes overrides_v1 proposals as meta:{sid} + approvals/applied scaffolding + latest pointer.
    """
    if EntryPolicyOverridesV1 is None:
        return 0
    n = 0
    for w in winners:
        sym = str(w["symbol"]).upper()
        rg = str(w["regime"]).lower()
        scn = str(w["scenario"]).lower()
        grp = str(w.get("group") or "default").lower()
        arm = str(w["winner_arm"]).upper()

        o = EntryPolicyOverridesV1(
            updated_ts_ms=_now_ms(),
            enabled=1,
            symbol=sym,
            regime=rg,
            scenario=scn,
            group=grp,
            force_active_arm=arm,
            freeze_active=0,
            overrides_hold_down_ms=int(os.getenv("OVR_HOLD_MS", "600000")),
            # Keep defaults for AB splits; can be overridden later
            ab_split_b=int(os.getenv("AB_SPLIT_B", "10")),
            ab_split_c=int(os.getenv("AB_SPLIT_C", "10")),
            ab_salt=str(os.getenv("AB_SALT", "v1")),
            extra={
                "source": "tm_policy_tuner",
                "method": "lcb_mean_r",
                "edge_lcb_r": float(w.get("edge_lcb_r") or 0.0),
                "a_lcb_r": float(w.get("a_lcb_r") or 0.0),
                "winner_lcb_r": float(w.get("winner_lcb_r") or 0.0),
                "n_a": int(w.get("n_a") or 0),
                "n_w": int(w.get("n_w") or 0),
            },
        )
        ok, why = o.validate()
        if not ok:
            continue
        sid = _sha1(f"overrides_v1|{sym}|{rg}|{scn}|{grp}|{arm}|{int(o.updated_ts_ms)}")

        meta_key = f"{suggest_prefix}:meta:{sid}"
        appr_key = f"{suggest_prefix}:approvals:{sid}"
        applied_key = f"{suggest_prefix}:applied:{sid}"
        latest_key = f"{suggest_prefix}:latest:overrides_v1:{sym}:{rg}:{grp}:{scn}"

        pipe = r.pipeline()
        pipe.set(meta_key, o.to_json())
        pipe.set(latest_key, sid)
        # initialize approvals as empty set/hash
        pipe.delete(appr_key)
        pipe.delete(applied_key)
        pipe.execute()
        n += 1
    return n


def render_report_md(winners: List[Dict[str, Any]], *, window_days: float) -> str:
    lines: List[str] = []
    lines.append(f"Autopilot Tier/Arm Report (window={window_days:.1f}d)")
    if not winners:
        lines.append("No eligible winners (min_n / edge / taxonomy).")
        return "\n".join(lines)
    lines.append("")
    lines.append("| symbol | regime | scenario | group | winner | edge LCB_R | n(A) | n(W) |")
    lines.append("|---|---|---|---|---|---:|---:|---:|")
    for w in winners[:50]:
        lines.append(
            f"| {w['symbol']} | {w['regime']} | {w['scenario']} | {w.get('group','default')} | {w['winner_arm']} | {float(w['edge_lcb_r']):.3f} | {int(w['n_a'])} | {int(w['n_w'])} |"
        )
    if len(winners) > 50:
        lines.append(f"... ({len(winners)-50} more)")
    return "\n".join(lines)


def render_report(winners: List[Dict[str, Any]], window_days: float = 7.0) -> str:
    """Wrapper for render_report_md to match autopilot_tm_reports_service import."""
    return render_report_md(winners, window_days=window_days)


def main() -> None:
    ap = argparse.ArgumentParser()
    # Support both --input and --in (legacy)
    ap.add_argument("--input", type=str, required=False, help="Input NDJSON")
    ap.add_argument("--in", dest="input_alias", type=str, required=False, help="Alias for --input")
    
    ap.add_argument("--window-days", type=float, default=7.0)
    
    # Support --min-samples and --min-n (legacy)
    ap.add_argument("--min-samples", type=int, default=int(os.getenv("LCB_MIN_SAMPLES", "30")))
    ap.add_argument("--min-n", dest="min_samples_alias", type=int, required=False, help="Alias for --min-samples")
    
    ap.add_argument("--min-edge-r", type=float, default=float(os.getenv("LCB_MIN_EDGE_R", "0.05")))
    ap.add_argument("--redis-write", action="store_true", default=False)
    ap.add_argument("--redis-url", type=str, default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    
    # Extra args for compatibility with scanners/bots
    ap.add_argument("--out-json", type=str, required=False, help="Path to write output JSON report")
    ap.add_argument("--out-md", type=str, required=False, help="Path to write output Markdown report")
    ap.add_argument("--out", type=str, required=False, help="Alias for --out-md (used by reporter bot)")
    ap.add_argument("--lcb-z", type=float, required=False, help="Override LCB Z-score (defaults to env)")
    ap.add_argument("--conf", type=str, required=False, help="Ignored (for compatibility)")
    
    args = ap.parse_args()
    
    # Resolve aliases
    input_path = args.input or args.input_alias
    if not input_path:
        # Fallback check or error
        print("Error: --input or --in is required.")
        sys.exit(1)
        
    min_samples = args.min_samples
    if args.min_samples_alias is not None:
        min_samples = args.min_samples_alias

    rows = load_rows(input_path)
    # stats = group_stats(rows, window_days=float(args.window_days)) # OLD
    grouped = group_rows_by_context(rows, window_days=float(args.window_days))
    
    min_samples_by_regime = {
        "thin": int(os.getenv("LCB_MIN_SAMPLES_THIN", str(min_samples))),
        "news": int(os.getenv("LCB_MIN_SAMPLES_THIN", str(min_samples))),
        "illiquid": int(os.getenv("LCB_MIN_SAMPLES_THIN", str(min_samples))),
    }
    
    winners = pick_winners(
        grouped,
        min_samples_default=int(min_samples),
        min_edge_r=float(args.min_edge_r),
        min_samples_by_regime=min_samples_by_regime,
        lcb_z=args.lcb_z,
    )

    if args.redis_write:
        r = redis.from_url(args.redis_url, decode_responses=True)
        n = write_proposals_overrides_v1(r=r, winners=winners)
        print(f"redis_written={n}")
        
    # Generate report
    md_report = render_report_md(winners, window_days=float(args.window_days))
    print(md_report)
    
    # Write files if requested
    if args.out_md or args.out:
        p = args.out_md or args.out
        try:
            with open(p, "w", encoding="utf-8") as f:
                f.write(md_report)
        except Exception as e:
            print(f"Warning: failed to write MD to {p}: {e}", file=sys.stderr)
            
    if args.out_json:
        try:
            out_data = {
                "winners": winners,
                "window_days": args.window_days,
                "min_n": min_samples,
                "generated_at_ts": _now_ms()
            }
            with open(args.out_json, "w", encoding="utf-8") as f:
                json.dump(out_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Warning: failed to write JSON to {args.out_json}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
