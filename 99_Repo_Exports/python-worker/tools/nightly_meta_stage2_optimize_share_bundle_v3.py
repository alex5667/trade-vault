#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nightly_meta_stage2_optimize_share_bundle_v3.py

Nightly tool for Stage2 share optimization per cell (SYMBOL|bucket) - v3 with group caps and per-symbol budget.

"min-over-cells safety": Group-cap по bucket'ам (range ≤ 0.50 всегда; и/или "пока trend < X → range ≤ Y").
Per-symbol budget на суммарный exec_rate_drop (turnover proxy): если у символа суммарный drop > budget → 
автоматически выбираем более низкие shares (по grid) так, чтобы уложиться в бюджет.

Flow:
  1. Reads meta:unfreeze:cells (Stage1 progress)
  2. Waits META_STAGE1_EVAL_DELAY_HOURS
  3. For each eligible symbol (stage==1, delay passed):
     - Groups cells by symbol (trend + range buckets)
     - For each bucket, builds options (share grid with constraints)
     - Picks best combo (trend, range) per symbol under:
       * Group caps: trend ≤ META_BUCKET_CAP_TREND, range ≤ META_BUCKET_CAP_RANGE
       * Per-symbol budget: sum(exec_rate_drop) ≤ META_SYMBOL_EXEC_DROP_BUDGET
       * Optional coupling: if trend < threshold → range ≤ cap
  4. Proposes Stage2 bundle (manual approve via bundle → preview → confirm)

Critical requirements:
  - events:trades (POSITION_CLOSED) must have:
    * meta_veto (0/1) — "model would have vetoed"
    * meta_enforce_key (string, deterministic key — SID/stable id)
    * meta_enforce_salt (string, usually enf_v1)
    * regime_group or regime (for bucket trend/range)
    * r_mult, exit_ts_ms/ts_ms, symbol

  Without meta_veto + meta_enforce_key we cannot correctly simulate different shares.

Usage:
  python -m tools.nightly_meta_stage2_optimize_share_bundle_v3
  (reads ENV vars from /etc/trade/of_reports.env or environment)
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import secrets
import subprocess
import sys
import time
import hmac
import hashlib
from typing import Any, Dict, List, Tuple

import redis

from common.log import setup_logger

logger = setup_logger("NightlyMetaStage2OptimizeShareV3")


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def sign(bid: str, secret: str) -> str:
    """Generates short HMAC signature for bundle_id (8 hex characters)."""
    d = hmac.new(secret.encode("utf-8"), bid.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float conversion."""
    try:
        return float(x)
    except Exception:
        return float(d)


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion."""
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _event_ts_ms(r: Dict[str, Any]) -> int:
    """Extracts event timestamp in milliseconds from trade record."""
    for k in ("exit_ts_ms", "ts_ms", "ts", "event_ts_ms"):
        if k in r:
            v = r.get(k)
            try:
                vv = int(float(v))
                if vv > 10_000_000_000:
                    return vv
                if 1_000_000_000 < vv < 10_000_000_000:
                    return vv * 1000
            except Exception:
                pass
    return 0


def regime_bucket(t: Dict[str, Any]) -> str:
    """Maps trade record to regime bucket (trend/range/news/thin/other)."""
    g = str(t.get("regime_group", "") or t.get("regime", "") or t.get("scenario_v4", "") or "")
    s = g.lower()
    if "news" in s:
        return "news"
    if "trend" in s or "bull" in s or "bear" in s:
        return "trend"
    from common.market_mode import is_range_regime; _r = is_range_regime(s)
    if _r:
        return "range"
    if "thin" in s or "illiquid" in s:
        return "thin"
    return "other"


def _hash01(s: str) -> float:
    """Deterministic hash to [0, 1) for canary selection."""
    h = hashlib.sha256(s.encode("utf-8")).digest()
    x = int.from_bytes(h[:8], "big", signed=False)
    return (x % 10_000_000) / 10_000_000.0


def pctl(xs: List[float], q: float) -> float:
    """Computes percentile q (0.0-1.0) from sorted list."""
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def stats(rs: List[float]) -> Dict[str, float]:
    """Computes basic statistics for returns list."""
    n = len(rs)
    if n == 0:
        return {"n": 0.0}
    mean = sum(rs) / n
    tail = sum(1 for x in rs if x <= -1.0) / n
    win = sum(1 for x in rs if x > 0.0) / n
    return {
        "n": float(n),
        "meanR": float(mean),
        "medianR": float(pctl(rs, 0.50)),
        "p05": float(pctl(rs, 0.05)),
        "p95": float(pctl(rs, 0.95)),
        "winrate": float(win),
        "tail_rate": float(tail),
    }


def simulate_share(rows: List[Dict[str, Any]], *, share: float, salt: str) -> Dict[str, Any]:
    """
    Counterfactual sim:
      apply_enforce = hash(salt:key) < share
      if apply_enforce==1 and meta_veto==1 => blocked -> 0.0 outcome
      else keep r_mult
    rows must have: meta_enforce_key, meta_veto, r_mult
    """
    share = max(0.0, min(1.0, share))
    opp = []
    exec_rs = []
    blocked = 0
    used = 0

    for r in rows:
        key = str(r.get("meta_enforce_key", "") or "")
        if not key:
            continue
        used += 1
        veto = int(r.get("meta_veto", 0) or 0)
        apply_enf = 1 if (_hash01(f"{salt}:{key}") < share) else 0
        if apply_enf == 1 and veto == 1:
            blocked += 1
            opp.append(0.0)
        else:
            rm = float(r.get("r_mult", 0.0) or 0.0)
            opp.append(rm)
            exec_rs.append(rm)

    exec_rate = (len(exec_rs) / used) if used else 0.0
    return {
        "share": share,
        "used": used,
        "blocked": blocked,
        "exec_rate": exec_rate,
        "opp": stats(opp),
        "exec": stats(exec_rs),
    }


def objective(
    rep: Dict[str, Any],
    *,
    exec_rate_ref: float,
    cur_share: float,
    share: float,
    lam_tail: float,
    lam_p05: float,
    lam_turn: float,
    lam_step: float,
) -> float:
    """Multi-objective function for share optimization."""
    opp_mean = float(rep["opp"]["meanR"])
    exec_tail = float(rep["exec"]["tail_rate"])
    opp_p05 = float(rep["opp"]["p05"])
    exec_rate = float(rep["exec_rate"])
    drop = max(0.0, exec_rate_ref - exec_rate)
    return (
        opp_mean
        - lam_tail * exec_tail
        - lam_p05 * max(0.0, -opp_p05)
        - lam_turn * drop
        - lam_step * abs(share - cur_share)
    )


def build_options(
    cell_rows: List[Dict[str, Any]],
    *,
    salt: str,
    cur_share: float,
    grid: List[float],
    share_cap: float,
    max_up_step: float,
    max_down_step: float,
    min_exec_rate: float,
    max_exec_rate_drop: float,
    tail_exec_cap: float,
    lam_tail: float,
    lam_p05: float,
    lam_turn: float,
    lam_step: float,
) -> List[Dict[str, Any]]:
    """
    Returns list of feasible options with fields:
      share, exec_rate, exec_rate_drop, obj, rep
    Includes always the 'cur_share' option.
    """
    cur_share = max(0.0, min(1.0, cur_share))
    lo = max(0.0, cur_share - max(0.0, max_down_step))
    hi = min(1.0, cur_share + max(0.0, max_up_step))
    cap = max(0.0, min(1.0, share_cap))

    # Reference at cur_share
    ref = simulate_share(cell_rows, share=cur_share, salt=salt)
    exec_rate_ref = float(ref["exec_rate"])

    opts = []
    # Ensure cur_share present
    cur_obj = objective(
        ref,
        exec_rate_ref=exec_rate_ref,
        cur_share=cur_share,
        share=cur_share,
        lam_tail=lam_tail,
        lam_p05=lam_p05,
        lam_turn=lam_turn,
        lam_step=lam_step,
    )
    opts.append({
        "share": cur_share,
        "exec_rate": exec_rate_ref,
        "exec_rate_drop": 0.0,
        "obj": cur_obj,
        "rep": ref,
        "is_cur": True,
    })

    for s in grid:
        if s < lo - 1e-12 or s > hi + 1e-12:
            continue
        if s > cap + 1e-12:
            continue
        if abs(s - cur_share) < 1e-12:
            continue

        rep = simulate_share(cell_rows, share=s, salt=salt)
        used = int(rep["used"])
        if used < 200:
            continue

        exec_rate = float(rep["exec_rate"])
        if exec_rate < min_exec_rate:
            continue

        drop = max(0.0, exec_rate_ref - exec_rate)
        if drop > max_exec_rate_drop:
            continue

        exec_tail = float(rep["exec"]["tail_rate"])
        if exec_tail > tail_exec_cap:
            continue

        obj = objective(
            rep,
            exec_rate_ref=exec_rate_ref,
            cur_share=cur_share,
            share=s,
            lam_tail=lam_tail,
            lam_p05=lam_p05,
            lam_turn=lam_turn,
            lam_step=lam_step,
        )

        opts.append({
            "share": s,
            "exec_rate": exec_rate,
            "exec_rate_drop": drop,
            "obj": obj,
            "rep": rep,
            "is_cur": False,
        })

    # keep top-K by objective to limit enumeration size
    opts = sorted(opts, key=lambda x: x["obj"], reverse=True)
    top_k = int(os.getenv("META_OPT_TOPK_PER_CELL", "8") or 8)
    return opts[:max(2, top_k)]


def pick_combo_under_budget(
    trend_opts: List[Dict[str, Any]] | None,
    range_opts: List[Dict[str, Any]] | None,
    *,
    budget: float,
    range_cap_when_trend_lt: float | None,
    trend_threshold: float | None,
) -> Dict[str, Any]:
    """
    Enumerate combos (small) and choose best total objective under:
      sum(exec_rate_drop) <= budget
      optional: if trend_share < threshold -> range_share <= range_cap_when_trend_lt
    """
    if trend_opts is None and range_opts is None:
        return {"ok": False}

    if trend_opts is None:
        trend_opts = [{"share": None, "exec_rate_drop": 0.0, "obj": 0.0, "rep": None, "is_cur": True}]
    if range_opts is None:
        range_opts = [{"share": None, "exec_rate_drop": 0.0, "obj": 0.0, "rep": None, "is_cur": True}]

    best = None
    best_obj = -1e18

    for to in trend_opts:
        for ro in range_opts:
            drop = float(to["exec_rate_drop"]) + float(ro["exec_rate_drop"])
            if drop > budget + 1e-12:
                continue

            # optional coupling: if trend low -> cap range
            if trend_threshold is not None and range_cap_when_trend_lt is not None:
                t_share = to["share"]
                r_share = ro["share"]
                if (t_share is not None) and (r_share is not None):
                    if float(t_share) < float(trend_threshold) - 1e-12:
                        if float(r_share) > float(range_cap_when_trend_lt) + 1e-12:
                            continue

            obj = float(to["obj"]) + float(ro["obj"])
            if obj > best_obj:
                best_obj = obj
                best = {
                    "trend": to,
                    "range": ro,
                    "sum_drop": drop,
                    "sum_obj": obj,
                }

    if best is None:
        # fallback: choose both cur if present
        def pick_cur(opts: List[Dict[str, Any]]) -> Dict[str, Any]:
            for x in opts:
                if x.get("is_cur"):
                    return x
            return opts[-1]
        best = {
            "trend": pick_cur(trend_opts),
            "range": pick_cur(range_opts),
            "sum_drop": float(pick_cur(trend_opts)["exec_rate_drop"]) + float(pick_cur(range_opts)["exec_rate_drop"]),
            "sum_obj": float(pick_cur(trend_opts)["obj"]) + float(pick_cur(range_opts)["obj"]),
            "fallback": "no_combo_under_budget",
        }
    return best


def notify(r: redis.Redis, text: str, buttons: List[List[Dict[str, str]]] | None = None) -> None:
    """Sends notification to Telegram stream."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True)


def main() -> None:
    """Main entry point: optimize share per symbol (trend+range combo) and propose Stage2 bundle."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    out_dir = os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out")
    prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    reg_unf = os.getenv("META_UNFREEZE_REGISTRY_KEY", "meta:unfreeze:cells")

    stage1_delay_h = float(os.getenv("META_STAGE1_EVAL_DELAY_HOURS", "24") or 24)

    # Export window (history for optimization)
    opt_hours = float(os.getenv("META_OPT_EXPORT_HOURS", "336") or 336)

    # Separate grids per bucket
    grid_trend = [float(x) for x in (os.getenv("META_OPT_SHARE_GRID_TREND", "0.10,0.25,0.35,0.50,0.75,1.00") or "").split(",") if x.strip()]
    grid_range = [float(x) for x in (os.getenv("META_OPT_SHARE_GRID_RANGE", "0.10,0.15,0.25,0.35,0.50") or "").split(",") if x.strip()]
    grid_trend = sorted(set([max(0.0, min(1.0, s)) for s in grid_trend])) or [0.10, 0.25, 0.35, 0.50]
    grid_range = sorted(set([max(0.0, min(1.0, s)) for s in grid_range])) or [0.10, 0.15, 0.25, 0.35]

    # Step regularization
    max_up_step = float(os.getenv("META_OPT_MAX_UP_STEP", "0.25") or 0.25)
    max_down_step = float(os.getenv("META_OPT_MAX_DOWN_STEP", "0.00") or 0.00)

    # Turnover proxy constraints
    min_exec_rate = float(os.getenv("META_OPT_MIN_EXEC_RATE", "0.30") or 0.30)
    max_exec_rate_drop = float(os.getenv("META_OPT_MAX_EXEC_RATE_DROP", "0.20") or 0.20)

    # Risk caps + objective weights
    tail_exec_cap = float(os.getenv("META_OPT_TAIL_EXEC_MAX", "0.18") or 0.18)
    lam_tail = float(os.getenv("META_OPT_LAM_TAIL", "0.50") or 0.50)
    lam_p05 = float(os.getenv("META_OPT_LAM_P05", "0.10") or 0.10)
    lam_turn = float(os.getenv("META_OPT_LAM_TURN", "0.30") or 0.30)
    lam_step = float(os.getenv("META_OPT_LAM_STEP", "0.05") or 0.05)

    # New safety layers
    cap_trend = float(os.getenv("META_BUCKET_CAP_TREND", "1.00") or 1.00)
    cap_range = float(os.getenv("META_BUCKET_CAP_RANGE", "0.50") or 0.50)

    symbol_budget = float(os.getenv("META_SYMBOL_EXEC_DROP_BUDGET", "0.25") or 0.25)  # sum drop across buckets
    # Optional coupling: if trend share < threshold -> range cap stricter
    trend_threshold = os.getenv("META_RANGE_CAP_IF_TREND_LT", "")
    range_cap_when_trend_lt = os.getenv("META_RANGE_CAP_WHEN_TREND_LT", "")
    trend_threshold_v = float(trend_threshold) if trend_threshold.strip() else None
    range_cap_when_trend_lt_v = float(range_cap_when_trend_lt) if range_cap_when_trend_lt.strip() else None

    max_cells = int(os.getenv("META_OPT_MAX_CELLS", "6") or 6)

    # Load stage1 progress cells (stage==1, delay passed)
    unf_map = r.hgetall(reg_unf) or {}
    if not unf_map:
        logger.info("No unfreezing cells found, skipping")
        return

    # candidates grouped by symbol: {sym: {bucket: (cell, rec)}}
    cand: Dict[str, Dict[str, Tuple[str, dict]]] = {}
    for ck, raw in unf_map.items():
        try:
            rec = json.loads(raw)
        except Exception:
            continue
        if int(rec.get("stage", 0) or 0) != 1:
            continue
        applied_ms = int(rec.get("applied_ms", 0) or 0)
        if applied_ms <= 0:
            continue
        if now_ms() - applied_ms < int(stage1_delay_h * 3600_000):
            continue

        cell = str(rec.get("cell", ck) or ck)
        if "|" not in cell:
            continue
        sym, bucket = cell.split("|", 1)
        sym = sym.upper().strip()
        bucket = bucket.lower().strip()
        if bucket not in ("trend", "range"):
            continue

        cand.setdefault(sym, {})
        if bucket not in cand[sym]:
            cand[sym][bucket] = (cell, rec)

    if not cand:
        logger.info("No eligible Stage1 cells found for Stage2 optimization")
        return

    # Export trades once
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{out_dir}/meta_opt_stage2_v3_{ts}"
    os.makedirs(run_dir, exist_ok=True)
    trades_out = f"{run_dir}/trades.ndjson"

    try:
        subprocess.check_call([
            sys.executable, "tools/export_trade_closed_ndjson.py",
            "--since-hours", str(opt_hours),
            "--out", trades_out,
            "--stream", os.getenv("TRADE_EVENTS_STREAM", "events:trades"),
            "--redis-url", redis_url,
            "--max-scan", os.getenv("TRADES_MAX_SCAN", "500000"),
        ])
    except subprocess.CalledProcessError as e:
        logger.error("Failed to export trades: %s", e)
        return
    except Exception as e:
        logger.error("Unexpected error during trade export: %s", e)
        return

    # Parse trades into compact rows; require meta_enforce_key + meta_veto
    rows: List[Dict[str, Any]] = []
    missing_key = 0
    missing_veto = 0
    total = 0

    try:
        with open(trades_out, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                t = json.loads(line)
                sym = str(t.get("symbol", "") or "").upper()
                ts_ms = _event_ts_ms(t)
                if ts_ms <= 0:
                    continue
                rm = t.get("r_mult", None)
                if rm is None:
                    continue
                total += 1

                key = t.get("meta_enforce_key", None)
                veto = t.get("meta_veto", None)
                if key is None:
                    missing_key += 1
                if veto is None:
                    missing_veto += 1

                rows.append({
                    "symbol": sym,
                    "bucket": regime_bucket(t),
                    "ts_ms": ts_ms,
                    "r_mult": _f(rm, 0.0),
                    "meta_enforce_key": "" if key is None else str(key),
                    "meta_enforce_salt": str(t.get("meta_enforce_salt", "enf_v1") or "enf_v1"),
                    "meta_veto": 0 if veto is None else _i(veto, 0),
                })
    except FileNotFoundError:
        logger.error("Trades export file not found: %s", trades_out)
        return
    except Exception as e:
        logger.error("Failed to parse trades: %s", e)
        return

    if total > 0 and (missing_key / total) > 0.30:
        notify(r, f"<b>Stage2 optimize v3 skipped</b>\nreason=<code>missing_meta_enforce_key</code>\nmissing={missing_key}/{total}")
        logger.warning("Stage2 optimize v3 skipped: missing_meta_enforce_key %d/%d", missing_key, total)
        return
    if total > 0 and (missing_veto / total) > 0.30:
        notify(r, f"<b>Stage2 optimize v3 skipped</b>\nreason=<code>missing_meta_veto</code>\nmissing={missing_veto}/{total}")
        logger.warning("Stage2 optimize v3 skipped: missing_meta_veto %d/%d", missing_veto, total)
        return

    # Build per-symbol combos, then create ops
    ops = []
    details = {}
    picked_cells = []

    # limit symbols processed per run
    sym_list = sorted(list(cand.keys()))[:max_cells]

    for sym in sym_list:
        buckets = cand[sym]
        # build options per bucket (trend/range)
        bucket_opts: Dict[str, List[Dict[str, Any]]] = {}

        for bucket in ("trend", "range"):
            if bucket not in buckets:
                continue

            cell, rec = buckets[bucket]
            cell_rows = [x for x in rows if x["symbol"] == sym and x["bucket"] == bucket]
            if len(cell_rows) < 500:
                # insufficient history for this bucket; skip this bucket
                logger.info("Cell %s has insufficient trades: %d < 500", cell, len(cell_rows))
                continue

            # salt: most common
            salt_counts = {}
            for x in cell_rows:
                s = x.get("meta_enforce_salt", "enf_v1")
                salt_counts[s] = salt_counts.get(s, 0) + 1
            salt = max(salt_counts.items(), key=lambda kv: kv[1])[0] if salt_counts else "enf_v1"

            hk = f"{prefix}{sym}"
            share_field = f"meta_enforce_share_{bucket}"
            cur_share = float(r.hget(hk, share_field) or (rec.get("target_share") or 0.10) or 0.10)

            grid = grid_trend if bucket == "trend" else grid_range
            cap = cap_trend if bucket == "trend" else cap_range

            opts = build_options(
                cell_rows,
                salt=salt,
                cur_share=cur_share,
                grid=grid,
                share_cap=cap,
                max_up_step=max_up_step,
                max_down_step=max_down_step,
                min_exec_rate=min_exec_rate,
                max_exec_rate_drop=max_exec_rate_drop,
                tail_exec_cap=tail_exec_cap,
                lam_tail=lam_tail,
                lam_p05=lam_p05,
                lam_turn=lam_turn,
                lam_step=lam_step,
            )

            bucket_opts[bucket] = opts
            details[cell] = {
                "symbol": sym,
                "bucket": bucket,
                "field": share_field,
                "cur_share": cur_share,
                "cap": cap,
                "salt": salt,
                "grid": grid,
                "opts_top": [
                    {"share": o["share"], "drop": o["exec_rate_drop"], "obj": o["obj"], "tail": o["rep"]["exec"]["tail_rate"], "exec_rate": o["exec_rate"]}
                    for o in opts[:6]
                ],
            }

        if not bucket_opts:
            continue

        # optional coupling: if trend low then stricter range cap
        combo = pick_combo_under_budget(
            bucket_opts.get("trend"),
            bucket_opts.get("range"),
            budget=symbol_budget,
            range_cap_when_trend_lt=range_cap_when_trend_lt_v,
            trend_threshold=trend_threshold_v,
        )

        # Apply combo: set shares for buckets where it changes
        hk = f"{prefix}{sym}"
        any_change = False

        for bucket in ("trend", "range"):
            if bucket not in buckets:
                continue
            cell, rec = buckets[bucket]
            share_field = f"meta_enforce_share_{bucket}"
            cur_share = float(r.hget(hk, share_field) or (rec.get("target_share") or 0.10) or 0.10)
            chosen = combo.get(bucket)
            if not chosen or chosen.get("share") is None:
                continue
            new_share = float(chosen["share"])
            if abs(new_share - cur_share) < 1e-9:
                continue

            ops.append({"op": "HSET", "key": hk, "field": "meta_model_enable", "value": "1"})
            ops.append({"op": "HSET", "key": hk, "field": "meta_model_mode", "value": "ENFORCE"})
            ops.append({"op": "HSET", "key": hk, "field": share_field, "value": f"{new_share:.2f}"})

            picked_cells.append(cell)
            details[cell]["chosen"] = {
                "new_share": new_share,
                "cur_share": cur_share,
                "exec_drop": float(chosen["exec_rate_drop"]),
                "obj": float(chosen["obj"]),
            }
            any_change = True

        # attach per-symbol combo info
        details[f"{sym}|__combo__"] = {
            "symbol_budget": symbol_budget,
            "sum_drop": combo.get("sum_drop"),
            "sum_obj": combo.get("sum_obj"),
            "fallback": combo.get("fallback", ""),
            "coupling": {
                "trend_threshold": trend_threshold_v,
                "range_cap_when_trend_lt": range_cap_when_trend_lt_v,
            },
        }

        if not any_change:
            # no change under caps/budget; still ok
            pass

    # Deduplicate ops (same key/field might repeat)
    seen = set()
    uniq_ops = []
    for op in ops:
        k = (op["key"], op["field"], op["value"])
        if k in seen:
            continue
        seen.add(k)
        uniq_ops.append(op)

    if not uniq_ops:
        logger.info("No Stage2 operations generated")
        return

    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)

    bundle = {
        "id": bundle_id,
        "created_ms": now_ms(),
        "ttl_sec": ttl,
        "who": "nightly_meta_stage2_optimize_share_bundle_v3",
        "ops": uniq_ops,
        "meta": {
            "kind": "meta_enforce_unfreeze_stage",
            "stage": 2,
            "optimized": True,
            "multiobjective": True,
            "safety_layer": "group_cap_and_symbol_budget",
            "picked_cells": picked_cells,
            "bucket_caps": {"trend": cap_trend, "range": cap_range},
            "symbol_budget_exec_drop": symbol_budget,
            "details": details,
        },
    }

    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)

    buttons = [[
        {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bundle_id}:{sig}"},
        {"text": "❌ Reject",           "callback": f"recs:reject:{bundle_id}:{sig}"},
    ]]

    notify(
        r,
        "<b>Stage2 UNFREEZE proposal (per-cell v3 + group caps + per-symbol budget)</b>\n"
        f"id=<code>{bundle_id}</code>\n"
        f"cells=<code>{picked_cells}</code>\n"
        f"caps=<code>{{'trend':{cap_trend:.2f},'range':{cap_range:.2f}}}</code>\n"
        f"symbol_budget_drop=<code>{symbol_budget:.2f}</code>\n"
        f"coupling=<code>{{'trend_lt':{trend_threshold_v},'range_cap':{range_cap_when_trend_lt_v}}}</code>",
        buttons=buttons,
    )
    logger.info("Proposed Stage2 unfreeze for %d cells", len(picked_cells))


if __name__ == "__main__":
    main()

