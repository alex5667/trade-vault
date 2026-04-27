#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nightly_meta_stage2_optimize_share_bundle.py

Nightly tool for Stage2 share optimization per cell (SYMBOL|bucket).

Flow:
  1. Reads meta:unfreeze:cells (Stage1 progress)
  2. Waits META_STAGE1_EVAL_DELAY_HOURS
  3. Checks "Stage1 window" (already implemented)
  4. If OK → optimizes share by grid and proposes Stage2 bundle (manual approve)

Critical requirements:
  - events:trades (POSITION_CLOSED) must have:
    * meta_veto (0/1) — "model would have vetoed"
    * meta_enforce_key (string, deterministic key — SID/stable id)
    * meta_enforce_salt (string, usually enf_v1)
    * regime_group or regime (for bucket trend/range)
    * r_mult, exit_ts_ms/ts_ms, symbol

  Without meta_veto + meta_enforce_key we cannot correctly simulate different shares.

Usage:
  python -m tools.nightly_meta_stage2_optimize_share_bundle
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

logger = setup_logger("NightlyMetaStage2OptimizeShare")


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


def notify(r: redis.Redis, text: str, buttons: List[List[Dict[str, str]]] | None = None) -> None:
    """Sends notification to Telegram stream."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True)


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
        "tail_rate": float(tail),
        "winrate": float(win),
        "medianR": float(pctl(rs, 0.5)),
    }


def simulate_share(
    rows: List[Dict[str, Any]],
    *,
    share: float,
    salt: str,
    min_exec_rate: float,
) -> Dict[str, Any]:
    """
    Simulates counterfactual share application.

    rows represent "opportunities" = closed trades that happened under current policy.
    We simulate counterfactual: if apply_enforce and meta_veto==1 -> trade is blocked (0 outcome).
    Otherwise keep r_mult.

    Requires:
      row["meta_veto"] (0/1)
      row["meta_enforce_key"] (string)

    Args:
        rows: List of trade records with meta_veto, meta_enforce_key, r_mult
        share: Share to simulate (0.0-1.0)
        salt: Salt for deterministic hashing
        min_exec_rate: Minimum execution rate constraint

    Returns:
        Dictionary with simulation results
    """
    share = max(0.0, min(1.0, share))
    exec_rs: List[float] = []
    opp_rs: List[float] = []
    blocked = 0
    n = len(rows)

    for r in rows:
        key = str(r.get("meta_enforce_key", "") or "")
        if not key:
            continue
        veto = int(r.get("meta_veto", 0) or 0)
        apply_enf = 1 if (_hash01(f"{salt}:{key}") < share) else 0
        if apply_enf == 1 and veto == 1:
            blocked += 1
            opp_rs.append(0.0)
        else:
            rm = float(r.get("r_mult", 0.0) or 0.0)
            opp_rs.append(rm)
            exec_rs.append(rm)

    exec_rate = (len(exec_rs) / n) if n else 0.0
    return {
        "share": share,
        "n": n,
        "blocked": blocked,
        "exec_rate": exec_rate,
        "opp": stats(opp_rs),
        "exec": stats(exec_rs),
        "ok_exec_rate": exec_rate >= min_exec_rate,
    }


def pick_best_share(
    rows: List[Dict[str, Any]],
    *,
    grid: List[float],
    salt: str,
    tail_cap_exec: float,
    min_exec_rate: float,
    lam_tail: float,
    lam_drop: float,
) -> Tuple[float, Dict[str, Any]]:
    """
    Picks best share from grid by optimizing objective function.

    Objective: maximize opp_meanR - lam_tail * exec_tail - lam_drop * (1 - exec_rate)
    Constraints:
      exec_tail <= tail_cap_exec
      exec_rate >= min_exec_rate

    Args:
        rows: List of trade records
        grid: List of share values to test
        salt: Salt for deterministic hashing
        tail_cap_exec: Maximum allowed tail rate for executed trades
        min_exec_rate: Minimum execution rate
        lam_tail: Lambda weight for tail rate penalty
        lam_drop: Lambda weight for execution rate drop penalty

    Returns:
        Tuple of (best_share, best_report)
    """
    best_s = grid[0]
    best_rep = None
    best_obj = -1e18

    for s in grid:
        rep = simulate_share(rows, share=s, salt=salt, min_exec_rate=min_exec_rate)
        if not rep["ok_exec_rate"]:
            continue
        exec_tail = float(rep["exec"]["tail_rate"])
        if exec_tail > tail_cap_exec:
            continue

        opp_mean = float(rep["opp"]["meanR"])
        exec_rate = float(rep["exec_rate"])
        obj = opp_mean - lam_tail * exec_tail - lam_drop * (1.0 - exec_rate)

        rep["objective"] = obj
        if obj > best_obj:
            best_obj = obj
            best_s = s
            best_rep = rep

    if best_rep is None:
        # fallback: smallest share (safest) but report why
        best_s = min(grid)
        best_rep = simulate_share(rows, share=best_s, salt=salt, min_exec_rate=0.0)
        best_rep["objective"] = None
        best_rep["fallback"] = "no_feasible_share"
    return best_s, best_rep


def main() -> None:
    """Main entry point: optimize share per cell and propose Stage2 bundle."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    out_dir = os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out")
    prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    reg_unf = os.getenv("META_UNFREEZE_REGISTRY_KEY", "meta:unfreeze:cells")

    stage1_delay_h = float(os.getenv("META_STAGE1_EVAL_DELAY_HOURS", "24") or 24)
    stage1_eval_h = float(os.getenv("META_STAGE1_EVAL_HOURS", "48") or 48)

    # optimization window
    opt_hours = float(os.getenv("META_OPT_EXPORT_HOURS", "336") or 336)  # 14d
    # share grid
    grid_str = os.getenv("META_OPT_SHARE_GRID", "0.10,0.25,0.50,1.00") or ""
    grid = [float(x) for x in grid_str.split(",") if x.strip()]
    grid = sorted(set([max(0.0, min(1.0, s)) for s in grid]))
    if not grid:
        grid = [0.10, 0.25, 0.50, 1.00]

    # constraints/objective params
    tail_cap_exec = float(os.getenv("META_OPT_TAIL_EXEC_MAX", "0.18") or 0.18)
    min_exec_rate = float(os.getenv("META_OPT_MIN_EXEC_RATE", "0.30") or 0.30)
    lam_tail = float(os.getenv("META_OPT_LAM_TAIL", "0.50") or 0.50)
    lam_drop = float(os.getenv("META_OPT_LAM_DROP", "0.05") or 0.05)

    max_cells = int(os.getenv("META_OPT_MAX_CELLS", "5") or 5)

    # load stage1 progress cells
    unf_map = r.hgetall(reg_unf) or {}
    if not unf_map:
        logger.info("No unfreezing cells found, skipping")
        return

    # export trades once
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{out_dir}/meta_opt_stage2_{ts}"
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

    # parse trades into compact list
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

    # hard fail if missing too much required tags
    if total > 0 and (missing_key / total) > 0.30:
        notify(r, f"<b>Stage2 optimize skipped</b>\nreason=<code>missing_meta_enforce_key</code>\nmissing={missing_key}/{total}")
        logger.warning("Stage2 optimize skipped: missing_meta_enforce_key %d/%d", missing_key, total)
        return
    if total > 0 and (missing_veto / total) > 0.30:
        notify(r, f"<b>Stage2 optimize skipped</b>\nreason=<code>missing_meta_veto</code>\nmissing={missing_veto}/{total}")
        logger.warning("Stage2 optimize skipped: missing_meta_veto %d/%d", missing_veto, total)
        return

    # select eligible cells: stage==1 and delay passed
    candidates = []
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
        # stage1 eval window end
        t_from = applied_ms
        t_to = min(now_ms(), applied_ms + int(stage1_eval_h * 3600_000))
        candidates.append((sym, bucket, cell, rec, t_from, t_to))

    if not candidates:
        logger.info("No eligible Stage1 cells found for Stage2 optimization")
        return

    ops = []
    meta_cells = []
    meta_details = {}

    # for each candidate cell, optimize share
    for sym, bucket, cell, rec, t_from, t_to in candidates[:max_cells]:
        cell_rows = [x for x in rows if x["symbol"] == sym and x["bucket"] == bucket and (t_from <= x["ts_ms"] < now_ms())]
        if len(cell_rows) < 200:
            logger.info("Cell %s has insufficient trades: %d < 200", cell, len(cell_rows))
            continue

        # choose salt: prefer most common
        salt = "enf_v1"
        salt_counts = {}
        for x in cell_rows:
            s = x.get("meta_enforce_salt", "enf_v1")
            salt_counts[s] = salt_counts.get(s, 0) + 1
        salt = max(salt_counts.items(), key=lambda kv: kv[1])[0] if salt_counts else "enf_v1"

        best_s, rep = pick_best_share(
            cell_rows,
            grid=grid,
            salt=salt,
            tail_cap_exec=tail_cap_exec,
            min_exec_rate=min_exec_rate,
            lam_tail=lam_tail,
            lam_drop=lam_drop,
        )

        # enforce monotonic: do not reduce below current stage1 target
        try:
            stage1_target = float(rec.get("target_share") or 0.10)
        except Exception:
            stage1_target = 0.10
        best_s = max(best_s, stage1_target)

        hk = f"{prefix}{sym}"
        ops.append({"op": "HSET", "key": hk, "field": "meta_model_enable", "value": "1"})
        ops.append({"op": "HSET", "key": hk, "field": "meta_model_mode", "value": "ENFORCE"})
        ops.append({"op": "HSET", "key": hk, "field": f"meta_enforce_share_{bucket}", "value": f"{best_s:.2f}"})

        meta_cells.append(cell)
        meta_details[cell] = {
            "best_share": best_s,
            "salt": salt,
            "rep": rep,
            "grid": grid,
        }

    if not ops:
        logger.info("No Stage2 operations generated")
        return

    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)

    bundle = {
        "id": bundle_id,
        "created_ms": now_ms(),
        "ttl_sec": ttl,
        "who": "nightly_meta_stage2_optimize_share_bundle",
        "ops": ops,
        "meta": {
            "kind": "meta_enforce_unfreeze_stage",
            "stage": 2,
            "cells": meta_cells,
            "optimized": True,
            "details": meta_details,
        },
    }

    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)

    buttons = [[
        {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bundle_id}:{sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{bundle_id}:{sig}"},
    ]]

    notify(
        r,
        "<b>Stage2 UNFREEZE proposal (per-cell optimized share)</b>\n"
        f"id=<code>{bundle_id}</code>\n"
        f"cells=<code>{meta_cells}</code>\n"
        f"grid=<code>{grid}</code>\n"
        f"constraints=<code>{{'tail_exec_max':{tail_cap_exec},'min_exec_rate':{min_exec_rate}}}</code>",
        buttons=buttons,
    )
    logger.info("Proposed Stage2 unfreeze for %d cells", len(meta_cells))


if __name__ == "__main__":
    main()

