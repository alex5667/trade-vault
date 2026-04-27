#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nightly_meta_cells_self_heal.py

Nightly self-healing runner for meta enforce cells: staged unfreeze + auto-refreeze.

Flow:
  1. Check frozen cells (meta:freeze:cells) - if 7+ days old with good stats → propose Stage1 (0.05 → 0.10)
  2. Check Stage1 cells (meta:unfreeze:cells, stage=1) - if degraded → auto-refreeze to 0.05
  3. Check Stage1 cells (meta:unfreeze:cells, stage=1) - if stable → propose Stage2 (0.10 → global_share)

Freeze floor: min(cur, 0.05) instead of 0.00
Staged unfreeze: 0.05 → 0.10 → global_share
Self-healing: auto-refreeze if Stage1 degrades

Usage:
  python -m tools.nightly_meta_cells_self_heal
  (reads ENV vars from /etc/trade/of_reports.env or environment)
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import random
import secrets
import subprocess
import sys
import time
import hmac
import hashlib
from typing import Any, Dict, List, Tuple

import redis

from common.log import setup_logger

logger = setup_logger("NightlyMetaCellsSelfHeal")


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
    return {"n": float(n), "meanR": float(mean), "tail_rate": float(tail), "medianR": float(pctl(rs, 0.5))}


def bootstrap_tail_delta(enf: List[float], ctl: List[float], iters: int, seed: int) -> Dict[str, float]:
    """Bootstrap confidence interval for tail rate delta (enforce - control)."""
    rng = random.Random(seed)
    if len(enf) < 30 or len(ctl) < 30:
        return {"ok": 0.0}
    
    def samp_tail(xs: List[float]) -> float:
        c = 0
        for _ in range(len(xs)):
            if xs[rng.randrange(0, len(xs))] <= -1.0:
                c += 1
        return c / len(xs)
    
    deltas = []
    for _ in range(iters):
        deltas.append(samp_tail(enf) - samp_tail(ctl))  # enforce - control
    deltas.sort()
    return {"ok": 1.0, "tail_delta_p50": float(deltas[int(0.50*(iters-1))]), "tail_delta_p95": float(deltas[int(0.95*(iters-1))])}


def cell_eval(
    trades: List[Dict[str, Any]],
    *,
    sym: str,
    bucket: str,
    t_from_ms: int,
    t_to_ms: int,
    min_enf_n: int,
    min_ctl_n: int,
    tail_cap: float,
    tail_improve_min: float,
    mean_delta_min: float,
    boot_iters: int,
    boot_seed: int,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Evaluates cell health: enforce vs control outcomes.
    
    Returns:
        (ok: bool, report: dict)
    """
    enf: List[float] = []
    ctl: List[float] = []
    missing_tag = 0
    total = 0

    for t in trades:
        if t["symbol"] != sym or t["bucket"] != bucket:
            continue
        ts = t["ts_ms"]
        if not (t_from_ms <= ts < t_to_ms):
            continue
        total += 1
        if t.get("applied") is None:
            missing_tag += 1
            continue
        if int(t["applied"]) == 1:
            enf.append(float(t["r_mult"]))
        else:
            ctl.append(float(t["r_mult"]))

    rep = {
        "sym": sym, "bucket": bucket,
        "n_total": total,
        "missing_tag": missing_tag,
        "n_enf": len(enf),
        "n_ctl": len(ctl),
    }

    if total > 0 and missing_tag > int(0.30 * total):
        rep["reason"] = "missing_meta_tags"
        return False, rep

    if len(enf) < min_enf_n or len(ctl) < min_ctl_n:
        rep["reason"] = f"insufficient_n(enf={len(enf)},ctl={len(ctl)})"
        return False, rep

    se = stats(enf)
    sc = stats(ctl)
    mean_delta = se["meanR"] - sc["meanR"]
    tail_improve = sc["tail_rate"] - se["tail_rate"]
    ci = bootstrap_tail_delta(enf, ctl, iters=boot_iters, seed=boot_seed)

    rep.update({"enf": se, "ctl": sc, "mean_delta": mean_delta, "tail_improve": tail_improve, "ci": ci})

    reasons = []
    if se["tail_rate"] > tail_cap:
        reasons.append(f"tail_cap({se['tail_rate']:.2f}>{tail_cap:.2f})")
    if tail_improve < tail_improve_min:
        reasons.append(f"tail_improve({tail_improve:.3f}<{tail_improve_min:.3f})")
    if mean_delta < mean_delta_min:
        reasons.append(f"mean_delta({mean_delta:.3f}<{mean_delta_min:.3f})")
    if ci.get("ok", 0.0) != 1.0 or float(ci.get("tail_delta_p95", 0.0)) >= 0.0:
        reasons.append("tail_ci_not_strict")

    rep["reasons"] = reasons
    return (len(reasons) == 0), rep


def notify(r: redis.Redis, text: str, buttons: List[List[Dict[str, str]]] | None = None) -> None:
    """Sends notification to notify:telegram stream."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True)


def apply_bundle_auto_hset(
    r: redis.Redis,
    *,
    ops: List[Dict[str, str]],
    meta: Dict[str, Any],
    who: str,
    ttl: int,
    secret: str,
) -> Tuple[str, str]:
    """
    Applies bundle automatically (without approval) for auto-refreeze.
    
    Returns:
        (bundle_id, signature)
    """
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    bundle = {"id": bundle_id, "created_ms": now_ms(), "ttl_sec": ttl, "who": who, "ops": ops, "meta": meta}
    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)

    audit = []
    pipe = r.pipeline()
    for op in ops:
        key = op["key"]
        field = op["field"]
        newv = op["value"]
        old = r.hget(key, field)
        audit.append({"op": "HSET", "key": key, "field": field, "old": ("" if old is None else str(old)), "old_null": (1 if old is None else 0), "new": newv})
        pipe.hset(key, field, newv)
    pipe.execute()

    ts = now_ms()
    for a in audit:
        a["ts_ms"] = ts
        a["who"] = who
        r.rpush(f"recs:audit:{bundle_id}", json.dumps(a, ensure_ascii=False, separators=(",", ":")))
    r.expire(f"recs:audit:{bundle_id}", ttl)
    r.set(f"recs:status:{bundle_id}", "APPLIED", ex=ttl)
    return bundle_id, sig


def main() -> None:
    """Main entry point: check frozen/unfreeze cells, propose staged unfreeze or auto-refreeze."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    # registries
    reg_freeze = os.getenv("META_FREEZE_REGISTRY_KEY", "meta:freeze:cells")
    reg_unf = os.getenv("META_UNFREEZE_REGISTRY_KEY", "meta:unfreeze:cells")

    freeze_map = r.hgetall(reg_freeze) or {}
    unf_map = r.hgetall(reg_unf) or {}

    if not freeze_map and not unf_map:
        logger.info("No frozen or unfreezing cells found, skipping")
        return

    # parameters
    min_days = int(os.getenv("META_UNFREEZE_MIN_DAYS", "7") or 7)
    stage1_share = float(os.getenv("META_UNFREEZE_STAGE1_SHARE", "0.10") or 0.10)
    freeze_floor = float(os.getenv("META_FREEZE_FLOOR", "0.05") or 0.05)

    stage1_eval_h = float(os.getenv("META_STAGE1_EVAL_HOURS", "48") or 48)
    stage1_delay_h = float(os.getenv("META_STAGE1_EVAL_DELAY_HOURS", "24") or 24)

    min_enf = int(os.getenv("META_CELL_MIN_ENF_N", "50") or 50)
    min_ctl = int(os.getenv("META_CELL_MIN_CTL_N", "50") or 50)
    tail_cap = float(os.getenv("META_CELL_TAIL_ENF_MAX", "0.18") or 0.18)
    tail_improve_min = float(os.getenv("META_CELL_TAIL_IMPROVE_MIN", "0.01") or 0.01)
    mean_delta_min = float(os.getenv("META_CELL_MEAN_DELTA_MIN", "-0.02") or -0.02)
    boot_iters = int(os.getenv("META_CELL_BOOT_ITERS", "800") or 800)
    boot_seed = int(os.getenv("META_CELL_BOOT_SEED", "42") or 42)

    # restore references
    last_share = float(r.get(os.getenv("META_RAMP_LAST_SHARE_KEY", "meta:ramp:last_share")) or 0.0)
    last_share_trend = float(r.get(os.getenv("META_RAMP_LAST_SHARE_TREND_KEY", "meta:ramp:last_share_trend")) or last_share)
    last_share_range = float(r.get(os.getenv("META_RAMP_LAST_SHARE_RANGE_KEY", "meta:ramp:last_share_range")) or last_share)

    # export trades once
    since_hours = float(os.getenv("META_SELF_HEAL_EXPORT_HOURS", str(min_days * 24 + 48)) or (min_days * 24 + 48))
    out_dir = os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out")
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{out_dir}/meta_self_heal_{ts}"
    os.makedirs(run_dir, exist_ok=True)
    trades_out = f"{run_dir}/trades.ndjson"

    logger.info("Exporting trades for self-heal evaluation (since_hours=%.1f)", since_hours)
    subprocess.check_call([
        sys.executable, "tools/export_trade_closed_ndjson.py",
        "--since-hours", str(since_hours),
        "--out", trades_out,
        "--stream", os.getenv("TRADE_EVENTS_STREAM", "events:trades"),
        "--redis-url", redis_url,
        "--max-scan", os.getenv("TRADES_MAX_SCAN", "500000"),
    ])

    # parse trades into compact list
    trades: List[Dict[str, Any]] = []
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
            applied = t.get("meta_enforce_applied", None)
            bucket = regime_bucket(t)
            trades.append({
                "symbol": sym,
                "bucket": bucket,
                "ts_ms": ts_ms,
                "applied": None if applied is None else _i(applied, 0),
                "r_mult": _f(rm, 0.0),
            })

    logger.info("Loaded %d trades for evaluation", len(trades))

    prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    # -------------------------
    # Part 1: Stage1 proposals (frozen -> 0.10) after 7d good stats
    # -------------------------
    stage1_cells = []
    stage1_ops = []
    stage1_restore_map = {}
    stage1_restore_final = {}

    cutoff_ms = now_ms() - int(min_days * 24 * 3600_000)
    max_cells = int(os.getenv("META_UNFREEZE_MAX_CELLS", "5") or 5)

    for ck, raw in freeze_map.items():
        try:
            rec = json.loads(raw)
        except Exception:
            continue
        cell = str(rec.get("cell", ck) or ck)
        if "|" not in cell:
            continue
        sym, bucket = cell.split("|", 1)
        sym = sym.upper().strip()
        bucket = bucket.lower().strip()

        # never unfreeze news/other
        if bucket not in ("trend", "range"):
            continue

        applied_ms = int(rec.get("applied_ms", 0) or 0)
        if applied_ms and applied_ms > cutoff_ms:
            continue

        ok, rep = cell_eval(
            trades,
            sym=sym, bucket=bucket,
            t_from_ms=cutoff_ms,
            t_to_ms=now_ms(),
            min_enf_n=min_enf, min_ctl_n=min_ctl,
            tail_cap=tail_cap,
            tail_improve_min=tail_improve_min,
            mean_delta_min=mean_delta_min,
            boot_iters=boot_iters,
            boot_seed=boot_seed,
        )
        if not ok:
            logger.debug("Cell %s failed evaluation: %s", cell, rep.get("reasons") or rep.get("reason"))
            continue

        # final restore target
        final = last_share_trend if bucket == "trend" else last_share_range
        if final <= 0.0:
            # fallback to prev_share from freeze record
            try:
                final = float(rec.get("prev_share") or 0.0)
            except Exception:
                final = 0.0
        final = max(0.0, min(1.0, final))

        stage1_target = min(final, stage1_share)
        if stage1_target <= freeze_floor + 1e-9:
            continue

        hk = f"{prefix}{sym}"
        stage1_ops.append({"op": "HSET", "key": hk, "field": "meta_model_enable", "value": "1"})
        stage1_ops.append({"op": "HSET", "key": hk, "field": "meta_model_mode", "value": "ENFORCE"})
        stage1_ops.append({"op": "HSET", "key": hk, "field": f"meta_enforce_share_{bucket}", "value": f"{stage1_target:.2f}"})

        stage1_cells.append(f"{sym}|{bucket}")
        stage1_restore_map[f"{sym}|{bucket}"] = f"{stage1_target:.2f}"
        stage1_restore_final[f"{sym}|{bucket}"] = f"{final:.2f}"

        if len(stage1_cells) >= max_cells:
            break

    if stage1_cells:
        bundle_id = secrets.token_hex(6)
        sig = sign(bundle_id, secret)
        bundle = {
            "id": bundle_id, "created_ms": now_ms(), "ttl_sec": ttl,
            "who": "nightly_meta_cells_self_heal",
            "ops": stage1_ops,
            "meta": {
                "kind": "meta_enforce_unfreeze_stage",
                "stage": 1,
                "cells": stage1_cells,
                "restore_map": stage1_restore_map,
                "restore_final_map": stage1_restore_final,
            },
        }
        r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
        r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)
        buttons = [[
            {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bundle_id}:{sig}"},
            {"text": "❌ Reject", "callback": f"recs:reject:{bundle_id}:{sig}"},
        ]]
        notify(r, f"<b>Stage1 UNFREEZE proposal</b>\n"
                  f"id=<code>{bundle_id}</code>\n"
                  f"cells=<code>{stage1_cells}</code>\n"
                  f"target=<code>{stage1_share:.2f}</code> (capped by final)", buttons=buttons)
        logger.info("Proposed Stage1 unfreeze for %d cells", len(stage1_cells))

    # -------------------------
    # Part 2: Stage1 monitoring → auto-refreeze if degraded OR propose stage2
    # -------------------------
    # cooldown
    refreeze_key = os.getenv("META_REFREEZE_COOLDOWN_KEY", "meta:refreeze:last_ms")
    cooldown_sec = int(os.getenv("META_REFREEZE_COOLDOWN_SEC", "21600") or 21600)
    last_refreeze = _i(r.get(refreeze_key), 0)

    stage2_cells = []
    stage2_ops = []
    stage2_restore_map = {}
    stage2_restore_final = {}

    for ck, raw in unf_map.items():
        try:
            rec = json.loads(raw)
        except Exception:
            continue
        stage = int(rec.get("stage", 0) or 0)
        if stage != 1:
            continue

        cell = str(rec.get("cell", ck) or ck)
        if "|" not in cell:
            continue
        sym, bucket = cell.split("|", 1)
        sym = sym.upper().strip()
        bucket = bucket.lower().strip()

        applied_ms = int(rec.get("applied_ms", 0) or 0)
        if applied_ms <= 0:
            continue

        # wait delay
        if now_ms() - applied_ms < int(stage1_delay_h * 3600_000):
            continue

        # evaluate last stage1_eval_h window after applied
        t_from = applied_ms
        t_to = min(now_ms(), applied_ms + int(stage1_eval_h * 3600_000))

        ok, rep = cell_eval(
            trades,
            sym=sym, bucket=bucket,
            t_from_ms=t_from, t_to_ms=t_to,
            min_enf_n=min_enf, min_ctl_n=min_ctl,
            tail_cap=tail_cap,
            tail_improve_min=tail_improve_min,
            mean_delta_min=mean_delta_min,
            boot_iters=boot_iters,
            boot_seed=boot_seed,
        )

        if not ok:
            # auto-refreeze (safety) with cooldown
            if last_refreeze and (now_ms() - last_refreeze) < cooldown_sec * 1000:
                logger.debug("Skipping auto-refreeze for %s due to cooldown", cell)
                continue

            hk = f"{prefix}{sym}"
            field = f"meta_enforce_share_{bucket}"
            ops = [{"key": hk, "field": field, "value": f"{freeze_floor:.2f}"}]

            bundle_id, sig = apply_bundle_auto_hset(
                r,
                ops=[{"op": "HSET", "key": o["key"], "field": o["field"], "value": o["value"]} for o in ops],
                meta={"kind": "meta_enforce_refreeze_auto", "cell": cell, "freeze_to": freeze_floor, "eval": rep},
                who="nightly_meta_cells_self_heal",
                ttl=ttl,
                secret=secret,
            )

            # update registries: move back to freeze, remove from unfreeze
            freeze_rec = {
                "cell": cell,
                "symbol": sym,
                "bucket": bucket,
                "applied_ms": now_ms(),
                "freeze_to": f"{freeze_floor:.2f}",
                "prev_share": str(rec.get("target_share", "")),
                "field": field,
                "cfg_key": hk,
                "bundle_id": bundle_id,
            }
            r.hset(reg_freeze, cell, json.dumps(freeze_rec, ensure_ascii=False, separators=(",", ":")))
            r.hdel(reg_unf, cell)
            r.expire(reg_freeze, ttl)
            r.expire(reg_unf, ttl)

            r.set(refreeze_key, str(now_ms()), ex=cooldown_sec)
            last_refreeze = now_ms()

            buttons = [[{"text": "↩ Rollback", "callback": f"recs:rollback:{bundle_id}:{sig}"}]]
            notify(r, f"<b>AUTO-REFREEZE</b>\ncell=<code>{cell}</code>\n"
                      f"to=<code>{freeze_floor:.2f}</code>\nreason=<code>{rep.get('reasons') or rep.get('reason')}</code>\n"
                      f"id=<code>{bundle_id}</code>", buttons=buttons)
            logger.warning("Auto-refreeze applied for cell %s: %s", cell, rep.get("reasons") or rep.get("reason"))
            continue

        # ok -> propose stage2 (restore to final)
        try:
            final = float(rec.get("restore_final") or 0.0)
        except Exception:
            final = 0.0
        final = max(0.0, min(1.0, final))
        if final <= 0.0:
            continue
        if final <= float(rec.get("target_share") or 0.0) + 1e-9:
            continue

        hk = f"{prefix}{sym}"
        stage2_ops.append({"op": "HSET", "key": hk, "field": "meta_model_enable", "value": "1"})
        stage2_ops.append({"op": "HSET", "key": hk, "field": "meta_model_mode", "value": "ENFORCE"})
        stage2_ops.append({"op": "HSET", "key": hk, "field": f"meta_enforce_share_{bucket}", "value": f"{final:.2f}"})

        stage2_cells.append(cell)
        stage2_restore_map[cell] = f"{final:.2f}"
        stage2_restore_final[cell] = f"{final:.2f}"

        if len(stage2_cells) >= int(os.getenv("META_STAGE2_MAX_CELLS", "5") or 5):
            break

    if stage2_cells:
        bundle_id = secrets.token_hex(6)
        sig = sign(bundle_id, secret)
        bundle = {
            "id": bundle_id, "created_ms": now_ms(), "ttl_sec": ttl,
            "who": "nightly_meta_cells_self_heal",
            "ops": stage2_ops,
            "meta": {
                "kind": "meta_enforce_unfreeze_stage",
                "stage": 2,
                "cells": stage2_cells,
                "restore_map": stage2_restore_map,
                "restore_final_map": stage2_restore_final,
            },
        }
        r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
        r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)
        buttons = [[
            {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bundle_id}:{sig}"},
            {"text": "❌ Reject", "callback": f"recs:reject:{bundle_id}:{sig}"},
        ]]
        notify(r, f"<b>Stage2 UNFREEZE proposal (to global)</b>\n"
                  f"id=<code>{bundle_id}</code>\n"
                  f"cells=<code>{stage2_cells}</code>", buttons=buttons)
        logger.info("Proposed Stage2 unfreeze for %d cells", len(stage2_cells))

    logger.info("Self-heal run completed: stage1=%d, stage2=%d", len(stage1_cells), len(stage2_cells))


if __name__ == "__main__":
    main()

