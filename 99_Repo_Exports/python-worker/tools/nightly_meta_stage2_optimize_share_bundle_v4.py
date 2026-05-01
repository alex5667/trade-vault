#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
nightly_meta_stage2_optimize_share_bundle_v4.py

Nightly tool for Stage2 share optimization per cell (SYMBOL|bucket) - v4 with adaptive budgets + global budget.

Stage2 (v4) ещё безопаснее за счёт:

1. Адаптивного per-symbol бюджета на exec_rate_drop (turnover-proxy) по "здоровью" рынка/потока
   (24h metrics:of_gate: latency/exec_risk/soft/ok).

2. Глобального бюджета по всей системе: суммарный exec_rate_drop всех символов ≤ GLOBAL_BUDGET,
   тоже адаптивный.

3. Если глобальный бюджет превышен — автоматически деградировать (снижать share) на тех символах,
   где "цена" по objective/вреду хуже (greedy downgrade).

Flow:
  1. Reads meta:unfreeze:cells (Stage1 progress)
  2. Waits META_STAGE1_EVAL_DELAY_HOURS
  3. Reads metrics:of_gate for health assessment (24h window)
  4. Calculates adaptive budgets (per-symbol and global) based on health factor
  5. For each eligible symbol (stage==1, delay passed):
     - Groups cells by symbol (trend + range buckets)
     - For each bucket, builds options (share grid with constraints)
     - Enumerates combos (trend, range) per symbol under per-symbol budget
  6. Selects best combos across all symbols under global budget (greedy downgrade if needed)
  7. Proposes Stage2 bundle (manual approve via bundle → preview → confirm)

Critical requirements:
  - events:trades (POSITION_CLOSED) must have:
    * meta_veto (0/1) — "model would have vetoed"
    * meta_enforce_key (string, deterministic key — SID/stable id)
    * meta_enforce_salt (string, usually enf_v1)
    * regime_group or regime (for bucket trend/range)
    * r_mult, exit_ts_ms/ts_ms, symbol

  - metrics:of_gate stream must have:
    * ts_ms or ts or timestamp (event timestamp)
    * symbol (optional, for per-symbol health)
    * latency_us (latency in microseconds)
    * exec_risk_norm (execution risk normalized)
    * ok (0/1, success flag)
    * ok_soft (0/1, soft failure flag)

  Without meta_veto + meta_enforce_key we cannot correctly simulate different shares.
  Without metrics:of_gate we cannot adapt budgets.

Usage:
  python -m tools.nightly_meta_stage2_optimize_share_bundle_v4
  (reads ENV vars from /etc/trade/of_reports.env or environment)
"""

from utils.time_utils import get_ny_time_millis

import json
import os
import secrets
import subprocess
import sys
import time
import hmac
import hashlib
from typing import Any, Dict, List, Tuple, Optional

import redis

from common.log import setup_logger

logger = setup_logger("NightlyMetaStage2OptimizeShareV4")


# -------------------- utils --------------------

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


def pctl(xs: List[float], q: float) -> float:
    """Computes percentile q (0.0-1.0) from sorted list."""
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def clamp01(x: float) -> float:
    """Clamps value to [0.0, 1.0]."""
    return max(0.0, min(1.0, x))


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


# -------------------- domain: regime bucket --------------------

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


# -------------------- counterfactual sim --------------------

def simulate_share(rows: List[Dict[str, Any]], *, share: float, salt: str) -> Dict[str, Any]:
    """
    Counterfactual:
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


def objective(rep: Dict[str, Any], *, exec_rate_ref: float, cur_share: float, share: float,
              lam_tail: float, lam_p05: float, lam_turn: float, lam_step: float) -> Tuple[float, float]:
    """
    Returns (obj, exec_drop)
    """
    opp_mean = float(rep["opp"]["meanR"])
    exec_tail = float(rep["exec"]["tail_rate"])
    opp_p05 = float(rep["opp"]["p05"])
    exec_rate = float(rep["exec_rate"])
    drop = max(0.0, exec_rate_ref - exec_rate)

    obj = (
        opp_mean
        - lam_tail * exec_tail
        - lam_p05 * max(0.0, -opp_p05)
        - lam_turn * drop
        - lam_step * abs(share - cur_share)
    )
    return obj, drop


def build_options(cell_rows: List[Dict[str, Any]],
                  *, salt: str, cur_share: float, grid: List[float], share_cap: float,
                  max_up_step: float, max_down_step: float,
                  min_exec_rate: float, max_exec_rate_drop: float, tail_exec_cap: float,
                  lam_tail: float, lam_p05: float, lam_turn: float, lam_step: float) -> List[Dict[str, Any]]:
    """
    Feasible options for a bucket cell.
    """
    cur_share = max(0.0, min(1.0, cur_share))
    lo = max(0.0, cur_share - max(0.0, max_down_step))
    hi = min(1.0, cur_share + max(0.0, max_up_step))
    cap = max(0.0, min(1.0, share_cap))

    ref = simulate_share(cell_rows, share=cur_share, salt=salt)
    exec_rate_ref = float(ref["exec_rate"])

    opts: List[Dict[str, Any]] = []
    ref_obj, ref_drop = objective(ref, exec_rate_ref=exec_rate_ref, cur_share=cur_share, share=cur_share,
                                  lam_tail=lam_tail, lam_p05=lam_p05, lam_turn=lam_turn, lam_step=lam_step)
    opts.append({
        "share": cur_share,
        "obj": ref_obj,
        "exec_rate": exec_rate_ref,
        "drop": ref_drop,
        "rep": ref,
        "is_cur": True,
        "exec_rate_ref": exec_rate_ref,
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

        obj, drop = objective(rep, exec_rate_ref=exec_rate_ref, cur_share=cur_share, share=s,
                              lam_tail=lam_tail, lam_p05=lam_p05, lam_turn=lam_turn, lam_step=lam_step)

        if drop > max_exec_rate_drop + 1e-12:
            continue
        if float(rep["exec"]["tail_rate"]) > tail_exec_cap + 1e-12:
            continue

        opts.append({
            "share": s,
            "obj": obj,
            "exec_rate": exec_rate,
            "drop": drop,
            "rep": rep,
            "is_cur": False,
            "exec_rate_ref": exec_rate_ref,
        })

    # keep top-K objective, but also include lowest-drop alternative
    opts = sorted(opts, key=lambda x: x["obj"], reverse=True)
    top_k = int(os.getenv("META_OPT_TOPK_PER_CELL", "8") or 8)
    core = opts[:max(2, top_k)]
    low_drop = min(opts, key=lambda x: x["drop"])
    if low_drop not in core:
        core.append(low_drop)
    return core


def enumerate_symbol_combos(trend_opts: Optional[List[Dict[str, Any]]],
                            range_opts: Optional[List[Dict[str, Any]]],
                            *,
                            symbol_budget: float,
                            coupling_trend_lt: Optional[float],
                            coupling_range_cap: Optional[float]) -> List[Dict[str, Any]]:
    """
    Produce feasible combos under per-symbol budget, return sorted by sum_obj desc, then sum_drop asc.
    """
    if trend_opts is None:
        trend_opts = [{"share": None, "obj": 0.0, "drop": 0.0, "rep": None, "is_cur": True}]
    if range_opts is None:
        range_opts = [{"share": None, "obj": 0.0, "drop": 0.0, "rep": None, "is_cur": True}]

    combos = []
    for t in trend_opts:
        for r in range_opts:
            drop = float(t["drop"]) + float(r["drop"])
            if drop > symbol_budget + 1e-12:
                continue

            # optional coupling: if trend share < threshold -> range share <= coupling cap
            if coupling_trend_lt is not None and coupling_range_cap is not None:
                ts = t["share"]; rs = r["share"]
                if ts is not None and rs is not None:
                    if float(ts) < float(coupling_trend_lt) - 1e-12 and float(rs) > float(coupling_range_cap) + 1e-12:
                        continue

            combos.append({
                "trend": t,
                "range": r,
                "sum_drop": drop,
                "sum_obj": float(t["obj"]) + float(r["obj"]),
            })

    combos.sort(key=lambda x: (-x["sum_obj"], x["sum_drop"]))
    # keep top M
    m = int(os.getenv("META_OPT_TOPM_PER_SYMBOL", "10") or 10)
    return combos[:max(2, m)]


# -------------------- metrics health -> adaptive budgets --------------------

def read_metrics_window(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> List[Dict[str, Any]]:
    """Reads metrics from Redis stream within time window."""
    rows = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            try:
                ts = int(float(fields.get("ts_ms", fields.get("ts", fields.get("timestamp", 0))) or 0))
            except Exception:
                ts = 0
            if ts and ts < since_ms:
                scanned = max_scan
                break
            row = dict(fields)
            row["_ts_ms"] = ts
            rows.append(row)
    rows.reverse()
    return rows


def calc_health_factor(st: Dict[str, float], *, exec_target: float, exec_span: float,
                       lat_target_us: float, lat_span_us: float,
                       soft_target: float, soft_span: float,
                       ok_target: float, ok_span: float,
                       w_exec: float, w_lat: float, w_soft: float, w_ok: float,
                       floor: float, cap: float) -> float:
    """
    Factor in [floor,cap] where 1.0 means healthy.
    Higher exec_p90 / lat_p99 / soft_rate reduce factor, lower ok_rate reduces factor.
    """
    exec_p90 = float(st.get("exec_p90", 0.0))
    lat_p99 = float(st.get("lat_p99_us", 0.0))
    soft = float(st.get("soft_rate", 0.0))
    ok = float(st.get("ok_rate", 0.0))

    # penalty terms clamp01
    pe = clamp01((exec_p90 - exec_target) / max(1e-9, exec_span))
    pl = clamp01((lat_p99 - lat_target_us) / max(1e-9, lat_span_us))
    ps = clamp01((soft - soft_target) / max(1e-9, soft_span))
    pk = clamp01((ok_target - ok) / max(1e-9, ok_span))  # if ok below target

    penalty = w_exec * pe + w_lat * pl + w_soft * ps + w_ok * pk
    factor = 1.0 - clamp01(penalty)
    return max(floor, min(cap, factor))


def summarize_metrics(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """Summarizes metrics rows into statistics."""
    from tools.of_gate_metrics_contract import derive_ok_fields, is_gate_row, scenario_key
    gate_rows = [r for r in rows if is_gate_row(r)]
    valid_rows = [r for r in gate_rows if scenario_key(r) != "dn_veto"]
    n = len(valid_rows)
    if n == 0:
        return {"n": 0.0}

    ok = 0
    soft = 0
    lat = []
    ex = []

    for r in gate_rows:
        lat.append(_f(r.get("latency_us", 0.0), 0.0))
        ex.append(_f(r.get("exec_risk_norm", 0.0), 0.0))

    for r in valid_rows:
        ok_i, soft_i, _, _ = derive_ok_fields(r)
        ok += ok_i
        soft += soft_i

    return {
        "n": float(n),
        "n_total": float(len(gate_rows)),
        "ok_rate": float(ok / n) if n > 0 else 0.0,
        "soft_rate": float(soft / n) if n > 0 else 0.0,
        "lat_p99_us": float(pctl(lat, 0.99)) if lat else 0.0,
        "exec_p90": float(pctl(ex, 0.90)) if ex else 0.0,
    }


# -------------------- global selection under global budget --------------------

def select_under_global_budget(symbol_plans: Dict[str, List[Dict[str, Any]]], global_budget: float) -> Dict[str, Dict[str, Any]]:
    """
    Each symbol has list of combos sorted by best first (sum_obj desc, sum_drop asc).
    Start with best for each symbol, then if sum_drop > global_budget:
      iteratively downgrade the symbol where we reduce drop most cheaply (min loss per drop reduction).
    """
    chosen_idx = {sym: 0 for sym in symbol_plans.keys()}
    def total_drop_obj() -> Tuple[float, float]:
        td = 0.0
        to = 0.0
        for sym, combos in symbol_plans.items():
            c = combos[chosen_idx[sym]]
            td += float(c["sum_drop"])
            to += float(c["sum_obj"])
        return td, to

    td, _ = total_drop_obj()
    if td <= global_budget + 1e-12:
        return {sym: symbol_plans[sym][chosen_idx[sym]] for sym in symbol_plans.keys()}

    # Build downgrade candidates until within budget
    for _iter in range(10_000):
        td, _ = total_drop_obj()
        if td <= global_budget + 1e-12:
            break

        best_sym = None
        best_cost = None  # objective loss per drop reduction
        best_next = None

        for sym, combos in symbol_plans.items():
            i = chosen_idx[sym]
            if i + 1 >= len(combos):
                continue
            cur = combos[i]
            nxt = combos[i + 1]
            drop_red = float(cur["sum_drop"]) - float(nxt["sum_drop"])
            if drop_red <= 1e-12:
                continue
            obj_loss = float(cur["sum_obj"]) - float(nxt["sum_obj"])
            cost = obj_loss / drop_red
            if best_cost is None or cost < best_cost:
                best_cost = cost
                best_sym = sym
                best_next = i + 1

        if best_sym is None:
            # cannot reduce further
            break
        chosen_idx[best_sym] = best_next

    return {sym: symbol_plans[sym][chosen_idx[sym]] for sym in symbol_plans.keys()}


# -------------------- main --------------------

def main() -> None:
    """Main entry point: optimize share per symbol with adaptive budgets and global budget."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    out_dir = os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out")
    prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    # Stage1 progress registry
    reg_unf = os.getenv("META_UNFREEZE_REGISTRY_KEY", "meta:unfreeze:cells")
    stage1_delay_h = float(os.getenv("META_STAGE1_EVAL_DELAY_HOURS", "24") or 24)

    unf_map = r.hgetall(reg_unf) or {}
    if not unf_map:
        logger.info("No unfreezing cells found, skipping")
        return

    # group candidates by symbol → buckets
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

    # -------- Adaptive budgets from metrics:of_gate --------
    metrics_stream = os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
    win_h = float(os.getenv("META_BUDGET_HEALTH_WINDOW_HOURS", "24") or 24)
    since_ms = now_ms() - int(win_h * 3600_000)
    max_scan = int(os.getenv("META_BUDGET_METRICS_MAX_SCAN", "400000") or 400000)

    mrows = read_metrics_window(r, metrics_stream, since_ms, max_scan=max_scan)

    # per-symbol metrics if `symbol` present, else global only
    per_sym: Dict[str, List[Dict[str, Any]]] = {}
    global_rows: List[Dict[str, Any]] = []
    for rr in mrows:
        global_rows.append(rr)
        sym = str(rr.get("symbol", "") or "").upper()
        if sym:
            per_sym.setdefault(sym, []).append(rr)

    gstat = summarize_metrics(global_rows)
    # health factor params
    exec_target = float(os.getenv("META_HEALTH_EXEC_TARGET", "0.75") or 0.75)
    exec_span = float(os.getenv("META_HEALTH_EXEC_SPAN", "0.25") or 0.25)
    lat_target = float(os.getenv("META_HEALTH_LAT_TARGET_US", "4000") or 4000)
    lat_span = float(os.getenv("META_HEALTH_LAT_SPAN_US", "6000") or 6000)
    soft_target = float(os.getenv("META_HEALTH_SOFT_TARGET", "0.35") or 0.35)
    soft_span = float(os.getenv("META_HEALTH_SOFT_SPAN", "0.35") or 0.35)
    ok_target = float(os.getenv("META_HEALTH_OK_TARGET", "0.20") or 0.20)
    ok_span = float(os.getenv("META_HEALTH_OK_SPAN", "0.20") or 0.20)

    w_exec = float(os.getenv("META_HEALTH_W_EXEC", "0.35") or 0.35)
    w_lat = float(os.getenv("META_HEALTH_W_LAT", "0.25") or 0.25)
    w_soft = float(os.getenv("META_HEALTH_W_SOFT", "0.25") or 0.25)
    w_ok = float(os.getenv("META_HEALTH_W_OK", "0.15") or 0.15)

    factor_floor = float(os.getenv("META_BUDGET_FACTOR_FLOOR", "0.35") or 0.35)
    factor_cap = float(os.getenv("META_BUDGET_FACTOR_CAP", "1.00") or 1.00)

    gfactor = calc_health_factor(
        gstat,
        exec_target=exec_target, exec_span=exec_span,
        lat_target_us=lat_target, lat_span_us=lat_span,
        soft_target=soft_target, soft_span=soft_span,
        ok_target=ok_target, ok_span=ok_span,
        w_exec=w_exec, w_lat=w_lat, w_soft=w_soft, w_ok=w_ok,
        floor=factor_floor, cap=factor_cap,
    )

    base_sym_budget = float(os.getenv("META_SYMBOL_EXEC_DROP_BUDGET_BASE", "0.25") or 0.25)
    base_glob_budget = float(os.getenv("META_GLOBAL_EXEC_DROP_BUDGET_BASE", "1.00") or 1.00)

    # Adaptive budgets
    sym_budgets: Dict[str, float] = {}
    for sym in cand.keys():
        if sym in per_sym and len(per_sym[sym]) >= int(os.getenv("META_BUDGET_SYM_MIN_N", "200") or 200):
            sstat = summarize_metrics(per_sym[sym])
            sfactor = calc_health_factor(
                sstat,
                exec_target=exec_target, exec_span=exec_span,
                lat_target_us=lat_target, lat_span_us=lat_span,
                soft_target=soft_target, soft_span=soft_span,
                ok_target=ok_target, ok_span=ok_span,
                w_exec=w_exec, w_lat=w_lat, w_soft=w_soft, w_ok=w_ok,
                floor=factor_floor, cap=factor_cap,
            )
        else:
            sfactor = gfactor
        sym_budgets[sym] = base_sym_budget * sfactor

    global_budget = base_glob_budget * gfactor

    # -------- Export trades for optimization --------
    opt_hours = float(os.getenv("META_OPT_EXPORT_HOURS", "336") or 336)
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{out_dir}/meta_opt_stage2_v4_{ts}"
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
                if sym not in cand:
                    continue
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
        notify(r, f"<b>Stage2 v4 skipped</b>\nreason=<code>missing_meta_enforce_key</code>\nmissing={missing_key}/{total}")
        logger.warning("Stage2 v4 skipped: missing_meta_enforce_key %d/%d", missing_key, total)
        return
    if total > 0 and (missing_veto / total) > 0.30:
        notify(r, f"<b>Stage2 v4 skipped</b>\nreason=<code>missing_meta_veto</code>\nmissing={missing_veto}/{total}")
        logger.warning("Stage2 v4 skipped: missing_meta_veto %d/%d", missing_veto, total)
        return

    # -------- optimizer params (same as v3, plus caps/budget/coupling) --------
    grid_trend = [float(x) for x in (os.getenv("META_OPT_SHARE_GRID_TREND", "0.10,0.25,0.35,0.50,0.75,1.00") or "").split(",") if x.strip()]
    grid_range = [float(x) for x in (os.getenv("META_OPT_SHARE_GRID_RANGE", "0.10,0.15,0.25,0.35,0.50") or "").split(",") if x.strip()]
    grid_trend = sorted(set([clamp01(s) for s in grid_trend])) or [0.10, 0.25, 0.35, 0.50]
    grid_range = sorted(set([clamp01(s) for s in grid_range])) or [0.10, 0.15, 0.25, 0.35]

    max_up_step = float(os.getenv("META_OPT_MAX_UP_STEP", "0.25") or 0.25)
    max_down_step = float(os.getenv("META_OPT_MAX_DOWN_STEP", "0.00") or 0.00)

    min_exec_rate = float(os.getenv("META_OPT_MIN_EXEC_RATE", "0.30") or 0.30)
    max_exec_rate_drop = float(os.getenv("META_OPT_MAX_EXEC_RATE_DROP", "0.20") or 0.20)
    tail_exec_cap = float(os.getenv("META_OPT_TAIL_EXEC_MAX", "0.18") or 0.18)

    lam_tail = float(os.getenv("META_OPT_LAM_TAIL", "0.50") or 0.50)
    lam_p05 = float(os.getenv("META_OPT_LAM_P05", "0.10") or 0.10)
    lam_turn = float(os.getenv("META_OPT_LAM_TURN", "0.30") or 0.30)
    lam_step = float(os.getenv("META_OPT_LAM_STEP", "0.05") or 0.05)

    cap_trend = float(os.getenv("META_BUCKET_CAP_TREND", "1.00") or 1.00)
    cap_range = float(os.getenv("META_BUCKET_CAP_RANGE", "0.50") or 0.50)

    coupling_trend_lt = os.getenv("META_RANGE_CAP_IF_TREND_LT", "")
    coupling_range_cap = os.getenv("META_RANGE_CAP_WHEN_TREND_LT", "")
    coupling_trend_lt_v = float(coupling_trend_lt) if coupling_trend_lt.strip() else None
    coupling_range_cap_v = float(coupling_range_cap) if coupling_range_cap.strip() else None

    # -------- build per-symbol plan candidates (multiple combos) --------
    symbol_plans: Dict[str, List[Dict[str, Any]]] = {}
    details: Dict[str, Any] = {"global_metrics": gstat, "global_factor": gfactor, "global_budget": global_budget, "sym_budget": sym_budgets}

    for sym, buckets in cand.items():
        # per-bucket option lists
        t_opts = None
        r_opts = None

        for bucket in ("trend", "range"):
            if bucket not in buckets:
                continue
            cell, rec = buckets[bucket]
            cell_rows = [x for x in rows if x["symbol"] == sym and x["bucket"] == bucket]
            if len(cell_rows) < 500:
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

            # attach to details
            details[cell] = {
                "symbol": sym, "bucket": bucket, "field": share_field, "cur_share": cur_share, "cap": cap, "salt": salt,
                "opts_top": [{"share": o["share"], "drop": o["drop"], "obj": o["obj"], "tail": o["rep"]["exec"]["tail_rate"], "exec_rate": o["exec_rate"]} for o in opts[:6]],
            }

            if bucket == "trend":
                t_opts = opts
            else:
                r_opts = opts

        if t_opts is None and r_opts is None:
            continue

        sym_budget = float(sym_budgets.get(sym, base_sym_budget))
        combos = enumerate_symbol_combos(t_opts, r_opts, symbol_budget=sym_budget,
                                         coupling_trend_lt=coupling_trend_lt_v,
                                         coupling_range_cap=coupling_range_cap_v)
        if combos:
            symbol_plans[sym] = combos

    if not symbol_plans:
        logger.info("No symbol plans generated")
        return

    # -------- global selection under global budget --------
    chosen = select_under_global_budget(symbol_plans, global_budget)

    # -------- build ops from chosen combos --------
    ops: List[Dict[str, str]] = []
    picked_cells: List[str] = []
    for sym, combo in chosen.items():
        hk = f"{prefix}{sym}"
        buckets = cand[sym]

        for bucket in ("trend", "range"):
            if bucket not in buckets:
                continue
            cell, rec = buckets[bucket]
            share_field = f"meta_enforce_share_{bucket}"
            cur_share = float(r.hget(hk, share_field) or (rec.get("target_share") or 0.10) or 0.10)
            ch = combo.get(bucket)
            if not ch or ch.get("share") is None:
                continue
            new_share = float(ch["share"])
            if abs(new_share - cur_share) < 1e-9:
                continue

            ops.append({"op": "HSET", "key": hk, "field": "meta_model_enable", "value": "1"})
            ops.append({"op": "HSET", "key": hk, "field": "meta_model_mode", "value": "ENFORCE"})
            ops.append({"op": "HSET", "key": hk, "field": share_field, "value": f"{new_share:.2f}"})
            picked_cells.append(cell)

            details[cell]["chosen"] = {
                "new_share": new_share,
                "cur_share": cur_share,
                "drop": float(ch["drop"]),
                "obj": float(ch["obj"]),
            }

        details[f"{sym}|__chosen__"] = {
            "sum_drop": float(combo.get("sum_drop", 0.0)),
            "sum_obj": float(combo.get("sum_obj", 0.0)),
            "sym_budget": float(sym_budgets.get(sym, base_sym_budget)),
        }

    # dedupe
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
        "who": "nightly_meta_stage2_optimize_share_bundle_v4",
        "ops": uniq_ops,
        "meta": {
            "kind": "meta_enforce_unfreeze_stage",
            "stage": 2,
            "optimized": True,
            "multiobjective": True,
            "safety_layer": "adaptive_budgets_global_and_per_symbol",
            "picked_cells": picked_cells,
            "bucket_caps": {"trend": cap_trend, "range": cap_range},
            "global_budget": global_budget,
            "base_budgets": {"sym_base": base_sym_budget, "global_base": base_glob_budget},
            "health": {"global_stat": gstat, "global_factor": gfactor},
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
        "<b>Stage2 UNFREEZE proposal (v4 adaptive budgets + global budget)</b>\n"
        f"id=<code>{bundle_id}</code>\n"
        f"cells=<code>{picked_cells}</code>\n"
        f"global_budget=<code>{global_budget:.2f}</code> (factor=<code>{gfactor:.2f}</code>)\n"
        f"sym_budget_base=<code>{base_sym_budget:.2f}</code> global_budget_base=<code>{base_glob_budget:.2f}</code>\n"
        f"metrics_global=<code>{gstat}</code>",
        buttons=buttons,
    )
    logger.info("Proposed Stage2 unfreeze for %d cells", len(picked_cells))


if __name__ == "__main__":
    main()

