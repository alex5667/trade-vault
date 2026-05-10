#!/usr/bin/env python3
from __future__ import annotations

from domain.evidence_keys import MetaKeys
from core.redis_keys import RedisStreams as RS

"""
nightly_meta_stage2_optimize_share_bundle_v5.py

Nightly tool for Stage2 share optimization per cell (SYMBOL|bucket) - v5 with per-bucket global budgets + hard-stop.

Stage2 (v5) добавляет поверх v4:

1. Отдельные глобальные бюджеты по bucket'ам: GLOBAL_BUDGET_TREND и GLOBAL_BUDGET_RANGE (оба адаптивные от health).

2. Hard-stop режим: если здоровье плохое (lat/exec_risk/soft/ok) — Stage2 не предлагает повышений, только downgrades (degrade-only).

3. Двухконтурный селектор: выбирает комбинации (trend/range) per-symbol под per-symbol budget, а затем глобально "дожимает" под две глобальные квоты (trend/range) через greedy-downgrade.

Flow:
  1. Reads meta:unfreeze:cells (Stage1 progress)
  2. Waits META_STAGE1_EVAL_DELAY_HOURS
  3. Reads metrics:of_gate for health assessment (24h window)
  4. Calculates adaptive budgets (per-symbol and per-bucket global) based on health factor
  5. Checks hard-stop conditions (lat/exec/soft/ok) -> if triggered, degrade_only=True
  6. For each eligible symbol (stage==1, delay passed):
     - Groups cells by symbol (trend + range buckets)
     - For each bucket, builds options (share grid with constraints, respect degrade_only)
     - Enumerates combos (trend, range) per symbol under per-symbol budget
  7. Selects best combos across all symbols under per-bucket global budgets (greedy downgrade if needed)
  8. Proposes Stage2 bundle (manual approve via bundle → preview → confirm)

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
  Without metrics:of_gate we cannot adapt budgets or detect hard-stop.

Usage:
  python -m tools.nightly_meta_stage2_optimize_share_bundle_v5
  (reads ENV vars from /etc/trade/of_reports.env or environment)
"""

import hashlib
import hmac
import json
import os
import secrets
import subprocess
import sys
import time
from typing import Any

import redis

from common.log import setup_logger
from core.ok_fields import get_ts_ms
from tools.of_gate_metrics_contract import derive_ok_fields, is_gate_row, scenario_key
from utils.time_utils import get_ny_time_millis

logger = setup_logger("NightlyMetaStage2OptimizeShareV5")


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
        return d


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion."""
    try:
        return int(float(x))
    except Exception:
        return d


def clamp01(x: float) -> float:
    """Clamp value to [0.0, 1.0]."""
    return max(0.0, min(1.0, x))


def _event_ts_ms(r: dict[str, Any]) -> int:
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


def pctl(xs: list[float], q: float) -> float:
    """Calculate percentile q (0.0-1.0) from sorted list."""
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def _hash01(s: str) -> float:
    """Deterministic hash to [0.0, 1.0) for share simulation."""
    h = hashlib.sha256(s.encode("utf-8")).digest()
    x = int.from_bytes(h[:8], "big", signed=False)
    return (x % 10_000_000) / 10_000_000.0


def notify(r: redis.Redis, text: str, buttons: list[list[dict[str, str]]] | None = None) -> None:
    """Send notification to Telegram via Redis stream."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), fields, maxlen=200000, approximate=True)


def stats(rs: list[float]) -> dict[str, float]:
    """Calculate statistics for return series."""
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


# -------------------- bucket --------------------

def regime_bucket(t: dict[str, Any]) -> str:
    """Classify regime into bucket: news, trend, range, thin, other."""
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

def simulate_share(rows: list[dict[str, Any]], *, share: float, salt: str) -> dict[str, Any]:
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
        key = (r.get(MetaKeys.ENFORCE_KEY, "") or "")
        if not key:
            continue
        used += 1
        veto = int(r.get(MetaKeys.VETO, 0) or 0)
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


def objective(rep: dict[str, Any], *, exec_rate_ref: float, cur_share: float, share: float,
              lam_tail: float, lam_p05: float, lam_turn: float, lam_step: float) -> tuple[float, float]:
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


def build_options(
    cell_rows: list[dict[str, Any]],
    *,
    salt: str,
    cur_share: float,
    grid: list[float],
    share_cap: float,
    max_up_step: float,
    max_down_step: float,
    degrade_only: bool,
    min_exec_rate: float,
    max_exec_rate_drop: float,
    tail_exec_cap: float,
    lam_tail: float,
    lam_p05: float,
    lam_turn: float,
    lam_step: float,
) -> list[dict[str, Any]]:
    """
    Feasible options for a bucket cell.
    If degrade_only=True: we do NOT allow increases (upper bound is cur_share).
    """
    cur_share = max(0.0, min(1.0, cur_share))
    lo = max(0.0, cur_share - max(0.0, max_down_step))
    hi = min(1.0, cur_share + max(0.0, max_up_step))
    cap = max(0.0, min(1.0, share_cap))

    if degrade_only:
        hi = min(hi, cur_share)

    ref = simulate_share(cell_rows, share=cur_share, salt=salt)
    exec_rate_ref = float(ref["exec_rate"])

    opts: list[dict[str, Any]] = []
    ref_obj, ref_drop = objective(ref, exec_rate_ref=exec_rate_ref, cur_share=cur_share, share=cur_share,
                                  lam_tail=lam_tail, lam_p05=lam_p05, lam_turn=lam_turn, lam_step=lam_step)
    opts.append({
        "share": cur_share,
        "obj": ref_obj,
        "drop": ref_drop,
        "exec_rate": exec_rate_ref,
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
            "drop": drop,
            "exec_rate": exec_rate,
            "rep": rep,
            "is_cur": False,
            "exec_rate_ref": exec_rate_ref,
        })

    # keep top-K by obj + always keep minimal drop option
    opts = sorted(opts, key=lambda x: x["obj"], reverse=True)
    top_k = int(os.getenv("META_OPT_TOPK_PER_CELL", "8") or 8)
    core = opts[:max(2, top_k)]
    low_drop = min(opts, key=lambda x: x["drop"])
    if low_drop not in core:
        core.append(low_drop)
    return core


def enumerate_symbol_combos(
    trend_opts: list[dict[str, Any]] | None,
    range_opts: list[dict[str, Any]] | None,
    *,
    symbol_budget: float,
    coupling_trend_lt: float | None,
    coupling_range_cap: float | None,
) -> list[dict[str, Any]]:
    """
    Produce feasible combos under per-symbol budget.
    """
    if trend_opts is None:
        trend_opts = [{"share": None, "obj": 0.0, "drop": 0.0, "rep": None, "is_cur": True}]
    if range_opts is None:
        range_opts = [{"share": None, "obj": 0.0, "drop": 0.0, "rep": None, "is_cur": True}]

    combos = []
    for t in trend_opts:
        for r in range_opts:
            td = float(t["drop"])
            rd = float(r["drop"])
            sd = td + rd
            if sd > symbol_budget + 1e-12:
                continue

            if coupling_trend_lt is not None and coupling_range_cap is not None:
                ts = t["share"]; rs = r["share"]
                if ts is not None and rs is not None:
                    if float(ts) < float(coupling_trend_lt) - 1e-12 and float(rs) > float(coupling_range_cap) + 1e-12:
                        continue

            combos.append({
                "trend": t,
                "range": r,
                "trend_drop": td,
                "range_drop": rd,
                "sum_drop": sd,
                "sum_obj": float(t["obj"]) + float(r["obj"]),
            })

    combos.sort(key=lambda x: (-x["sum_obj"], x["sum_drop"]))
    m = int(os.getenv("META_OPT_TOPM_PER_SYMBOL", "10") or 10)
    return combos[:max(2, m)]


# -------------------- metrics health -> budgets + hard stop --------------------

def read_metrics_window(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> list[dict[str, Any]]:
    """Read metrics from Redis stream within time window."""
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
            ts = get_ts_ms(fields)
            if ts and ts < since_ms:
                scanned = max_scan
                break
            row = dict(fields)
            row["_ts_ms"] = ts
            rows.append(row)
    rows.reverse()
    return rows


def summarize_metrics(rows: list[dict[str, Any]]) -> dict[str, float]:
    """Summarize metrics window into health statistics."""
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
        "ok_rate": float(ok / n),
        "soft_rate": float(soft / n),
        "lat_p99_us": float(pctl(lat, 0.99)),
        "exec_p90": float(pctl(ex, 0.90)),
    }


def calc_health_factor(st: dict[str, float], *, exec_target: float, exec_span: float,
                       lat_target_us: float, lat_span_us: float,
                       soft_target: float, soft_span: float,
                       ok_target: float, ok_span: float,
                       w_exec: float, w_lat: float, w_soft: float, w_ok: float,
                       floor: float, cap: float) -> float:
    """Calculate health factor (0.0-1.0) from metrics summary."""
    exec_p90 = float(st.get("exec_p90", 0.0))
    lat_p99 = float(st.get("lat_p99_us", 0.0))
    soft = float(st.get("soft_rate", 0.0))
    ok = float(st.get("ok_rate", 0.0))

    pe = clamp01((exec_p90 - exec_target) / max(1e-9, exec_span))
    pl = clamp01((lat_p99 - lat_target_us) / max(1e-9, lat_span_us))
    ps = clamp01((soft - soft_target) / max(1e-9, soft_span))
    pk = clamp01((ok_target - ok) / max(1e-9, ok_span))

    penalty = w_exec * pe + w_lat * pl + w_soft * ps + w_ok * pk
    factor = 1.0 - clamp01(penalty)
    return max(floor, min(cap, factor))


def hard_stop(st: dict[str, float]) -> tuple[bool, list[str]]:
    """
    If triggered: degrade-only (no increases).
    """
    reasons = []
    lat_p99 = float(st.get("lat_p99_us", 0.0))
    exec_p90 = float(st.get("exec_p90", 0.0))
    soft = float(st.get("soft_rate", 0.0))
    ok = float(st.get("ok_rate", 0.0))

    lat_thr = float(os.getenv("META_HARDSTOP_LAT_P99_US", "12000") or 12000)
    exec_thr = float(os.getenv("META_HARDSTOP_EXEC_P90", "0.92") or 0.92)
    soft_thr = float(os.getenv("META_HARDSTOP_SOFT_RATE", "0.60") or 0.60)
    ok_min = float(os.getenv("META_HARDSTOP_OK_RATE_MIN", "0.10") or 0.10)

    if lat_p99 > lat_thr:
        reasons.append(f"lat_p99_us>{lat_thr}")
    if exec_p90 > exec_thr:
        reasons.append(f"exec_p90>{exec_thr}")
    if soft > soft_thr:
        reasons.append(f"soft_rate>{soft_thr}")
    if ok < ok_min:
        reasons.append(f"ok_rate<{ok_min}")

    return (len(reasons) > 0), reasons


# -------------------- selection under per-bucket global budgets --------------------

def totals(chosen: dict[str, dict[str, Any]]) -> tuple[float, float, float, float]:
    """Calculate totals across chosen combos: trend_drop, range_drop, sum_drop, sum_obj."""
    td = 0.0
    rd = 0.0
    sd = 0.0
    so = 0.0
    for sym, c in chosen.items():
        td += float(c.get("trend_drop", 0.0))
        rd += float(c.get("range_drop", 0.0))
        sd += float(c.get("sum_drop", 0.0))
        so += float(c.get("sum_obj", 0.0))
    return td, rd, sd, so


def select_under_bucket_budgets(
    plans: dict[str, list[dict[str, Any]]],
    *,
    budget_trend: float,
    budget_range: float,
    budget_total: float | None,
) -> dict[str, dict[str, Any]]:
    """
    Start with best (index 0) for each symbol.
    If any budget exceeded, downgrade symbol-by-symbol (next combo) by minimal cost per effective reduction.
    Budgets:
      sum(trend_drop) <= budget_trend
      sum(range_drop) <= budget_range
      optional sum(sum_drop) <= budget_total
    """
    idx = dict.fromkeys(plans.keys(), 0)

    def cur(sym: str) -> dict[str, Any]:
        return plans[sym][idx[sym]]

    chosen = {sym: cur(sym) for sym in plans}

    for _iter in range(50_000):
        td, rd, sd, _ = totals(chosen)
        et = max(0.0, td - budget_trend)
        er = max(0.0, rd - budget_range)
        ea = max(0.0, sd - budget_total) if budget_total is not None else 0.0

        if et <= 1e-12 and er <= 1e-12 and ea <= 1e-12:
            break

        # weights: focus on the most exceeded constraint(s)
        wt = 1.0 if et > 1e-12 else 0.0
        wr = 1.0 if er > 1e-12 else 0.0
        wa = 1.0 if (budget_total is not None and ea > 1e-12) else 0.0

        best_sym = None
        best_next = None
        best_cost = None

        for sym, combos in plans.items():
            i = idx[sym]
            if i + 1 >= len(combos):
                continue
            c0 = combos[i]
            c1 = combos[i + 1]

            dtrend = float(c0["trend_drop"]) - float(c1["trend_drop"])
            drange = float(c0["range_drop"]) - float(c1["range_drop"])
            dsum = float(c0["sum_drop"]) - float(c1["sum_drop"])

            eff_red = wt * max(0.0, dtrend) + wr * max(0.0, drange) + wa * max(0.0, dsum)
            if eff_red <= 1e-12:
                continue

            obj_loss = float(c0["sum_obj"]) - float(c1["sum_obj"])
            cost = obj_loss / eff_red

            if best_cost is None or cost < best_cost:
                best_cost = cost
                best_sym = sym
                best_next = i + 1

        if best_sym is None:
            break

        idx[best_sym] = best_next  # downgrade
        chosen[best_sym] = plans[best_sym][idx[best_sym]]

    return chosen


# -------------------- main --------------------

def main() -> None:
    """Main entry point for Stage2 v5 optimization."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    out_dir = os.getenv("OUT_DIR", "/var/lib/trade/of_reports/out")
    prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    reg_unf = os.getenv("META_UNFREEZE_REGISTRY_KEY", "meta:unfreeze:cells")
    stage1_delay_h = float(os.getenv("META_STAGE1_EVAL_DELAY_HOURS", "24") or 24)

    # candidates grouped by symbol
    unf_map = r.hgetall(reg_unf) or {}
    if not unf_map:
        logger.info("No unfreeze candidates found")
        return

    cand: dict[str, dict[str, tuple[str, dict]]] = {}
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
        logger.info("No eligible candidates after filtering")
        return

    # -------- Metrics health (global only; per-symbol optional) --------
    metrics_stream = os.getenv("OF_GATE_METRICS_STREAM", RS.OF_GATE_METRICS)
    win_h = float(os.getenv("META_BUDGET_HEALTH_WINDOW_HOURS", "24") or 24)
    since_ms = now_ms() - int(win_h * 3600_000)
    max_scan = int(os.getenv("META_BUDGET_METRICS_MAX_SCAN", "400000") or 400000)

    mrows = read_metrics_window(r, metrics_stream, since_ms, max_scan)
    gstat = summarize_metrics(mrows)

    # factor parameters
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

    hs, hs_reasons = hard_stop(gstat)
    degrade_only = hs or (int(os.getenv("META_DEGRADE_ONLY_FORCE", "0") or 0) == 1)

    logger.info(f"Health factor: {gfactor:.3f}, hard-stop: {hs}, reasons: {hs_reasons}")

    # -------- Budgets: per-bucket global + per-symbol adaptive --------
    base_sym_budget = float(os.getenv("META_SYMBOL_EXEC_DROP_BUDGET_BASE", "0.25") or 0.25)
    base_glob_trend = float(os.getenv("META_GLOBAL_EXEC_DROP_BUDGET_TREND_BASE", "0.60") or 0.60)
    base_glob_range = float(os.getenv("META_GLOBAL_EXEC_DROP_BUDGET_RANGE_BASE", "0.40") or 0.40)

    # If hard stop -> shrink budgets further (safety)
    hs_mult = float(os.getenv("META_HARDSTOP_BUDGET_MULT", "0.50") or 0.50) if degrade_only else 1.0

    sym_budget = base_sym_budget * gfactor * hs_mult
    glob_trend_budget = base_glob_trend * gfactor * hs_mult
    glob_range_budget = base_glob_range * gfactor * hs_mult

    # optional total budget too
    use_total = int(os.getenv("META_GLOBAL_TOTAL_BUDGET_ENABLED", "1") or 1) == 1
    base_total = float(os.getenv("META_GLOBAL_EXEC_DROP_BUDGET_TOTAL_BASE", str(base_glob_trend + base_glob_range)) or (base_glob_trend + base_glob_range))
    total_budget = (base_total * gfactor * hs_mult) if use_total else None

    logger.info(f"Budgets: sym={sym_budget:.3f}, trend={glob_trend_budget:.3f}, range={glob_range_budget:.3f}, total={total_budget}")

    # -------- Export trades for optimization --------
    opt_hours = float(os.getenv("META_OPT_EXPORT_HOURS", "336") or 336)
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = f"{out_dir}/meta_opt_stage2_v5_{ts}"
    os.makedirs(run_dir, exist_ok=True)
    trades_out = f"{run_dir}/trades.ndjson"

    subprocess.check_call([
        sys.executable, "tools/export_trade_closed_ndjson.py",
        "--since-hours", str(opt_hours),
        "--out", trades_out,
        "--stream", os.getenv("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES),
        "--redis-url", redis_url,
        "--max-scan", os.getenv("TRADES_MAX_SCAN", "500000"),
    ])

    rows: list[dict[str, Any]] = []
    missing_key = 0
    missing_veto = 0
    total = 0

    with open(trades_out, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            t = json.loads(line)
            sym = (t.get("symbol", "") or "").upper()
            if sym not in cand:
                continue
            ts_ms = _event_ts_ms(t)
            if ts_ms <= 0:
                continue
            rm = t.get("r_mult", None)
            if rm is None:
                continue
            total += 1
            key = t.get(MetaKeys.ENFORCE_KEY, None)
            veto = t.get(MetaKeys.VETO, None)
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
                "meta_enforce_salt": (t.get(MetaKeys.ENFORCE_SALT, "enf_v1") or "enf_v1"),
                "meta_veto": 0 if veto is None else _i(veto, 0),
            })

    if total > 0 and (missing_key / total) > 0.30:
        notify(r, f"<b>Stage2 v5 skipped</b>\nreason=<code>missing_meta_enforce_key</code>\nmissing={missing_key}/{total}")
        logger.warning(f"Stage2 v5 skipped: missing_meta_enforce_key {missing_key}/{total}")
        return
    if total > 0 and (missing_veto / total) > 0.30:
        notify(r, f"<b>Stage2 v5 skipped</b>\nreason=<code>missing_meta_veto</code>\nmissing={missing_veto}/{total}")
        logger.warning(f"Stage2 v5 skipped: missing_meta_veto {missing_veto}/{total}")
        return

    # -------- optimizer params --------
    # Separate grids: if degrade_only use degrade grids (lower values) to make downgrades possible
    if degrade_only:
        grid_trend = [float(x) for x in (os.getenv("META_DEGRADE_GRID_TREND", "0.05,0.10,0.15,0.25,0.35") or "").split(",") if x.strip()]
        grid_range = [float(x) for x in (os.getenv("META_DEGRADE_GRID_RANGE", "0.05,0.10,0.15,0.25") or "").split(",") if x.strip()]
    else:
        grid_trend = [float(x) for x in (os.getenv("META_OPT_SHARE_GRID_TREND", "0.10,0.25,0.35,0.50,0.75,1.00") or "").split(",") if x.strip()]
        grid_range = [float(x) for x in (os.getenv("META_OPT_SHARE_GRID_RANGE", "0.10,0.15,0.25,0.35,0.50") or "").split(",") if x.strip()]
    grid_trend = sorted(set([clamp01(s) for s in grid_trend])) or [0.10, 0.25]
    grid_range = sorted(set([clamp01(s) for s in grid_range])) or [0.10, 0.15]

    max_up_step = float(os.getenv("META_OPT_MAX_UP_STEP", "0.25") or 0.25)
    max_down_step = float(os.getenv("META_OPT_MAX_DOWN_STEP", "0.25") or 0.25)  # allow down moves
    if degrade_only:
        max_up_step = 0.0  # hard-stop: no increases

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

    # -------- build per-symbol plans --------
    plans: dict[str, list[dict[str, Any]]] = {}
    details: dict[str, Any] = {
        "health": {"stat": gstat, "factor": gfactor, "hard_stop": degrade_only, "hard_stop_reasons": hs_reasons},
        "budgets": {
            "sym_budget": sym_budget,
            "global_trend_budget": glob_trend_budget,
            "global_range_budget": glob_range_budget,
            "global_total_budget": total_budget,
            "hs_mult": hs_mult,
        },
    }

    max_symbols = int(os.getenv("META_OPT_MAX_SYMBOLS_PER_RUN", "12") or 12)
    for sym in sorted(list(cand.keys()))[:max_symbols]:
        buckets = cand[sym]
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
                s = x.get(MetaKeys.ENFORCE_SALT, "enf_v1")
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
                degrade_only=degrade_only,
                min_exec_rate=min_exec_rate,
                max_exec_rate_drop=max_exec_rate_drop,
                tail_exec_cap=tail_exec_cap,
                lam_tail=lam_tail,
                lam_p05=lam_p05,
                lam_turn=lam_turn,
                lam_step=lam_step,
            )

            details[cell] = {
                "symbol": sym,
                "bucket": bucket,
                "field": share_field,
                "cur_share": cur_share,
                "cap": cap,
                "salt": salt,
                "opts_top": [{"share": o["share"], "drop": o["drop"], "obj": o["obj"], "tail": o["rep"]["exec"]["tail_rate"], "exec_rate": o["exec_rate"]} for o in opts[:6]],
            }

            if bucket == "trend":
                t_opts = opts
            else:
                r_opts = opts

        if t_opts is None and r_opts is None:
            continue

        combos = enumerate_symbol_combos(
            t_opts,
            r_opts,
            symbol_budget=sym_budget,
            coupling_trend_lt=coupling_trend_lt_v,
            coupling_range_cap=coupling_range_cap_v,
        )
        if combos:
            plans[sym] = combos

    if not plans:
        logger.info("No plans generated")
        return

    chosen = select_under_bucket_budgets(
        plans,
        budget_trend=glob_trend_budget,
        budget_range=glob_range_budget,
        budget_total=total_budget,
    )

    # -------- build ops --------
    ops: list[dict[str, str]] = []
    picked_cells: list[str] = []

    for sym, combo in chosen.items():
        hk = f"{prefix}{sym}"
        buckets = cand[sym]
        details[f"{sym}|__chosen__"] = {
            "trend_drop": combo.get("trend_drop"),
            "range_drop": combo.get("range_drop"),
            "sum_drop": combo.get("sum_drop"),
            "sum_obj": combo.get("sum_obj"),
        }

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

            # in degrade-only, this is guaranteed to be <= cur_share (no increases)
            ops.append({"op": "HSET", "key": hk, "field": "meta_model_enable", "value": "1"})
            ops.append({"op": "HSET", "key": hk, "field": "meta_model_mode", "value": "ENFORCE"})
            ops.append({"op": "HSET", "key": hk, "field": share_field, "value": f"{new_share:.2f}"})
            picked_cells.append(cell)

            if cell in details:
                details[cell]["chosen"] = {"cur": cur_share, "new": new_share, "drop": float(ch.get("drop", 0.0)), "obj": float(ch.get("obj", 0.0))}

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
        # optional: notify hard-stop state
        if degrade_only and int(os.getenv("META_NOTIFY_ON_HARDSTOP_NOOP", "1") or 1) == 1:
            notify(r, f"<b>Stage2 v5: HARD-STOP active</b>\n(no share changes)\nreasons=<code>{hs_reasons}</code>\nmetrics=<code>{gstat}</code>")
        logger.info("No operations generated")
        return

    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)

    bundle = {
        "id": bundle_id,
        "created_ms": now_ms(),
        "ttl_sec": ttl,
        "who": "nightly_meta_stage2_optimize_share_bundle_v5",
        "ops": uniq_ops,
        "meta": {
            "kind": "meta_enforce_unfreeze_stage",
            "stage": 2,
            "optimized": True,
            "multiobjective": True,
            "safety_layer": "per-bucket-global-budgets + hard-stop degrade-only",
            "picked_cells": picked_cells,
            "health": {"stat": gstat, "factor": gfactor, "hard_stop": degrade_only, "hard_stop_reasons": hs_reasons},
            "budgets": {"sym_budget": sym_budget, "trend": glob_trend_budget, "range": glob_range_budget, "total": total_budget},
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
        "<b>Stage2 UNFREEZE proposal (v5 per-bucket budgets + hard-stop)</b>\n"
        f"id=<code>{bundle_id}</code>\n"
        f"cells=<code>{picked_cells}</code>\n"
        f"degrade_only=<code>{int(degrade_only)}</code> reasons=<code>{hs_reasons}</code>\n"
        f"budgets=<code>{{'sym':{sym_budget:.2f},'trend':{glob_trend_budget:.2f},'range':{glob_range_budget:.2f},'total':{total_budget}}}</code>\n"
        f"metrics=<code>{gstat}</code>",
        buttons=buttons,
    )

    logger.info(f"Stage2 v5 bundle created: {bundle_id}, cells: {len(picked_cells)}")


if __name__ == "__main__":
    main()

