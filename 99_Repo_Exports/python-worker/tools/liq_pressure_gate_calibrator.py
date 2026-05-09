#!/usr/bin/env python3
from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
Liq Pressure Gate Calibrator (LiqPressureGate — P2d).

Reads `decisions:final` Redis stream for the last N hours.
Joins with closed trades by sid (R-multiple).
Computes Δ R̄ between trades where liq_pressure_boost > 0 vs boost == 0.

If boost consistently correlates with better PnL → proposes next mode step
on the ladder:  off → boost → penalty → both → enforce

Proposal goes via interactive Telegram bundle (✅ Approve / ❌ Reject).
Idempotency guard: meta:liq_cal:pending (TTL 23 h).
Hold-down:        meta:liq_cal:last_step_ms (min 72 h between steps).

Usage:
  python3 -m tools.liq_pressure_gate_calibrator --hours 168 --dry-run
  python3 -m tools.liq_pressure_gate_calibrator --hours 168 --force-propose

ENV:
  REDIS_URL                      default redis://localhost:6379/0
  LIQ_CAL_HOURS                  default 168 (7 days)
  LIQ_CAL_MIN_BOOST_HITS         default 10
  LIQ_CAL_MIN_R_DELTA            default 0.05   (boost_r_mean >= pass_r_mean + delta)
  LIQ_CAL_ENFORCE_HOLDDOWN_H     default 72     (min hours between mode ladder steps)
  LIQ_CAL_PENDING_KEY            default meta:liq_cal:pending
  LIQ_CAL_STEP_TS_KEY            default meta:liq_cal:last_step_ms
  RECS_HMAC_SECRET               for bundle signing
"""

import argparse
import collections
import hashlib
import hmac
import json
import logging
import math
import os
import statistics
import subprocess
import sys
import tempfile
import time
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis
import contextlib

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Mode ladder: each step requires calibration data to proceed to next
# ---------------------------------------------------------------------------
MODE_LADDER: list[str] = ["off", "boost", "penalty", "both", "enforce"]


def _next_mode(current_mode: str) -> str | None:
    """Return the next mode on the ladder, or None if already at the top."""
    c = current_mode.strip().lower()
    try:
        idx = MODE_LADDER.index(c)
    except ValueError:
        idx = 0  # treat unknown as 'off'
    nxt = idx + 1
    if nxt >= len(MODE_LADDER):
        return None
    return MODE_LADDER[nxt]


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def _get_redis_url() -> str:
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    if "redis-worker-1" in url and not os.path.exists("/.dockerenv"):
        url = "redis://localhost:6379/0"
    return url


def _get_redis() -> redis.Redis:
    return redis.Redis.from_url(_get_redis_url(), decode_responses=True)


# ---------------------------------------------------------------------------
# Signing (same pattern as liqmap_gate_calibrator)
# ---------------------------------------------------------------------------

def _sign_bundle(bundle_id: str, secret: str) -> str:
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


# ---------------------------------------------------------------------------
# Safe converters
# ---------------------------------------------------------------------------

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default


# ---------------------------------------------------------------------------
# Step 1 — read decisions:final stream
# ---------------------------------------------------------------------------

def query_decisions_from_stream(
    r: redis.Redis,
    hours: float,
    symbol_filter: str | None = None,
    stream: str = "decisions:final",
    batch: int = 2000,
) -> dict[str, dict[str, Any]]:
    """
    Scan `decisions:final` stream for the last `hours` hours.

    Returns dict: sid → {
        liq_boost, liq_pen, liq_veto, liq_reason,
        liq_q_align, liq_ofi_align,
        gate_mode, symbol, direction, ts_ms
    },

    Only includes records where liq_pressure indicators are present
    (i.e. liq_pressure_gate_mode was not "off" at the time of the decision).
    """
    since_ms = get_ny_time_millis() - int(hours * 3_600_000)
    start_id = f"{since_ms}-0"

    decisions: dict[str, dict[str, Any]] = {}
    cur = start_id

    logger.info(f"Reading stream '{stream}' from {start_id} (last {hours:.0f}h)...")

    first = True
    while True:
        min_id = cur if first else f"({cur}"
        first = False
        try:
            rows = r.xrange(stream, min=min_id, max="+", count=batch)
        except Exception as exc:
            logger.warning(f"xrange error on {stream}: {exc}")
            break

        if not rows:
            break

        for sid_stream, fields in rows:
            cur = str(sid_stream)
            raw_payload = fields.get("payload") or ""
            if not raw_payload:
                continue
            try:
                rec = json.loads(raw_payload)
            except Exception:
                continue

            if not isinstance(rec, dict):
                continue

            indicators = rec.get("indicators") or {}
            if not isinstance(indicators, dict):
                # Try nested path
                evidence = rec.get("evidence") or {}
                indicators = evidence if isinstance(evidence, dict) else {}

            # Only include if gate was active (boost key present and mode not off)
            # We check the presence of liq_pressure_reason as the sentinel
            liq_reason = (indicators.get("liq_pressure_reason") or "")
            liq_boost = _safe_float(indicators.get("liq_pressure_boost"), -1.0)
            if liq_boost < 0:
                # Key absent → gate was "off", skip
                continue

            sid = (rec.get("sid") or "").strip()
            if not sid:
                continue

            symbol = (rec.get("symbol") or "").upper()
            if symbol_filter and symbol != symbol_filter.upper():
                continue

            decisions[sid] = {
                "liq_boost":   liq_boost,
                "liq_pen":     _safe_float(indicators.get("liq_pressure_pen"), 0.0),
                "liq_veto":    _safe_int(indicators.get("liq_pressure_veto"), 0),
                "liq_reason":  liq_reason,
                "liq_q_align": _safe_int(indicators.get("liq_q_align"), 0),
                "liq_ofi_align": _safe_int(indicators.get("liq_ofi_align"), 0),
                "symbol":      symbol,
                "direction":   (rec.get("direction") or "").upper(),
                "ts_ms":       _safe_int(rec.get("ts_ms"), 0),
            },

    logger.info(f"Decisions with active LiqPressureGate found: {len(decisions)}")
    return decisions


# ---------------------------------------------------------------------------
# Step 2 — load closed trades
# ---------------------------------------------------------------------------

def _load_trades(hours: float) -> dict[str, dict[str, Any]]:
    """
    Export closed trades via tools/export_trade_closed_ndjson.py.
    Returns dict: sid → {r_mult, symbol, direction}
    """
    redis_url = _get_redis_url()

    with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as tf:
        trades_path = tf.name

    try:
        logger.info(f"Exporting closed trades for {hours:.0f}h to {trades_path}...")
        subprocess.check_call(
            [
                sys.executable,
                "tools/export_trade_closed_ndjson.py",
                "--since-hours", str(hours),
                "--out", trades_path,
                "--redis-url", redis_url,
            ],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
    except subprocess.CalledProcessError as exc:
        logger.error(f"export_trade_closed_ndjson.py failed: {exc}")
        return {}

    trades: dict[str, dict[str, Any]] = {}
    try:
        with open(trades_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    t = json.loads(line)
                    sid = (t.get("sid") or "").strip()
                    if sid:
                        trades[sid] = {
                            "r_mult":    _safe_float(t.get("r_mult"), 0.0),
                            "symbol":    (t.get("symbol") or "").upper(),
                            "direction": str(t.get("direction") or t.get("side") or "").upper(),
                        },
                except Exception:
                    pass
    finally:
        with contextlib.suppress(Exception):
            os.unlink(trades_path)

    logger.info(f"Closed trades loaded: {len(trades)}")
    return trades


# ---------------------------------------------------------------------------
# Step 3 — compute stats
# ---------------------------------------------------------------------------

def _mean(vals: list[float]) -> float:
    return statistics.mean(vals) if vals else 0.0


def _median(vals: list[float]) -> float:
    return statistics.median(vals) if vals else 0.0


def compute_stats(
    decisions: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """
    Join decisions + trades by sid.

    Segments:
      boost_group  — liq_boost > 0  (gate confirmed alignment)
      pass_group   — liq_boost == 0 (gate saw neutral / no alignment)
    """
    joined = 0
    boost_r: list[float] = []
    pass_r:  list[float] = []
    veto_r:  list[float] = []
    reasons: dict[str, int] = collections.Counter()
    by_symbol: dict[str, dict[str, Any]] = {}

    for sid, dec in decisions.items():
        trade = trades.get(sid)
        if trade is None:
            continue
        joined += 1
        r = trade["r_mult"]
        sym = dec["symbol"] or trade["symbol"]

        if sym not in by_symbol:
            by_symbol[sym] = {
                "boost_count": 0, "boost_r": [],
                "pass_count":  0, "pass_r":  [],
                "veto_count":  0, "veto_r":  [],
            },
        s = by_symbol[sym]

        if dec["liq_veto"] == 1:
            veto_r.append(r)
            reasons[dec["liq_reason"]] += 1
            s["veto_count"] += 1
            s["veto_r"].append(r)
        elif dec["liq_boost"] > 0:
            boost_r.append(r)
            s["boost_count"] += 1
            s["boost_r"].append(r)
        else:
            pass_r.append(r)
            s["pass_count"] += 1
            s["pass_r"].append(r)

    # Flatten by_symbol
    by_sym_flat: dict[str, Any] = {}
    for sym, s in sorted(by_symbol.items()):
        by_sym_flat[sym] = {
            "boost_count":  s["boost_count"],
            "boost_r_mean": round(_mean(s["boost_r"]), 3),
            "pass_count":   s["pass_count"],
            "pass_r_mean":  round(_mean(s["pass_r"]), 3),
            "veto_count":   s["veto_count"],
            "veto_r_mean":  round(_mean(s["veto_r"]), 3),
        },

    return {
        "total_decisions": len(decisions),
        "total_joined":    joined,
        "boost_hits":      len(boost_r),
        "pass_hits":       len(pass_r),
        "veto_hits":       len(veto_r),
        "boost_r_mean":    round(_mean(boost_r),  3),
        "boost_r_median":  round(_median(boost_r), 3),
        "boost_r_sum":     round(sum(boost_r),   3),
        "pass_r_mean":     round(_mean(pass_r),   3),
        "pass_r_median":   round(_median(pass_r),  3),
        "veto_r_mean":     round(_mean(veto_r),   3),
        "veto_r_median":   round(_median(veto_r),  3),
        "reasons":         dict(reasons.most_common()),
        "by_symbol":       by_sym_flat,
    },


def _should_propose(
    stats: dict[str, Any],
    min_boost_hits: int,
    min_r_delta: float,
) -> tuple[bool, str]:
    """
    Return (should_propose, reason_msg).
    Propose when boost R-mean is notably better than pass R-mean.
    """
    if stats["boost_hits"] < min_boost_hits:
        return False, (
            f"boost_hits={stats['boost_hits']} < min={min_boost_hits}"
        )
    delta = stats["boost_r_mean"] - stats["pass_r_mean"]
    if delta < min_r_delta:
        return False, (
            f"Δ R̄={delta:+.3f} < min_r_delta={min_r_delta:+.3f}"
        )
    return True, (
        f"boost_hits={stats['boost_hits']} >= {min_boost_hits} "
        f"AND Δ R̄={delta:+.3f} >= {min_r_delta:+.3f}"
    )


# ---------------------------------------------------------------------------
# Step 4 — idempotency & hold-down guards
# ---------------------------------------------------------------------------

def _check_guards(
    r: redis.Redis,
    pending_key: str,
    step_ts_key: str,
    holddown_h: float,
) -> tuple[bool, str]:
    """
    Returns (blocked, reason).
    blocked=True → skip proposal.
    """
    if r.exists(pending_key):
        return True, f"pending proposal exists ({pending_key})"

    last_ms_raw = r.get(step_ts_key)
    if last_ms_raw:
        try:
            age_h = (get_ny_time_millis() - float(last_ms_raw)) / 3_600_000
            if age_h < holddown_h:
                return True, (
                    f"hold-down: last step {age_h:.1f}h ago < {holddown_h}h"
                )
        except Exception:
            pass

    return False, ""


# ---------------------------------------------------------------------------
# Step 5 — read current mode from Redis
# ---------------------------------------------------------------------------

def _current_mode(r: redis.Redis, cfg_key: str = "cfg:crypto_orderflow") -> str:
    """Read liq_pressure_gate_mode from Redis cfg hash. Default 'off'."""
    try:
        v = r.hget(cfg_key, "liq_pressure_gate_mode")
        return (v or "off").strip().lower()
    except Exception:
        return "off"


# ---------------------------------------------------------------------------
# Step 6 — Build and send Telegram proposal
# ---------------------------------------------------------------------------

def _fmt_by_symbol(by_sym: dict[str, Any]) -> str:
    if not by_sym:
        return "—"
    lines = []
    for sym, s in list(by_sym.items())[:8]:
        lines.append(
            f"  <code>{sym:<12}</code> "
            f"boost:{s['boost_count']} R̄={s['boost_r_mean']:+.2f}  "
            f"pass:{s['pass_count']} R̄={s['pass_r_mean']:+.2f}  "
            f"veto:{s['veto_count']} R̄={s['veto_r_mean']:+.2f}"
        )
    return "\n".join(lines)


def create_and_send_proposal(
    r: redis.Redis,
    stats: dict[str, Any],
    hours: float,
    current_mode: str,
    next_mode: str,
    pending_key: str,
    step_ts_key: str,
    holddown_h: float,
    cfg_key: str = "cfg:crypto_orderflow",
) -> str:
    """Store bundle, send Telegram message. Returns bundle_id."""
    bundle_id = f"liq_pressure_{next_mode}_{int(time.time())}"
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    sig = _sign_bundle(bundle_id, secret)

    ops = [
        {
            "op":    "HSET",
            "key":   cfg_key,
            "field": "liq_pressure_gate_mode",
            "value": next_mode,
        },
    ]

    meta = {
        "title": f"LiqPressureGate: {current_mode} → {next_mode}",
        "details": {
            "period_hours":  hours,
            "current_mode":  current_mode,
            "next_mode":     next_mode,
            "boost_hits":    stats["boost_hits"],
            "boost_r_mean":  stats["boost_r_mean"],
            "pass_r_mean":   stats["pass_r_mean"],
            "delta_r_mean":  round(stats["boost_r_mean"] - stats["pass_r_mean"], 3),
            "veto_hits":     stats["veto_hits"],
        },
    },

    bundle = {
        "id":         bundle_id,
        "created_ms": get_ny_time_millis(),
        "ops":        ops,
        "meta":       meta,
    },

    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle))
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=86400)
    # Mark idempotency: 23h TTL
    r.set(pending_key, bundle_id, ex=int(holddown_h * 3600 - 3600))

    delta = stats["boost_r_mean"] - stats["pass_r_mean"]
    text = (
        f"<b>📊 LiqPressureGate Calibrator</b>\n\n"
        f"Период: <b>последние {int(hours)}ч</b>\n"
        f"Текущий режим: <code>{current_mode}</code> → предлагается <code>{next_mode}</code>\n\n"
        f"<b>boost (liq_boost &gt; 0)</b>: {stats['boost_hits']} трейдов  "
        f"R̄ = <b>{stats['boost_r_mean']:+.3f}</b>  "
        f"медиана = {stats['boost_r_median']:+.3f}\n"
        f"<b>neutral (boost = 0)</b>:     {stats['pass_hits']} трейдов  "
        f"R̄ = <b>{stats['pass_r_mean']:+.3f}</b>  "
        f"медиана = {stats['pass_r_median']:+.3f}\n"
        f"<b>veto</b>:                    {stats['veto_hits']} трейдов  "
        f"R̄ = {stats['veto_r_mean']:+.3f}\n\n"
        f"Δ R̄ (boost − neutral) = <b>{delta:+.3f}R</b>\n\n"
        f"<b>По символам:</b>\n{_fmt_by_symbol(stats['by_symbol'])}\n\n"
        f"Лестница: <code>off → boost → penalty → both → enforce</code>\n"
        f"Следующий шаг: <code>liq_pressure_gate_mode={next_mode}</code>"
    )

    buttons = [
        [
            {"text": "✅ Approve", "callback_data": f"recs:confirm:{bundle_id}:{sig}"},
            {"text": "❌ Reject",  "callback_data": f"recs:reject:{bundle_id}:{sig}"},
        ]
    ]

    notify_stream = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
    r.xadd(
        notify_stream,
        {
            "type":       "report",
            "subtype":    "liq_pressure_calibrator",
            "ts":         str(get_ny_time_millis()),
            "text":       text,
            "parse_mode": "HTML",
            "buttons":    json.dumps(buttons),
        },
    )
    logger.info(f"Telegram proposal sent: bundle_id={bundle_id} ({current_mode} → {next_mode})")
    return bundle_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="LiqPressure Gate Calibrator")
    parser.add_argument(
        "--hours", type=float,
        default=float(os.getenv("LIQ_CAL_HOURS", "168")),
        help="Lookback window in hours (default: 168 = 7 days)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print stats only; do not send Telegram or write Redis bundle",
    )
    parser.add_argument(
        "--force-propose", action="store_true",
        help="Send proposal regardless of thresholds and guards",
    )
    parser.add_argument(
        "--min-boost-hits", type=int,
        default=int(os.getenv("LIQ_CAL_MIN_BOOST_HITS", "10")),
        help="Min boost trades with known R-multiple outcome",
    )
    parser.add_argument(
        "--min-r-delta", type=float,
        default=float(os.getenv("LIQ_CAL_MIN_R_DELTA", "0.05")),
        help="Δ R̄ (boost_r_mean − pass_r_mean) must be >= this to propose",
    )
    parser.add_argument(
        "--holddown-h", type=float,
        default=float(os.getenv("LIQ_CAL_ENFORCE_HOLDDOWN_H", "72")),
        help="Min hours between ladder steps (default: 72)",
    )
    parser.add_argument(
        "--pending-key", type=str,
        default=os.getenv("LIQ_CAL_PENDING_KEY", "meta:liq_cal:pending"),
        help="Redis key for idempotency guard",
    )
    parser.add_argument(
        "--step-ts-key", type=str,
        default=os.getenv("LIQ_CAL_STEP_TS_KEY", "meta:liq_cal:last_step_ms"),
        help="Redis key storing last step timestamp (ms)",
    )
    parser.add_argument(
        "--stream", type=str,
        default=os.getenv("DECISIONS_FINAL_STREAM", "decisions:final"),
        help="Redis stream name (default: decisions:final)",
    )
    parser.add_argument(
        "--symbol", type=str, default="",
        help="Optional: filter by symbol (e.g. BTCUSDT)",
    )
    parser.add_argument(
        "--cfg-key", type=str, default="cfg:crypto_orderflow",
        help="Redis hash key holding liq_pressure_gate_mode",
    )
    args = parser.parse_args()

    sym_filter = args.symbol.strip().upper().replace("/", "").replace("-", "") or None
    r = _get_redis()

    # Current mode + next mode on ladder
    current_mode = _current_mode(r, args.cfg_key)
    logger.info(f"Current liq_pressure_gate_mode = '{current_mode}'")

    if current_mode == "enforce":
        logger.info("Already at 'enforce' (top of ladder). Nothing to promote.")
        return

    next_mode_ = _next_mode(current_mode)
    if next_mode_ is None:
        logger.info("No next mode available.")
        return

    logger.info(
        f"Running LiqPressure Gate Calibrator — "
        f"hours={args.hours}, mode ladder: {current_mode} → {next_mode_}"
    )

    # Step 1 — decisions
    decisions = query_decisions_from_stream(
        r, hours=args.hours, symbol_filter=sym_filter, stream=args.stream
    )

    if not decisions:
        logger.warning("No LiqPressureGate decisions found in stream. Exiting.")
        return

    # Step 2 — closed trades
    trades = _load_trades(args.hours)
    if not trades:
        logger.warning("No closed trades loaded. Exiting.")
        return

    # Step 3 — stats
    stats = compute_stats(decisions, trades)

    logger.info(
        f"Stats: total_decisions={stats['total_decisions']} "
        f"joined={stats['total_joined']} "
        f"boost_hits={stats['boost_hits']} "
        f"boost_r_mean={stats['boost_r_mean']:.3f} "
        f"pass_r_mean={stats['pass_r_mean']:.3f} "
        f"Δ={stats['boost_r_mean']-stats['pass_r_mean']:+.3f} "
        f"veto_hits={stats['veto_hits']}"
    )
    logger.info(f"By symbol: {json.dumps(stats['by_symbol'], ensure_ascii=False)}")

    # Step 4 — decide whether to propose
    should_propose = False
    reason_msg = ""

    if args.force_propose:
        should_propose = True
        reason_msg = "forced via --force-propose"
        logger.info("Forcing proposal (--force-propose).")
    else:
        ok, reason_msg = _should_propose(stats, args.min_boost_hits, args.min_r_delta)
        if ok:
            # Check guards
            blocked, guard_reason = _check_guards(
                r, args.pending_key, args.step_ts_key, args.holddown_h
            )
            if blocked:
                logger.info(f"Guards block proposal: {guard_reason}")
            else:
                should_propose = True
                logger.info(f"Thresholds met: {reason_msg}")
        else:
            logger.info(f"Thresholds NOT met: {reason_msg}. No proposal.")

    if should_propose:
        if args.dry_run:
            logger.info("DRY-RUN: Would have sent Telegram proposal and written Redis bundle.")
            logger.info(f"DRY-RUN next_mode={next_mode_}, reason={reason_msg}")
            logger.info(f"DRY-RUN stats:\n{json.dumps(stats, ensure_ascii=False, indent=2)}")
        else:
            bundle_id = create_and_send_proposal(
                r, stats, args.hours,
                current_mode=current_mode,
                next_mode=next_mode_,
                pending_key=args.pending_key,
                step_ts_key=args.step_ts_key,
                holddown_h=args.holddown_h,
                cfg_key=args.cfg_key,
            )
            logger.info(f"Done. bundle_id={bundle_id}")
    elif args.dry_run:
        logger.info(f"DRY-RUN: thresholds not met. Stats: {json.dumps(stats, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
