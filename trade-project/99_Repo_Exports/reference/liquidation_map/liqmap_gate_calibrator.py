#!/usr/bin/env python3
"""
LiqMap Gate Calibrator.

Reads `decisions:final` Redis stream to extract liqmap gate shadow_veto flags
and metrics (rr, risk_bps, reason, symbol, direction).
Joins with closed-trade PnL (R-multiple) via export_trade_closed_ndjson.py.

If veto'd trades have meaningfully worse avg R than passed trades, proposes
switching `liqmap_gate_mode` to `enforce` via interactive Telegram message.

Usage:
  python3 -m tools.liqmap_gate_calibrator --hours 168 --dry-run
  python3 -m tools.liqmap_gate_calibrator --hours 168 --force-propose

ENV:
  REDIS_URL                    default redis://localhost:6379/0
  LIQMAP_CALIBRATOR_HOURS      default 168 (7 days)
  RECS_HMAC_SECRET             for bundle signing (default CHANGE_ME)
"""

from __future__ import annotations

import argparse
import collections
import hmac
import hashlib
import json
import logging
import math
import os
import statistics
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple

import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def _get_redis_url() -> str:
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    # When running outside Docker, docker hostname won't resolve
    if "redis-worker-1" in url and not os.path.exists("/.dockerenv"):
        url = "redis://localhost:6379/0"
    return url


def _get_redis() -> redis.Redis:
    return redis.Redis.from_url(_get_redis_url(), decode_responses=True)


# ---------------------------------------------------------------------------
# Signing (reuse bundle pattern from taker_flow_gate_calibrator)
# ---------------------------------------------------------------------------

def _sign_bundle(bundle_id: str, secret: str) -> str:
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


# ---------------------------------------------------------------------------
# Step 1 — read decisions:final stream for liqmap gate data
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


def query_decisions_from_stream(
    r: redis.Redis,
    hours: float,
    symbol_filter: Optional[str] = None,
    stream: str = "decisions:final",
    batch: int = 2000,
) -> Dict[str, Dict[str, Any]]:
    """
    Scan `decisions:final` stream for the last `hours` hours.
    Returns a dict: sid -> {shadow_veto, veto, rr, risk_bps, reason, mode, symbol, direction, ts_ms}
    Only includes records where liqmap gate mode != 'off' and != ''.
    """
    since_ms = int(time.time() * 1000) - int(hours * 3_600_000)
    start_id = f"{since_ms}-0"

    decisions: Dict[str, Dict[str, Any]] = {}
    cur = start_id

    logger.info(f"Reading stream '{stream}' from {start_id} (last {hours}h)...")

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

            # Extract liqmap gate sub-dict
            lm = rec.get("liqmap")
            if not isinstance(lm, dict):
                continue
            gate = lm.get("gate")
            if not isinstance(gate, dict):
                continue

            mode = str(gate.get("mode") or "").lower()
            if mode in ("", "off"):
                continue

            sid = str(rec.get("sid") or "").strip()
            if not sid:
                continue

            symbol = str(rec.get("symbol") or "").upper()
            if symbol_filter and symbol != symbol_filter.upper():
                continue

            decisions[sid] = {
                "shadow_veto": _safe_int(gate.get("shadow_veto"), 0),
                "veto":        _safe_int(gate.get("veto"), 0),
                "rr":          _safe_float(gate.get("rr"), 0.0),
                "risk_bps":    _safe_float(gate.get("risk_bps"), 0.0),
                "reward_bps":  _safe_float(gate.get("reward_bps"), 0.0),
                "reason":      str(gate.get("reason") or "ok"),
                "mode":        mode,
                "symbol":      symbol,
                "direction":   str(rec.get("direction") or "").upper(),
                "ts_ms":       _safe_int(rec.get("ts_ms"), 0),
            }

    logger.info(f"Decisions with active liqmap gate found: {len(decisions)}")
    return decisions


# ---------------------------------------------------------------------------
# Step 2 — load closed trades (for R-multiple)
# ---------------------------------------------------------------------------

def _load_trades(hours: float) -> Dict[str, Dict[str, Any]]:
    """
    Export closed trades via tools/export_trade_closed_ndjson.py and parse.
    Returns dict: sid -> {r_mult, symbol, direction}
    """
    redis_url = _get_redis_url()

    with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as tf:
        trades_path = tf.name

    try:
        logger.info(f"Exporting closed trades for {hours}h to {trades_path}...")
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

    trades: Dict[str, Dict[str, Any]] = {}
    try:
        with open(trades_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    t = json.loads(line)
                    sid = str(t.get("sid") or "").strip()
                    if sid:
                        trades[sid] = {
                            "r_mult":    _safe_float(t.get("r_mult"), 0.0),
                            "symbol":    str(t.get("symbol") or "").upper(),
                            "direction": str(t.get("direction") or t.get("side") or "").upper(),
                        }
                except Exception:
                    pass
    finally:
        try:
            os.unlink(trades_path)
        except Exception:
            pass

    logger.info(f"Closed trades loaded: {len(trades)}")
    return trades


# ---------------------------------------------------------------------------
# Step 3 — compute stats
# ---------------------------------------------------------------------------

def _mean(vals: List[float]) -> float:
    return statistics.mean(vals) if vals else 0.0


def _median(vals: List[float]) -> float:
    return statistics.median(vals) if vals else 0.0


def compute_stats(
    decisions: Dict[str, Dict[str, Any]],
    trades: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Join decisions + trades by sid. Compute PnL stats by shadow_veto segment."""

    joined = 0
    veto_r: List[float] = []
    pass_r: List[float] = []
    veto_reasons: Dict[str, int] = collections.Counter()
    by_symbol: Dict[str, Dict[str, Any]] = {}

    for sid, dec in decisions.items():
        trade = trades.get(sid)
        if trade is None:
            continue
        joined += 1
        r = trade["r_mult"]
        sym = dec["symbol"] or trade["symbol"]

        if sym not in by_symbol:
            by_symbol[sym] = {"veto_count": 0, "veto_r": [], "pass_count": 0, "pass_r": []}
        s = by_symbol[sym]

        if dec["shadow_veto"] == 1:
            veto_r.append(r)
            veto_reasons[dec["reason"]] += 1
            s["veto_count"] += 1
            s["veto_r"].append(r)
        else:
            pass_r.append(r)
            s["pass_count"] += 1
            s["pass_r"].append(r)

    # Flatten by_symbol for serialisation
    by_sym_flat: Dict[str, Any] = {}
    for sym, s in sorted(by_symbol.items()):
        by_sym_flat[sym] = {
            "veto_count":  s["veto_count"],
            "veto_r_mean": round(_mean(s["veto_r"]), 3),
            "veto_r_sum":  round(sum(s["veto_r"]), 3),
            "pass_count":  s["pass_count"],
            "pass_r_mean": round(_mean(s["pass_r"]), 3),
        }

    return {
        "total_decisions": len(decisions),
        "total_joined":    joined,
        "veto_hits":       len(veto_r),
        "pass_hits":       len(pass_r),
        "veto_r_sum":    round(sum(veto_r), 3),
        "veto_r_mean":   round(_mean(veto_r), 3),
        "veto_r_median": round(_median(veto_r), 3),
        "pass_r_mean":   round(_mean(pass_r), 3),
        "pass_r_median": round(_median(pass_r), 3),
        "veto_reasons":  dict(veto_reasons.most_common()),
        "by_symbol":     by_sym_flat,
    }


# ---------------------------------------------------------------------------
# Step 4 — build Telegram message and Redis bundle
# ---------------------------------------------------------------------------

def _fmt_reasons(reasons: Dict[str, int]) -> str:
    if not reasons:
        return "—"
    return " | ".join(f"<code>{k}</code>:{v}" for k, v in reasons.items())


def _fmt_by_symbol(by_sym: Dict[str, Any]) -> str:
    if not by_sym:
        return "—"
    lines = []
    for sym, s in list(by_sym.items())[:8]:  # max 8 rows in message
        lines.append(
            f"  <code>{sym:<12}</code> veto:{s['veto_count']}  "
            f"R̄={s['veto_r_mean']:+.2f}  pass:{s['pass_count']}  "
            f"R̄={s['pass_r_mean']:+.2f}"
        )
    return "\n".join(lines)


def create_and_send_proposal(
    r: redis.Redis,
    stats: Dict[str, Any],
    hours: float,
) -> str:
    """Store bundle, stream Telegram message. Returns bundle_id."""
    bundle_id = f"liqmap_enforce_{int(time.time())}"
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    sig = _sign_bundle(bundle_id, secret)

    ops = [
        {
            "op": "HSET",
            "key": "config:orderflow:GLOBAL",
            "field": "liqmap_gate_mode",
            "value": "enforce",
        }
    ]

    meta = {
        "title": "Enable LiqMapGate enforce mode",
        "details": {
            "period_hours": hours,
            "veto_hits":    stats["veto_hits"],
            "veto_r_mean":  stats["veto_r_mean"],
            "pass_r_mean":  stats["pass_r_mean"],
            "reasons":      stats["veto_reasons"],
        },
    }

    bundle = {
        "id":         bundle_id,
        "created_ms": int(time.time() * 1000),
        "ops":        ops,
        "meta":       meta,
    }

    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle))
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=86400)

    # Build Telegram HTML
    delta = stats["veto_r_mean"] - stats["pass_r_mean"]
    text = (
        f"<b>📊 LiqMap Gate Calibrator</b>\n\n"
        f"Период: <b>последние {int(hours)}ч</b>\n"
        f"Решений с liqmap-гейтом: <b>{stats['total_decisions']}</b> "
        f"(с трейдами: <b>{stats['total_joined']}</b>)\n\n"
        f"<b>shadow_veto=1</b>: {stats['veto_hits']} трейдов  "
        f"R̄ = <b>{stats['veto_r_mean']:+.3f}</b>  "
        f"медиана = {stats['veto_r_median']:+.3f}\n"
        f"<b>shadow_veto=0</b>: {stats['pass_hits']} трейдов  "
        f"R̄ = <b>{stats['pass_r_mean']:+.3f}</b>  "
        f"медиана = {stats['pass_r_median']:+.3f}\n\n"
        f"Δ R̄ (pass − veto) = <b>{delta:+.3f}R</b>\n"
        f"Причины вето: {_fmt_reasons(stats['veto_reasons'])}\n\n"
        f"<b>По символам:</b>\n{_fmt_by_symbol(stats['by_symbol'])}\n\n"
        f"Предлагаю включить <code>liqmap_gate_mode=enforce</code> (GLOBAL)."
    )

    buttons = [
        [
            {"text": "✅ Approve", "callback_data": f"recs:confirm:{bundle_id}:{sig}"},
            {"text": "❌ Reject",  "callback_data": f"recs:reject:{bundle_id}:{sig}"},
        ]
    ]

    r.xadd(
        "notify:telegram",
        {
            "type":       "report",
            "subtype":    "liqmap_calibrator",
            "ts":         str(int(time.time() * 1000)),
            "text":       text,
            "parse_mode": "HTML",
            "buttons":    json.dumps(buttons),
        },
    )
    logger.info(f"Telegram proposal sent for bundle_id={bundle_id}")
    return bundle_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="LiqMap Gate Calibrator")
    parser.add_argument(
        "--hours", type=float,
        default=float(os.getenv("LIQMAP_CALIBRATOR_HOURS", "168")),
        help="Lookback window in hours (default: 168 = 7 days)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute stats only; do not send Telegram or write Redis bundle",
    )
    parser.add_argument(
        "--force-propose", action="store_true",
        help="Send proposal regardless of thresholds",
    )
    parser.add_argument(
        "--min-veto-hits", type=int, default=3,
        help="Min shadow_veto=1 joined trades required to propose",
    )
    parser.add_argument(
        "--max-r-mult", type=float, default=-0.5,
        help="veto_r_mean must be <= this to propose (default: -0.5)",
    )
    parser.add_argument(
        "--symbol", type=str, default="",
        help="Optional: filter by symbol (e.g. BTCUSDT)",
    )
    parser.add_argument(
        "--stream", type=str,
        default=os.getenv("DECISIONS_FINAL_STREAM", "decisions:final"),
        help="Redis stream name (default: decisions:final)",
    )
    args = parser.parse_args()

    sym_filter = args.symbol.strip().upper().replace("/", "").replace("-", "") or None

    r = _get_redis()

    # Step 1: decisions from stream
    decisions = query_decisions_from_stream(
        r,
        hours=args.hours,
        symbol_filter=sym_filter,
        stream=args.stream,
    )

    if not decisions:
        logger.warning("No liqmap gate decisions found in stream. Exiting.")
        return

    # Step 2: closed trades
    trades = _load_trades(args.hours)

    if not trades:
        logger.warning("No closed trades loaded. Exiting.")
        return

    # Step 3: compute stats
    stats = compute_stats(decisions, trades)

    logger.info(
        f"Stats: decisions={stats['total_decisions']} joined={stats['total_joined']} "
        f"veto_hits={stats['veto_hits']} veto_r_mean={stats['veto_r_mean']:.3f} "
        f"pass_r_mean={stats['pass_r_mean']:.3f}"
    )
    logger.info(f"Veto reasons: {stats['veto_reasons']}")
    logger.info(f"By symbol: {json.dumps(stats['by_symbol'], ensure_ascii=False)}")

    # Step 4: decide whether to propose
    should_propose = False
    if args.force_propose:
        should_propose = True
        logger.info("Forcing proposal (--force-propose).")
    elif (
        stats["veto_hits"] >= args.min_veto_hits
        and stats["veto_r_mean"] <= args.max_r_mult
    ):
        should_propose = True
        logger.info(
            f"Thresholds met: veto_hits={stats['veto_hits']} >= {args.min_veto_hits} "
            f"AND veto_r_mean={stats['veto_r_mean']:.3f} <= {args.max_r_mult}"
        )
    else:
        logger.info(
            f"Thresholds NOT met "
            f"(veto_hits={stats['veto_hits']}/{args.min_veto_hits}, "
            f"veto_r_mean={stats['veto_r_mean']:.3f}/{args.max_r_mult}). "
            f"No proposal."
        )

    if should_propose:
        if args.dry_run:
            logger.info("DRY-RUN: Would have sent Telegram proposal and written Redis bundle.")
            logger.info(f"DRY-RUN stats: {json.dumps(stats, ensure_ascii=False, indent=2)}")
        else:
            bundle_id = create_and_send_proposal(r, stats, args.hours)
            logger.info(f"Done. bundle_id={bundle_id}")
    elif args.dry_run:
        logger.info(f"DRY-RUN: thresholds not met. Stats: {json.dumps(stats, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
