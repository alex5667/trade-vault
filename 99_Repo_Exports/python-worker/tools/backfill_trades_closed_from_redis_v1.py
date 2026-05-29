"""Backfill trades_closed from Redis stream trades:closed.

Recovers rows that failed analytics_db.save_trade_closed due to schema
drift (column "ab_arm" was missing 2026-05-28 between 15:09-18:15 UTC).

Idempotent: partial UNIQUE INDEX idx_trades_closed_sid_final_uniq blocks
duplicates at the DB layer; ON CONFLICT (order_id) clause merges if
order_id already exists. Safe to re-run.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from types import SimpleNamespace

import redis

sys.path.insert(0, "/app/python-worker")
sys.path.insert(0, "/app")

from services import analytics_db  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_trades_closed")


def _to_bool(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _f(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def _i(v, default: int = 0) -> int:
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return default


def _entry_to_namespace(fields: dict) -> SimpleNamespace:
    sp: dict = {}
    sp_raw = fields.get("signal_payload")
    if sp_raw:
        try:
            sp = json.loads(sp_raw)
        except Exception:
            sp = {}

    features: dict = {}
    f_raw = fields.get("features")
    if f_raw:
        try:
            features = json.loads(f_raw)
        except Exception:
            features = {}

    # Reconstruct canonical DB sid: {entry_reason}:{symbol}:{fill_ts_ms}:{direction[0]}
    # Stream's `sid` field uses `crypto-of:` prefix; DB uses kind-specific prefix.
    # The partial UNIQUE INDEX is keyed on this canonical sid, so reconstructing
    # makes the backfill idempotent against rows written via other code paths.
    entry_reason = (fields.get("entry_reason") or "").strip()
    symbol = (fields.get("symbol") or "").strip()
    fill_ts_ms = fields.get("fill_ts_ms") or fields.get("signal_ts_ms") or fields.get("entry_ts_ms")
    direction = (fields.get("direction") or fields.get("side") or "").strip().upper()
    canonical_sid = ""
    if entry_reason and symbol and fill_ts_ms and direction:
        canonical_sid = f"{entry_reason}:{symbol}:{_i(fill_ts_ms)}:{direction[:1]}"
    sid_final = canonical_sid or fields.get("sid") or fields.get("signal_id") or ""

    return SimpleNamespace(
        order_id=fields.get("order_id") or fields.get("trade_id") or "",
        sid=sid_final,
        strategy=fields.get("strategy") or "orderflow",
        source=fields.get("source") or "CryptoOrderFlow",
        symbol=fields.get("symbol") or "",
        tf=fields.get("tf") or "tick",
        direction=fields.get("direction") or fields.get("side") or "",
        entry_ts_ms=_i(fields.get("entry_ts_ms")),
        exit_ts_ms=_i(fields.get("exit_ts_ms")),
        entry_price=_f(fields.get("entry_price") or fields.get("entry_px")),
        exit_price=_f(fields.get("exit_price") or fields.get("exit_px")),
        lot=_f(fields.get("lot") or fields.get("qty")),
        notional_usd=_f(fields.get("notional_usd")),
        pnl_net=_f(fields.get("pnl_net")),
        pnl_gross=_f(fields.get("pnl_gross")),
        fees=_f(fields.get("fees") or fields.get("fees_usd")),
        pnl_pct=_f(fields.get("pnl_pct")),
        pnl_if_fixed_exit=_f(fields.get("pnl_if_fixed_exit")),
        tp1_hit=_to_bool(fields.get("tp1_hit", "0")),
        tp2_hit=_to_bool(fields.get("tp2_hit", "0")),
        tp3_hit=_to_bool(fields.get("tp3_hit", "0")),
        tp_hits=_i(fields.get("tp_hits")),
        tp_before_sl=_i(fields.get("tp_before_sl")),
        trailing_started=_to_bool(fields.get("trailing_started", "0")),
        trailing_active=_to_bool(fields.get("trailing_active", "0")),
        trailing_moves=_i(fields.get("trailing_moves")),
        mfe_pnl=_f(fields.get("mfe_pnl")),
        mae_pnl=_f(fields.get("mae_pnl")),
        giveback=_f(fields.get("giveback")),
        missed_profit=_f(fields.get("missed_profit")),
        one_r_money=_f(fields.get("one_r_money"), default=1.0),
        r_multiple=_f(fields.get("r_multiple") or fields.get("r_mult")),
        duration_ms=_i(fields.get("duration_ms") or fields.get("hold_ms")),
        close_reason=fields.get("close_reason") or "",
        close_reason_raw=fields.get("close_reason") or "",
        close_reason_detail=fields.get("close_reason_detail") or "",
        is_final_close=True,
        signal_payload=sp,
        features=features,
        mae_bps=_f(fields.get("mae_bps")),
        mfe_bps=_f(fields.get("mfe_bps")),
        time_to_mfe_ms=_i(fields.get("time_to_mfe_ms")),
        hold_ms=_i(fields.get("hold_ms")),
        spread_bps_at_entry=_f(fields.get("spread_bps_at_entry")),
        slippage_bps_est=_f(fields.get("slippage_bps_est")),
        book_age_ms=_i(fields.get("book_age_ms")),
        scenario=fields.get("scenario") or "na",
        regime=fields.get("regime") or "na",
        session=fields.get("session") or "",
        entry_reason=fields.get("entry_reason") or "",
        is_virtual=_to_bool(fields.get("is_virtual", "0")),
        max_favorable_price=_f(fields.get("max_favorable_price")),
        max_favorable_ts=_i(fields.get("max_favorable_ts")),
        entry_tag=fields.get("entry_tag") or "",
        v_gate_reason=fields.get("v_gate_reason") or "",
        baseline_exit_reason=fields.get("baseline_exit_reason") or "",
        baseline_exit_ts_ms=_i(fields.get("baseline_exit_ts_ms")),
        baseline_exit_price=_f(fields.get("baseline_exit_price")),
        atr=_f(fields.get("atr")),
        sl=_f(fields.get("sl_price")),
        tp1_price=_f(fields.get("tp1_price")),
    )


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL_WORKER_1") or "redis://redis-worker-1:6379/0",
    )
    p.add_argument("--stream", default="trades:closed")
    p.add_argument("--min-ms", type=int, required=True, help="Start stream ID (epoch_ms)")
    p.add_argument("--max-ms", type=int, required=True, help="End stream ID (epoch_ms)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    r = redis.from_url(args.redis_url, decode_responses=True)
    log.info(
        "reading %s from %d to %d (dry_run=%s)",
        args.stream, args.min_ms, args.max_ms, args.dry_run,
    )

    entries = r.xrange(args.stream, min=f"{args.min_ms}-0", max=f"{args.max_ms}-0")
    log.info("found %d stream entries in window", len(entries))

    written = 0
    parse_err = 0
    write_err = 0
    skipped_missing_ids = 0

    for stream_id, fields in entries:
        try:
            closed = _entry_to_namespace(fields)
        except Exception as e:
            parse_err += 1
            log.warning("parse error on %s: %s: %s", stream_id, type(e).__name__, e)
            continue

        if not closed.order_id or not closed.sid:
            skipped_missing_ids += 1
            continue

        if args.dry_run:
            log.info(
                "would write stream_id=%s order_id=%s sid=%s symbol=%s direction=%s pnl_net=%.4f",
                stream_id, closed.order_id, closed.sid, closed.symbol,
                closed.direction, closed.pnl_net,
            )
            continue

        try:
            analytics_db.save_trade_closed(closed)
            written += 1
        except Exception as e:
            write_err += 1
            log.warning(
                "write error on stream_id=%s order_id=%s sid=%s: %s: %s",
                stream_id, closed.order_id, closed.sid, type(e).__name__, e,
            )

    log.info(
        "DONE: written=%d parse_err=%d write_err=%d skipped_missing_ids=%d",
        written, parse_err, write_err, skipped_missing_ids,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
