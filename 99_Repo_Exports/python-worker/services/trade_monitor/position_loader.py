# services/trade_monitor/position_loader.py
"""
Position recovery from Redis on service startup.

Extracted from TradeMonitorService._recover_open_positions,
_position_from_hash, _to_int_ms, _warmup_price_cache
(monolith lines 362-489, 1163-1235, 1663-1783, 1684-1703).

The standalone `parse_open_position_hash` function (lines 362-489) is also
re-exported here for unit testing without constructing the full service.

Design:
  - `PositionLoader` is a one-shot startup helper — call recover() once then
    discard.  The loaded positions are returned as plain dicts for the caller to
    insert into its own PositionStateStore.
  - All methods are fail-open; they never raise.
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
from typing import Any

from domain.models import PositionState
from infra.order_schema import (
    extract_profile,
    extract_tp_fills,
    extract_tp_levels,
    normalize_side,
    parse_json_dict,
)
from services.horizon_contract import (
    apply_position_horizon_scalars_from_hash,
    hydrate_position_from_signal_payload,
)
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Standalone pure parser (public — re-exported via __init__.py)
# ---------------------------------------------------------------------------

def _normalize_side_local(v: Any) -> str:
    """Normalize direction to 'LONG' / 'SHORT'."""
    if v is None:
        return "LONG"
    s = str(v).strip()
    sl = s.lower()
    if sl in ("long", "buy"):
        return "LONG"
    if sl in ("short", "sell"):
        return "SHORT"
    su = s.upper()
    return su if su in ("LONG", "SHORT") else "LONG"


def to_int_ms(v: Any, default: int = 0) -> int:
    """
    Safe conversion of an epoch-ms timestamp to int.

    CRITICAL: never pass 13-digit ms timestamps through float() — precision loss.
    """
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return default
        if isinstance(v, int):
            return v
        s = str(v).strip()
        if not s:
            return default
        if "." in s:
            s = s.split(".", 1)[0]
        return int(s)
    except Exception:
        return default


def parse_open_position_hash(
    h: dict[str, str],
    *,
    to_int_ms_fn=None,
    log: logging.Logger | None = None,
) -> PositionState | None:
    """
    Pure parser for crash/restart recovery.

    Extracted from TradeMonitorService._position_from_hash() to be
    unit-testable without constructing the full service.

    Args:
        h            — raw Redis hash (decode_responses=True, all str).
        to_int_ms_fn — optional callable(v, default) for timestamp coercion;
                       defaults to the module-level ``to_int_ms``.
        log          — optional logger.
    """
    _ms = to_int_ms_fn or to_int_ms
    _log = log or logger
    try:
        if h.get("status") != "open":
            return None

        # --- TP levels: prefer JSON array, fallback to legacy tp1/tp2/tp3 ---
        tp_levels = []
        if h.get("tp_levels"):
            with contextlib.suppress(Exception):
                tp_levels = json.loads(h["tp_levels"])
        if not tp_levels:
            tp_levels = [
                float(h.get("tp1") or 0),
                float(h.get("tp2") or 0),
                float(h.get("tp3") or 0),  # type: ignore
            ]
        tp_levels = [float(x) for x in tp_levels if float(x) > 0][:3]

        pos = PositionState(  # type: ignore
            id=(h.get("id")),
            sid=(h.get("sid") or ""),
            strategy=(h.get("strategy") or "unknown"),
            source=(h.get("source") or "Unknown"),
            symbol=(h.get("symbol") or "UNKNOWN"),
            tf=(h.get("tf") or "tick"),
            direction=_normalize_side_local(h.get("direction") or "LONG"),
            entry_price=float(h.get("entry_price") or 0.0),
            entry_ts_ms=_ms(h.get("entry_ts_ms") or h.get("entry_time"), 0),
            lot=float(h.get("lot") or 0.0),
            remaining_qty=float(h.get("remaining_qty") or h.get("lot") or 0.0),
            sl=float(h.get("sl") or 0.0),
            tp_levels=tp_levels,
            tp_hits=int(float(h.get("tp_hits") or 0)),
            tp1_hit=(h.get("tp1_hit") or "0") == "1",
            tp2_hit=(h.get("tp2_hit") or "0") == "1",
            tp3_hit=(h.get("tp3_hit") or "0") == "1",
            trailing_started=(h.get("trailing_started") or "0") == "1",
            trailing_active=(h.get("trailing_active") or "0") == "1",
            trailing_moves_count=int(float(h.get("trailing_moves") or 0)),
            trailing_distance=float(h.get("trailing_distance") or 0.0),
            trailing_point=float(h.get("trailing_point") or 0.0),
            max_favorable_price=float(h.get("max_favorable_price") or 0.0),
            max_favorable_ts=_ms(h.get("max_favorable_ts"), 0),
            atr=float(h.get("atr") or 0.0),
            is_virtual=(h.get("is_virtual") or "0") == "1",
            v_gate_status=(h.get("v_gate_status") or "na"),
            v_gate_reason=(h.get("v_gate_reason") or ""),
        )

        # Optional fields (best-effort)
        with contextlib.suppress(Exception):
            from domain.evidence_keys import MetaKeys

            pos.entry_tag = h.get("entry_tag") or ""
            pos.p0_signal_id = h.get("p0_signal_id") or h.get("sid")
            pos.p0_regime = h.get("p0_regime")
            pos.p0_scenario = h.get("p0_scenario")
            pos.p0_session = h.get("p0_session")
            pos.p0_entry_reason = h.get("p0_entry_reason") or pos.entry_tag

            if h.get("p0_spread_bps"):
                pos.p0_spread_bps_at_entry = float(h["p0_spread_bps"])
            if h.get("p0_book_age_ms"):
                pos.p0_book_age_ms = int(h["p0_book_age_ms"])
            if h.get("p0_features_json"):
                with contextlib.suppress(Exception):
                    pos.p0_features_snapshot = json.loads(h["p0_features_json"])

            if not pos.max_favorable_price:
                pos.max_favorable_price = pos.entry_price
            if not getattr(pos, "max_adverse_price", None):
                pos.max_adverse_price = pos.entry_price
            if getattr(pos, "max_favorable_ts_ms", 0) == 0:
                pos.max_favorable_ts_ms = pos.entry_ts_ms
            if getattr(pos, "max_adverse_ts_ms", 0) == 0:
                pos.max_adverse_ts_ms = pos.entry_ts_ms

            pos.trail_profile = h.get("trail_profile") or h.get("trailing_profile") or ""
            pos.trailing_min_lock_r = float(h.get("trailing_min_lock_r") or 0.0)
            pos.min_lock_price = float(h.get("min_lock_price") or 0.0)
            pos.baseline_mode = h.get("baseline_mode") or pos.baseline_mode
            pos.baseline_horizon_ms = _ms(h.get("baseline_horizon_ms"), pos.baseline_horizon_ms)
            pos.baseline_sl = float(h.get("baseline_sl") or pos.baseline_sl or pos.sl)
            pos.baseline_tp1 = float(
                h.get("baseline_tp1")
                or pos.baseline_tp1
                or (pos.tp_levels[0] if pos.tp_levels else 0.0)
            )
            pos.baseline_tp2 = float(
                h.get("baseline_tp2")
                or pos.baseline_tp2
                or (pos.tp_levels[1] if len(pos.tp_levels) > 1 else 0.0)
            )
            pos.baseline_tp3 = float(
                h.get("baseline_tp3")
                or pos.baseline_tp3
                or (pos.tp_levels[2] if len(pos.tp_levels) > 2 else 0.0)
            )

            pos.meta_enforce_cov_bucket = h.get(MetaKeys.ENFORCE_COV_BUCKET) or ""
            if h.get(MetaKeys.ENFORCE_APPLIED):
                with contextlib.suppress(ValueError, TypeError):
                    pos.meta_enforce_applied = int(float(h[MetaKeys.ENFORCE_APPLIED]))

        # Phase 0.3: scalar-first recovery, then nested hydration
        with contextlib.suppress(Exception):
            apply_position_horizon_scalars_from_hash(pos, h, source="pure_hash_recovery")
        with contextlib.suppress(Exception):
            if h.get("signal_payload"):
                pos.signal_payload.update(parse_json_dict(h.get("signal_payload")))
            hydrate_position_from_signal_payload(pos, source="pure_hash_recovery")

        return pos
    except Exception as e:
        if _log:
            _log.warning("Failed to recover position from hash: %s", e)
        return None


# ---------------------------------------------------------------------------
# Service-level loader (requires repo + lock access via callbacks)
# ---------------------------------------------------------------------------

class PositionLoader:
    """
    One-shot startup helper: load open positions from Redis and warm up the
    price cache.

    Receives callbacks instead of direct references so it doesn't need to know
    about the TradeMonitorService internals.

    Args:
        redis       — Redis client (decode_responses=True).
        repo        — RedisTradeRepository instance.
        add_pos_fn  — callable(pos: PositionState) to register position in store.
        recover_fsm_fn — callable(pos) to re-attach FSM after recovery.
        get_open_symbols_fn — callable() -> set[str] for warmup.
        set_price_fn — callable(symbol, ts_ms, price) to seed price cache.
    """

    def __init__(
        self,
        redis: Any,
        repo: Any,
        *,
        add_pos_fn: Any,
        recover_fsm_fn: Any | None = None,
        get_open_symbols_fn: Any | None = None,
        set_price_fn: Any | None = None,
        log: logging.Logger | None = None,
    ) -> None:
        self._redis = redis
        self._repo = repo
        self._add_pos = add_pos_fn
        self._recover_fsm = recover_fsm_fn
        self._get_open_symbols = get_open_symbols_fn
        self._set_price = set_price_fn
        self._logger = log or logger

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    def recover_open_positions(self) -> int:
        """
        Load all open positions from Redis and register them via add_pos_fn.

        Returns number of positions recovered.
        """
        count = 0
        try:
            rows = self._repo.load_open_positions(limit=5000)
            for h in rows:
                oid = h.get("id") or ""
                if not oid:
                    continue
                pos = parse_open_position_hash(h, log=self._logger)
                if not pos:
                    continue
                self._add_pos(pos)
                if self._recover_fsm is not None:
                    with contextlib.suppress(Exception):
                        self._recover_fsm(pos)
                count += 1
            self._logger.info("♻️ recovered open positions: %d", count)
        except Exception as e:
            self._logger.warning("⚠️ recovery failed: %s", e)
        return count

    # ------------------------------------------------------------------
    # Warmup
    # ------------------------------------------------------------------

    def warmup_price_cache(self) -> None:
        """
        Seed _last_price_by_symbol from redis-ticks after restart.

        ENV:
          REDIS_TICKS_URL            — Redis ticks shard URL.
          TM_WARMUP_MAX_PRICE_AGE_MS — max tick age to accept (default: 600 000 ms).
        """
        if self._get_open_symbols is None or self._set_price is None:
            return

        ticks_url = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")
        max_age_ms = int(os.getenv("TM_WARMUP_MAX_PRICE_AGE_MS", "600000"))
        now_ms = get_ny_time_millis()

        try:
            import redis as redis_lib  # lazy import

            r_ticks = redis_lib.from_url(
                ticks_url,
                decode_responses=True,
                socket_timeout=2.0,
                socket_connect_timeout=2.0,
            )
        except Exception as e:
            self._logger.warning(
                "⚠️ [warmup] cannot connect to redis-ticks (%s): %s", ticks_url, e
            )
            return

        symbols = self._get_open_symbols()
        warmed = skipped_old = skipped_err = 0

        for sym in symbols:
            if not sym:
                continue
            try:
                stream_key = f"stream:tick_{sym}"
                entries = r_ticks.xrevrange(stream_key, "+", "-", count=1)
                if not entries:
                    skipped_err += 1
                    continue
                entry_id, fields = entries[0]
                try:
                    ts_ms = int(str(entry_id).split("-")[0])
                except Exception:
                    ts_ms = now_ms

                age_ms = now_ms - ts_ms
                if age_ms > max_age_ms:
                    self._logger.debug(
                        "[warmup] %s: price too old (%d ms), skipping", sym, age_ms
                    )
                    skipped_old += 1
                    continue

                price = float(
                    fields.get("mid")
                    or fields.get("price")
                    or fields.get("last")
                    or 0.0
                )
                if price > 0:
                    self._set_price(sym, ts_ms, price)
                    warmed += 1
                else:
                    skipped_err += 1
            except Exception as e:
                self._logger.debug("[warmup] %s: error reading tick: %s", sym, e)
                skipped_err += 1

        with contextlib.suppress(Exception):
            r_ticks.close()

        self._logger.info(
            "🔥 [warmup] price cache: warmed=%d skipped_old=%d skipped_err=%d / total_symbols=%d",
            warmed,
            skipped_old,
            skipped_err,
            len(symbols),
        )
