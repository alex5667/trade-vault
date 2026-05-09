import os
from typing import Any


class TickDQPolicy:
    """
    Centralized Data Quality policy for ticks and klines.
    Mirrors Liquidation DQ Policy (go-worker/internal/liquidation/dq.go).
    - Detects bad timestamps (bad_ts_unit/bad_ts)
    - Detects stale/future skew
    - Detects out-of-order events
    - Does NOT silently sanitize malformed data.

    P0 contract after timestamp-resolution refactor (2026-04):
    - payload_ts_ms: raw original payload ts (set by tick_processor BEFORE resolve)
      → used for bad_ts / bad_ts_unit unit-checks only
    - ts_ms:         resolved event_ts_ms (may come from stream_id or now)
      → used for stale / future_skew / out_of_order checks

    If payload_ts_ms is absent (legacy callers: klines, direct tests), DQ falls
    back to ts_ms for all checks (backward-compatible).
    """

    def __init__(
        self,
        max_event_age_ms: int = 10_000,
        max_future_skew_ms: int = 2_000,
        max_out_of_order_ms: int = 2_000,
        latency_lenient_mode: bool = False,
    ):
        # When True, we increase staleness budget (e.g. for klines)
        if latency_lenient_mode:
            self.max_event_age_ms = int(os.getenv("KLINE_DQ_MAX_AGE_MS", "65000"))
        else:
            self.max_event_age_ms = int(os.getenv("TICK_DQ_MAX_AGE_MS", str(max_event_age_ms)))

        self.max_future_skew_ms = int(os.getenv("TICK_DQ_MAX_SKEW_MS", str(max_future_skew_ms)))
        self.max_out_of_order_ms = int(os.getenv("TICK_DQ_MAX_OOO_MS", str(max_out_of_order_ms)))

        self.last_ts_ms: dict[str, int] = {}

    def validate(self, payload: dict[str, Any], current_ts_ms: int) -> tuple[bool, str]:
        """
        Validate tick or kline payload.
        Returns: (is_valid, reason)

        Two-layer timestamp check:
        1. payload_ts_ms (raw, before resolution) → bad_ts / bad_ts_unit
        2. ts_ms (resolved event_ts_ms)           → stale / future_skew / out_of_order

        Reason codes (stable, used in metrics + tests):
          missing_symbol, bad_ts, bad_ts_unit, stale, future_skew, out_of_order
        """
        symbol = str(payload.get("symbol") or payload.get("s") or "").upper()
        if not symbol:
            return False, "missing_symbol"

        # ── Layer 1: raw payload unit check (bad_ts / bad_ts_unit) ───────────
        # Use payload_ts_ms if present (set by tick_processor after P0 refactor),
        # otherwise fall back to ts_ms for backward compatibility with kline/other paths.
        raw_ts_val = payload.get("payload_ts_ms")
        if raw_ts_val is None:
            # Legacy path: klines and direct callers that haven't been updated yet
            raw_ts_val = (
                payload.get("ts_ms")
                or payload.get("ts")
                or payload.get("event_time")
                or payload.get("E")
                or payload.get("time")
                or payload.get("written_at")
            )

        if raw_ts_val is None or raw_ts_val is False:
            return False, "bad_ts"

        try:
            raw_ts_ms = int(float(str(raw_ts_val).strip()))
        except (ValueError, TypeError):
            return False, "bad_ts"

        if raw_ts_ms <= 0:
            return False, "bad_ts"

        # Explicit unit check: seconds instead of ms → < 1e11 (~year 2001 boundary)
        if raw_ts_ms < 100_000_000_000:
            return False, "bad_ts_unit"

        # ── Layer 2: resolved ts age / skew / OOO checks ─────────────────────
        # Use ts_ms (resolved event_ts_ms after coerce_event_ts_ms).
        # If not present, fall back to raw_ts_ms (kline/legacy paths).
        resolved_ts_val = payload.get("ts_ms") or raw_ts_val
        try:
            ts_ms = int(float(str(resolved_ts_val).strip()))
        except (ValueError, TypeError):
            ts_ms = raw_ts_ms

        # Age check (stale)
        if self.max_event_age_ms > 0:
            if current_ts_ms - ts_ms > self.max_event_age_ms:
                return False, "stale"

        # Future skew check
        if self.max_future_skew_ms > 0:
            if ts_ms - current_ts_ms > self.max_future_skew_ms:
                return False, "future_skew"

        # Out-of-order check
        if self.max_out_of_order_ms > 0:
            last = self.last_ts_ms.get(symbol, 0)
            if ts_ms >= last:
                self.last_ts_ms[symbol] = ts_ms
                return True, "pass"

            # Allow small out-of-order window
            if last - ts_ms <= self.max_out_of_order_ms:
                return True, "pass"

            return False, "out_of_order"

        return True, "pass"
