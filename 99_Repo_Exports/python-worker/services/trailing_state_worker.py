"""Phase B: TrailingStateWorker — FSM-based trailing-SL state machine.

Назначение:
  Читает events:trades (XREADGROUP), обрабатывает TP_HIT / SL_HIT /
  POSITION_CLOSED событий, поддерживает watermark-трейлинг.
  На каждом тике обновляет high/low watermark и вычисляет кандидата на
  новый SL. При TRAILING_STATE_SHADOW=0 публикует команды в
  events:trailing:commands.

State enum: NONE → TP_HIT_RECEIVED → TRAILING_ARMED → TRAILING_ACTIVE
            → EXIT_PENDING → EXITED | ERROR

Redis keys:
  trailing:state:{sid}              — HSET (hash, TTL TRAILING_STATE_TTL_SEC)
  trail:cmd:{sid}:{position_id}:{rounded_sl}  — SETNX idempotency lock
  events:trailing:commands          — XADD command stream
  events:trailing:state             — XADD audit stream

ENV:
  TRAILING_STATE_ENABLED=0          — master switch (default OFF)
  TRAILING_STATE_SHADOW=1           — 1=shadow compute only, 0=real commands
  TRAILING_STATE_TTL_SEC=86400
  TRAILING_MIN_MOVE_TICKS=5
  TRAILING_MIN_UPDATE_INTERVAL_MS=3000
  TRAILING_MAX_UPDATES_PER_POSITION=30
  TRAILING_PRICE_STALE_MS=3000
"""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

import redis

from common.log import setup_logger
from core.redis_keys import RedisStreams as RS, RK

log = setup_logger("trailing_state_worker")

# ── Prometheus (fail-open) ────────────────────────────────────────────────────
try:
    from prometheus_client import Counter, Gauge

    trailing_state_created_total = Counter(
        "trailing_state_created_total",
        "TrailingState objects created",
        ["profile", "symbol"],
    )
    trailing_state_transition_total = Counter(
        "trailing_state_transition_total",
        "State transitions",
        ["from_state", "to_state", "reason_code"],
    )
    trailing_sl_move_total = Counter(
        "trailing_sl_move_total",
        "SL moves computed",
        ["symbol", "side", "profile"],
    )
    trailing_sl_move_skipped_total = Counter(
        "trailing_sl_move_skipped_total",
        "SL moves skipped",
        ["reason_code"],
    )
    trailing_command_sent_total = Counter(
        "trailing_command_sent_total",
        "Commands sent to events:trailing:commands",
        ["symbol", "profile"],
    )
    trailing_command_duplicate_total = Counter(
        "trailing_command_duplicate_total",
        "Duplicate commands blocked by SETNX",
        ["symbol"],
    )
    trailing_state_active_gauge = Gauge(
        "trailing_state_active_gauge",
        "Active TRAILING_ACTIVE states per symbol",
        ["symbol"],
    )
    # ── Section 6 (audit add) ──────────────────────────────────────────────
    from prometheus_client import Histogram as _Histogram
    trailing_command_failed_total = Counter(
        "trailing_command_failed_total",
        "Command emissions that failed (XADD or downstream)",
        ["reason_code"],
    )
    trailing_price_age_ms = _Histogram(
        "trailing_price_age_ms",
        "Age of tick price at on_tick entry (ms)",
        ["symbol"],
        buckets=[10, 50, 100, 500, 1000, 3000, 10000],
    )
    trailing_update_latency_ms = _Histogram(
        "trailing_update_latency_ms",
        "on_tick processing latency per state (ms)",
        ["symbol"],
        buckets=[1, 5, 10, 50, 100, 500],
    )
    trailing_state_load_failed_total = Counter(
        "trailing_state_load_failed_total",
        "Failures loading state from Redis",
        ["reason"],
    )
    trailing_state_save_failed_total = Counter(
        "trailing_state_save_failed_total",
        "Failures saving state to Redis",
        ["reason"],
    )
    trailing_tick_loop_errors_total = Counter(
        "trailing_tick_loop_errors_total",
        "Errors in the tick reader loop",
    )
    # ── Audit §6 add ──────────────────────────────────────────────────────
    trailing_state_index_rebuild_total = Counter(
        "trailing_state_index_rebuild_total",
        "Times _symbol_index was rebuilt from Redis",
    )
    trailing_state_index_rebuild_active_count = Gauge(
        "trailing_state_index_rebuild_active_count",
        "Active sids re-indexed on last rebuild",
    )
    trailing_command_stream_lag = Gauge(
        "trailing_command_stream_lag",
        "Pending messages in events:trailing:commands consumer group",
    )
    _HAS_PROM = True
except Exception:  # pragma: no cover
    _HAS_PROM = False
    trailing_command_failed_total = None  # type: ignore[assignment]
    trailing_price_age_ms = None  # type: ignore[assignment]
    trailing_update_latency_ms = None  # type: ignore[assignment]
    trailing_state_load_failed_total = None  # type: ignore[assignment]
    trailing_state_save_failed_total = None  # type: ignore[assignment]
    trailing_tick_loop_errors_total = None  # type: ignore[assignment]
    trailing_state_index_rebuild_total = None  # type: ignore[assignment]
    trailing_state_index_rebuild_active_count = None  # type: ignore[assignment]
    trailing_command_stream_lag = None  # type: ignore[assignment]


def _inc(counter: Any, *args: str) -> None:
    """Prometheus increment, fail-open."""
    if not _HAS_PROM:
        return
    try:
        counter.labels(*args).inc()
    except Exception:
        pass


def _gauge_set(gauge: Any, value: float, *args: str) -> None:
    """Prometheus gauge set, fail-open."""
    if not _HAS_PROM:
        return
    try:
        gauge.labels(*args).set(value)
    except Exception:
        pass


# ── State enum ────────────────────────────────────────────────────────────────

class TrailingStateEnum(str, Enum):
    NONE = "none"
    TP_HIT_RECEIVED = "tp_hit_received"
    TRAILING_ARMED = "trailing_armed"
    TRAILING_ACTIVE = "trailing_active"
    EXIT_PENDING = "exit_pending"
    EXITED = "exited"
    ERROR = "error"


# ── Dataclass ─────────────────────────────────────────────────────────────────

@dataclass
class TrailingState:
    v: int = 1
    sid: str = ""
    position_id: str = ""
    symbol: str = ""
    side: str = ""              # "LONG" | "SHORT"
    state: str = TrailingStateEnum.NONE.value
    entry_price: float = 0.0
    current_sl: float = 0.0
    last_sent_sl: float = 0.0
    high_watermark: float | None = None
    low_watermark: float | None = None
    atr_value: float = 0.0
    atr_mult: float = 1.0
    trail_distance: float = 0.0
    tick_size: float = 0.01
    min_move_ticks: int = 5
    min_update_interval_ms: int = 3000
    activated_tp_level: int = 1
    profile: str = ""
    profile_hash: str = ""
    policy_hash: str = ""
    created_ts_ms: int = 0
    updated_ts_ms: int = 0
    last_cmd_ts_ms: int = 0
    updates_sent: int = 0
    max_updates: int = 30

    # ── Serialization ────────────────────────────────────────────────────────

    def to_dict(self) -> dict[str, str]:
        """Flat dict[str, str] for Redis HSET (None → empty string)."""
        d = asdict(self)
        result: dict[str, str] = {}
        for k, v in d.items():
            if v is None:
                result[k] = ""
            else:
                result[k] = str(v)
        return result

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "TrailingState":
        """Parse from Redis HGETALL (all values are strings)."""

        def _float_or_none(val: str) -> float | None:
            if val == "" or val is None:
                return None
            return float(val)

        def _float(val: str, default: float = 0.0) -> float:
            if val == "" or val is None:
                return default
            return float(val)

        def _int(val: str, default: int = 0) -> int:
            if val == "" or val is None:
                return default
            return int(val)

        return cls(
            v=_int(data.get("v", "1"), 1),
            sid=data.get("sid", ""),
            position_id=data.get("position_id", ""),
            symbol=data.get("symbol", ""),
            side=data.get("side", ""),
            state=data.get("state", TrailingStateEnum.NONE.value),
            entry_price=_float(data.get("entry_price", "0")),
            current_sl=_float(data.get("current_sl", "0")),
            last_sent_sl=_float(data.get("last_sent_sl", "0")),
            high_watermark=_float_or_none(data.get("high_watermark", "")),
            low_watermark=_float_or_none(data.get("low_watermark", "")),
            atr_value=_float(data.get("atr_value", "0")),
            atr_mult=_float(data.get("atr_mult", "1"), 1.0),
            trail_distance=_float(data.get("trail_distance", "0")),
            tick_size=_float(data.get("tick_size", "0.01"), 0.01),
            min_move_ticks=_int(data.get("min_move_ticks", "5"), 5),
            min_update_interval_ms=_int(data.get("min_update_interval_ms", "3000"), 3000),
            activated_tp_level=_int(data.get("activated_tp_level", "1"), 1),
            profile=data.get("profile", ""),
            profile_hash=data.get("profile_hash", ""),
            policy_hash=data.get("policy_hash", ""),
            created_ts_ms=_int(data.get("created_ts_ms", "0")),
            updated_ts_ms=_int(data.get("updated_ts_ms", "0")),
            last_cmd_ts_ms=_int(data.get("last_cmd_ts_ms", "0")),
            updates_sent=_int(data.get("updates_sent", "0")),
            max_updates=_int(data.get("max_updates", "30"), 30),
        )


# ── Pure computation helpers ──────────────────────────────────────────────────

def round_to_tick(price: float, tick_size: float, up: bool = False) -> float:
    """Round price to nearest tick.

    up=True rounds away from entry for SHORT SL (ceil).
    """
    if tick_size <= 0:
        return price
    factor = round(1.0 / tick_size)
    if up:
        return math.ceil(price * factor) / factor
    return math.floor(price * factor) / factor


def compute_new_sl(state: TrailingState, price: float) -> float | None:
    """Compute candidate new SL given current price.

    Returns None if no move warranted.

    LONG:  hwm rises → sl = hwm - atr * mult. Never lower than current_sl.
    SHORT: lwm falls → sl = lwm + atr * mult. Never higher than current_sl.
    Min move: abs(candidate - last_sent_sl) >= min_move_ticks * tick_size.
    Max updates: if updates_sent >= max_updates, return None.
    """
    if state.updates_sent >= state.max_updates:
        return None

    trail_dist = state.trail_distance if state.trail_distance > 0 else (state.atr_value * state.atr_mult)
    if trail_dist <= 0:
        return None

    min_move = state.min_move_ticks * state.tick_size
    reference_sl = state.last_sent_sl if state.last_sent_sl != 0.0 else state.current_sl

    if state.side == "LONG":
        # Update high watermark
        hwm = state.high_watermark if state.high_watermark is not None else price
        new_hwm = max(hwm, price)
        candidate = new_hwm - trail_dist
        candidate = round_to_tick(candidate, state.tick_size, up=False)

        # Never retreat
        if candidate <= state.current_sl:
            return None

        # Min move filter
        if abs(candidate - reference_sl) < min_move:
            return None

        return candidate

    elif state.side == "SHORT":
        # Update low watermark
        lwm = state.low_watermark if state.low_watermark is not None else price
        new_lwm = min(lwm, price)
        candidate = new_lwm + trail_dist
        candidate = round_to_tick(candidate, state.tick_size, up=True)

        # Never retreat (for SHORT, SL never rises)
        if candidate >= state.current_sl:
            return None

        # Min move filter (distance, always positive)
        if abs(candidate - reference_sl) < min_move:
            return None

        return candidate

    return None


# ── Worker ────────────────────────────────────────────────────────────────────

_STATE_KEY_PREFIX = "trailing:state:"
_CMD_KEY_PREFIX = "trail:cmd:"
_EVENTS_TRAILING_COMMANDS = "events:trailing:commands"
_EVENTS_TRAILING_STATE = "events:trailing:state"
_CMD_DEDUP_TTL_SEC = 300  # idempotency key TTL


class TrailingStateWorker:
    """Processes trade events and tick prices to maintain trailing-SL FSM.

    I/O:
      Input:  events:trades       (XREADGROUP, handled externally or via run())
              stream:tick_{SYMBOL} (polled via on_tick calls)
      Output: events:trailing:commands  (XADD, only when SHADOW=0)
              events:trailing:state     (XADD, audit, always)
              trailing:state:{sid}      (HSET, state persistence)
    """

    _AUTOCAL_REFRESH_S: float = 60.0   # how often to re-read autocal state key

    def __init__(self, redis_client: redis.Redis | None = None) -> None:
        self.enabled = os.getenv("TRAILING_STATE_ENABLED", "0") == "1"
        self.shadow = os.getenv("TRAILING_STATE_SHADOW", "1") == "1"
        self.ttl_sec = int(os.getenv("TRAILING_STATE_TTL_SEC", "86400"))
        self.min_move_ticks = int(os.getenv("TRAILING_MIN_MOVE_TICKS", "5"))
        self.min_update_interval_ms = int(os.getenv("TRAILING_MIN_UPDATE_INTERVAL_MS", "3000"))
        self.max_updates = int(os.getenv("TRAILING_MAX_UPDATES_PER_POSITION", "30"))
        self.price_stale_ms = int(os.getenv("TRAILING_PRICE_STALE_MS", "3000"))

        if redis_client is not None:
            self.r = redis_client
        else:
            redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
            self.r = redis.from_url(redis_url, decode_responses=True)

        # symbol → list of active sids (in-memory index for fast tick routing)
        self._symbol_index: dict[str, set[str]] = {}

        # Audit §8: rebuild in-memory _symbol_index by scanning trailing:state:*
        # so a worker restart doesn't lose tick routing for live positions.
        self._rebuild_index_from_redis()

        # Autocal reader state
        self._autocal_last_refresh: float = 0.0
        self._refresh_autocal_shadow()  # initial read

    # ── State persistence ────────────────────────────────────────────────────

    _SIGNAL_KEY_PREFIXES = (
        "signals:",
        "signals:audit:",
        "signals:crypto:",
        "signal:",
        "signal:snap:",
    )

    def _lookup_signal(self, sid: str) -> dict[str, Any] | None:
        """Try to read signal hash from Redis using known key prefixes."""
        for prefix in self._SIGNAL_KEY_PREFIXES:
            try:
                raw = self.r.get(f"{prefix}{sid}")
                if raw:
                    return json.loads(raw if isinstance(raw, str) else raw.decode())  # type: ignore[union-attr]
            except Exception:
                pass
        return None

    # ── Autocal reader ───────────────────────────────────────────────────────

    def _refresh_autocal_shadow(self) -> None:
        """Read autocal:trailing_state:state and update self.shadow if promoted."""
        try:
            raw = self.r.get(RK.AUTOCAL_TRAILING_STATE)
            if not raw:
                return
            snap = json.loads(raw if isinstance(raw, str) else raw.decode())  # type: ignore[union-attr]
            promoted_shadow = snap.get("shadow", True)
            if self.shadow and not promoted_shadow:
                log.info(
                    "TrailingStateWorker: autocal promoted shadow=false — switching to LIVE"
                )
            elif not self.shadow and promoted_shadow:
                log.info(
                    "TrailingStateWorker: autocal shadow=true — reverting to shadow"
                )
            self.shadow = bool(promoted_shadow)
        except Exception as exc:
            log.debug("_refresh_autocal_shadow: %s", exc)
        finally:
            self._autocal_last_refresh = time.time()

    def _maybe_refresh_autocal(self) -> None:
        if time.time() - self._autocal_last_refresh >= self._AUTOCAL_REFRESH_S:
            self._refresh_autocal_shadow()

    # ── State persistence ────────────────────────────────────────────────────

    @staticmethod
    def _state_key(sid: str) -> str:
        return f"{_STATE_KEY_PREFIX}{sid}"

    def _save_state(self, state: TrailingState) -> None:
        try:
            key = self._state_key(state.sid)
            pipe = self.r.pipeline()
            pipe.hset(key, mapping=state.to_dict())  # type: ignore[arg-type]
            pipe.expire(key, self.ttl_sec)
            pipe.execute()
        except Exception as exc:
            log.warning("_save_state failed sid=%s: %s", state.sid, exc)
            _inc(trailing_state_save_failed_total, type(exc).__name__)

    def _load_state(self, sid: str) -> TrailingState | None:
        try:
            data = self.r.hgetall(self._state_key(sid))
        except Exception as exc:
            log.warning("_load_state failed sid=%s: %s", sid, exc)
            _inc(trailing_state_load_failed_total, type(exc).__name__)
            return None
        if not data:
            return None
        try:
            return TrailingState.from_dict(data)  # type: ignore[arg-type]
        except Exception as exc:
            log.warning("_load_state parse error sid=%s: %s", sid, exc)
            _inc(trailing_state_load_failed_total, "parse_error")
            return None

    def _delete_state(self, sid: str) -> None:
        try:
            self.r.delete(self._state_key(sid))
        except Exception as exc:
            log.warning("_delete_state failed sid=%s: %s", sid, exc)

    # ── Symbol index helpers ─────────────────────────────────────────────────

    def _index_add(self, symbol: str, sid: str) -> None:
        self._symbol_index.setdefault(symbol, set()).add(sid)

    def _index_remove(self, symbol: str, sid: str) -> None:
        sids = self._symbol_index.get(symbol)
        if sids:
            sids.discard(sid)

    def _active_sids_for(self, symbol: str) -> list[str]:
        return list(self._symbol_index.get(symbol, set()))

    def _rebuild_index_from_redis(self) -> int:
        """Scan trailing:state:* and re-populate _symbol_index for active states.

        Called once at __init__ so a worker restart resumes tick routing for
        live positions instead of waiting for new TP events. Returns the
        number of active sids re-indexed.
        """
        n_active = 0
        n_total = 0
        try:
            cursor = 0
            while True:
                cursor, keys = self.r.scan(  # type: ignore[misc]
                    cursor=cursor,
                    match=f"{_STATE_KEY_PREFIX}*",
                    count=200,
                )
                for key in keys or []:
                    n_total += 1
                    try:
                        data = self.r.hgetall(key)
                        if not data:
                            continue
                        state_val = data.get("state", "")  # type: ignore[union-attr]
                        if state_val != TrailingStateEnum.TRAILING_ACTIVE.value:
                            continue
                        sid = data.get("sid", "")  # type: ignore[union-attr]
                        symbol = data.get("symbol", "")  # type: ignore[union-attr]
                        if sid and symbol:
                            self._index_add(symbol, sid)
                            n_active += 1
                    except Exception as inner_exc:
                        log.debug("rebuild: parse key=%s: %s", key, inner_exc)
                if cursor == 0:
                    break
        except Exception as exc:
            log.warning("_rebuild_index_from_redis failed: %s", exc)
        log.info(
            "TrailingStateWorker: index rebuild scanned=%d active=%d symbols=%d",
            n_total, n_active, len(self._symbol_index),
        )
        # Emit metrics
        if _HAS_PROM and trailing_state_index_rebuild_total is not None:
            try:
                trailing_state_index_rebuild_total.inc()
            except Exception:
                pass
        if _HAS_PROM and trailing_state_index_rebuild_active_count is not None:
            try:
                trailing_state_index_rebuild_active_count.set(float(n_active))
            except Exception:
                pass
        return n_active

    # ── Idempotency / command emit ───────────────────────────────────────────

    def _dedup_key(self, state: TrailingState, new_sl: float) -> str:
        rounded = round_to_tick(new_sl, state.tick_size)
        return f"{_CMD_KEY_PREFIX}{state.sid}:{state.position_id}:{rounded}"

    def _emit_command(self, state: TrailingState, new_sl: float, reason_code: str) -> bool:
        """XADD to events:trailing:commands with SETNX idempotency.

        Returns True if command was emitted, False if duplicate.
        """
        dedup_key = self._dedup_key(state, new_sl)
        try:
            acquired = self.r.set(dedup_key, "1", nx=True, ex=_CMD_DEDUP_TTL_SEC)
        except Exception as exc:
            log.warning("_emit_command: SETNX failed key=%s: %s", dedup_key, exc)
            acquired = True  # fail-open: allow command

        if not acquired:
            _inc(trailing_command_duplicate_total, state.symbol)
            log.debug("_emit_command: duplicate blocked key=%s", dedup_key)
            return False

        payload = {
            "sid": state.sid,
            "position_id": state.position_id,
            "symbol": state.symbol,
            "side": state.side,
            "new_sl": str(new_sl),
            "reason_code": reason_code,
            "profile": state.profile,
            "ts_ms": str(int(time.time() * 1000)),
            "shadow": "1" if self.shadow else "0",
        }
        try:
            self.r.xadd(
                _EVENTS_TRAILING_COMMANDS,
                payload,
                maxlen=10_000,
                approximate=True,
            )
            _inc(trailing_command_sent_total, state.symbol, state.profile)
            log.info(
                "trailing_cmd sid=%s symbol=%s new_sl=%.6f reason=%s shadow=%s",
                state.sid, state.symbol, new_sl, reason_code, self.shadow,
            )
        except Exception as exc:
            log.warning("_emit_command: XADD failed: %s", exc)
            _inc(trailing_command_failed_total, f"xadd:{type(exc).__name__}")

        return True

    def _emit_audit(
        self,
        state: TrailingState,
        from_state: str,
        event_type: str,
        reason_code: str,
        price: float | None = None,
        old_sl: float | None = None,
        new_sl: float | None = None,
    ) -> None:
        """XADD audit row to events:trailing:state."""
        payload: dict[str, str] = {
            "sid": state.sid,
            "position_id": state.position_id,
            "symbol": state.symbol,
            "side": state.side,
            "from_state": from_state,
            "to_state": state.state,
            "event_type": event_type,
            "reason_code": reason_code,
            "profile": state.profile,
            "ts_ms": str(int(time.time() * 1000)),
        }
        if price is not None:
            payload["price"] = str(price)
        if old_sl is not None:
            payload["old_sl"] = str(old_sl)
        if new_sl is not None:
            payload["new_sl"] = str(new_sl)
        if state.high_watermark is not None:
            payload["high_watermark"] = str(state.high_watermark)
        if state.low_watermark is not None:
            payload["low_watermark"] = str(state.low_watermark)
        payload["atr_value"] = str(state.atr_value)
        payload["atr_mult"] = str(state.atr_mult)

        try:
            self.r.xadd(
                _EVENTS_TRAILING_STATE,
                payload,
                maxlen=50_000,
                approximate=True,
            )
        except Exception as exc:
            log.warning("_emit_audit: XADD failed: %s", exc)

    # ── Transition helpers ───────────────────────────────────────────────────

    def _transition(
        self,
        state: TrailingState,
        to_state: TrailingStateEnum,
        reason_code: str,
        event_type: str,
        price: float | None = None,
        old_sl: float | None = None,
        new_sl: float | None = None,
    ) -> None:
        from_state = state.state
        state.state = to_state.value
        state.updated_ts_ms = int(time.time() * 1000)

        _inc(trailing_state_transition_total, from_state, state.state, reason_code)
        self._emit_audit(
            state,
            from_state=from_state,
            event_type=event_type,
            reason_code=reason_code,
            price=price,
            old_sl=old_sl,
            new_sl=new_sl,
        )

    # ── Public event handlers ────────────────────────────────────────────────

    def on_tp_hit(self, event: dict[str, Any]) -> TrailingState | None:
        """Handle TP_HIT / TP1_HIT / TP2_HIT event.

        Creates or updates TrailingState, transitions to TRAILING_ACTIVE.
        Returns the state if successful, None on error.
        """
        if not self.enabled:
            return None

        sid = event.get("sid") or event.get("signal_id", "")
        if not sid:
            log.warning("on_tp_hit: missing sid in event=%s", event)
            return None

        # Field normalization — TP_HIT stream events use lowercase `direction`
        # (long/short) instead of `side`. Some events also miss `symbol` if the
        # listener parsed a flat-only payload; derive from `sid` (format
        # "<kind>:SYMBOL:ts:L|S") as last-resort fallback.
        symbol = event.get("symbol") or ""
        side_raw = event.get("side") or event.get("direction") or ""
        side = side_raw.upper() if isinstance(side_raw, str) else ""
        if side in ("LONG", "BUY", "L"):
            side = "LONG"
        elif side in ("SHORT", "SELL", "S"):
            side = "SHORT"
        # sid-tail fallback for side (only if still missing)
        if not side and ":" in sid:
            tail = sid.rsplit(":", 1)[-1].upper()
            if tail == "L":
                side = "LONG"
            elif tail == "S":
                side = "SHORT"
        # symbol fallback from sid: kind:SYMBOL:ts:dir
        if not symbol and sid.count(":") >= 2:
            parts = sid.split(":")
            if len(parts) >= 2:
                symbol = parts[1]

        if not symbol or side not in ("LONG", "SHORT"):
            log.warning(
                "on_tp_hit: missing symbol/side sid=%s symbol=%r side=%r",
                sid, symbol, side,
            )
            return None

        # Load existing or create new
        existing = self._load_state(sid)
        if existing and existing.state == TrailingStateEnum.EXITED.value:
            log.debug("on_tp_hit: sid=%s already EXITED, ignoring", sid)
            return None

        now_ms = int(time.time() * 1000)
        tp_level = 1
        event_type = event.get("event_type", "TP_HIT")
        if "TP2" in event_type:
            tp_level = 2
        elif "TP3" in event_type:
            tp_level = 3

        # Build / update state
        state = existing or TrailingState(
            sid=sid,
            symbol=symbol,
            side=side,
            created_ts_ms=now_ms,
        )
        state.position_id = event.get("position_id", state.position_id)
        state.symbol = symbol
        state.side = side
        state.entry_price = float(event.get("entry_price", state.entry_price) or 0)
        state.current_sl = float(event.get("current_sl", state.current_sl) or 0)
        state.atr_value = float(event.get("atr_value", state.atr_value) or 0)
        state.atr_mult = float(event.get("atr_mult", state.atr_mult) or 1.0)

        # TP_HIT events rarely carry atr_value / original SL; look up the signal.
        if state.atr_value == 0 or state.current_sl == 0:
            sig = self._lookup_signal(sid)
            if sig:
                ind = sig.get("indicators") or {}
                if isinstance(ind, str):
                    try:
                        ind = json.loads(ind)
                    except Exception:
                        ind = {}
                if state.atr_value == 0:
                    atr_from_sig = float(
                        sig.get("atr") or sig.get("atr_value")
                        or ind.get("atr") or ind.get("atr_value") or 0
                    )
                    if atr_from_sig > 0:
                        state.atr_value = atr_from_sig
                        log.debug("on_tp_hit: atr_value=%.5f from signal lookup sid=%s", atr_from_sig, sid)
                # Seed initial SL from signal so first SL_MOVE has a meaningful old_sl.
                if state.current_sl == 0:
                    sl_from_sig = float(sig.get("sl") or sig.get("current_sl") or 0)
                    if sl_from_sig > 0:
                        state.current_sl = sl_from_sig
                        state.last_sent_sl = sl_from_sig

        state.profile = event.get("profile", state.profile) or ""
        state.profile_hash = event.get("profile_hash", state.profile_hash) or ""
        state.policy_hash = event.get("policy_hash", state.policy_hash) or ""
        state.min_move_ticks = self.min_move_ticks
        state.min_update_interval_ms = self.min_update_interval_ms
        state.max_updates = self.max_updates
        state.activated_tp_level = tp_level
        state.updated_ts_ms = now_ms

        # Compute tick_size from event or keep default
        tick_size = float(event.get("tick_size", state.tick_size) or 0.01)
        if tick_size > 0:
            state.tick_size = tick_size

        # Compute trail_distance: prefer explicit, else atr*mult
        trail_dist = float(event.get("trail_distance", 0) or 0)
        if trail_dist > 0:
            state.trail_distance = trail_dist
        elif state.atr_value > 0 and state.atr_mult > 0:
            state.trail_distance = state.atr_value * state.atr_mult

        # Seed watermarks from current price if provided.
        # TP_HIT events carry fill_price / tp_price, not `price`.
        price = (
            event.get("price")
            or event.get("fill_price")
            or event.get("tp_price")
        )
        if price is not None:
            try:
                px = float(price)
            except (TypeError, ValueError):
                px = 0.0
            if px > 0:
                if state.side == "LONG":
                    state.high_watermark = max(state.high_watermark or px, px)
                else:
                    state.low_watermark = min(state.low_watermark if state.low_watermark is not None else px, px)
                # Also seed entry_price if missing
                if not state.entry_price:
                    state.entry_price = px

        was_new = existing is None
        self._transition(
            state,
            TrailingStateEnum.TRAILING_ACTIVE,
            reason_code="tp_hit",
            event_type=event_type,
            price=float(price) if price is not None else None,
        )

        self._save_state(state)
        self._index_add(symbol, sid)

        if was_new:
            _inc(trailing_state_created_total, state.profile, symbol)

        log.info(
            "on_tp_hit: sid=%s symbol=%s side=%s → TRAILING_ACTIVE trail_dist=%.6f",
            sid, symbol, side, state.trail_distance,
        )
        return state

    def on_tick(self, symbol: str, price: float, ts_ms: int) -> list[str]:
        """Process a price tick for all active states for symbol.

        Returns list of sids where SL was moved.
        """
        if not self.enabled:
            return []

        now_ms = int(time.time() * 1000)
        price_age_ms = max(0, now_ms - ts_ms)
        if _HAS_PROM and trailing_price_age_ms is not None:
            try:
                trailing_price_age_ms.labels(symbol=symbol).observe(price_age_ms)
            except Exception:
                pass
        if price_age_ms > self.price_stale_ms:
            _inc(trailing_sl_move_skipped_total, "stale_price")
            log.debug("on_tick: stale price symbol=%s ts_ms=%d now=%d", symbol, ts_ms, now_ms)
            return []

        t_start = time.time()
        moved: list[str] = []
        for sid in self._active_sids_for(symbol):
            state = self._load_state(sid)
            if state is None:
                self._index_remove(symbol, sid)
                continue
            if state.state != TrailingStateEnum.TRAILING_ACTIVE.value:
                if state.state in (TrailingStateEnum.EXITED.value, TrailingStateEnum.ERROR.value):
                    self._index_remove(symbol, sid)
                continue

            # Rate-limit: min_update_interval_ms between SL moves
            if state.last_cmd_ts_ms and (now_ms - state.last_cmd_ts_ms) < state.min_update_interval_ms:
                _inc(trailing_sl_move_skipped_total, "rate_limited")
                continue

            # Update watermarks on state
            if state.side == "LONG":
                old_hwm = state.high_watermark
                state.high_watermark = max(old_hwm if old_hwm is not None else price, price)
            else:
                old_lwm = state.low_watermark
                state.low_watermark = min(old_lwm if old_lwm is not None else price, price)

            candidate = compute_new_sl(state, price)
            if candidate is None:
                # Track skip reasons
                if state.updates_sent >= state.max_updates:
                    _inc(trailing_sl_move_skipped_total, "max_updates")
                else:
                    _inc(trailing_sl_move_skipped_total, "no_move")
                state.updated_ts_ms = now_ms
                self._save_state(state)
                continue

            old_sl = state.current_sl
            # Update state
            state.current_sl = candidate
            state.last_sent_sl = candidate
            state.updates_sent += 1
            state.last_cmd_ts_ms = now_ms
            state.updated_ts_ms = now_ms

            self._emit_audit(
                state,
                from_state=TrailingStateEnum.TRAILING_ACTIVE.value,
                event_type="SL_MOVE",
                reason_code="watermark_advance",
                price=price,
                old_sl=old_sl,
                new_sl=candidate,
            )
            _inc(trailing_sl_move_total, symbol, state.side, state.profile)

            # Send command only in non-shadow mode
            if not self.shadow:
                self._emit_command(state, candidate, "watermark_advance")
            else:
                log.debug(
                    "on_tick: SHADOW sid=%s symbol=%s new_sl=%.6f (not sent)",
                    sid, symbol, candidate,
                )

            self._save_state(state)
            moved.append(sid)

        # Update active gauge
        active_count = len(self._active_sids_for(symbol))
        _gauge_set(trailing_state_active_gauge, float(active_count), symbol)

        if _HAS_PROM and trailing_update_latency_ms is not None:
            try:
                trailing_update_latency_ms.labels(symbol=symbol).observe(
                    (time.time() - t_start) * 1000
                )
            except Exception:
                pass

        return moved

    def on_position_closed(self, event: dict[str, Any]) -> bool:
        """Handle SL_HIT / POSITION_CLOSED event.

        Transitions state to EXITED, publishes audit.
        Returns True if state was found and updated.
        """
        if not self.enabled:
            return False

        sid = event.get("sid") or event.get("signal_id", "")
        if not sid:
            log.warning("on_position_closed: missing sid")
            return False

        state = self._load_state(sid)
        if state is None:
            log.debug("on_position_closed: no state found sid=%s", sid)
            return False

        if state.state == TrailingStateEnum.EXITED.value:
            log.debug("on_position_closed: already EXITED sid=%s", sid)
            return True

        event_type = event.get("event_type", "POSITION_CLOSED")
        price = event.get("price")

        self._transition(
            state,
            TrailingStateEnum.EXITED,
            reason_code="position_closed",
            event_type=event_type,
            price=float(price) if price is not None else None,
        )

        self._save_state(state)
        self._index_remove(state.symbol, sid)

        # Update active gauge
        active_count = len(self._active_sids_for(state.symbol))
        _gauge_set(trailing_state_active_gauge, float(active_count), state.symbol)

        log.info(
            "on_position_closed: sid=%s symbol=%s → EXITED event_type=%s",
            sid, state.symbol, event_type,
        )
        return True

    # ── Convenience: dispatch by event_type ──────────────────────────────────

    def dispatch_event(self, event: dict[str, Any]) -> None:
        """Route a trade event to the appropriate handler."""
        event_type = (event.get("event_type") or "").upper()
        if event_type in ("TP_HIT", "TP1_HIT", "TP2_HIT", "TP3_HIT"):
            self.on_tp_hit(event)
        elif event_type in ("SL_HIT", "POSITION_CLOSED", "TRAIL_SL_HIT", "CLOSED"):
            self.on_position_closed(event)
        else:
            log.debug("dispatch_event: unhandled event_type=%s", event_type)

    # ── Tick loop (background thread) ────────────────────────────────────────

    def run_tick_loop(
        self,
        tick_redis: redis.Redis | None = None,
        poll_interval_s: float = 0.5,
        stop_flag: list[bool] | None = None,
    ) -> None:
        """Read stream:tick_{SYMBOL} for each active symbol and call on_tick().

        Designed to run in a daemon thread started by the containing service.
        `stop_flag` is a list[bool] — set stop_flag[0]=True to exit.
        Uses the main redis client if tick_redis is None.
        """
        r_tick = tick_redis or self.r
        # Per-symbol read cursor: symbol → last-seen stream ID
        cursors: dict[str, str] = {}
        log.info("TrailingStateWorker tick loop started (shadow=%s)", self.shadow)

        while not (stop_flag and stop_flag[0]):
            try:
                # Refresh autocal state (shadow → live promote without restart)
                self._maybe_refresh_autocal()

                # Sample events:trailing:commands lag (best-effort)
                if _HAS_PROM and trailing_command_stream_lag is not None:
                    try:
                        info = self.r.xinfo_groups(_EVENTS_TRAILING_COMMANDS)
                        total_pending = 0
                        for g in info or []:  # type: ignore[union-attr]
                            total_pending += int(g.get("pending", 0))
                        trailing_command_stream_lag.set(float(total_pending))
                    except Exception:
                        pass

                symbols = list(self._symbol_index.keys())
                if not symbols:
                    time.sleep(poll_interval_s)
                    continue

                now_ms = int(time.time() * 1000)
                for sym in symbols:
                    stream_key = RS.TICK_TPL.format(symbol=sym)
                    cursor = cursors.get(sym, "$")
                    try:
                        results = r_tick.xread({stream_key: cursor}, count=20, block=0)  # type: ignore[arg-type]
                    except Exception as exc:
                        log.debug("xread tick %s: %s", stream_key, exc)
                        continue

                    for _stream, messages in (results or []):  # type: ignore[union-attr]
                        for msg_id, fields in messages:
                            cursors[sym] = msg_id
                            try:
                                price = float(fields.get("price") or fields.get("last_price") or 0.0)
                                tick_ts = int(fields.get("ts") or fields.get("ts_ms") or now_ms)
                                if price > 0:
                                    self.on_tick(sym, price, tick_ts)
                            except Exception as exc:
                                log.debug("tick parse %s: %s", sym, exc)

                time.sleep(poll_interval_s)

            except Exception as exc:
                log.warning("TrailingStateWorker tick loop error: %s", exc)
                if _HAS_PROM and trailing_tick_loop_errors_total is not None:
                    try:
                        trailing_tick_loop_errors_total.inc()
                    except Exception:
                        pass
                time.sleep(1.0)

        log.info("TrailingStateWorker tick loop stopped")
