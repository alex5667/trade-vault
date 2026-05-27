import json
import logging
import os
import sys
import time
from typing import Any

import redis

from common.decision_trace import ensure_trace, trace_enabled, trace_gate
from common.normalization import generate_signal_id, normalize_side_3
from core.redis_keys import RedisStreams as RS
from services.outbox.atomic_outbox import atomic_xadd_sync
from services.outbox.envelope_builder import build_trace_sidecar_meta
from utils.time_utils import get_ny_time_millis

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.order_builder import OrderBuilder

log = logging.getLogger("binance-iceberg")
logging.basicConfig(level=logging.INFO)


def _json_dumps_safe(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def _build_iceberg_signal_payload(
    *,
    symbol: str,
    direction: str,
    price: float,
    state: Any,
    level_info: dict[str, Any],
    atr: float = 0.0,
) -> dict[str, Any]:
    """
    Build a payload in a strictly JSON-friendly shape.
    IMPORTANT:
      - Keep existing fields for backward compatibility (direction/entry/type/subtype/confidence/ts/metadata).
      - Add normalized mirrors (side/kind/price/entry_price/confidence_pct/ts_ms) for unified consumers.
    """
    ts_ms = get_ny_time_millis()

    # --- Side Normalization (P0) ---
    side_norm = normalize_side_3(direction)

    # --- SL/TP Calculation ---
    atr_safe = atr if (atr is not None and atr > 0) else 0.0
    sl_dist = (2.0 * atr_safe) if atr_safe > 0 else (price * 0.005) # 2ATR or 0.5%

    if direction == "LONG":
        sl = price - sl_dist
        tp1 = price + sl_dist      # 1R
        tp2 = price + (2 * sl_dist) # 2R
    else:
        sl = price + sl_dist
        tp1 = price - sl_dist
        tp2 = price - (2 * sl_dist)

    # Rounding
    sl = round(sl, 2)
    tp1 = round(tp1, 2)
    tp2 = round(tp2, 2)


    # --- Signal ID generation (P0) ---
    sid = generate_signal_id(
        kind="iceberg",
        symbol=symbol,
        ts_ms=ts_ms,
        direction=side_norm.direction.value,
    )

    # NOTE: confidence historically used 0..1 in this detector.
    # We keep it, and also provide confidence_pct for unified consumers (0..100).
    conf01 = 0.8
    payload: dict[str, Any] = {
        "sid": sid,
        "signal_id": sid,          # canonical mirror for unified consumers
        "trace_id": sid,           # correlation id for DecisionTrace
        "symbol": symbol,
        "direction": side_norm.direction.value,  # LONG/SHORT
        "side": side_norm.side.value,            # BUY/SELL
        "side_int": side_norm.side_int,          # 1/-1
        "kind": "iceberg",          # normalized kind for unified pipeline
        "venue": "binance",         # explicit venue
        "entry": float(price),      # legacy
        "entry_price": float(price),# normalized mirror
        "price": float(price),      # normalized mirror (audit-friendly)
        "type": "orderflow",
        "subtype": "iceberg",
        "source_service": os.getenv("SERVICE_NAME", "binance_iceberg_detector"),
        "confidence": float(conf01),               # legacy scale 0..1
        "confidence_pct": float(conf01 * 100.0),   # unified scale 0..100
        "ts": ts_ms,     # legacy (ms in this detector)
        "ts_ms": ts_ms,  # explicit epoch ms
        "sl": sl,
        "tp_levels": [tp1, tp2],
        "atr": atr_safe,
        "atr_used_for_levels": atr_safe,
        "atr_at_entry": atr_safe,
        "metadata": {
            "iceberg": {
                "level_kind": level_info.get("kind"),
                "level_price": level_info.get("price"),
                "refresh_count": getattr(state, "refresh_count", None),
                "visible_qty": getattr(state, "visible_qty", None),
                "duration_sec": float(time.time() - float(getattr(state, "since_ts", time.time()) or time.time())),
                "atr_used": atr_safe,
            }
        },
    }
    return payload




def create_redis_client_with_retry(
    redis_url: str,
    decode_responses: bool = False,
    max_retries: int = 40,
    retry_delay: float = 2.0,
    client_name: str = "redis"
) -> redis.Redis:
    """
    Создает Redis клиент с retry логикой для обработки загрузки dataset.
    """
    for attempt in range(max_retries):
        try:
            client = redis.Redis.from_url(
                redis_url,
                decode_responses=decode_responses,
                socket_connect_timeout=30,
                socket_timeout=120,
                health_check_interval=30
            )
            client.ping()
            print(f"✅ {client_name} connection established: {redis_url}")
            return client

        except Exception as e:
            error_str = str(e).lower()

            # Check for recursion errors first
            if "maximum recursion depth" in error_str or "recursion" in error_str:
                print(f"❌ Recursion detected while connecting to {client_name}: {e}")
                raise

            # Check for loading/connection errors
            is_loading_error = (
                "loading the dataset in memory" in error_str or
                "busy loading" in error_str or
                "redis is loading" in error_str
            )

            if attempt < max_retries - 1:
                delay = min(retry_delay * (1.2 ** attempt), 10.0)

                if is_loading_error:
                    print(f"⚠️  {client_name} is loading dataset (attempt {attempt + 1}/{max_retries}): {e}")
                else:
                    print(f"⚠️  {client_name} connection error (attempt {attempt + 1}/{max_retries}): {e}")

                print(f"   Retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                if is_loading_error:
                    print(f"❌ {client_name} still loading after {max_retries} attempts")
                else:
                    print(f"❌ Failed to connect to {client_name} after {max_retries} attempts: {e}")
                raise

    raise redis.exceptions.ConnectionError(f"Failed to connect to {client_name} after {max_retries} attempts")

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
BOOK_SNAPSHOT_KEY = "book:latest:{symbol}"
CANDLES_KEY = "candles:{symbol}:1h"
LEVELS_STREAM_KEY = "stream:levels_{symbol}"
LEVELS_HASH_KEY = "levels:{symbol}"

ICEBERG_MIN_DURATION = float(os.getenv("ICEBERG_MIN_DURATION", "1.5"))
ICEBERG_MIN_REFRESH = int(os.getenv("ICEBERG_MIN_REFRESH", "2"))
ICEBERG_MIN_EXECUTED = float(os.getenv("ICEBERG_MIN_EXECUTED", "20.0"))
MAX_DIST_ATR = float(os.getenv("ICEBERG_MAX_DIST_ATR", "0.5"))
MAX_DIST_REL = float(os.getenv("ICEBERG_MAX_DIST_REL", "0.0025"))

METRIC_SIG = os.getenv("ICEBERG_METRIC_SIGNAL", "metrics:iceberg_signals_total")
METRIC_GAP = os.getenv("ICEBERG_METRIC_GAP", "metrics:iceberg_gaps_total")
METRIC_BLOCKED_TREND = os.getenv("ICEBERG_METRIC_BLOCKED", "metrics:iceberg_blocked_by_trend")
# Binance executor queue (separate from MT5 queue).
ORDERS_QUEUE = os.getenv("ORDERS_QUEUE_BINANCE") or os.getenv("ORDERS_QUEUE") or RS.ORDERS_QUEUE_BINANCE
RAW_SIGNAL_STREAM = os.getenv("ICEBERG_RAW_STREAM", RS.CRYPTO_RAW)
NOTIFY_STREAM = os.getenv("ICEBERG_NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
# Publish iceberg signals to signals:of:inputs so ML dataset builder can join them.
OF_INPUTS_STREAM = os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS)
_OF_INPUTS_MAXLEN = int(os.getenv("OF_INPUTS_STREAM_MAXLEN", "1000000") or 1000000)


class BestLevelState:
    __slots__ = ("price", "since_ts", "refresh_count", "visible_qty")

    def __init__(self, price: float, qty: float, ts: float):
        self.price = price
        self.since_ts = ts
        self.refresh_count = 0
        self.visible_qty = qty


class BinanceIcebergDetector:
    """
    Детектор айсбергов/абсорбции по Binance USDT-M:
      - читает best bid/ask из `book:latest:{symbol}`
      - проверяет refresh + длительность уровня
      - сверяет близость к уровню (стрим/хэш/свечи)
      - публикует сигнал, дублирует в orders:queue
    """

    def __init__(self, r_core: redis.Redis, r_ticks: redis.Redis, symbol: str):
        self.r_core = r_core
        self.r_ticks = r_ticks
        self.symbol = symbol.upper()

        self.bid_state: BestLevelState | None = None
        self.ask_state: BestLevelState | None = None

        self.order_builder = OrderBuilder(self.r_core)

    # ---------- загрузка уровней ----------

    def _load_levels_from_stream(self) -> list[dict[str, Any]]:
        key = LEVELS_STREAM_KEY.format(symbol=self.symbol)
        try:
            rows = self.r_core.xrevrange(key, count=50)
        except redis.exceptions.BusyLoadingError:
            log.debug("Redis loading dataset, skipping levels from stream")
            return []
        except redis.ResponseError:
            return []
        except Exception as e:
            log.debug("Error loading levels from stream: %s", e)
            return []

        levels: list[dict[str, Any]] = []
        for _id, fields in rows:
            level_raw = fields.get(b"level") or fields.get("level")
            if not level_raw:
                continue
            if isinstance(level_raw, bytes):
                level_raw = level_raw.decode()
            try:
                price = float(level_raw)
            except ValueError:
                continue

            kind_raw = fields.get(b"kind") or fields.get("kind") or "custom"
            if isinstance(kind_raw, bytes):
                kind_raw = kind_raw.decode()

            levels.append({"price": price, "kind": kind_raw})
        return levels

    def _load_levels_from_hash(self) -> list[dict[str, Any]]:
        key = LEVELS_HASH_KEY.format(symbol=self.symbol)
        try:
            data = self.r_core.hgetall(key)
        except redis.exceptions.BusyLoadingError:
            log.debug("Redis loading dataset, skipping levels from hash")
            return []
        except Exception as e:
            log.debug("Error loading levels from hash: %s", e)
            return []
        if not data:
            return []

        out: list[dict[str, Any]] = []
        for raw_kind, raw_price in data.items():
            kind = raw_kind.decode() if isinstance(raw_kind, bytes) else raw_kind
            value = raw_price.decode() if isinstance(raw_price, bytes) else raw_price
            try:
                price = float(value)
            except ValueError:
                continue
            out.append({"price": price, "kind": kind})
        return out

    def _load_levels_from_candles(self) -> list[dict[str, Any]]:
        key = CANDLES_KEY.format(symbol=self.symbol)
        try:
            data = self.r_core.hgetall(key)
        except redis.exceptions.BusyLoadingError:
            log.debug("Redis loading dataset, skipping levels from candles")
            return []
        except Exception as e:
            log.debug("Error loading levels from candles: %s", e)
            return []
        if not data:
            return []

        out: list[dict[str, Any]] = []
        high = data.get("high") or data.get(b"high")
        low = data.get("low") or data.get(b"low")
        if high:
            out.append({"price": float(high), "kind": "h1_high"})
        if low:
            out.append({"price": float(low), "kind": "h1_low"})
        return out

    def _nearest_levels(self, price: float) -> list[dict[str, Any]]:
        levels = self._load_levels_from_stream()
        if not levels:
            levels = self._load_levels_from_hash()
        if not levels:
            levels = self._load_levels_from_candles()
        if not levels:
            return []

        atr = self._get_atr()
        filtered = []
        for lvl in levels:
            lp = lvl["price"]
            dist = abs(price - lp)
            if atr and atr > 0:
                if dist <= atr * MAX_DIST_ATR:
                    lvl["dist_abs"] = dist
                    filtered.append(lvl)
            else:
                if dist / price <= MAX_DIST_REL:
                    lvl["dist_abs"] = dist
                    filtered.append(lvl)

        filtered.sort(key=lambda x: x.get("dist_abs", 0.0))
        return filtered[:5]

    def _get_atr(self) -> float | None:
        """Resolve ATR for level-distance gating with TF fallback ladder.

        Historical bug: this method read `HGET candles:{symbol}:1h atr` but
        the production ATR feeder writes to a different namespace
        (`atr:{SYMBOL}:{TF}` as plain string, TFs 1m/5m/15m; 1h is NOT
        written). The legacy hash path is kept first for backward
        compatibility, then we fall through to the canonical string keys.

        ATR-distance threshold `MAX_DIST_ATR` was originally calibrated for
        1h ATR. Falling back to 5m / 1m ATR yields a narrower absolute
        distance — acceptable as conservative behaviour (fewer false-positive
        iceberg matches) until the 1h feeder is restored or
        `ICEBERG_ATR_TF_LADDER` is tuned.

        Override the ladder via ``ICEBERG_ATR_TF_LADDER=1h,5m,1m`` (CSV).
        r_core uses decode_responses=False → all reads return bytes; explicit
        decode + float() cast prevents the TypeError that the old `return atr`
        path produced when bytes met `atr > 0`.
        """
        # 1) Legacy hash path: candles:{symbol}:1h["atr"]
        try:
            hash_key = CANDLES_KEY.format(symbol=self.symbol)
            raw = self.r_core.hget(hash_key, "atr") or self.r_core.hget(hash_key, b"atr")
            if raw:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", "replace")
                try:
                    v = float(raw)
                    if v > 0:
                        return v
                except (TypeError, ValueError):
                    pass
        except redis.exceptions.BusyLoadingError:
            log.debug("Redis loading dataset, skipping ATR hash lookup")
            return None
        except Exception as e:
            log.debug("ATR hash lookup error: %s", e)

        # 2) Canonical string keys: atr:{SYMBOL}:{TF}
        ladder = (os.getenv("ICEBERG_ATR_TF_LADDER", "1h,5m,1m") or "1h,5m,1m").split(",")
        for tf in (t.strip() for t in ladder if t.strip()):
            key = f"atr:{self.symbol}:{tf}"
            try:
                raw = self.r_core.get(key)
            except redis.exceptions.BusyLoadingError:
                return None
            except Exception:
                continue
            if not raw:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", "replace")
            try:
                v = float(raw)
            except (TypeError, ValueError):
                continue
            if v > 0:
                return v
        return None

    def _load_adx(self) -> dict[str, float] | None:
        key = f"adx:{self.symbol}"
        try:
            raw = self.r_core.hgetall(key)
        except redis.exceptions.BusyLoadingError:
            log.debug("Redis loading dataset, skipping ADX")
            return None
        except Exception as e:
            log.debug("Error loading ADX: %s", e)
            return None
        if not raw:
            return None
        result: dict[str, float] = {}
        for k, v in raw.items():
            key_str = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            try:
                result[key_str] = float(val)
            except (TypeError, ValueError):
                continue
        return result or None

    def _trend_allows(self, direction: str) -> bool:
        adx = self._load_adx()
        if not adx:
            return True

        adx_val = adx.get("adx")
        plus_di = adx.get("plusDI")
        minus_di = adx.get("minusDI")

        if adx_val is None or adx_val < 15:
            return True

        if direction == "LONG" and plus_di is not None and minus_di is not None:
            if minus_di > plus_di and adx_val >= 20:
                return False
        if direction == "SHORT" and plus_di is not None and minus_di is not None:
            if plus_di > minus_di and adx_val >= 20:
                return False

        return True

    # ---------- источники данных ----------

    def _load_latest_book(self) -> dict[str, Any] | None:
        key = BOOK_SNAPSHOT_KEY.format(symbol=self.symbol)
        try:
            data = self.r_ticks.hgetall(key)
            payload = None
            if data:
                payload = data.get("payload") or data.get(b"payload")
            if payload is None:
                raw = self.r_ticks.get(key)
                payload = raw
            if not payload:
                return None
            if isinstance(payload, bytes):
                payload = payload.decode()
            try:
                return json.loads(payload)
            except json.JSONDecodeError:
                return None
        except redis.exceptions.BusyLoadingError:
            log.debug("Redis-ticks loading dataset, skipping book snapshot")
            return None
        except Exception as e:
            log.debug("Error loading latest book: %s", e)
            return None

    # ---------- логика детектора ----------

    def process_once(self):
        book = self._load_latest_book()
        if not book:
            return

        now = time.time()
        bids = book.get("bids") or book.get("b")
        asks = book.get("asks") or book.get("a")
        if not bids or not asks:
            return

        best_bid = bids[0]
        best_ask = asks[0]

        bb_price, bb_qty = float(best_bid[0]), float(best_bid[1])
        ba_price, ba_qty = float(best_ask[0]), float(best_ask[1])

        self._update_level(side="bid", price=bb_price, qty=bb_qty, now_ts=now)
        self._update_level(side="ask", price=ba_price, qty=ba_qty, now_ts=now)

    def _update_level(self, side: str, price: float, qty: float, now_ts: float):
        state = self.bid_state if side == "bid" else self.ask_state

        if state is None:
            st = BestLevelState(price, qty, now_ts)
            if side == "bid":
                self.bid_state = st
            else:
                self.ask_state = st
            return

        if abs(state.price - price) < 1e-8:
            if qty > state.visible_qty:
                state.refresh_count += 1
            state.visible_qty = qty
            duration = now_ts - state.since_ts

            if duration >= ICEBERG_MIN_DURATION and state.refresh_count >= ICEBERG_MIN_REFRESH:
                near = self._nearest_levels(price)
                if near:
                    direction = "LONG" if side == "bid" else "SHORT"
                    if self._trend_allows(direction):
                        try:
                            self.r_core.incr(METRIC_SIG)
                            self._publish_signal(direction, price, state, near[0])
                        except redis.exceptions.BusyLoadingError:
                            log.debug("Redis loading dataset, skipping signal publish")
                        except Exception as e:
                            log.warning("Error publishing signal: %s", e)
                    else:
                        try:
                            self.r_core.incr(METRIC_BLOCKED_TREND)
                        except (redis.exceptions.BusyLoadingError, Exception):
                            pass  # Не критично
                else:
                    try:
                        self.r_core.incr(METRIC_GAP)
                    except (redis.exceptions.BusyLoadingError, Exception):
                        pass  # Не критично

                if side == "bid":
                    self.bid_state = None
                else:
                    self.ask_state = None
        else:
            st = BestLevelState(price, qty, now_ts)
            if side == "bid":
                self.bid_state = st
            else:
                self.ask_state = st

    # ---------- публикация ----------

    def _publish_signal(self, direction: str, price: float, state: BestLevelState, level_info: dict[str, Any]) -> None:
        """
        Producer #2 (sync): iceberg detector.
        Fixes:
          - adds contract mirrors (signal_id/ts_ms/side_int/entry_price) without breaking legacy
          - unified stream writer (fail-open + BusyLoading-aware)
        """
        # Next level: optionally publish via outbox for unified retries/DLQ.
        use_outbox = os.getenv("ICEBERG_USE_OUTBOX_DISPATCHER", "0").lower() in {"1","true","yes","on"}
        shadow_outbox = os.getenv("ICEBERG_SHADOW_OUTBOX", "0").lower() in {"1","true","yes","on"}
        outbox_stream = os.getenv("SIGNAL_OUTBOX_STREAM", RS.SIGNAL_OUTBOX)

        # Get ATR for SL calculation
        atr_val = self._get_atr() or 0.0

        # Сконцентрируем контракт в одном месте (у вас уже есть helper сверху файла)
        signal_payload = _build_iceberg_signal_payload(
            symbol=str(self.symbol),
            direction=(direction or "").upper(),
            price=float(price),
            state=state,
            level_info=level_info,
            atr=atr_val,
        )

        # Sid alignment (P1 fix): the payload builder allocates its own sid via
        # generate_signal_id() and ships it as payload["sid"]/["signal_id"]/
        # ["trace_id"]. Earlier code built a parallel "signal:{symbol}:iceberg:{ts}"
        # sid here and used it for `signals:{sid}` SET + outbox envelope key,
        # producing a mismatch with payload.sid that broke trade_close_joiner
        # SID joins. Source-of-truth is payload.sid.
        sid = str(signal_payload.get("sid") or "")
        trace_id = sid  # correlation id

        # Outbox envelope (notify/raw + optional snapshot signals:{sid})
        if use_outbox or shadow_outbox:
            try:
                from services.outbox.envelope_builder import build_envelope

                targets = {}
                meta = {}
                if RAW_SIGNAL_STREAM:
                    targets["audit_payload"] = {"payload": json.dumps(signal_payload, ensure_ascii=False)}
                    meta["audit_stream"] = RAW_SIGNAL_STREAM
                if NOTIFY_STREAM:
                    # In your iceberg flow NOTIFY_STREAM expects {"payload": "..."} fields.
                    targets["notify"] = {"payload": json.dumps(signal_payload, ensure_ascii=False)}
                # Keep signals:{sid} snapshot via dispatcher (optional)
                targets["snapshot"] = signal_payload
                meta["snap_key"] = f"signals:{sid}"
                meta["snap_ttl"] = 3600

                env = build_envelope(sid=sid, payload=signal_payload, targets_obj=targets, meta=meta)
                # ------------------------------------------------------------
                # ✅ Atomic outbox write + meta sidecar (DecisionTrace full)
                # ------------------------------------------------------------
                meta_obj = None
                try:
                    ctx_min = SimpleNamespace()  # type: ignore
                    ctx_min.ts_ms = get_ny_time_millis()
                    try:
                        ctx_min.symbol = (env.get("symbol") or "")
                        ctx_min.kind = (env.get("kind") or "iceberg")
                    except Exception:
                        pass
                    if trace_enabled():
                        ensure_trace(ctx_min, sid=str(sid))
                        trace_gate(ctx_min, stage="detector", name="iceberg_detector", passed=True, veto=False, reason_code="OK", duration_ms=0.0)
                        meta_obj = build_trace_sidecar_meta(ctx=ctx_min, sid=str(sid))  # type: ignore
                except Exception:
                    meta_obj = None

                atomic_xadd_sync(
                    self.r_core,
                    stream_key=str(outbox_stream),
                    signal_id=str(sid),
                    payload_obj=env,  # envelope dict
                    kind=(env.get("kind") or "iceberg"),
                    symbol=(env.get("symbol") or ""),
                    ts=(env.get("ts_ms") or ""),
                    meta_obj=meta_obj,
                )
                log.info("🧊 Iceberg outbox env: %s %s @ %.2f", self.symbol, direction, price)
            except Exception as e:
                log.warning("Iceberg outbox publish failed: %s", e)

        # Publish to signals:of:inputs so ML dataset builder can join iceberg signals
        # to trades:closed entries. Fail-open: never block the primary publish path.
        if OF_INPUTS_STREAM:
            try:
                self.r_core.xadd(
                    OF_INPUTS_STREAM,
                    {"payload": json.dumps(signal_payload, ensure_ascii=False, default=str)},
                    maxlen=_OF_INPUTS_MAXLEN,
                    approximate=True,
                )
            except Exception as _oi_e:
                log.debug("iceberg of_inputs publish failed: %s", _oi_e)

        if use_outbox:
            # Dispatcher will publish notify/raw/snapshot.
            order_payload = self.order_builder.build_order_from_signal(signal_payload)
            self.r_core.lpush(ORDERS_QUEUE, json.dumps(order_payload))
            return

        try:
            preprocess_signal_for_publish(signal_payload, symbol=str(self.symbol), source="IcebergDetector", logger=log)  # type: ignore
            self.r_core.set(f"signals:{sid}", json.dumps(signal_payload, ensure_ascii=False, default=str), ex=3600)

            pub = SyncSignalPublisher(redis_client=self.r_core, source="IcebergDetector", metrics_prefix="iceberg_publish", logger=log)  # type: ignore
            if RAW_SIGNAL_STREAM:
                pub.xadd_json(
                    sink=StreamSink(name=str(RAW_SIGNAL_STREAM), field="payload", maxlen=2000),  # type: ignore
                    payload=signal_payload,
                    symbol=str(self.symbol),
                )
            if NOTIFY_STREAM:
                pub.xadd_json(
                    sink=StreamSink(name=str(NOTIFY_STREAM), field="payload", maxlen=1000),  # type: ignore
                    payload=signal_payload,
                    symbol=str(self.symbol),
                )
            order_payload = self.order_builder.build_order_from_signal(signal_payload)
            self.r_core.lpush(ORDERS_QUEUE, json.dumps(order_payload, ensure_ascii=False, default=str))
            log.info("🧊 Iceberg signal: %s %s @ %.2f (lvl=%s)", self.symbol, direction, price, level_info.get("kind"))
        except redis.exceptions.BusyLoadingError:
            log.warning("⚠️  Redis loading dataset, signal not published: %s %s @ %.2f", self.symbol, direction, price)
        except Exception as e:
            log.error("❌ Failed to publish iceberg signal: %s", e)

        # decision_snapshot publish (events:decision_snapshot): writer mirrors to
        # decision:{sid} so trade_close_joiner can resolve POSITION_CLOSED → decision.
        # Fail-open: never block primary publish path.
        try:
            self._publish_decision_snapshot(signal_payload)
        except Exception as e:
            log.warning("iceberg decision_snapshot publish failed: %s", e)

    def _publish_decision_snapshot(self, signal_payload: dict[str, Any]) -> None:
        """Emit a snapshot event for the iceberg signal.

        Uses build_decision_snapshot_event to produce a DTO-validated payload, so
        the downstream writer mirrors it under decision:{sid} with the same shape
        as crypto-orderflow snapshots.
        """
        from services.orderflow.decision_snapshot import build_decision_snapshot_event

        sid = str(signal_payload.get("sid") or "")
        if not sid:
            return

        sig = dict(signal_payload)
        sig.setdefault("decision_ts_ms", sig.get("ts_ms"))
        # Iceberg has no microstructure indicators; book/spread/depth absent here.
        snapshot = build_decision_snapshot_event(
            signal=sig,
            indicators=None,
            runtime=None,
            schema_version=1,
        )
        stream = os.getenv("DECISION_SNAPSHOT_STREAM", RS.DECISION_SNAPSHOT)
        maxlen = int(os.getenv("DECISION_SNAPSHOT_STREAM_MAXLEN", "1000000") or 1000000)
        self.r_core.xadd(
            stream,
            {"payload": _json_dumps_safe(snapshot)},
            maxlen=maxlen,
            approximate=True,
        )

    # ---------- вспомогательные ----------

def _load_symbols(r: redis.Redis) -> list[str]:
    env = os.getenv("ORDERFLOW_SYMBOLS")
    if env:
        return [s.strip().upper() for s in env.split(",") if s.strip()]
    try:
        symbols = r.smembers("crypto:symbols")
        if symbols:
            return [s.decode() if isinstance(s, bytes) else s for s in symbols]
    except redis.exceptions.BusyLoadingError:
        log.warning("⚠️  Redis loading dataset, using default symbols")
    except Exception as e:
        log.debug("Error loading symbols from Redis: %s", e)
    return DEFAULT_SYMBOLS


def main():
    # Создаем Redis клиенты с retry логикой
    redis_core_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    redis_ticks_url = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")

    print(f"🔌 Connecting to Redis (core: {redis_core_url}, ticks: {redis_ticks_url})...")

    try:
        r_core = create_redis_client_with_retry(
            redis_core_url,
            decode_responses=False,
            max_retries=40,
            retry_delay=2.0,
            client_name="Redis-core"
        )
        r_ticks = create_redis_client_with_retry(
            redis_ticks_url,
            decode_responses=False,
            max_retries=40,
            retry_delay=2.0,
            client_name="Redis-ticks"
        )
    except Exception as e:
        print(f"❌ Failed to establish Redis connections: {e}")
        sys.exit(1)

    symbols = _load_symbols(r_core)
    detectors = {s: BinanceIcebergDetector(r_core, r_ticks, s) for s in symbols}

    log.info("✅ Binance Iceberg Detector started for symbols: %s", ", ".join(detectors.keys()))

    consecutive_errors = 0
    max_consecutive_errors = 10

    while True:
        try:
            for detector in detectors.values():
                try:
                    detector.process_once()
                    consecutive_errors = 0  # Сброс счетчика при успехе
                except redis.exceptions.BusyLoadingError:
                    log.debug("Redis loading dataset, skipping processing for %s", detector.symbol)
                    time.sleep(1.0)  # Увеличиваем задержку при загрузке
                except Exception as e:
                    consecutive_errors += 1
                    log.exception("Failed to process iceberg for %s: %s", detector.symbol, e)
                    if consecutive_errors >= max_consecutive_errors:
                        log.error("❌ Too many consecutive errors (%d), exiting", consecutive_errors)
                        sys.exit(1)
            time.sleep(0.1)
        except KeyboardInterrupt:
            log.info("🛑 Shutting down Binance Iceberg Detector...")
            break
        except Exception as e:
            log.error("❌ Fatal error in main loop: %s", e)
            time.sleep(1.0)


if __name__ == "__main__":
    main()
