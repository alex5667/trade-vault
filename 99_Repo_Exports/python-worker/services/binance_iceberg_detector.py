from utils.time_utils import get_ny_time_millis
import json
import logging
import os
import sys
import time
import uuid
import statistics
from typing import Any, Dict, List, Optional

import redis
from core.redis_keys import RedisStreams as RS
from services.outbox.atomic_outbox import atomic_xadd_sync
from services.outbox.envelope_builder import build_trace_sidecar_meta
from common.decision_trace import ensure_trace, trace_gate, trace_enabled

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
    level_info: Dict[str, Any],
    atr: float = 0.0,
) -> Dict[str, Any]:
    """
    Build a payload in a strictly JSON-friendly shape.
    IMPORTANT:
      - Keep existing fields for backward compatibility (direction/entry/type/subtype/confidence/ts/metadata).
      - Add normalized mirrors (side/kind/price/entry_price/confidence_pct/ts_ms) for unified consumers.
    """
    ts_ms = get_ny_time_millis()

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


    # Avoid rare collisions in high-frequency bursts (same ms).
    # Keep legacy prefix for easy grep/partitioning.
    add_suffix = (os.getenv("ICEBERG_SID_RANDOM_SUFFIX", "1").strip().lower() in {"1", "true", "yes", "on"})
    sid = f"signal:{symbol}:iceberg:{ts_ms}"
    if add_suffix:
        sid = f"{sid}:{uuid.uuid4().hex[:8]}"

    # NOTE: confidence historically used 0..1 in this detector.
    # We keep it, and also provide confidence_pct for unified consumers (0..100).
    conf01 = 0.8
    payload: Dict[str, Any] = {
        "sid": sid,
        "signal_id": sid,          # canonical mirror for unified consumers
        "trace_id": sid,           # correlation id for DecisionTrace
        "symbol": symbol,
        "direction": direction,     # legacy
        "side": direction,          # normalized mirror (LONG/SHORT)
        "kind": "iceberg",          # normalized kind for unified pipeline
        "entry": float(price),      # legacy
        "entry_price": float(price),# normalized mirror
        "price": float(price),      # normalized mirror (audit-friendly)
        "type": "orderflow",
        "subtype": "iceberg",
        "confidence": float(conf01),               # legacy scale 0..1
        "confidence_pct": float(conf01 * 100.0),   # unified scale 0..100
        "ts": ts_ms,     # legacy (ms in this detector)
        "ts_ms": ts_ms,  # explicit epoch ms
        "sl": sl,
        "tp_levels": [tp1, tp2],
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

        self.bid_state: Optional[BestLevelState] = None
        self.ask_state: Optional[BestLevelState] = None

        self.order_builder = OrderBuilder(self.r_core)

    # ---------- загрузка уровней ----------

    def _load_levels_from_stream(self) -> List[Dict[str, Any]]:
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

        levels: List[Dict[str, Any]] = []
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

    def _load_levels_from_hash(self) -> List[Dict[str, Any]]:
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

        out: List[Dict[str, Any]] = []
        for raw_kind, raw_price in data.items():
            kind = raw_kind.decode() if isinstance(raw_kind, bytes) else raw_kind
            value = raw_price.decode() if isinstance(raw_price, bytes) else raw_price
            try:
                price = float(value)
            except ValueError:
                continue
            out.append({"price": price, "kind": kind})
        return out

    def _load_levels_from_candles(self) -> List[Dict[str, Any]]:
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

        out: List[Dict[str, Any]] = []
        high = data.get("high") or data.get(b"high")
        low = data.get("low") or data.get(b"low")
        if high:
            out.append({"price": float(high), "kind": "h1_high"})
        if low:
            out.append({"price": float(low), "kind": "h1_low"})
        return out

    def _nearest_levels(self, price: float) -> List[Dict[str, Any]]:
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

    def _get_atr(self) -> Optional[float]:
        key = CANDLES_KEY.format(symbol=self.symbol)
        try:
            atr = self.r_core.hget(key, "atr")
            if not atr:
                atr = self.r_core.hget(key, b"atr")
            if atr:
                try:
                    return float(atr)
                except ValueError:
                    return None
        except redis.exceptions.BusyLoadingError:
            log.debug("Redis loading dataset, skipping ATR")
            return None
        except Exception as e:
            log.debug("Error getting ATR: %s", e)
            return None
        return None

    def _load_adx(self) -> Optional[Dict[str, float]]:
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
        result: Dict[str, float] = {}
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

    def _load_latest_book(self) -> Optional[Dict[str, Any]]:
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

    def _publish_signal(self, direction: str, price: float, state: BestLevelState, level_info: Dict[str, Any]) -> None:
        """
        Producer #2 (sync): iceberg detector.
        Fixes:
          - adds contract mirrors (signal_id/ts_ms/side_int/entry_price) without breaking legacy
          - unified stream writer (fail-open + BusyLoading-aware)
        """
        # Next level: optionally publish via outbox for unified retries/DLQ.
        use_outbox = os.getenv("ICEBERG_USE_OUTBOX_DISPATCHER", "0").lower() in {"1","true","yes","on"}
        shadow_outbox = os.getenv("ICEBERG_SHADOW_OUTBOX", "0").lower() in {"1","true","yes","on"}
        outbox_stream = os.getenv("SIGNAL_OUTBOX_STREAM", "stream:signals:outbox")

        ts_ms = get_ny_time_millis()
        sid = f"signal:{self.symbol}:iceberg:{ts_ms}"
        trace_id = sid  # correlation id

        # Get ATR for SL calculation
        atr_val = self._get_atr() or 0.0

        # Сконцентрируем контракт в одном месте (у вас уже есть helper сверху файла)
        signal_payload = _build_iceberg_signal_payload(
            symbol=str(self.symbol),
            direction=str(direction or "").upper(),
            price=float(price),
            state=state,
            level_info=level_info,
            atr=atr_val,
        )

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
                    ctx_min = SimpleNamespace()
                    setattr(ctx_min, "ts_ms", get_ny_time_millis())
                    try:
                        setattr(ctx_min, "symbol", str(env.get("symbol") or ""))
                        setattr(ctx_min, "kind", str(env.get("kind") or "iceberg"))
                    except Exception:
                        pass
                    if trace_enabled():
                        ensure_trace(ctx_min, sid=str(sid))
                        trace_gate(ctx_min, stage="detector", name="iceberg_detector", passed=True, veto=False, reason_code="OK", duration_ms=0.0)
                        meta_obj = build_trace_sidecar_meta(ctx=ctx_min, sid=str(sid))
                except Exception:
                    meta_obj = None

                atomic_xadd_sync(
                    self.r_core,
                    stream_key=str(outbox_stream),
                    signal_id=str(sid),
                    payload_obj=env,  # envelope dict
                    kind=str(env.get("kind") or "iceberg"),
                    symbol=str(env.get("symbol") or ""),
                    ts=str(env.get("ts_ms") or ""),
                    meta_obj=meta_obj,
                )
                log.info("🧊 Iceberg outbox env: %s %s @ %.2f", self.symbol, direction, price)
            except Exception as e:
                log.warning("Iceberg outbox publish failed: %s", e)

        if use_outbox:
            # Dispatcher will publish notify/raw/snapshot.
            order_payload = self.order_builder.build_order_from_signal(signal_payload)
            self.r_core.lpush(ORDERS_QUEUE, json.dumps(order_payload))
            return

        try:
            preprocess_signal_for_publish(signal_payload, symbol=str(self.symbol), source="IcebergDetector", logger=log)
            self.r_core.set(f"signals:{sid}", json.dumps(signal_payload, ensure_ascii=False, default=str), ex=3600)

            pub = SyncSignalPublisher(redis_client=self.r_core, source="IcebergDetector", metrics_prefix="iceberg_publish", logger=log)
            if RAW_SIGNAL_STREAM:
                pub.xadd_json(
                    sink=StreamSink(name=str(RAW_SIGNAL_STREAM), field="payload", maxlen=2000),
                    payload=signal_payload,
                    symbol=str(self.symbol),
                )
            if NOTIFY_STREAM:
                pub.xadd_json(
                    sink=StreamSink(name=str(NOTIFY_STREAM), field="payload", maxlen=1000),
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

    # ---------- вспомогательные ----------

def _load_symbols(r: redis.Redis) -> List[str]:
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

