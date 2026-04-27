"""
worker.py — Основной цикл Regime Worker.

Читает kline данные из Redis Stream `candles:data`,
вычисляет ADX/ATR по Уайлдеру, классифицирует режим,
публикует результат в `stream:regime` и KV `regime:{symbol}`.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
import traceback

import redis
from redis.connection import ConnectionPool

from adx_atr import WilderState, update_adx_atr
from classify import classify_regime, confidence
from quantiles import load_quantiles

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [regime-worker] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REDIS_READ_HOST = os.getenv("REDIS_READ_HOST", "redis")
REDIS_READ_PORT = int(os.getenv("REDIS_READ_PORT", "6379"))

REDIS_WRITE_HOST = os.getenv("REDIS_HOST", "redis-worker-1")
REDIS_WRITE_PORT = int(os.getenv("REDIS_PORT", "6379"))

STREAM_IN = "candles:data"
SUPPORTED_TIMEFRAMES = frozenset(
    ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M", "3M", "1y"]
)

STREAM_OUT = os.getenv("REGIME_STREAM_OUT", "stream:regime")
CONSUMER_GROUP = os.getenv("REGIME_CONSUMER_GROUP", "regime-worker-group")
CONSUMER_NAME = os.getenv("REGIME_CONSUMER_NAME", f"regime-worker-{os.getpid()}")
WILDER_N = int(os.getenv("REGIME_WILDER_N", "14"))
READ_COUNT = int(os.getenv("REGIME_READ_COUNT", "10"))
READ_BLOCK_MS = int(os.getenv("REGIME_READ_BLOCK_MS", "1000"))

# ---------------------------------------------------------------------------
# Redis clients
# ---------------------------------------------------------------------------
_POOL_KWARGS = dict(
    socket_keepalive=True,
    socket_connect_timeout=30,
    socket_timeout=120,
    retry_on_timeout=True,
    health_check_interval=30,
    decode_responses=True,
)

REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

if REDIS_USERNAME:
    _POOL_KWARGS["username"] = REDIS_USERNAME
if REDIS_PASSWORD:
    _POOL_KWARGS["password"] = REDIS_PASSWORD

pool_read = ConnectionPool(
    host=REDIS_READ_HOST,
    port=REDIS_READ_PORT,
    max_connections=50,
    **_POOL_KWARGS,
)

pool_write = ConnectionPool(
    host=REDIS_WRITE_HOST,
    port=REDIS_WRITE_PORT,
    max_connections=20,
    **_POOL_KWARGS,
)

rclient_read = redis.Redis(connection_pool=pool_read)
rclient_write = redis.Redis(connection_pool=pool_write)

# ---------------------------------------------------------------------------
# Per-symbol state: (symbol, tf) → {w, prev, prevCandle}
# ---------------------------------------------------------------------------
states: dict = {}

# ---------------------------------------------------------------------------
# Shutdown flag
# ---------------------------------------------------------------------------
_shutdown = False


def _handle_sigterm(signum, frame) -> None:  # noqa: ANN001
    global _shutdown
    _shutdown = True
    logger.info("Получен сигнал %s, начинаем graceful shutdown…", signum)


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


# ---------------------------------------------------------------------------
# Consumer group setup
# ---------------------------------------------------------------------------
def ensure_consumer_groups() -> None:
    """Создаёт consumer group для stream `candles:data`."""
    try:
        rclient_read.xgroup_create(STREAM_IN, CONSUMER_GROUP, id="0", mkstream=True)
        logger.info("Consumer group '%s' создана для '%s'", CONSUMER_GROUP, STREAM_IN)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            logger.info(
                "Consumer group '%s' уже существует для '%s'",
                CONSUMER_GROUP,
                STREAM_IN,
            )
        else:
            logger.warning("Ошибка создания group для %s: %s", STREAM_IN, exc)


# ---------------------------------------------------------------------------
# Kline processing
# ---------------------------------------------------------------------------
def process_kline(fields: dict) -> None:
    """
    Обрабатывает одну kline-запись из stream.

    Ожидаемый формат fields:
        symbol  — тикер (BTCUSDT)
        tf      — таймфрейм (1m, 5m, …)
        ts      — timestamp (ms)
        payload — JSON строка с {open, high, low, close, …}
    """
    symbol = fields.get("symbol", "").upper()
    tf = fields.get("tf", "1m")
    ts = int(fields.get("ts", 0))

    try:
        candle_data = json.loads(fields.get("payload", "{}"))
    except json.JSONDecodeError as exc:
        logger.error("Ошибка парсинга payload для %s@%s: %s", symbol, tf, exc)
        return

    try:
        h = float(candle_data["high"])
        lo = float(candle_data["low"])
        c = float(candle_data["close"])
    except (KeyError, ValueError, TypeError) as exc:
        logger.error("Ошибка OHLC для %s@%s: %s", symbol, tf, exc)
        return

    if not symbol or h <= 0 or lo <= 0 or c <= 0:
        return

    key = (symbol, tf)
    if key not in states:
        states[key] = {
            "w": WilderState(),
            "prev": {"adx": None, "atrPct": None},
            "prevCandle": None,
        }

    prev_candle = states[key]["prevCandle"]
    if prev_candle:
        ph, pl, pc = prev_candle["h"], prev_candle["l"], prev_candle["c"]
    else:
        ph, pl, pc = h, lo, c

    states[key]["prevCandle"] = {"h": h, "l": lo, "c": c}

    state, res = update_adx_atr(states[key]["w"], h, lo, c, ph, pl, pc, n=WILDER_N)
    if not res:
        return

    atr_pct = res["atr"] / c if c else 0.0
    q = load_quantiles(symbol, tf)

    regime, adx_slope, atrp_slope = classify_regime(
        res["adx"],
        states[key]["prev"]["adx"],
        atr_pct,
        states[key]["prev"]["atrPct"],
        res["plusDI"],
        res["minusDI"],
        q,
    )
    states[key]["prev"] = {"adx": res["adx"], "atrPct": atr_pct}

    payload = {
        "symbol": symbol,
        "timeframe": tf,
        "ts_event_ms": ts,
        "atr": res["atr"],
        "atrPct": atr_pct,
        "plusDI": res["plusDI"],
        "minusDI": res["minusDI"],
        "adx": res["adx"],
        "adxSlope": adx_slope,
        "atrPctSlope": atrp_slope,
        "regime": regime,
        "confidence": confidence(regime, res["adx"], q),
    }

    try:
        rclient_write.xadd(STREAM_OUT, {"data": json.dumps(payload)}, maxlen=1000, approximate=True)
        rclient_write.set(f"regime:{symbol}", regime, ex=300)
    except Exception as exc:
        logger.error("Ошибка публикации в %s: %s", STREAM_OUT, exc)


# ---------------------------------------------------------------------------
# Retry-safe xack
# ---------------------------------------------------------------------------
_XACK_RETRIES = 3
_XACK_BACKOFF_BASE = 1  # seconds


def _safe_xack(client: redis.Redis, stream: str, group: str, msg_id: str) -> None:
    """xack with retry on transient connection errors."""
    for attempt in range(1, _XACK_RETRIES + 1):
        try:
            client.xack(stream, group, msg_id)
            return
        except (redis.exceptions.ConnectionError, ConnectionResetError, OSError) as exc:
            if attempt == _XACK_RETRIES:
                logger.error("xack failed after %d attempts for %s: %s", _XACK_RETRIES, msg_id, exc)
                return  # give up — message will be redelivered via PEL
            wait = _XACK_BACKOFF_BASE * (2 ** (attempt - 1))
            logger.debug(
                "xack attempt %d/%d failed for %s: %s — retrying in %ds",
                attempt, _XACK_RETRIES, msg_id, exc, wait,
            )
            time.sleep(wait)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def run() -> None:
    """Основной цикл: читаем из `candles:data` через XREADGROUP."""
    ensure_consumer_groups()

    logger.info("Regime Worker запущен")
    logger.info("  Читаем из: %s (redis:%s)", STREAM_IN, REDIS_READ_PORT)
    logger.info("  Поддерживаемые таймфреймы: %s", sorted(SUPPORTED_TIMEFRAMES))
    logger.info(
        "  Пишем в: %s (redis-worker-1:%s)", STREAM_OUT, REDIS_WRITE_PORT
    )
    logger.info("  Consumer group: %s / %s", CONSUMER_GROUP, CONSUMER_NAME)

    processed = 0
    skipped = 0

    while not _shutdown:
        try:
            messages = rclient_read.xreadgroup(
                CONSUMER_GROUP,
                CONSUMER_NAME,
                {STREAM_IN: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS,
            )

            if not messages:
                continue

            for stream_name, stream_messages in messages:
                for message_id, fields in stream_messages:
                    try:
                        tf = fields.get("tf", "")
                        if tf in SUPPORTED_TIMEFRAMES:
                            process_kline(fields)
                            processed += 1
                        else:
                            skipped += 1

                        _safe_xack(rclient_read, stream_name, CONSUMER_GROUP, message_id)

                    except redis.exceptions.ConnectionError as exc:
                        logger.error(
                            "Ошибка соединения при обработке %s: %s", message_id, exc,
                        )
                        _safe_xack(rclient_read, stream_name, CONSUMER_GROUP, message_id)

                    except Exception as exc:
                        logger.error(
                            "Ошибка обработки сообщения %s: %s\n%s",
                            message_id,
                            exc,
                            traceback.format_exc(),
                        )
                        # Подтверждаем даже ошибочные сообщения, чтобы не застревать
                        _safe_xack(rclient_read, stream_name, CONSUMER_GROUP, message_id)

        except redis.exceptions.ConnectionError as exc:
            logger.error("Ошибка подключения к Redis: %s", exc)
            time.sleep(5)

        except Exception as exc:
            logger.error("Неожиданная ошибка: %s", exc)
            if "NOGROUP" in str(exc).upper():
                logger.warning("Обнаружен NOGROUP, пересоздаём consumer group…")
                ensure_consumer_groups()
            time.sleep(1)

    logger.info(
        "Regime Worker остановлен. Обработано: %d, пропущено: %d", processed, skipped
    )


if __name__ == "__main__":
    run()
