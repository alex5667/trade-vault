"""
Tick Ingest Server v2 - FastAPI сервис для приема тиков и DOM от MT5 EA через HTTP.

ФУНКЦИОНАЛ:
- Прием POST запросов от MQL5 EA с тиковыми данными (/tick)
- Прием POST запросов с Order Book данными (/book) - NEW v2
- Валидация структуры данных
- Публикация в Redis Streams (stream:tick_XAUUSD, stream:book_XAUUSD)
- Кеширование последнего DOM snapshot (book:latest:<SYMBOL>)
- Поддержка DualRedisClient для устойчивости
- Метрики и мониторинг

ИНТЕГРАЦИЯ:
- MT5 (Wine) → TickBridge EA → HTTP POST /tick → Redis Stream
- MT5 (Wine) → BookBridge EA → HTTP POST /book → Redis + Cache
- Используется вашей существующей потоковой архитектурой
- Consumer groups читают из streams

ЗАПУСК:
    uvicorn services.tick_ingest_server:app --host 0.0.0.0 --port 8088

Docker:
    См. docker-compose.yml секцию tick-ingest-server
"""

import os
import sys
import json
import time
from typing import Dict, Any
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import redis

# Добавляем путь к core для импорта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from core.dual_redis_client import get_dual_signals_redis
    from core.ticks_redis_client import get_dual_ticks_redis
    from core.config import (
        XAU_TICK_STREAM,
        XAU_TICK_STREAM_MAXLEN,
        XAU_BOOK_STREAM,
        XAU_BOOK_STREAM_MAXLEN,
    )
    USE_DUAL = True
    USE_TICKS_REDIS = True
except ImportError:
    USE_DUAL = False
    USE_TICKS_REDIS = False
    print("⚠️ DualRedisClient не найден, используем обычный Redis")

# Конфигурация
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
REDIS_TICKS_URL = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")
STREAM = XAU_TICK_STREAM
BOOK_STREAM = XAU_BOOK_STREAM
MAXLEN = XAU_TICK_STREAM_MAXLEN
BOOK_MAXLEN = XAU_BOOK_STREAM_MAXLEN
USE_MAXLEN = os.getenv("XAU_TICK_USE_MAXLEN", "true").lower() in ("true", "1", "yes")
ALLOW_SYMBOLS = {sym.strip().upper() for sym in os.getenv("ALLOW_SYMBOLS", "XAUUSD,BTCUSDT,ETHUSDT").split(",") if sym.strip()}




CRYPTO_TICK_MAXLEN = int(os.getenv("CRYPTO_TICK_STREAM_MAXLEN", str(MAXLEN)))
CRYPTO_BOOK_MAXLEN = int(os.getenv("CRYPTO_BOOK_STREAM_MAXLEN", "100000"))
from services.price_latest_cache import write_price_latest

CRYPTO_SOURCE_WHITELIST = {
    src.strip().lower()
    for src in os.getenv("CRYPTO_TICK_SOURCES", "binance-futures,binance-futures-testnet").split(",")
    if src.strip()
}

# Метрики
stats = {
    "total_ticks": 0,
    "total_books": 0,
    "errors": 0,
    "started_at": time.time(),
    "last_tick_ts": 0,
    "last_book_ts": 0
}

# FastAPI приложение
app = FastAPI(
    title="XAU Tick/Book Ingest Server",
    description="Прием тиков и Order Book от MT5 EA и публикация в Redis Streams",
    version="2.0.0"
)

# Redis клиенты - ✅ LAZY INITIALIZATION для избежания рекурсии при старте
# 🎯 Для тиков и книг используем redis-ticks (DualTicksRedisClient)
# Это обеспечивает запись в scanner-redis-ticks с fallback на основной Redis
ticks_redis_client = None
redis_client = None

def get_ticks_client():
    """Lazy initialization для ticks Redis client"""
    global ticks_redis_client
    if ticks_redis_client is None:
        if USE_TICKS_REDIS:
            # ✅ Retry логика для избежания рекурсии
            max_retries = 10
            retry_delay = 2.0
            for attempt in range(max_retries):
                try:
                    ticks_redis_client = get_dual_ticks_redis()
                    # Проверяем подключение
                    ticks_redis_client.primary.ping()
                    print("✅ Используется DualTicksRedisClient для записи в redis-ticks")
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"⚠️ Ошибка подключения к redis-ticks (попытка {attempt+1}/{max_retries}): {e}")
                        print(f"   Повторная попытка через {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 1.2, 10.0)
                    else:
                        print(f"❌ Не удалось подключиться к redis-ticks после {max_retries} попыток: {e}")
                        # Fallback на обычный клиент
                        ticks_redis_client = redis.from_url(REDIS_TICKS_URL, decode_responses=True)
                        print("⚠️ Используется обычный Redis клиент для тиков (fallback)")
        else:
            ticks_redis_client = redis.from_url(REDIS_TICKS_URL, decode_responses=True)
            print("⚠️ Используется обычный Redis клиент для тиков")
    return ticks_redis_client

def get_signals_client():
    """Lazy initialization для signals Redis client"""
    global redis_client
    if redis_client is None:
        if USE_DUAL:
            # ✅ Retry логика для избежания рекурсии
            max_retries = 10
            retry_delay = 2.0
            for attempt in range(max_retries):
                try:
                    redis_client = get_dual_signals_redis()
                    # Проверяем подключение
                    redis_client.ping()
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"⚠️ Ошибка подключения к redis-signals (попытка {attempt+1}/{max_retries}): {e}")
                        print(f"   Повторная попытка через {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 1.2, 10.0)
                    else:
                        print(f"❌ Не удалось подключиться к redis-signals после {max_retries} попыток: {e}")
                        # Fallback на обычный клиент
                        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        else:
            redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    return redis_client

print("✅ Tick Ingest Server v2 инициализирован")
print(f"   Redis: {REDIS_URL}")
print(f"   Tick Stream: {STREAM}")
print(f"   Book Stream: {BOOK_STREAM}")

print(f"   MAXLEN: {MAXLEN if USE_MAXLEN else 'disabled (batch trimmer)'}")
print(f"   Allowed symbols: {ALLOW_SYMBOLS}")
sys.stdout.flush()


@app.post("/tick")
async def receive_tick(request: Request) -> Dict[str, Any]:
    """
    Прием тика от MT5 EA.
    
    Ожидаемый JSON:
    {
        "ts": 1698765432000,      // timestamp в миллисекундах
        "bid": 1880.50,           // bid цена
        "ask": 1880.75,           // ask цена
        "last": 1880.60,          // last цена сделки
        "volume": 10.5,           // объем
        "flags": 2,               // флаги направления
        "symbol": "XAUUSD"        // символ
    }
    """
    global stats
    
    try:
        # Парсим JSON
        try:
            data = await request.json()
        except Exception as e:
            stats["errors"] += 1
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")
        
        # Валидация символа
        symbol = str(data["symbol"]).upper()
        if ALLOW_SYMBOLS and symbol not in ALLOW_SYMBOLS:
            stats["errors"] += 1
            raise HTTPException(
                status_code=403,
                detail=f"Symbol {symbol} not allowed"
            )
        
        source = str(data.get("source", "")).lower()
        market = str(data.get("market", "")).upper()
        is_crypto = (source in CRYPTO_SOURCE_WHITELIST) or market in {"USDT-M", "CRYPTO"}

        if is_crypto:
            required_crypto_fields = ["ts", "symbol", "price", "qty", "side"]
            missing = [f for f in required_crypto_fields if f not in data]
            if missing:
                stats["errors"] += 1
                raise HTTPException(
                    status_code=422,
                    detail=f"Missing required fields: {', '.join(missing)}"
                )

            try:
                ts_val = int(data["ts"])
            except (TypeError, ValueError):
                stats["errors"] += 1
                raise HTTPException(status_code=422, detail="Invalid ts value")

            side = str(data["side"]).upper()
            if side not in {"BUY", "SELL"}:
                stats["errors"] += 1
                raise HTTPException(status_code=422, detail="side must be BUY or SELL")

            normalized_tick = {
                "symbol": symbol,
                "ts": ts_val,
                "price": str(data["price"]),
                "qty": str(data["qty"]),
                "side": side,
                "source": data.get("source", "binance-futures"),
                "market": data.get("market", "USDT-M"),
            }
            if "trade_id" in data:
                normalized_tick["trade_id"] = str(data["trade_id"])
            if "written_at" in data:
                normalized_tick["written_at"] = str(data["written_at"])
            # Optional: external CVD level (USD/notional) from upstream source
            # If provided, enables strict two-baseline detection in tick_cvd.py
            # If not provided, Python will use fallback delta-based jump detection
            if "cvd_usd" in data or "cvd_notional" in data or "cvd_tick_usd" in data:
                cvd_val = data.get("cvd_usd") or data.get("cvd_notional") or data.get("cvd_tick_usd")
                try:
                    normalized_tick["cvd_usd"] = str(float(cvd_val))
                except (ValueError, TypeError):
                    pass

            payload = {key: str(value) for key, value in normalized_tick.items()}
            payload["data"] = json.dumps(normalized_tick)

            tick_stream = f"stream:tick_{symbol}"

            try:
                # 🎯 Используем ticks_redis_client для записи в redis-ticks
                get_ticks_client().xadd(
                    tick_stream,
                    payload,
                    maxlen=CRYPTO_TICK_MAXLEN,
                    approximate=True,
                )
            except Exception as e:  # noqa: BLE001
                stats["errors"] += 1
                print(f"❌ Ошибка публикации crypto тик в Redis: {e}")
                sys.stdout.flush()
                raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")

            # ---------------------------------------------------------------------
            # NEW: latest-price cache for cross-symbol features (SMT/coherence, drift alarm).
            # This must be fail-open and must not affect tick ingest reliability.
            # Key: price:latest:{SYMBOL}
            # ---------------------------------------------------------------------
            try:
                write_price_latest(
                    get_ticks_client(),
                    symbol=str(symbol),
                    ts_ms=int(ts_val),
                    bid=None,  # crypto ticks may not have bid/ask
                    ask=None,
                    last=float(data["price"]) if "price" in data else None,
                    mid=None,
                    venue=str(data.get("source", "na")),
                )
            except Exception:
                pass

            stats["total_ticks"] += 1
            stats["last_tick_ts"] = ts_val

            return {
                "ok": True,
                "stream": tick_stream,
                "symbol": symbol,
                "ts": ts_val,
            }

        # Forex/MT5 формат
        required_fields = ["ts", "bid", "ask", "last", "volume", "flags", "symbol"]
        missing = [f for f in required_fields if f not in data]
        
        if missing:
            stats["errors"] += 1
            raise HTTPException(
                status_code=422,
                detail=f"Missing required fields: {', '.join(missing)}"
            )
        
        try:
            # Валидация и коррекция timestamp
            raw_ts = int(data["ts"])
            current_ts_ms = int(time.time() * 1000)
            
            # Проверка на некорректный timestamp (в будущем или слишком старый)
            # Если timestamp в будущем более чем на 1 час - используем текущее время
            # Если timestamp старше 24 часов - используем текущее время
            if raw_ts > current_ts_ms + 3600000:  # > 1 час в будущем
                print(f"⚠️ Некорректный timestamp {raw_ts} (в будущем), используем текущее время")
                corrected_ts = current_ts_ms
            elif raw_ts < current_ts_ms - 86400000:  # > 24 часа назад
                print(f"⚠️ Некорректный timestamp {raw_ts} (слишком старый), используем текущее время")
                corrected_ts = current_ts_ms
            else:
                corrected_ts = raw_ts
            
            tick_data = {
                "ts": corrected_ts,
                "bid": float(data["bid"]),
                "ask": float(data["ask"]),
                "last": float(data["last"]),
                "volume": float(data["volume"]),
                "flags": int(data["flags"]),
                "symbol": symbol
            }
        except (ValueError, TypeError) as e:
            stats["errors"] += 1
            raise HTTPException(
                status_code=422,
                detail=f"Invalid field type: {str(e)}"
            )
        
        if tick_data["bid"] <= 0 or tick_data["ask"] <= 0:
            stats["errors"] += 1
            raise HTTPException(
                status_code=422,
                detail="Bid and Ask must be positive"
            )
        
        if tick_data["ask"] < tick_data["bid"]:
            stats["errors"] += 1
            raise HTTPException(
                status_code=422,
                detail="Ask must be >= Bid"
            )
        
        try:
            # 🎯 Для XAUUSD используем ticks_redis_client для записи в redis-ticks
            # Формируем stream name динамически для поддержки разных символов
            tick_stream_name = f"stream:tick_{symbol}" if symbol != "XAUUSD" else STREAM
            payload = {"data": json.dumps(tick_data)}
            
            if USE_MAXLEN:
                get_ticks_client().xadd(
                    tick_stream_name,
                    payload,
                    maxlen=MAXLEN,
                    approximate=True
                )
            else:
                get_ticks_client().xadd(tick_stream_name, payload, maxlen=50000)




            # ---------------------------------------------------------------------
            # NEW: latest-price cache for cross-symbol features (SMT/coherence, drift alarm).
            # This must be fail-open and must not affect tick ingest reliability.
            # Key: price:latest:{SYMBOL}
            # ---------------------------------------------------------------------
            try:
                # venue is best-effort; default "mt5" for this ingest
                venue = (os.getenv("TICKS_VENUE", "mt5") or "mt5").strip().lower()
                write_price_latest(
                    get_ticks_client(),
                    symbol=str(tick_data["symbol"]),
                    ts_ms=int(tick_data["ts"]),
                    bid=float(tick_data["bid"]),
                    ask=float(tick_data["ask"]),
                    last=float(tick_data["last"]),
                    mid=None,
                    venue=venue,
                )
            except Exception:
                pass

            stats["total_ticks"] += 1
            stats["last_tick_ts"] = tick_data["ts"]
            
            return {
                "ok": True,
                "stream": STREAM,
                "symbol": tick_data["symbol"],
                "ts": tick_data["ts"]
            }
            
        except Exception as e:
            stats["errors"] += 1
            print(f"❌ Ошибка публикации в Redis: {e}")
            sys.stdout.flush()
            raise HTTPException(
                status_code=500,
                detail=f"Redis error: {str(e)}"
            )
    
    except HTTPException:
        raise
    except Exception as e:
        stats["errors"] += 1
        print(f"❌ Неожиданная ошибка: {e}")
        sys.stdout.flush()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint для мониторинга.
    """
    try:
        # Проверка подключения к Redis
        redis_ok = get_signals_client().ping()
        
        return {
            "status": "healthy",
            "redis": "ok" if redis_ok else "error",
            "stream": STREAM,
            "uptime_seconds": int(time.time() - stats["started_at"])
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e)
            }
        )


@app.post("/book")
async def receive_book(request: Request) -> Dict[str, Any]:
    """
    Прием Order Book snapshot от MT5 EA.
    
    Ожидаемый JSON:
    {
        "ts": 1698765432000,      // timestamp в миллисекундах
        "symbol": "XAUUSD",       // символ
        "bids": [                 // bid уровни (сортированы по убыванию цены)
            [1880.50, 100.5],     // [price, volume]
            [1880.45, 50.0],
            ...
        ],
        "asks": [                 // ask уровни (сортированы по возрастанию цены)
            [1880.75, 80.0],
            [1880.80, 60.0],
            ...
        ]
    }
    """
    global stats
    
    try:
        # Парсим JSON
        try:
            data = await request.json()
        except Exception as e:
            stats["errors"] += 1
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")
        
        # Валидация символа
        symbol = str(data["symbol"]).upper()
        if ALLOW_SYMBOLS and symbol not in ALLOW_SYMBOLS:
            stats["errors"] += 1
            raise HTTPException(
                status_code=403,
                detail=f"Symbol {symbol} not allowed"
            )

        source = str(data.get("source", "")).lower()
        market = str(data.get("market", "")).upper()
        is_crypto = (source in CRYPTO_SOURCE_WHITELIST) or market in {"USDT-M", "CRYPTO"}

        required_fields = ["ts", "symbol", "bids", "asks"]
        missing = [f for f in required_fields if f not in data]
        if missing:
            stats["errors"] += 1
            raise HTTPException(
                status_code=422,
                detail=f"Missing required fields: {', '.join(missing)}"
            )
        
        if is_crypto:
            try:
                ts_val = int(data["ts"])
            except (TypeError, ValueError):
                stats["errors"] += 1
                raise HTTPException(status_code=422, detail="Invalid ts value")

            first_id = data.get("first_id") or data.get("firstId") or data.get("U")
            final_id = data.get("final_id") or data.get("finalId") or data.get("u")
            prev_final = data.get("prev_final") or data.get("prevFinal") or data.get("pu")

            def _safe_int_field(value):
                if value is None:
                    return None
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return None

            book_snapshot = {
                "symbol": symbol,
                "ts": ts_val,
                "source": data.get("source", "binance-futures"),
                "market": data.get("market", "USDT-M"),
            }

            first_id_int = _safe_int_field(first_id)
            final_id_int = _safe_int_field(final_id)
            prev_final_int = _safe_int_field(prev_final)

            if first_id is not None:
                book_snapshot["first_id_raw"] = first_id
            if final_id is not None:
                book_snapshot["final_id_raw"] = final_id
            if prev_final is not None:
                book_snapshot["prev_final_raw"] = prev_final

            if first_id_int is not None:
                book_snapshot["first_id"] = first_id_int
                book_snapshot["U"] = first_id_int
            if final_id_int is not None:
                book_snapshot["final_id"] = final_id_int
                book_snapshot["u"] = final_id_int
            if prev_final_int is not None:
                book_snapshot["prev_final"] = prev_final_int
                book_snapshot["pu"] = prev_final_int

            bids_raw = data["bids"] or []
            asks_raw = data["asks"] or []
            book_snapshot["bids"] = json.dumps(bids_raw)
            book_snapshot["asks"] = json.dumps(asks_raw)

            payload = {key: str(value) for key, value in book_snapshot.items()}
            payload["data"] = json.dumps({
                **book_snapshot,
                "bids": bids_raw,
                "asks": asks_raw,
            })

            book_stream = f"stream:book_{symbol}"
            cache_key = f"book:latest:{symbol}"

            try:
                # 🎯 Используем ticks_redis_client для записи в redis-ticks
                get_ticks_client().set(cache_key, payload["data"])
                get_ticks_client().xadd(
                    book_stream,
                    payload,
                    maxlen=CRYPTO_BOOK_MAXLEN,
                    approximate=True,
                )
            except Exception as e:  # noqa: BLE001
                stats["errors"] += 1
                print(f"❌ Ошибка публикации crypto Order Book в Redis: {e}")
                sys.stdout.flush()
                raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")

            stats["total_books"] += 1
            stats["last_book_ts"] = ts_val

            return {
                "ok": True,
                "stream": book_stream,
                "cache_key": cache_key,
                "symbol": symbol,
                "ts": ts_val,
                "depth": {
                    "bids": len(bids_raw),
                    "asks": len(asks_raw)
                }
            }

        # Forex/MT5 формат
        def validate_levels(levels, name):
            if not isinstance(levels, list):
                return f"{name} must be a list"
            for item in levels:
                if not isinstance(item, (list, tuple)) or len(item) != 2:
                    return f"{name} items must be [price, volume] pairs"
                try:
                    float(item[0])
                    float(item[1])
                except (ValueError, TypeError):
                    return f"{name} must contain numeric price/volume"
            return None
        
        error_msg = validate_levels(data["bids"], "bids")
        if error_msg:
            stats["errors"] += 1
            raise HTTPException(status_code=422, detail=error_msg)
        
        error_msg = validate_levels(data["asks"], "asks")
        if error_msg:
            stats["errors"] += 1
            raise HTTPException(status_code=422, detail=error_msg)
        
        book_data = {
            "ts": int(data["ts"]),
            "symbol": symbol,
            "bids": [[float(p), float(v)] for p, v in data["bids"]],
            "asks": [[float(p), float(v)] for p, v in data["asks"]]
        }
        
        try:
            book_json = json.dumps(book_data)
            
            cache_key = f"book:latest:{symbol}"
            # 🎯 Используем ticks_redis_client для записи в redis-ticks
            get_ticks_client().set(cache_key, book_json)
            
            # Формируем stream name динамически для поддержки разных символов
            book_stream = f"stream:book_{symbol}" if symbol != "XAUUSD" else BOOK_STREAM
            payload = {"data": book_json}
            
            if USE_MAXLEN:
                get_ticks_client().xadd(
                    book_stream,
                    payload,
                    maxlen=BOOK_MAXLEN,
                    approximate=True
                )
            else:
                get_ticks_client().xadd(book_stream, payload, maxlen=50000)
            
            stats["total_books"] += 1
            stats["last_book_ts"] = book_data["ts"]
            
            return {
                "ok": True,
                "stream": book_stream,
                "cache_key": cache_key,
                "symbol": symbol,
                "ts": book_data["ts"],
                "depth": {
                    "bids": len(book_data["bids"]),
                    "asks": len(book_data["asks"])
                }
            }
            
        except Exception as e:
            stats["errors"] += 1
            print(f"❌ Ошибка публикации Order Book в Redis: {e}")
            sys.stdout.flush()
            raise HTTPException(
                status_code=500,
                detail=f"Redis error: {str(e)}"
            )
    
    except HTTPException:
        raise
    except Exception as e:
        stats["errors"] += 1
        print(f"❌ Неожиданная ошибка при обработке Order Book: {e}")
        sys.stdout.flush()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """
    Статистика работы сервера.
    """
    uptime = time.time() - stats["started_at"]
    tick_rate = stats["total_ticks"] / uptime if uptime > 0 else 0
    book_rate = stats["total_books"] / uptime if uptime > 0 else 0
    
    # Последний тик timestamp
    last_tick_ago = None
    if stats["last_tick_ts"] > 0:
        last_tick_ago = int(time.time() * 1000 - stats["last_tick_ts"]) / 1000
    
    # Последний book timestamp
    last_book_ago = None
    if stats["last_book_ts"] > 0:
        last_book_ago = int(time.time() * 1000 - stats["last_book_ts"]) / 1000
    
    # Длина стримов в Redis
    try:
        client = get_signals_client()
        tick_stream_len = client.client_1.xlen(STREAM) if USE_DUAL and hasattr(client, 'client_1') else client.xlen(STREAM)
    except Exception:  # best-effort: don't fail /stats if Redis is down
        tick_stream_len = None
    
    try:
        client = get_signals_client()
        book_stream_len = client.client_1.xlen(BOOK_STREAM) if USE_DUAL and hasattr(client, 'client_1') else client.xlen(BOOK_STREAM)
    except Exception:  # best-effort: don't fail /stats if Redis is down
        book_stream_len = None
    
    return {
        "total_ticks": stats["total_ticks"],
        "total_books": stats["total_books"],
        "errors": stats["errors"],
        "uptime_seconds": int(uptime),
        "ticks_per_second": round(tick_rate, 2),
        "books_per_second": round(book_rate, 2),
        "last_tick_ago_seconds": round(last_tick_ago, 2) if last_tick_ago else None,
        "last_book_ago_seconds": round(last_book_ago, 2) if last_book_ago else None,
        "tick_stream": STREAM,
        "book_stream": BOOK_STREAM,
        "tick_stream_length": tick_stream_len,
        "book_stream_length": book_stream_len,
        "started_at": datetime.fromtimestamp(stats["started_at"], tz=timezone.utc).isoformat()
    }


@app.get("/")
async def root():
    """
    Корневой endpoint с информацией о сервисе.
    """
    return {
        "service": "XAU Tick/Book Ingest Server",
        "version": "2.0.0",
        "endpoints": {
            "POST /tick": "Прием тика от MT5 EA (TickBridge)",
            "POST /book": "Прием Order Book от MT5 EA (BookBridge)",
            "GET /health": "Health check",
            "GET /stats": "Статистика",
            "GET /": "Эта информация"
        },
        "streams": {
            "ticks": STREAM,
            "books": BOOK_STREAM
        },
        "cache": "book:latest:<SYMBOL>",
        "status": "running"
    }


if __name__ == "__main__":
    import uvicorn
    
    # Запуск сервера
    host = os.getenv("TICK_INGEST_HOST", "0.0.0.0")
    port = int(os.getenv("TICK_INGEST_PORT", "8088"))
    
    print(f"🚀 Запуск Tick Ingest Server на {host}:{port}")
    sys.stdout.flush()
    
    uvicorn.run(app, host=host, port=port, log_level="info")

