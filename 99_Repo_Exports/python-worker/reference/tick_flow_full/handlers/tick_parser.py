from __future__ import annotations

"""
### 6.1 Юнит-тесты (детерминизм) — базовые примитивы парсинга/дельты

Этот модуль намеренно делает парсинг *чистым* и тестируемым:
  - parse_tick(raw, now_ms=...) НЕ читает время сам, если now_ms передан
  - детектит seconds vs ms по порогу 1e12 (Binance/крипта стандартно в ms)
  - поддерживает "json-in-data", flat dict и nested dict
  - аккуратно фейлится на bad types (возвращает None)

Почему отдельно от handler:
  - юнит-тесты не должны поднимать handler с Redis/сервисами.
  - один детерминированный контракт парсинга снижает "магические" баги.
"""

import json
import math
from dataclasses import dataclass
from typing import Any, Optional, Mapping


# Флаги (как вы описывали): 1=trade, 2=buy, 4=sell
FLAG_TRADE = 1
FLAG_BUY = 2
FLAG_SELL = 4


@dataclass(slots=True)
class Tick:
    """
    Минимальная модель тика для ваших пайплайнов.
    Если в проекте уже есть Tick — можно:
      - либо заменить импорт в handler на ваш общий Tick,
      - либо оставить этот класс только для тестов/парсера (не экспортировать наружу).
    """
    ts: int                 # timestamp в ms (после нормализации)
    bid: float
    ask: float
    last: float
    volume: float
    flags: int
    is_buyer_maker: Optional[bool] = None
    raw: Optional[dict[str, Any]] = None  # debug: небольшой сыро-слой (НЕ кладите сюда огромные payloads)


def _isfinite(x: float) -> bool:
    return not (math.isnan(x) or math.isinf(x))


def _to_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, (int,)):
            return int(x)
        if isinstance(x, float):
            if not _isfinite(float(x)):
                return None
            return int(x)
        if isinstance(x, str):
            s = x.strip()
            if not s:
                return None
            # допускаем "1700.0"
            v = float(s)
            if not _isfinite(v):
                return None
            return int(v)
        return None
    except Exception:
        return None


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return float(int(x))
        if isinstance(x, (int, float)):
            v = float(x)
            if not _isfinite(v):
                return None
            return v
        if isinstance(x, str):
            s = x.strip()
            if not s:
                return None
            v = float(s)
            if not _isfinite(v):
                return None
            return v
        return None
    except Exception:
        return None


def normalize_ts_ms(ts: Any) -> Optional[int]:
    """
    Нормализация времени:
      - если < 1e12 -> секунды -> *1000
      - иначе считаем, что это ms
    """
    v = _to_int(ts)
    if v is None:
        return None
    # 1e12 ~ 2001-09-09 в ms. Любой нормальный ms-timestamp сейчас > 1e12.
    if v < 1_000_000_000_000:
        return int(v * 1000)
    return int(v)


def _decode_json_maybe(s: Any) -> Any:
    """
    Если поле содержит JSON-строку, аккуратно декодируем.
    Важно: если строка не JSON — возвращаем как есть.
    """
    if not isinstance(s, str):
        return s
    t = s.strip()
    if not t:
        return s
    if (t.startswith("{") and t.endswith("}")) or (t.startswith("[") and t.endswith("]")):
        try:
            return json.loads(t)
        except Exception:
            return s
    return s


def _dig(d: Mapping[str, Any], *keys: str) -> Any:
    """
    Пробуем достать значение по одному из ключей (первый найденный).
    """
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def _unwrap_raw(raw: Any) -> Any:
    """
    Поддерживаем варианты:
      1) raw = dict тик
      2) raw = {"data": "<json-string>"}  или {"data": {...}}
      3) raw = {"data": {"tick": {...}}} / {"tick": {...}}
    """
    if isinstance(raw, str):
        return _decode_json_maybe(raw)
    if not isinstance(raw, dict):
        return raw

    # 1) "data" слой (Redis-stream часто так приносит)
    data = raw.get("data")
    if data is not None:
        data = _decode_json_maybe(data)
        if isinstance(data, dict):
            # иногда data содержит tick внутри
            t = data.get("tick")
            if isinstance(t, dict):
                return t
            return data
        # если data не dict — оставим raw как есть (вдруг flat)

    # 2) nested "tick"
    t2 = raw.get("tick")
    if isinstance(t2, dict):
        return t2
    return raw


def parse_tick(raw: Any, *, now_ms: Optional[int] = None) -> Optional[Tick]:
    """
    Возвращает Tick или None (если вход некорректный).

    now_ms нужен ТОЛЬКО если у входа вообще нет ts (для тестов лучше всегда задавать ts и now_ms).
    """
    u = _unwrap_raw(raw)
    if isinstance(u, Tick):
        return u
    if not isinstance(u, dict):
        return None

    # Поддерживаем распространённые схемы:
    # - "ts" / "T" (Binance trade time) / "E" event time
    ts = normalize_ts_ms(_dig(u, "ts", "T", "E", "time", "timestamp"))
    if ts is None:
        # детерминизм: если now_ms не передан — не угадываем время
        if now_ms is None:
            return None
        ts = int(now_ms)

    bid = _to_float(_dig(u, "bid", "b"))
    ask = _to_float(_dig(u, "ask", "a"))
    last = _to_float(_dig(u, "last", "p", "price"))
    vol = _to_float(_dig(u, "volume", "q", "qty", "amount"))

    # flags могут быть переданы напрямую, или мы выводим их из типа события
    flags = _to_int(_dig(u, "flags")) or 0

    # is_buyer_maker (Binance: "m": True => buyer is maker => агрессор SELL)
    ibm = u.get("is_buyer_maker")
    if ibm is None:
        ibm = u.get("m")
    if isinstance(ibm, str):
        s = ibm.strip().lower()
        if s in {"true", "1", "yes"}:
            ibm = True
        elif s in {"false", "0", "no"}:
            ibm = False
        else:
            ibm = None
    if not isinstance(ibm, bool):
        ibm = None

    # Если bid/ask не пришли, но last есть — ставим bid=ask=last (упрощение для downstream).
    # Важно для детерминизма: не пытаемся "доставать" из внешних сервисов.
    if bid is None or ask is None:
        if last is not None and last > 0:
            bid = float(last)
            ask = float(last)
        else:
            # без bid/ask/last тик бесполезен
            return None

    # last fallback: mid
    if last is None:
        last = float((bid + ask) / 2.0)

    # volume fallback:
    # - bookTicker (без объёма) -> volume=0, flags без TRADE
    # - trade tick без volume -> считаем некорректным (volume=0 и TRADE не ставим)
    if vol is None:
        vol = 0.0

    # Определяем trade/non-trade:
    # 1) если flags уже содержит TRADE — оставляем
    # 2) иначе, если есть признаки Binance aggTrade/trade (p,q,m) -> TRADE
    if (flags & FLAG_TRADE) == 0:
        # эвристика: если пришли price(p/last) и qty(q/volume) и есть m/is_buyer_maker -> это trade
        if (("p" in u or "last" in u or "price" in u) and ("q" in u or "volume" in u or "qty" in u)) and (("m" in u) or ("is_buyer_maker" in u)):
            flags |= FLAG_TRADE

    # Сторона сделки:
    # - если явно проставлены buy/sell флаги — доверяем
    # - иначе выводим из is_buyer_maker (True => taker sell => FLAG_SELL)
    if (flags & FLAG_TRADE) != 0 and (flags & (FLAG_BUY | FLAG_SELL)) == 0:
        if ibm is True:
            flags |= FLAG_SELL
        elif ibm is False:
            flags |= FLAG_BUY

    # Санити-чек чисел
    bid_f = float(bid)
    ask_f = float(ask)
    last_f = float(last)
    vol_f = float(vol)
    if not (_isfinite(bid_f) and _isfinite(ask_f) and _isfinite(last_f) and _isfinite(vol_f)):
        return None
    if bid_f <= 0 or ask_f <= 0 or last_f <= 0:
        return None

    return Tick(
        ts=int(ts),
        bid=bid_f,
        ask=ask_f,
        last=last_f,
        volume=max(0.0, vol_f),
        flags=int(flags),
        is_buyer_maker=ibm,
        raw=None,  # оставляем пустым: в outbox/логи это не должно утечь
    )


def classify_delta(tick: Tick) -> float:
    """
    ### 6.1: _classify_delta
    Требования:
      - bookTicker (без volume / не TRADE) -> 0
      - trade tick -> signed vol
    """
    if (tick.flags & FLAG_TRADE) == 0:
        return 0.0
    v = float(tick.volume or 0.0)
    if v <= 0:
        return 0.0

    # 1) Явные флаги имеют приоритет
    if (tick.flags & FLAG_BUY) != 0 and (tick.flags & FLAG_SELL) == 0:
        return +v
    if (tick.flags & FLAG_SELL) != 0 and (tick.flags & FLAG_BUY) == 0:
        return -v

    # 2) Если флагов нет — выводим из is_buyer_maker
    # Binance: isBuyerMaker=True => taker sell => negative
    if tick.is_buyer_maker is True:
        return -v
    if tick.is_buyer_maker is False:
        return +v

    return 0.0
