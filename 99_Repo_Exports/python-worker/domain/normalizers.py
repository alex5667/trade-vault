# domain/normalizers.py
from __future__ import annotations
from typing import Any, Dict, List


_CRYPTO_SOURCE_ALIASES = {
    "cryptoorderflow": "CryptoOrderFlow"
    "crypto-orderflow": "CryptoOrderFlow"
    "orderflow": "OrderFlow"
    "technicalanalysis": "TechnicalAnalysis"
    "ta": "TechnicalAnalysis"
    "aggregatedhub-v2": "AggregatedHub-V2"
    "aggregated": "AggregatedHub-V2"
}

_STRATEGY_BY_SOURCE = {
    "cryptoorderflow": "cryptoorderflow"
    "crypto-orderflow": "cryptoorderflow"
    "orderflow": "orderflow"
    "technicalanalysis": "ta"
    "ta": "ta"
    "aggregatedhub-v2": "aggregated"
    "aggregated": "aggregated"
}

_REVERSE_SOURCE_ALIASES = {
    "cryptoorderflow": "CryptoOrderFlow"
    "orderflow": "OrderFlow"
    "ta": "TechnicalAnalysis"
    "aggregated": "AggregatedHub-V2"
}

def source_from_strategy(strategy: str, fallback_source: str) -> str:
    sl = (strategy or "").strip().lower()
    return _REVERSE_SOURCE_ALIASES.get(sl, fallback_source)

#
# --------------------------
# Timeframe canonicalization
# --------------------------
#
# В проде у вас встречаются варианты TF:
#   tick, 1m, 5m, 15m, 1h, 4h, 1d, 1w, 1month
# плюс легаси формы:
#   M1/M5 (и после .lower() -> m1/m5)
#
# Историческая проблема:
#   старый canon_tf() делал только .lower()
#   => "M1" превращался в "m1" и расходился с "1m"
#   => ключи Redis:
#        closed:...:m1:...
#      vs closed:...:1m:...
#      могли быть параллельными
#      и periodic_reporter/аналитика "теряли" часть сделок.
#
# Решение:
#   1) canon_tf() приводит к канону: "1m", "5m", ...
#   2) tf_variants() возвращает список вариантов ключей (канон + легаси)
#      чтобы писать индексы/листы в оба ключа
#      читать из обоих ключей без миграций.
#
_TF_ALIASES: Dict[str, str] = {
    # легаси "m1/m5" -> канонические
    "m1": "1m"
    "m5": "5m"
    "m15": "15m"
    "h1": "1h"
    "h4": "4h"
    "d1": "1d"
    "w1": "1w"
    # варианты month
    "month": "1month"
    "mo": "1month"
    "1mo": "1month"
    # текстовые минуты
    "1min": "1m"
    "5min": "5m"
    "15min": "15m"
    # секунды (иногда встречается в конфиге/логике)
    "60s": "1m"
    "300s": "5m"
    "900s": "15m"
}

# нормализованные категории для отчётности/статистики
_TP_BUCKET = {"TP1", "TP2", "TP3", "TP", "TAKE_PROFIT", "MANUAL_TP"}
_SL_BUCKET = {"SL", "STOP", "STOP_LOSS", "LIQUIDATION", "ADL", "FORCE_CLOSE", "SL_AFTER_TP1", "SL_AFTER_TP2", "SL_AFTER_TP3", "INITIAL_SL"}
_TRAIL_BUCKET = {"TRAILING_STOP", "TRAIL", "TRAILING", "TRAILING_STOP_HIT", "TRAILING_PROFIT", "TRAIL_SL"}


def canon_symbol(v) -> str:
    s = ("" if v is None else str(v)).strip().upper()
    return s or "UNKNOWN"


def canon_tf(v) -> str:
    s0 = ("" if v is None else str(v)).strip()
    if not s0:
        return "tick"
    s = s0.strip().lower()
    # нормализовать частую пунктуацию/пробелы
    s = s.replace(" ", "")
    # карта основных алиасов
    s = _TF_ALIASES.get(s, s)
    return s or "tick"


def canon_strategy(v) -> str:
    s = ("" if v is None else str(v)).strip().lower()
    return s or "unknown"


def canon_source(v) -> str:
    s = ("" if v is None else str(v)).strip()
    if not s:
        return "Unknown"
    sl = s.lower()
    return _CRYPTO_SOURCE_ALIASES.get(sl, s)


def tf_variants(v) -> List[str]:
    """
    Возвращает список TF-ключей (канон + легаси), чтобы:
      - писать индексы/листы в оба ключа
      - читать из обоих ключей без миграций.

    Пример:
      v="M1" -> ["1m", "m1"]
      v="1m" -> ["1m", "m1"]
      v="tick" -> ["tick"]
    """
    raw = ("" if v is None else str(v)).strip()
    c = canon_tf(raw)
    out: List[str] = []
    if c:
        out.append(c)
    legacy = raw.strip().lower()
    # если было "M1" -> legacy "m1" имеет смысл хранить/читать
    if legacy and legacy not in out:
        out.append(legacy)
    # фиксируем самые важные пары совместимости
    if c == "1m" and "m1" not in out:
        out.append("m1")
    if c == "5m" and "m5" not in out:
        out.append("m5")
    if c == "15m" and "m15" not in out:
        out.append("m15")
    if c == "1h" and "h1" not in out:
        out.append("h1")
    if c == "4h" and "h4" not in out:
        out.append("h4")
    if c == "1d" and "d1" not in out:
        out.append("d1")
    if c == "1w" and "w1" not in out:
        out.append("w1")
    if c == "1month" and "month" not in out:
        out.append("month")
    # уникальные с сохранением порядка
    seen = set()
    uniq: List[str] = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def strategy_from_source(source: str) -> str:
    sl = (source or "").strip().lower()
    return _STRATEGY_BY_SOURCE.get(sl, sl or "unknown")


def norm_close_reason(raw: str) -> str:
    r = (raw or "").strip().upper().replace(" ", "_")
    if not r:
        return ""

    # -----------------------------
    # Orphan / Expiry normalization
    # -----------------------------
    # TradeMonitor forced-close (вошли, но не вышли):
    #   ORPHAN_TIMEOUT
    #   ORPHAN_TIMEOUT_NO_PRICE
    # и т.п.
    # Мы сохраняем как отдельный "нормализованный" reason
    # а bucket уже решает как агрегировать (например в EXPIRED).
    if r.startswith("ORPHAN_TIMEOUT"):
        return "ORPHAN_TIMEOUT"
    if r in ("EXPIRED_NO_ENTRY", "EXPIRED_NO_TARGET"):
        return r

    # оставить TP1/TP2/TP3 как есть (более информативно)
    # ВАЖНО: проверка должна быть ДО _TP_BUCKET, т.к. TP1/TP2/TP3 входят в _TP_BUCKET
    if r in ("TP1", "TP2", "TP3"):
        return r

    if r in _TRAIL_BUCKET:
        return "TRAILING_STOP"
    if r in _TP_BUCKET:
        return "TP"
    if r in _SL_BUCKET or r.startswith("SL_AFTER_"):
        return "SL"
    return r


def bucket_close_reason(raw: str) -> str:
    """
    Финальный bucket для статистики/отчёта.
    Используется для агрегаций.

    Категории (requested CANONICAL set):
      - TRAIL_SL    (trailing / SL after TP / lock / trailing stop)
      - INITIAL_SL  (первичный SL до переносов)
      - TP          (take profit, including TP1/TP2/TP3)
      - TIMEOUT     (orphan / expired / time-based exit)
      - MANUAL      (ручное закрытие)
      - ERROR       (ошибки / unknown)
    """
    r = (raw or "").strip().upper().replace(" ", "_")
    if not r or r == "UNKNOWN":
        return "ERROR"

    # 1) TP
    # TP1, TP2, TP3, TP, TAKE_PROFIT, MANUAL_TP
    # Но исключаем, если это SL_AFTER_TP (это TRAIL_SL)
    if any(x in r for x in ("TP", "TAKE_PROFIT")) and "SL_AFTER" not in r:
        return "TP"

    # 2) TRAIL_SL
    # Все виды трейлинга + SL после TP + lock + moved sl
    if any(x in r for x in ("TRAIL", "TRAILING", "SL_AFTER", "MOVED_SL", "LOCK")):
        return "TRAIL_SL"

    # 3) INITIAL_SL
    # Обычный стоп-лосс (без трейлинга)
    if r in ("SL", "STOP", "STOP_LOSS", "SL_HIT", "LIQUIDATION", "ADL", "FORCE_CLOSE", "INITIAL_SL"):
        return "INITIAL_SL"
    if r.startswith("SL_") and "AFTER" not in r:
        return "INITIAL_SL"

    # 4) TIMEOUT
    if any(x in r for x in ("ORPHAN", "EXPIRED", "TIMEOUT", "TIME_EXIT")):
        return "TIMEOUT"

    # 5) MANUAL
    if "MANUAL" in r:
        return "MANUAL"

    return "ERROR"

