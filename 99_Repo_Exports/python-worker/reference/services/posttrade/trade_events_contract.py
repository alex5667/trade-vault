# -*- coding: utf-8 -*-
"""Trade Events Contract — A3 V2.

Строгий, но fail-open контракт для событий events:trades (POSITION_CLOSED).

Гарантирует:
  - sid всегда присутствует (join-ключ с decision_snapshot)
  - ts и exit_ts_ms — epoch ms (строкой, совместим с Redis Streams)
  - event_id — SHA1 (40 hex) для идемпотентности
  - тяжёлые поля (feature_vector, evidence, raw_signal, …) никогда не
    попадают в Redis Stream
  - при битом событии не блокируем поток: publish_dlq + ACK/continue

V2 изменения:
  - normalize_position_closed_event() теперь возвращает (dict, list[str]) — список ошибок
  - validate_position_closed_event() возвращает (bool, list[str])
  - Добавлены join-critical поля: side, order_id, qty, fee_bps, px/price, venue
  - Псевдонимы: lot→qty, source→venue, price→px
  - Вычисление fee_bps из fees_usd+turnover_roundtrip (если fee_bps отсутствует)
  - Детерминированный ts: берётся из exit_ts_ms / ts_ms, а НЕ time.time()

Этот модуль НЕ выполняет IO. Используется production-кодом (trade_events_logger) и тестами.

Минимальный контракт POSITION_CLOSED:
  - event_type  str  == "POSITION_CLOSED"
  - sid         str  (join key → decision_snapshot)
  - ts          str  epoch ms > 0
  - exit_ts_ms  str  epoch ms > 0
  - event_id    str  SHA1 hex (40 chars)
  - side        str  LONG|SHORT
  - order_id    str  (может быть position_id как fallback)
  - qty         str  float > 0
  - fee_bps     str  float >= 0
  - px / price  str  float > 0
  - venue       str  (источник: binance/mt5/bybit/paper…)
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from typing import Any, Dict, List, Optional, Tuple

# --------------------------------------------------------------------------- #
# Тяжёлые аналитические поля, которые НИКОГДА не должны попасть в Redis Stream.
# Полный payload хранится в PostgreSQL (stream-archiver) и trade:events:{sid}.
# --------------------------------------------------------------------------- #
_HEAVY_FIELDS: frozenset = frozenset({
    "config_snapshot"
    "calibrated_specs"
    "indicators_snapshot"
    "trail_profile_config"
    "evidence"
    "feature_vector"
    "trail_profile"
    "raw_signal"
    "signal_payload"
})

# Минимальная разумная граница epoch ms: 2020-01-01T00:00:00Z
_MIN_EPOCH_MS: int = 1_577_836_800_000

# SHA1 hex pattern
_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")


# --------------------------------------------------------------------------- #
# Private helpers (unit-testable, no IO)
# --------------------------------------------------------------------------- #

def _safe_int_ms(v: Any) -> Optional[int]:
    """Конвертирует v в epoch ms (int). Возвращает None при невалидном значении."""
    if v is None:
        return None
    try:
        i = int(float(str(v)))
    except (ValueError, TypeError):
        return None
    # seconds → ms (heuristic: если меньше 1e10, то скорее секунды)
    if 0 < i < 10_000_000_000:
        i *= 1000
    return i if i > _MIN_EPOCH_MS else None


def _safe_str(v: Any) -> Optional[str]:
    """Возвращает str(v) или None если v is None/пустая строка."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _as_str(v: Any) -> str:
    """Перевести любое значение в строку (пустая строка если None)."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    return str(v)


def _as_float_str(v: Any) -> str:
    """Перевести в строку float. Возвращает '' если невалидно."""
    if v is None or v == "":
        return ""
    try:
        f = float(v)
        if not math.isfinite(f):
            return ""
        return str(f)
    except Exception:
        return ""


def _as_int_ms_str(v: Any) -> str:
    """Перевести в строку epoch ms. Возвращает '' если невалидно."""
    ms = _safe_int_ms(v)
    return str(ms) if ms is not None else ""


def _norm_side(v: Any) -> str:
    """Нормализовать side → LONG | SHORT."""
    s = _as_str(v).strip().upper()
    if s in ("LONG", "SHORT"):
        return s
    if s == "BUY":
        return "LONG"
    if s == "SELL":
        return "SHORT"
    return s


def _json_dumps(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"), default=str)


def _make_event_id(
    event_type: str
    sid: str
    ts: Optional[int]
    pnl: Optional[str] = None
    position_id: Optional[str] = None
) -> str:
    """Детерминированный SHA1 для идемпотентности.

    Формат: ``event_type|sid|ts|pnl|position_id``
    Совпадает с TradeEventsLogger._mk_event_id для POSITION_CLOSED.
    """
    raw = f"{event_type}|{sid}|{ts or ''}|{pnl or ''}|{position_id or ''}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _compute_fee_bps(
    *
    fees_usd: Optional[float]
    turnover_roundtrip: Optional[float]
    qty: Optional[float]
    price: Optional[float]
) -> Optional[float]:
    """Вычислить fee_bps из имеющихся данных (best-effort)."""
    try:
        f = float(fees_usd) if fees_usd is not None else None
    except Exception:
        f = None
    if f is None or f < 0:
        return None

    try:
        t = float(turnover_roundtrip) if turnover_roundtrip is not None else 0.0
    except Exception:
        t = 0.0
    if t and t > 0:
        return (f / t) * 10000.0

    try:
        q = float(qty) if qty is not None else 0.0
        p = float(price) if price is not None else 0.0
        notional = abs(q) * p
        if notional > 0:
            return (f / notional) * 10000.0
    except Exception:
        return None
    return None


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

def strip_heavy_fields(evt: Dict[str, Any]) -> Dict[str, Any]:
    """Убрать тяжёлые аналитические поля из события.

    Работает in-place? — нет, возвращает новый dict (не мутирует входной).
    """
    return {k: v for k, v in evt.items() if k not in _HEAVY_FIELDS}


def normalize_position_closed_event(
    doc: Dict[str, Any]
) -> Tuple[Dict[str, str], List[str]]:
    """Нормализовать событие POSITION_CLOSED в канонический Redis-формат (V2).

    V2: возвращает (normalized_dict, errors_list).
    Все значения — строки (требование Redis Streams).
    Гарантирует наличие ``event_id``, ``ts``, ``exit_ts_ms``, ``event_type``.
    Нормализует join-critical поля: sid, side, order_id, qty, fee_bps, px, venue.

    Args:
        doc: Плоский dict (может содержать смешанные типы).

    Returns:
        Tuple: (dict[str, str] готов для redis.xadd(), list[str] ошибок).
    """
    d = dict(doc or {})
    errs: List[str] = []
    out: Dict[str, str] = {}

    # 1. event_type — всегда POSITION_CLOSED при нормализации через этот модуль
    out["event_type"] = str(d.get("event_type") or "POSITION_CLOSED")
    if out["event_type"] != "POSITION_CLOSED":
        errs.append("event_type!=POSITION_CLOSED")
        out["event_type"] = "POSITION_CLOSED"

    # 2. sid — join key (обязателен)
    sid = (
        d.get("sid")
        or d.get("signal_id")
        or d.get("client_order_id")
    )
    sid_str = str(sid).strip() if sid is not None else ""
    out["sid"] = sid_str
    if not sid_str:
        errs.append("missing_sid")

    # 3. symbol
    symbol = _as_str(d.get("symbol") or "").strip().upper()
    if not symbol:
        errs.append("missing_symbol")
    out["symbol"] = symbol

    # 4. source/venue (псевдонимы)
    source = _as_str(d.get("source") or d.get("venue") or "").strip() or "unknown"
    out["source"] = source
    out["venue"] = _as_str(d.get("venue") or source).strip() or source

    # 5. Timestamps: ts и exit_ts_ms (epoch ms, строкой)
    raw_ts = d.get("ts") or d.get("timestamp") or d.get("ts_ms")
    raw_exit = (
        d.get("exit_ts_ms")
        or d.get("close_ts_ms")
        or d.get("exit_ts")
        or raw_ts  # fallback: ts == exit_ts если явно не задан
    )

    ts_ms = _safe_int_ms(raw_ts)
    exit_ms = _safe_int_ms(raw_exit)

    # V2: не используем time.time() как fallback — ts должен быть передан явно
    ts_str = str(ts_ms) if ts_ms else ""
    exit_str = str(exit_ms) if exit_ms else ""

    # Приоритет: exit_ts_ms более точный для fill time
    if exit_str and ts_str:
        try:
            if abs(int(ts_str) - int(exit_str)) > 60_000:
                # exit_ts_ms доминирует если расхождение > 1 минуты
                ts_str = exit_str
        except Exception:
            ts_str = exit_str
    elif exit_str and not ts_str:
        ts_str = exit_str

    if not ts_str:
        errs.append("missing_ts")
    if not exit_str:
        exit_str = ts_str
        errs.append("missing_exit_ts_ms")

    out["ts"] = ts_str
    out["exit_ts_ms"] = exit_str

    # ts_fill_ms: alias для ts_fill_ms/fill_ts_ms
    ts_fill_str = _as_int_ms_str(d.get("ts_fill_ms") or d.get("fill_ts_ms") or "")
    if not ts_fill_str:
        ts_fill_str = exit_str or ts_str
    out["ts_fill_ms"] = ts_fill_str

    # 6. event_id — SHA1 для идемпотентности
    existing_id = _safe_str(d.get("event_id"))
    if existing_id and len(existing_id) == 40 and _SHA1_RE.match(existing_id.lower()):
        out["event_id"] = existing_id
    else:
        if existing_id:
            errs.append("bad_event_id")
        out["event_id"] = _make_event_id(
            event_type=out["event_type"]
            sid=sid_str
            ts=ts_ms
            pnl=_safe_str(d.get("pnl"))
            position_id=_safe_str(d.get("position_id"))
        )

    # 7. price / px (join-critical: A3)
    price_str = _as_float_str(d.get("price") or d.get("px") or "")
    if not price_str:
        errs.append("missing_price")
    out["price"] = price_str
    out["px"] = _as_float_str(d.get("px") or d.get("price") or "")
    if not out["px"]:
        out["px"] = price_str

    # 8. pnl
    pnl_str = _as_float_str(d.get("pnl") or "")
    if not pnl_str:
        errs.append("missing_pnl")
    out["pnl"] = pnl_str

    # 9. side (LONG|SHORT — required for TCA)
    side = _norm_side(d.get("side") or d.get("direction") or "")
    if not side:
        errs.append("missing_side")
    out["side"] = side

    # 10. qty (lot→qty alias)
    qty_str = _as_float_str(d.get("qty") or d.get("quantity") or d.get("lot") or "")
    if not qty_str:
        errs.append("missing_qty")
    out["qty"] = qty_str
    # backward compat: keep lot if already present, otherwise alias
    if d.get("lot") is not None:
        out["lot"] = _as_float_str(d.get("lot") or "")
    elif qty_str:
        out["lot"] = qty_str

    # 11. order_id (join-critical: A3; fallback to position_id or client_order_id)
    order_id = _as_str(
        d.get("order_id") or d.get("client_order_id") or d.get("position_id") or ""
    ).strip()
    if not order_id:
        errs.append("missing_order_id")
    out["order_id"] = order_id

    # 12. fee_bps — required for TCA (but computable from fees_usd+turnover)
    fee_bps_str = _as_float_str(d.get("fee_bps") or d.get("fees_bps") or "")
    if not fee_bps_str:
        # Attempt to compute from fees_usd + turnover_roundtrip
        try:
            fees_usd_val = float(d.get("fees_usd")) if d.get("fees_usd") not in (None, "") else None
        except Exception:
            fees_usd_val = None
        try:
            turnover_val = float(d.get("turnover_roundtrip")) if d.get("turnover_roundtrip") not in (None, "") else None
        except Exception:
            turnover_val = None
        qty_f = None
        try:
            qty_f = float(qty_str) if qty_str else None
        except Exception:
            pass
        px_f = None
        try:
            px_f = float(price_str) if price_str else None
        except Exception:
            pass
        computed = _compute_fee_bps(fees_usd=fees_usd_val, turnover_roundtrip=turnover_val, qty=qty_f, price=px_f)
        if computed is not None:
            fee_bps_str = str(float(computed))
    if not fee_bps_str:
        errs.append("missing_fee_bps")
        fee_bps_str = "0.0"  # default for fail-open
    out["fee_bps"] = fee_bps_str

    # 13. risk_usd and r_mult (optional but important for downstream)
    risk_usd_str = _as_float_str(d.get("risk_usd") or "")
    if not risk_usd_str:
        errs.append("missing_risk_usd")
    out["risk_usd"] = risk_usd_str or "0.0"

    r_mult_str = _as_float_str(d.get("r_mult") or "")
    if not r_mult_str:
        errs.append("missing_r_mult")
    out["r_mult"] = r_mult_str or "0.0"

    # 14. Optional fields (не блокируют при отсутствии)
    for field in ("position_id", "close_reason"):
        v = d.get(field)
        if v is not None:
            out[field] = _as_str(v)

    # close_reason может лежать в metadata (legacy)
    if "close_reason" not in out:
        meta = d.get("metadata") or d.get("meta")
        if isinstance(meta, dict) and meta.get("close_reason"):
            out["close_reason"] = str(meta["close_reason"])
        elif isinstance(meta, str):
            try:
                m = json.loads(meta)
                if isinstance(m, dict) and m.get("close_reason"):
                    out["close_reason"] = str(m["close_reason"])
            except Exception:
                pass

    # 15. metadata / meta — сериализуем в строку для Stream
    for alias in ("meta", "metadata"):
        v = d.get(alias)
        if v is not None and alias not in out:
            if isinstance(v, (dict, list)):
                out[alias] = _json_dumps(v)
            else:
                out[alias] = _as_str(v)

    # 16. Serialize any remaining non-string fields (generic pass-through)
    for k, v in d.items():
        if k in out or k in _HEAVY_FIELDS:
            continue
        if v is None:
            continue
        if isinstance(v, (dict, list)):
            out[k] = _json_dumps(v)
        else:
            out[k] = _as_str(v)

    # 17. Strip heavy fields from output
    out = {k: v for k, v in out.items() if k not in _HEAVY_FIELDS}

    return out, errs


def validate_position_closed_event(doc: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Валидировать событие POSITION_CLOSED.

    V2: Принимает raw dict (не нормализованный) — нормализация происходит внутри.
    Возвращает (ok, errors).
    """
    _, errs = normalize_position_closed_event(doc)
    return len(errs) == 0, errs
