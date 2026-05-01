from __future__ import annotations

import os
from typing import Any, Dict, List


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

#
# Если compact-stream включён:
#   stream => минимальный payload
#   детали => order:{id} hash
# Поэтому hydrate в compact режиме ВСЕГДА делает HGETALL (batch через pipeline).
#

def _norm_flat(d: Dict[str, Any]) -> Dict[str, str]:
    # Redis (decode_responses=True) обычно уже отдаёт str, но нормализуем.
    return {str(k): str(v) for k, v in (d or {}).items() if v is not None}


def _normalize_profile_aliases(d: Dict[str, str]) -> Dict[str, str]:
    """
    trail_profile vs trailing_profile:
      - TradeClosed: trailing_profile (канон)
      - PositionState/часть кода: trail_profile (легаси)
    В hydrator возвращаем оба ключа, если есть хоть один.
    """
    prof = (d.get("trailing_profile") or d.get("trail_profile") or "").strip()
    if prof:
        d.setdefault("trailing_profile", prof)
        d.setdefault("trail_profile", prof)
    return d


def hydrate_trade_closed(
    redis,
    fields: Dict[str, Any],
    *,
    compact_env: str = "TRADES_CLOSED_STREAM_COMPACT",
    require_closed: bool = True,
    merge_precedence: str = "hash",
) -> Dict[str, str]:
    """
    Делает запись закрытой сделки "максимально полной".

    - Если включён compact-stream (TRADES_CLOSED_STREAM_COMPACT=1) — всегда подтягиваем HGETALL(order:{id}).
    - Если compact выключен — подтягиваем hash только когда не хватает ключевых полей.
    - merge_precedence:
        "hash"   -> hash доминирует (полезно, если stream частичный/устаревший)
        "stream" -> stream доминирует
    """
    f = _norm_flat(fields or {})
    oid = str(f.get("order_id") or f.get("id") or "").strip()
    if not oid:
        return _normalize_profile_aliases(f)

    compact = _env_bool(compact_env, default=False)
    need_hash = compact

    if not need_hash:
        required_keys = (
            "exit_ts_ms", "pnl_net", "close_reason", "status",
            "one_r_money", "risk_amount",
            "mfe_pnl", "mfe_usd", "pnl_pct", "pnl_if_fixed_exit",
            "sl_atr", "tp_atr", "atr",
            "signal_payload"
        )
        for k in required_keys:
            if k not in f:
                need_hash = True
                break

    if not need_hash:
        return _normalize_profile_aliases(f)

    # В compact режиме order hash — источник истины, stream — best-effort.
    # Поэтому дефолт merge_precedence="hash" для production.
    try:
        h = redis.hgetall(f"order:{oid}") or {}
    except Exception:
        h = {}

    h2 = _norm_flat(h)
    if require_closed:
        st = (h2.get("status") or "").strip().lower()
        if st and st != "closed":
            return _normalize_profile_aliases(f)

    if merge_precedence == "stream":
        merged = dict(h2)
        merged.update(f)
        merged["order_id"] = oid
        return _normalize_profile_aliases(merged)

    merged = dict(f)
    merged.update(h2)
    merged["order_id"] = oid
    return _normalize_profile_aliases(merged)


def hydrate_trade_closed_batch(
    redis,
    rows: List[Dict[str, Any]],
    *,
    compact_env: str = "TRADES_CLOSED_STREAM_COMPACT",
    require_closed: bool = False,
    merge_precedence: str = "hash",
) -> List[Dict[str, str]]:
    """
    Batch-версия для ускорения consumer'ов:
      - собираем order_id
      - делаем pipeline HGETALL
      - мерджим один проход
    """
    compact = _env_bool(compact_env, default=False)
    # если compact выключен — batch всё равно полезен, но можно "умно" выбирать нужные
    want: List[str] = []
    base: List[Dict[str, str]] = []
    for r in rows:
        f = _norm_flat(r or {})
        oid = str(f.get("order_id") or f.get("id") or "").strip()
        base.append(f)
        if not oid:
            want.append("")
            continue
        need = compact
        if not need:
            # Expanded required fields to ensure R-metrics and Exit Stats are calculated
            required_keys = (
                "exit_ts_ms", "pnl_net", "close_reason", "status",
                "one_r_money", "risk_amount",
                "mfe_pnl", "mfe_usd", "pnl_pct", "pnl_if_fixed_exit",
                "sl_atr", "tp_atr", "atr",
                "signal_payload"
            )
            for k in required_keys:
                if k not in f:
                    need = True
                    break
        want.append(oid if need else "")

    # pipeline hgetall for those we want
    hashes: List[Dict[str, Any]] = []
    if any(want):
        try:
            pipe = redis.pipeline()
            for oid in want:
                if oid:
                    pipe.hgetall(f"order:{oid}")
            raw = pipe.execute()
            # raw содержит только для oid!= "" (сжатый список)
            it = iter(raw)
            for oid in want:
                if oid:
                    hashes.append(next(it) or {})
                else:
                    hashes.append({})
        except Exception:
            hashes = [{} for _ in want]
    else:
        hashes = [{} for _ in want]

    out: List[Dict[str, str]] = []
    for f, oid, h in zip(base, want, hashes):
        if not oid:
            out.append(_normalize_profile_aliases(f))
            continue
        h2 = _norm_flat(h or {})
        if require_closed:
            st = (h2.get("status") or "").strip().lower()
            if st and st != "closed":
                out.append(_normalize_profile_aliases(f))
                continue
        if merge_precedence == "stream":
            merged = dict(h2)
            merged.update(f)
            merged["order_id"] = oid
            out.append(_normalize_profile_aliases(merged))
        else:
            merged = dict(f)
            merged.update(h2)
            merged["order_id"] = oid
            out.append(_normalize_profile_aliases(merged))
    return out
