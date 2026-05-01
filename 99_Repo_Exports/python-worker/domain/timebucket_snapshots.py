from __future__ import annotations

import os
from typing import Any, Dict, List, Optional


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _parse_csv_ints(s: str) -> List[int]:
    out: List[int] = []
    for part in (s or "").split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except Exception:
            pass
    return out


def _snapshot_buckets_ms() -> List[int]:
    """
    Buckets for storing excursion snapshots.
    By default we reuse EMP_TIME_BUCKETS_MINUTES (used by runtime empirical levels)
    to ensure the writer and reader are aligned.
    """
    mins = _parse_csv_ints(os.getenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3,5,8,13,21,34,45"))
    ms = [m * 60_000 for m in mins if m and m > 0]
    ms.sort()
    return ms


def _ensure_dict_attr(obj: Any, name: str) -> Optional[Dict[int, float]]:
    """
    slots-safe: if dataclass defines the field -> it exists.
    fail-open: if not, we try to set it dynamically.
    Returns None if cannot ensure a dict.
    """
    v = getattr(obj, name, None)
    if isinstance(v, dict):
        return v  # type: ignore[return-value]
    try:
        setattr(obj, name, {})
        return getattr(obj, name)  # type: ignore[return-value]
    except Exception:
        # last resort: no place to store => do nothing upstream
        return None


def maybe_snapshot_time_buckets(pos: Any, *, ts_ms: int, spec: Any) -> None:
    """
    Store "MFE/MAE up to time T" snapshots into:
      pos.mfe_pnl_t[bucket_ms] = mfe_pnl_so_far
      pos.mae_pnl_t[bucket_ms] = mae_pnl_so_far

    IMPORTANT:
    - We do NOT snapshot raw price at T.
      We snapshot *excursion up to T* (i.e. max favorable / max adverse so far),
      which is what you need for quantile(MFE@T) / quantile(MAE@T).
    - We avoid calling spec.pnl_money unless we must.
      If process_tick already keeps pos.mfe_pnl/pos.mae_pnl updated, we just store those values.
    - If we do call pnl_money, we match your exact signature:
        spec.pnl_money(entry_price, price, lot, direction, symbol=pos.symbol)
    - Fail-open: never breaks trading loop.
    """
    try:
        if not _env_bool("EMP_TIME_SNAPSHOTS_ENABLED", True):
            return
    except Exception:
        return

    try:
        entry_ts = int(getattr(pos, "entry_ts_ms", 0) or getattr(pos, "entry_time", 0) or 0)
        if entry_ts <= 0 or ts_ms <= entry_ts:
            return
        elapsed = int(ts_ms - entry_ts)
    except Exception:
        return

    buckets = _snapshot_buckets_ms()
    if not buckets:
        return

    mfe_map = _ensure_dict_attr(pos, "mfe_pnl_t")
    mae_map = _ensure_dict_attr(pos, "mae_pnl_t")
    if mfe_map is None or mae_map is None:
        # no storage available
        return

    # Fast path: most ticks won't cross a new bucket.
    # We only snapshot buckets that are <= elapsed and not yet stored.
    for b in buckets:
        if b > elapsed:
            break
        if b in mfe_map and b in mae_map:
            continue

        # Prefer already computed excursions in money (exactly what you already compute in process_tick).
        mfe_pnl = getattr(pos, "mfe_pnl", None)
        mae_pnl = getattr(pos, "mae_pnl", None)
        if isinstance(mfe_pnl, (int, float)) and isinstance(mae_pnl, (int, float)):
            try:
                mfe_map[int(b)] = float(mfe_pnl)
                mae_map[int(b)] = float(mae_pnl)
                continue
            except Exception:
                pass

        # Fallback: recompute using your exact pnl_money signature.
        try:
            entry_price = float(getattr(pos, "entry_price", 0.0) or 0.0)
            lot = float(getattr(pos, "lot", 0.0) or 0.0)
            direction = getattr(pos, "direction", None)
            symbol = str(getattr(pos, "symbol", "") or "")
            if entry_price <= 0.0 or lot == 0.0:
                continue

            # Use the same price-selection logic you use for global MFE/MAE.
            max_price = float(getattr(pos, "max_price_seen", 0.0) or 0.0)
            min_price = float(getattr(pos, "min_price_seen", 0.0) or 0.0)
            if max_price <= 0.0 or min_price <= 0.0:
                continue

            # SHORT: favorable is down (min), adverse is up (max)
            is_long = False
            try:
                is_long = bool(getattr(pos, "is_long")() if callable(getattr(pos, "is_long", None)) else False)
            except Exception:
                # fallback by direction string
                s = str(direction or "").strip().lower()
                is_long = s in {"long", "buy"}

            if is_long:
                mfe_price = max_price
                mae_price = min_price
            else:
                mfe_price = min_price
                mae_price = max_price

            mfe_val = float(spec.pnl_money(entry_price, mfe_price, lot, direction, symbol=symbol))
            mae_val = float(spec.pnl_money(entry_price, mae_price, lot, direction, symbol=symbol))
            mfe_map[int(b)] = float(mfe_val)
            mae_map[int(b)] = float(mae_val)
        except Exception:
            # fail-open
            continue


def attach_timebucket_snapshots_to_closed(pos: Any, closed: Any) -> None:
    """
    Copies pos.mfe_pnl_t / pos.mae_pnl_t into TradeClosed object as flat fields,
    so Redis repo can persist them and StatsAggregator can consume them.

    Output fields on closed:
      closed.mfe_pnl_t60000, closed.mae_pnl_t60000, ...
    """
    try:
        mfe_map = getattr(pos, "mfe_pnl_t", None)
        mae_map = getattr(pos, "mae_pnl_t", None)
        if not isinstance(mfe_map, dict) or not isinstance(mae_map, dict):
            return
        for b_ms, v in mfe_map.items():
            try:
                b = int(b_ms)
                setattr(closed, f"mfe_pnl_t{b}", float(v))
            except Exception:
                pass
        for b_ms, v in mae_map.items():
            try:
                b = int(b_ms)
                setattr(closed, f"mae_pnl_t{b}", float(v))
            except Exception:
                pass
    except Exception:
        pass
