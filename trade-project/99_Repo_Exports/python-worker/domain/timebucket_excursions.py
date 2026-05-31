from __future__ import annotations

import os
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib


def _parse_csv_ints(s: str) -> list[int]:
    out: list[int] = []
    for part in (s or "").split(","):
        p = part.strip()
        if not p:
            continue
        with contextlib.suppress(Exception):
            out.append(int(float(p)))
    return out


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _buckets_ms_from_env(tf: str) -> list[int]:
    """
    Time buckets for snapshots.

    For 1m scalping TTD(tp1) is typically minutes (5-45m), so minute-buckets are best.
    You can override with:
      EMP_TIME_BUCKETS_MINUTES="1,2,3,5,8,13,21,34,45"
    or directly:
      EMP_TIME_BUCKETS_MS="60000,120000,..."
    """
    ms_raw = os.getenv("EMP_TIME_BUCKETS_MS", "").strip()
    if ms_raw:
        xs = _parse_csv_ints(ms_raw)
        return [x for x in xs if x > 0]

    # Default minutes buckets tuned for 1m TF.
    # If later you add other TFs, you can make this conditional on tf.
    mins_raw = os.getenv("EMP_TIME_BUCKETS_MINUTES", "1,2,3,5,8,13,21,34,45").strip()
    mins = _parse_csv_ints(mins_raw)
    out = [m * 60_000 for m in mins if m > 0]
    return out


def _now_ms() -> int:
    return get_ny_time_millis()


def maybe_snapshot_time_buckets(
    pos: Any,
    *,
    mid_price: float,
    ts_ms: int,
    spec: Any,
    tf: str,
) -> None:
    """
    Writes *time-bucket* MFE/MAE snapshots into the open position (fail-open).

    Why:
      To compute TP1 levels strictly by:
        T = median(TTD_tp1)
        TP1_bps = quantile(MFE@T, q=0.6)
        SL_bps  = quantile(MAE@T, q=0.8)

    Design:
      - Buckets are evaluated on tick-time: when elapsed crosses bucket boundary.
      - Snapshot is taken only if the position is alive at that time (proper censoring).
      - We store PnL snapshots (quote currency) and convert to bps later in StatsAggregator
        using notional (more robust than trying to infer bps here).

    Stored on pos (dynamic attrs, fail-open):
      pos.mfe_pnl_t = {bucket_ms: mfe_pnl_at_bucket}
      pos.mae_pnl_t = {bucket_ms: mae_pnl_at_bucket}
    """
    try:
        if not _env_bool("EMP_TIME_SNAPSHOTS_ENABLED", True):
            return
    except Exception:
        return

    try:
        entry_ts = int(getattr(pos, "entry_ts_ms", 0) or 0)
        if entry_ts <= 0:
            return
        now_ts = int(ts_ms or 0)
        if now_ts <= entry_ts:
            return
        elapsed = now_ts - entry_ts
        tf_s = (tf or "1m").strip().lower() or "1m"
        buckets = _buckets_ms_from_env(tf_s)
        if not buckets:
            return
    except Exception:
        return

    # Existing excursions are updated elsewhere; we only snapshot current extremes.
    # We compute PnL snapshots similarly to your existing on-the-fly mfe/mae code.
    try:
        is_long = bool(pos.is_long()) if hasattr(pos, "is_long") else (str(getattr(pos, "direction", "")).lower() in {"long", "buy"})
    except Exception:
        is_long = True

    # Ensure dict containers exist (pos is assumed mutable and not slots-restricted).
    try:
        mfe_map: dict[int, float] = getattr(pos, "mfe_pnl_t", None)  # type: ignore[assignment]
        if not isinstance(mfe_map, dict):
            mfe_map = {}
            pos.mfe_pnl_t = mfe_map
        mae_map: dict[int, float] = getattr(pos, "mae_pnl_t", None)  # type: ignore[assignment]
        if not isinstance(mae_map, dict):
            mae_map = {}
            pos.mae_pnl_t = mae_map
    except Exception:
        return

    # Snapshot each bucket only once.
    for b in buckets:
        try:
            b_ms = int(b)
            if b_ms <= 0:
                continue
            if elapsed < b_ms:
                continue
            if b_ms in mfe_map or b_ms in mae_map:
                continue

            entry_price = float(getattr(pos, "entry_price", 0.0) or 0.0)
            lot = float(getattr(pos, "lot", 0.0) or 0.0)
            if entry_price <= 0 or lot == 0:
                # Without entry/lot we cannot compute PnL safely.
                continue

            # Use current recorded extremes if present; fallback to mid_price.
            max_seen = float(getattr(pos, "max_price_seen", mid_price) or mid_price)
            min_seen = float(getattr(pos, "min_price_seen", mid_price) or mid_price)

            if is_long:
                mfe_price = max_seen
                mae_price = min_seen
            else:
                mfe_price = min_seen
                mae_price = max_seen

            # pnl_money(entry, price, lot, ...) must return signed PnL.
            mfe_pnl = float(spec.pnl_money(entry_price, mfe_price, lot, getattr(pos, "fees_rate", None), getattr(pos, "slippage_bps", None)))
            mae_pnl = float(spec.pnl_money(entry_price, mae_price, lot, getattr(pos, "fees_rate", None), getattr(pos, "slippage_bps", None)))

            mfe_map[b_ms] = float(mfe_pnl)
            mae_map[b_ms] = float(mae_pnl)
        except Exception:
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
        return
