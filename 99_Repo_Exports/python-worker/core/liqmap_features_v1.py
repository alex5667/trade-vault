"""Liquidation Map (liqmap) -> compact feature layer.

This module converts a liqmap snapshot (stored in Redis under keys:
  liqmap:snapshot:<SYMBOL>:<WINDOW>
as JSON) into a small, flat set of scalar features suitable for:
  - online indicators dict (runtime)
  - meta-features schema (train==serve parity)

Design goals:
  - deterministic: same snapshot + same price -> same feature dict
  - fail-open: parsing errors should not break pipeline; caller decides metrics/quarantine
  - compact: avoid high cardinality / avoid per-level vectors

Train==serve contract (important):
  - output keys MUST be stable and depend on the *requested* window (routing key)
    rather than trusting snapshot payload fields.

Snapshot schema (produced by services/liquidation_map_service.py):
  {
    "ts_ms": <int>,
    "symbol": "BTCUSDT",
    "window": "5m",
    "levels": [
      {"price": "12345.6", "long_usd": "1000", "short_usd": "800", "total_usd": "1800"},
      ...
    ]
  }
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import json
import math

try:
    import orjson
except ImportError:
    orjson = None


EPS = 1e-12


@dataclass(frozen=True)
class LiqMapLevel:
    price: float
    long_usd: float
    short_usd: float
    total_usd: float


@dataclass(frozen=True)
class LiqMapSnapshot:
    ts_ms: int
    symbol: str
    window: str
    levels: Tuple[LiqMapLevel, ...]


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return float(default)
        return v
    except Exception:
        return float(default)


def _i(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return int(default)


def _parse_levels(levels_raw: Any) -> Tuple[LiqMapLevel, ...]:
    if not isinstance(levels_raw, list):
        return tuple()

    levels: List[LiqMapLevel] = []
    for it in levels_raw:
        if not isinstance(it, dict):
            continue
        price = _f(it.get("price") or it.get("p"), 0.0)
        if price <= 0:
            continue

        long_usd = _f(it.get("long_usd") or it.get("long") or it.get("l"), 0.0)
        short_usd = _f(it.get("short_usd") or it.get("short") or it.get("s"), 0.0)
        total_usd = _f(it.get("total_usd") or it.get("total") or it.get("t"), 0.0)

        usd_side = _f(it.get("usd") or it.get("u"), 0.0)
        if usd_side > 0.0 and total_usd <= 0.0 and long_usd <= 0.0 and short_usd <= 0.0:
            total_usd = float(usd_side)
            side = str(it.get("side") or it.get("sd") or "").strip().lower()
            if side in ("bid", "long", "buy", "b", "l"):
                long_usd = float(usd_side)
            elif side in ("ask", "short", "sell", "a", "s"):
                short_usd = float(usd_side)

        if total_usd <= 0.0:
            total_usd = long_usd + short_usd

        # sanitize negatives
        if long_usd < 0:
            long_usd = 0.0
        if short_usd < 0:
            short_usd = 0.0
        if total_usd < 0:
            total_usd = 0.0

        # prefer consistent total
        if total_usd <= 0 and (long_usd > 0 or short_usd > 0):
            total_usd = long_usd + short_usd

        levels.append(LiqMapLevel(price=price, long_usd=long_usd, short_usd=short_usd, total_usd=total_usd))

    # sort by price asc for deterministic search
    levels.sort(key=lambda x: x.price)
    return tuple(levels)


def parse_liqmap_snapshot(raw_json: str) -> LiqMapSnapshot:
    """Strict parser.

    Raises:
        ValueError: on invalid JSON or invalid required fields.

    For runtime usage prefer parse_liqmap_snapshot_v1(... expected_symbol/window ...) which
    tolerates missing/inconsistent payload fields.
    """
    try:
        if orjson is not None:
            obj = orjson.loads(raw_json)
        else:
            obj = json.loads(raw_json)
    except Exception as e:
        raise ValueError(f"liqmap: invalid json: {e}")

    if not isinstance(obj, dict):
        raise ValueError("liqmap: snapshot root is not dict")

    ts_ms = _i(obj.get("ts_ms"), -1)
    symbol = str(obj.get("symbol", "") or "").strip().upper()
    window = str(obj.get("window", "") or "").strip()
    if ts_ms <= 0:
        raise ValueError("liqmap: missing/invalid ts_ms")
    if not symbol:
        raise ValueError("liqmap: missing symbol")
    if not window:
        raise ValueError("liqmap: missing window")

    levels = _parse_levels(obj.get("levels", []))
    return LiqMapSnapshot(ts_ms=ts_ms, symbol=symbol, window=window, levels=levels)


def parse_liqmap_snapshot_v1(
    raw_json: str,
    *,
    expected_symbol: str = "",
    expected_window: str = "",
) -> LiqMapSnapshot:
    """Runtime-friendly parser.

    - Tolerates missing or inconsistent symbol/window inside snapshot payload.
    - Forces the output to expected values (stable key contract).

    Raises:
        ValueError: on invalid JSON or if ts_ms is missing/invalid.
    """
    try:
        if orjson is not None:
            obj = orjson.loads(raw_json)
        else:
            obj = json.loads(raw_json)
    except Exception as e:
        raise ValueError(f"liqmap: invalid json: {e}")

    if not isinstance(obj, dict):
        raise ValueError("liqmap: snapshot root is not dict")

    ts_ms = _i(obj.get("ts_ms"), -1)
    if ts_ms <= 0:
        raise ValueError("liqmap: missing/invalid ts_ms")

    sym = str(obj.get("symbol", "") or "").strip().upper()
    wnd = str(obj.get("window", "") or "").strip()

    exp_sym = str(expected_symbol or "").strip().upper()
    exp_wnd = str(expected_window or "").strip()

    if not sym and exp_sym:
        sym = exp_sym
    if not wnd and exp_wnd:
        wnd = exp_wnd

    # Force stable routing identity
    if exp_sym and sym != exp_sym:
        sym = exp_sym
    if exp_wnd and wnd != exp_wnd:
        wnd = exp_wnd

    if not sym or not wnd:
        raise ValueError("liqmap: missing symbol/window")

    levels = _parse_levels(obj.get("levels", []))
    return LiqMapSnapshot(ts_ms=ts_ms, symbol=sym, window=wnd, levels=levels)


def liqmap_feature_keys(window: str) -> List[str]:
    w = str(window)
    p = f"liqmap_{w}_"
    return [
        f"{p}age_ms",
        f"{p}levels_n",
        f"{p}total_usd",
        f"{p}near_total_usd",
        f"{p}near_long_usd",
        f"{p}near_short_usd",
        f"{p}near_imb",
        f"{p}dist_up_bps",
        f"{p}dist_dn_bps",
        f"{p}peak_up1_usd",
        f"{p}peak_dn1_usd",
        f"{p}peak_up1_share",
        f"{p}peak_dn1_share",
        f"{p}peaks_up",
        f"{p}peaks_dn",
    ]


def make_liqmap_default_features(windows: Iterable[str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for w in windows:
        for k in liqmap_feature_keys(w):
            out.setdefault(k, 0.0)
    return out


def _compute_liqmap_features_one(
    snapshot: LiqMapSnapshot,
    *,
    now_ms: int,
    price: float,
    near_band_bps: float = 20.0,
    peak_min_share: float = 0.05,
    require_peak_usd_min: float = 0.0,
) -> Dict[str, float]:
    """Compute compact features for one window."""

    w = snapshot.window
    pref = f"liqmap_{w}_"

    out: Dict[str, float] = {k: 0.0 for k in liqmap_feature_keys(w)}

    age_ms = max(0, int(now_ms) - int(snapshot.ts_ms))
    out[f"{pref}age_ms"] = float(age_ms)
    out[f"{pref}levels_n"] = float(len(snapshot.levels))

    if price <= 0.0:
        return out

    # total
    total_usd = 0.0
    for lvl in snapshot.levels:
        total_usd += float(lvl.total_usd)
    out[f"{pref}total_usd"] = float(total_usd)

    # --- Near band sums ---
    band = abs(float(price)) * float(near_band_bps) / 10000.0
    near_long = 0.0
    near_short = 0.0
    for lvl in snapshot.levels:
        if abs(float(lvl.price) - float(price)) <= band:
            near_long += float(lvl.long_usd)
            near_short += float(lvl.short_usd)

    near_total = near_long + near_short
    out[f"{pref}near_total_usd"] = float(near_total)
    out[f"{pref}near_long_usd"] = float(near_long)
    out[f"{pref}near_short_usd"] = float(near_short)
    out[f"{pref}near_imb"] = float((near_long - near_short) / max(near_total, EPS))

    # --- Peak selection (closest peaks up/down that pass threshold) ---
    share_thr = float(peak_min_share) * float(total_usd)
    peak_thr = max(float(require_peak_usd_min), float(share_thr))

    up_best: Optional[LiqMapLevel] = None
    dn_best: Optional[LiqMapLevel] = None
    peaks_up = 0
    peaks_dn = 0

    for lvl in snapshot.levels:
        if float(lvl.total_usd) < peak_thr:
            continue
        if float(lvl.price) > float(price):
            peaks_up += 1
            if up_best is None or float(lvl.price) < float(up_best.price):
                up_best = lvl
        elif float(lvl.price) < float(price):
            peaks_dn += 1
            if dn_best is None or float(lvl.price) > float(dn_best.price):
                dn_best = lvl

    out[f"{pref}peaks_up"] = float(peaks_up)
    out[f"{pref}peaks_dn"] = float(peaks_dn)

    if up_best is not None:
        dist_up_bps = (float(up_best.price) - float(price)) / float(price) * 10000.0
        out[f"{pref}dist_up_bps"] = float(max(0.0, dist_up_bps))
        out[f"{pref}peak_up1_usd"] = float(up_best.total_usd)
        out[f"{pref}peak_up1_share"] = float(float(up_best.total_usd) / max(total_usd, EPS))

    if dn_best is not None:
        dist_dn_bps = (float(price) - float(dn_best.price)) / float(price) * 10000.0
        out[f"{pref}dist_dn_bps"] = float(max(0.0, dist_dn_bps))
        out[f"{pref}peak_dn1_usd"] = float(dn_best.total_usd)
        out[f"{pref}peak_dn1_share"] = float(float(dn_best.total_usd) / max(total_usd, EPS))

    return out


def compute_liqmap_features(
    snapshot: LiqMapSnapshot,
    *,
    price: float,
    windows: Iterable[str],
    near_band_bps: float = 20.0,
    peak_min_share: float = 0.05,
    now_ms: int,
    require_peak_usd_min: float = 0.0,
) -> Dict[str, float]:
    """Compute liqmap features with a stable multi-window contract.

    Runtime usually calls this once per window, but we return defaults for *all*
    requested windows to keep the indicators dict stable.

    Contract:
      - output keys are forced to the requested window(s), not snapshot.window
    """
    wlist = [str(w).strip() for w in (windows or []) if str(w).strip()]
    if not wlist:
        wlist = [str(getattr(snapshot, "window", "") or "").strip()]
    out = make_liqmap_default_features(wlist)

    # Use first requested window for actual computation.
    w_req = wlist[0]
    if str(getattr(snapshot, "window", "") or "").strip() != w_req:
        snapshot = LiqMapSnapshot(ts_ms=int(snapshot.ts_ms), symbol=str(snapshot.symbol), window=str(w_req), levels=tuple(snapshot.levels))

    out.update(
        _compute_liqmap_features_one(
            snapshot,
            now_ms=int(now_ms),
            price=float(price),
            near_band_bps=float(near_band_bps),
            peak_min_share=float(peak_min_share),
            require_peak_usd_min=float(require_peak_usd_min),
        )
    )
    return out
