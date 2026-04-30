from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict


def _env(name: str, default: Any) -> Any:
    v = os.getenv(name)
    return default if v is None or str(v).strip() == "" else v


def _sym_base(symbol: str) -> str:
    """
    BTCUSDT -> BTC
    ETHUSDT -> ETH
    SOLUSDT -> SOL
    Fallback: symbol itself uppercased.
    """
    s = (symbol or "").strip().upper()
    if s.endswith("USDT") and len(s) > 4:
        return s[:-4]
    return s


@dataclass(frozen=True)
class RiskCfgResolver:
    """
    Builds cfg for compute_levels(entry, atr, side, cfg).

    Resolution order for a key:
      1) <BASE>_<KEY>  (e.g. BTC_STOP_MODE, BTC_STOP_ATR_MULT, BTC_TP_RR)
      2) <KEY>         (e.g. STOP_MODE, STOP_ATR_MULT, TP_RR)
      3) default

    Notes:
      - In your repo BTC_* exists, ETH_* stop config seems absent.
        So ETH will fall back to STOP_* or defaults.
    """

    def resolve(self, symbol: str) -> Dict[str, Any]:
        base = _sym_base(symbol)

        def pick(key: str, default: Any) -> Any:
            return _env(f"{base}_{key}", _env(key, default))

        # STOP
        stop_mode = str(pick("STOP_MODE", "ATR")).upper()
        stop_atr_mult = float(pick("STOP_ATR_MULT", 0.6))
        stop_atr_mult_base = float(pick("STOP_ATR_MULT_BASE", stop_atr_mult))
        stop_pct = float(pick("STOP_PCT", 0.2))
        stop_points = float(pick("STOP_POINTS", 1.0))

        # TP
        tp_mode = str(pick("TP_MODE", "RR")).upper()
        tp_rr = str(pick("TP_RR", "1,2,3"))
        tp_atr_mults = str(pick("TP_ATR_MULTS", "0.6,1.0,1.5"))

        # Optional: profile hook (if you want rocket_v1 behavior in crypto later)
        trail_profile = pick("TRAIL_PROFILE", pick("trail_profile", ""))  # allow both spellings
        rocket_tp1 = float(pick("ROCKET_TP1_ATR_MULT", 0.0))
        min_lock_r = float(pick("TRAILING_MIN_LOCK_R", 0.0))

        cfg: Dict[str, Any] = {
            "STOP_MODE": stop_mode
            "STOP_ATR_MULT": stop_atr_mult
            "STOP_ATR_MULT_BASE": stop_atr_mult_base
            "STOP_PCT": stop_pct
            "STOP_POINTS": stop_points
            "TP_MODE": tp_mode
            "TP_RR": tp_rr
            "TP_ATR_MULTS": tp_atr_mults
        }
        if str(trail_profile).strip():
            cfg["trail_profile"] = str(trail_profile).strip()
        if rocket_tp1 > 0:
            cfg["ROCKET_TP1_ATR_MULT"] = rocket_tp1
        if min_lock_r > 0:
            cfg["trailing_min_lock_r"] = min_lock_r

        return cfg

