from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional
import json


@dataclass
class SymbolSnapshot:
    """
    Compact snapshot per symbol for SMT aggregator.

    Design rules:
    - deterministic timestamps (ts_ms from bar/tick)
    - small, stable schema (forward-compatible: unknown keys ignored)
    - computed/updated on bar_close (1s microbar) + some fields from last events
    """

    symbol=""
    ts_ms: int = 0

    # Trend / structure (proxy BOS)
    trend_dir: str = "NONE"              # "UP"|"DOWN"|"NONE"
    close_px: float = 0.0

    # BOS proxy: close crossed last swing level
    close_cross: int = 0           # 1/0
    close_cross_dir: str = "NONE"        # "UP"|"DOWN"|"NONE"
    close_cross_level: float = 0.0

    # Swings (store last two highs/lows for SMT divergence)
    swing_high_0: float = 0.0
    swing_high_1: float = 0.0
    swing_low_0: float = 0.0
    swing_low_1: float = 0.0
    swing_ts_high_0: int = 0
    swing_ts_high_1: int = 0
    swing_ts_low_0: int = 0
    swing_ts_low_1: int = 0

    # OF / strong confirmation ingredients
    of_strong: int = 0             # 1/0 (recent strong-of gate passed)
    of_dir: str = "NONE"                 # "LONG"|"SHORT"|"NONE"
    of_ts_ms: int = 0

    weak_progress: int = 0         # 1/0
    reclaim: int = 0               # 1/0 (recent reclaim)
    reclaim_dir: str = "NONE"            # "LONG"|"SHORT"|"NONE"
    reclaim_ts_ms: int = 0

    sweep: int = 0                 # 1/0 (recent sweep)
    sweep_dir: str = "NONE"              # "LONG"|"SHORT"|"NONE"
    sweep_ts_ms: int = 0

    obi_stable_sec: float = 0.0       # 0..inf
    iceberg_strict: int = 0         # 1/0

    # Divergence kind (optional)
    div_kind: str = "none"               # e.g. "bullish_hidden"/"bearish_regular"/"none"
    div_ts_ms: int = 0

    # Ranking features
    rsi14: float = 0.0
    cvd_slope: float = 0.0
    retrace_atr: float = 0.0

    # New fields for SMT V2
    delta_z: float = 0.0
    delta_eff_norm: float = 0.0
    zone_dist_bp: float = 0.0
    zone_ok: int = 0
    near_zone: int = 0
    abs_lvl_ok: int = 0
    
    # ADX-aware regime strength (0..1)
    adx_q: float = 0.5
    adx14: float = 0.0
    # Market churn/pressure proxy (signals/sec EMA) for dynamic strictness & AB split
    pressure_sps: float = 0.0   # candidates/sec over last ~60s (smoothed)
    pressure_hi: int = 0        # 1 if pressure above configured threshold
    # cooldown filtered rate (signals/sec EMA). high => too many near-signals => stricter policy.
    cooldown_sps: float = 0.0
    # spread z (optional) for entry policy / penalties
    spread_z: float = 0.0
    
    # Book Health
    book_rate_hz: float = 0.0
    book_rate_z: float = 0.0
    book_age_ms: int = 0
    book_health_ok: int = 1
    book_health: str = "OK"     # "OK"|"WARN"|"ERR"
    
    # Data-quality (deterministic, at snapshot ts_ms)
    spread_bp: float = 0.0
    obi_age_ms: int = 10**9
    iceberg_age_ms: int = 10**9
    pressure_1m: float = 0.0  # signals/min
    cooldown_sps: float = 0.0
    
    # Real nearest zone (HTF) - filled by CryptoOrderflowService from zones:htf:v1:<symbol>
    zone_id: str = ""
    zone_type: str = ""
    zone_src: str = ""
    zone_side: str = ""
    zone_px_lo: float = 0.0
    zone_px_hi: float = 0.0
    zone_ts_ms: int = 0
    zone_weight: float = 0.0

    # Market context
    regime: str = "na"         # "range"|"trend"|... (your labels)
    atr: float = 0.0

    # Absorption-level calibration readiness/stability
    abs_lvl_ready: int = 0
    abs_lvl_th_unstable: int = 0

    # Strong OF gate diagnostics (last decision)
    of_confirm_score: float = 0.0
    strong_gate_have: int = 0
    strong_gate_need: int = 0
    strong_gate_scn: str = ""

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "SymbolSnapshot":
        def _f(k: str, default: float = 0.0) -> float:
            try: return float(d.get(k, default))
            except Exception: return float(default)
        def _i(k: str, default: int = 0) -> int:
            try: return int(d.get(k, default))
            except Exception: return int(default)
        def _s(k: str, default: str = "") -> str:
            try: return str(d.get(k, default) or default)
            except Exception: return str(default)

        return SymbolSnapshot(
            symbol=_s("symbol", ""),
            ts_ms=_i("ts_ms", 0),
            trend_dir=_s("trend_dir", "NONE"),
            close_px=_f("close_px", 0.0),
            close_cross=_i("close_cross", 0),
            close_cross_dir=_s("close_cross_dir", "NONE"),
            close_cross_level=_f("close_cross_level", 0.0),
            swing_high_0=_f("swing_high_0", 0.0),
            swing_high_1=_f("swing_high_1", 0.0),
            swing_low_0=_f("swing_low_0", 0.0),
            swing_low_1=_f("swing_low_1", 0.0),
            swing_ts_high_0=_i("swing_ts_high_0", 0),
            swing_ts_high_1=_i("swing_ts_high_1", 0),
            swing_ts_low_0=_i("swing_ts_low_0", 0),
            swing_ts_low_1=_i("swing_ts_low_1", 0),
            of_strong=_i("of_strong", 0),
            of_dir=_s("of_dir", "NONE"),
            of_ts_ms=_i("of_ts_ms", 0),
            weak_progress=_i("weak_progress", 0),
            reclaim=_i("reclaim", 0),
            reclaim_dir=_s("reclaim_dir", "NONE"),
            reclaim_ts_ms=_i("reclaim_ts_ms", 0),
            sweep=_i("sweep", 0),
            sweep_dir=_s("sweep_dir", "NONE"),
            sweep_ts_ms=_i("sweep_ts_ms", 0),
            obi_stable_sec=_f("obi_stable_sec", 0.0),
            iceberg_strict=_i("iceberg_strict", 0),
            div_kind=_s("div_kind", "none"),
            div_ts_ms=_i("div_ts_ms", 0),
            rsi14=_f("rsi14", 0.0),
            cvd_slope=_f("cvd_slope", 0.0),
            retrace_atr=_f("retrace_atr", 0.0),
            
            # New fields for SMT V2
            delta_z=_f("delta_z", 0.0),
            delta_eff_norm=_f("delta_eff_norm", 0.0),
            zone_dist_bp=_f("zone_dist_bp", 0.0),
            zone_ok=_i("zone_ok", 0),
            near_zone=_i("near_zone", 0),
            abs_lvl_ok=_i("abs_lvl_ok", 0),
            
            # Real zone fields
            zone_id=_s("zone_id", ""),
            zone_type=_s("zone_type", ""),
            zone_src=_s("zone_src", ""),
            zone_side=_s("zone_side", ""),
            zone_px_lo=_f("zone_px_lo", 0.0),
            zone_px_hi=_f("zone_px_hi", 0.0),
            zone_ts_ms=_i("zone_ts_ms", 0),
            zone_weight=_f("zone_weight", 0.0),
            
            # Market context
            regime=_s("regime", "na"),
            atr=_f("atr", 0.0),
            
            # Absorption/Gate diagnostics
            abs_lvl_ready=_i("abs_lvl_ready", 0),
            abs_lvl_th_unstable=_i("abs_lvl_th_unstable", 0),
            of_confirm_score=_f("of_confirm_score", 0.0),
            strong_gate_have=_i("strong_gate_have", 0),
            strong_gate_need=_i("strong_gate_need", 0),
            strong_gate_scn=_s("strong_gate_scn", ""),
            
            # NEW DQ / Pressure
            pressure_sps=_f("pressure_sps", 0.0),
            pressure_hi=_i("pressure_hi", 0),
            pressure_1m=_f("pressure_1m", 0.0),
            spread_bp=_f("spread_bp", 0.0),
            
            book_rate_hz=_f("book_rate_hz", 0.0),
            book_rate_z=_f("book_rate_z", 0.0),
            book_age_ms=_i("book_age_ms", 0),
            book_health_ok=_i("book_health_ok", 1),
            book_health=_s("book_health", "OK"),
            
            obi_age_ms=_i("obi_age_ms", 10**9),
            iceberg_age_ms=_i("iceberg_age_ms", 10**9),
            cooldown_sps=_f("cooldown_sps", 0.0),
            spread_z=_f("spread_z", 0.0),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"), ensure_ascii=False)


@dataclass
class SMTDiv:
    kind: str  # "bullish_regular", "bullish_hidden", "bearish_regular", "bearish_hidden", "bullish_smt", "bearish_smt"
    ts_ms: int


def detect_smt_divergence(leader: SymbolSnapshot, sat: SymbolSnapshot) -> Optional[SMTDiv]:
    """
    Detect SMT divergence between leader and satellite.
    Simplified logic for SMT V2 basket.
    """
    # 1. Compare last Swing Lows (for Bullish)
    # Leader makes Lower Low (LL): L0 < L1
    # Satellite makes Higher Low (HL): S0 > S1
    # => Bullish SMT (Satellite shows strength)
    
    l_l0 = leader.swing_low_0
    l_l1 = leader.swing_low_1
    s_l0 = sat.swing_low_0
    s_l1 = sat.swing_low_1
    
    if l_l0 < l_l1 and s_l0 > s_l1:
        if l_l0 > 0 and l_l1 > 0 and s_l0 > 0 and s_l1 > 0:
            return SMTDiv(kind="bullish_smt", ts_ms=int(sat.ts_ms))

    # 2. Compare last Swing Highs (for Bearish)
    # Leader makes Higher High (HH): H0 > H1
    # Satellite makes Lower High (LH): H0 < H1
    # => Bearish SMT (Satellite shows weakness)
    
    l_h0 = leader.swing_high_0
    l_h1 = leader.swing_high_1
    s_h0 = sat.swing_high_0
    s_h1 = sat.swing_high_1
    
    if l_h0 > l_h1 and s_h0 < s_h1:
        if l_h0 > 0 and l_h1 > 0 and s_h0 > 0 and s_h1 > 0:
            return SMTDiv(kind="bearish_smt", ts_ms=int(sat.ts_ms))

    return None
