from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict

import json


@dataclass
class OFInputsV1:
    """
    Minimal, deterministic inputs used to compute StrongGateDecision + OFConfirmV3.
    This is the key for golden replay: replay doesn't need book/microbar streams.
    """
    v: int
    symbol: str
    ts_ms: int
    regime: str                    # "na" | "trend" | "range" | "thin" ... (best-effort)
    direction: str                 # LONG/SHORT
    scenario: str                  # reversal/continuation

    # Reversal inputs
    delta_z: float
    weak_progress: int             # 1/0
    sweep_recent: int              # 1/0
    reclaim_recent: int            # 1/0
    obi_stable: int                # 1/0
    iceberg_strict: int            # 1/0
    abs_lvl_ok: int                # 1/0

    # Continuation inputs
    trend_dir: str                 # LONG/SHORT/NONE (required for continuation)
    hidden_ctx_recent: int         # 1/0
    cont_ctx_recent: int           # 1/0

    # Config subset (to replay exactly even if prod config changes later)
    cfg: Dict[str, Any]

    # Calibration inputs (from last microbar)
    fp_eff_quote: float
    fp_quote_delta: float

    # --- Optional fields (all with defaults below this line) ---
    sid: str = ""                  # Correlation ID with StrongGateDecision
    fp_move_bp: float = 0.0        # optional diagnostic

    # Hawkes-like intensities (burst features)
    hawkes_taker_lam: float = 0.0  # Hawkes intensity for taker events (events/sec)
    hawkes_cancel_lam: float = 0.0  # Hawkes intensity for cancel events (events/sec)
    hawkes_churn_lam: float = 0.0  # Hawkes intensity for churn (taker + cancel) (events/sec)
    hawkes_dt_s: float = 0.0       # Time delta since last update (seconds)

    # Regime grouping for ML dataset (default: "na" for backward compatibility)
    regime_group: str = "na"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        """Deterministic JSON (stable keys) for replay / training."""
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass
class OFInputsV2(OFInputsV1):
    """
    Extended inputs contract with OFI and FP edge as first-class fields.
    Inherits all fields from OFInputsV1 and adds:
    - OFI (microstructure) fields: ofi, ofi_z, ofi_stable, ofi_dir_ok, ofi_stable_secs, ofi_stability_score, ofi_age_ms
    - FP edge (absorption/edge) fields: fp_edge_absorb, fp_edge_absorb_strength, fp_edge_age_ms
    
    Version field 'v' should be set to 2 for V2 inputs.
    """
    # Version identifier (should be 2 for V2)
    # Note: 'v' is inherited from OFInputsV1, but we document it here for clarity
    
    # --- OFI (microstructure) ---
    ofi: float = 0.0                    # OFI value
    ofi_z: float = 0.0                  # OFI z-score
    ofi_stable: int = 0                 # 1 if OFI is stable, 0 otherwise
    ofi_dir_ok: int = 0                 # 1 if OFI direction matches signal direction, 0 otherwise
    ofi_stable_secs: float = 0.0        # Duration of OFI stability (seconds)
    ofi_stability_score: float = 0.0    # Stability score (0..1)
    ofi_age_ms: int = -1               # Age of last OFI event (ms), -1 if not available (critical for determinism)
    
    # --- FP edge (absorption/edge) ---
    fp_edge_absorb: int = 0            # 1 if FP edge absorption detected, 0 otherwise
    fp_edge_absorb_strength: float = 0.0  # Strength of FP edge absorption (normalized)
    fp_edge_age_ms: int = -1            # Age of last FP edge event (ms), -1 if not available (critical for determinism)

    # --- Execution risk / Slippage ---
    spread_bps: float = 0.0
    expected_slippage_bps: float = 0.0

    # --- Session one-hot (train==serve: deterministic from ts_ms only) ---
    # Computed by session_onehot(tick_ts_ms) in tick_processor.py (A5 block).
    # Mutually exclusive: exactly one of these is 1 per record.
    # Backward-compatible: old readers that don’t know about these fields simply ignore them.
    session_asia: int = 0
    session_eu: int = 0
    session_us: int = 0
    session_off: int = 0

    # --- Confirmations as first-class ML features (Stage 4, partial) ---
    # Keep these as stable ints (0/1) for train==serve determinism.
    rsi_agree: int = 0
    div_match: int = 0

    # --- Sweep Distinction (Stage 4) ---
    sweep_eqh: int = 0
    sweep_eql: int = 0

    # --- LOB pressure (P91) ---
    # All features computed by BookProcessor from top-5 L2 snapshot.
    # Kept as first-class dataclass fields (not dict) to guarantee train==serve parity.
    lob_qi_mean: float = 0.0               # Mean queue imbalance L1..L5
    lob_qi_max_abs: float = 0.0            # Max absolute queue imbalance across L1..L5
    lob_qi_slope: float = 0.0              # Slope of queue imbalance over depth levels
    lob_micro_mid_div_bps: float = 0.0     # Microprice divergence vs mid (bps); +ve = bid pressure
    lob_micro_shift_bps: float = 0.0       # Microprice shift vs previous snapshot (bps)
    lob_depth_slope_imb: float = 0.0       # Depth slope imbalance (bid - ask), qty per bps
    lob_depth_convexity_imb: float = 0.0   # Depth convexity imbalance (bid - ask), far/near ratio proxy
    lob_dw_obi: float = 0.0                # Depth-weighted OBI using 1/level weights
    lob_dw_obi_z: float = 0.0             # Robust z-score of dw_obi (rolling median/MAD)
    lob_dw_obi_stability_score: float = 0.0  # Stability quality score [0..1]
    lob_dw_obi_stable_secs: float = 0.0   # Continuous seconds dw_obi stayed in stable direction
    lob_dw_obi_stable: int = 0            # 1 if dw_obi is stable (score + secs above thresholds)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["v"] = 2  # enforce version field — inherited v is 1 by default
        return d


@dataclass
class OFInputsV3(OFInputsV2):
    """
    V3 extends V2 with LOB pressure features (Queue imbalance, Microprice, Slope/Convexity, Depth-weighted OBI)
    for execution-aware scoring and ML training.
    """

    # Queue imbalance per level (L1..L5) and weighted mean
    qimb_l1: float = 0.0
    qimb_l2: float = 0.0
    qimb_l3: float = 0.0
    qimb_l4: float = 0.0
    qimb_l5: float = 0.0
    qimb_wmean: float = 0.0

    # Microprice / microprice shift vs mid (bps)
    mp_mid_bps: float = 0.0
    mp_shift_bps: float = 0.0

    # Depth (sum qty) to L5
    depth_bid_5: float = 0.0
    depth_ask_5: float = 0.0

    # Book slope/convexity (per side)
    book_slope_bid: float = 0.0
    book_slope_ask: float = 0.0
    book_convex_bid: float = 0.0
    book_convex_ask: float = 0.0

    # Depth-weighted OBI and OFI proxy for ML (normalized)
    obi_dw: float = 0.0
    ofi_ml_norm: float = 0.0

    # Book snapshot age (ms) relative to tick_ts_ms
    book_age_ms: int = 0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["v"] = 3
        return d
