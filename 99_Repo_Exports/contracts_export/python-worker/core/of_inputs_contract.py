from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict


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
    fp_move_bp: float = 0.0        # optional diagnostic

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
