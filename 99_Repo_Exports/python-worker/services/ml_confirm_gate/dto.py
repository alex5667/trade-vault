from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass(slots=True)
class MLConfirmInput:
    sid: str
    symbol: str
    ts_ms: int
    direction: str
    scenario: str
    indicators: Dict[str, Any]
    rule_score: float
    rule_have: int
    rule_need: int
    ok_rule: int
    cancel_spike_veto: int


@dataclass(slots=True)
class MLConfirmOutput:
    allow: bool
    mode: str
    status: str
    p_edge: float
    p_min: float
    p_margin: float
    score: float
    bucket: str
    model_ver: str
    missing: List[str]
    latency_us: int
    reason: str


@dataclass
class MLConfirmDecision:
    mode: str = "OFF"          # OFF|SHADOW|ENFORCE|ERR
    kind: str = "none"         # util_mh_v1|...
    allow: bool = True

    # backwards compatible fields used by OFConfirmEngine final_reason (p_edge/p_min)
    p_edge: float = 0.0
    p_min: float = 0.0

    # util_mh fields
    best_h_ms: int = 0
    score: float = 0.0
    floor: float = 0.0
    bucket: str = "other"
    util_pred: Optional[Dict[str, float]] = None
    unc: Optional[Dict[str, float]] = None
    missing: Optional[List[str]] = None

    model_run_id: str = ""
    model_path: str = ""
    reason: str = ""
    error: str = ""

    # SRE / perf
    latency_us: int = 0

    # SRE / quality (selective prediction)
    abstain: bool = False
    conf: float = 0.0        # 0..1 proxy (see below)
    p_margin: float = 0.0    # p_edge - p_min (works for util_mh too)
    status: str = ""         # ALLOW|BLOCK|ABSTAIN_*|MISSING_*|SHADOW|OFF|ERR
    
    # calibration fields (for metrics and drift tracking)
    p_edge_raw: float = 0.0   # pre-calibration probability
    p_edge_cal: float = 0.0   # post-calibration probability (effective p_edge)
    calib_type: str = ""      # platt_logit|none

    # expert recommendations & risk fields (P74+)
    exec_risk_ref_bps: float = 0.0
    exec_risk_bps: float = 0.0
    exec_risk_norm: float = 0.0
    exec_pen: float = 0.0
    score_breakdown_small: Optional[Dict[str, Any]] = None
    score_breakdown_json: str = ""

    # cfg diagnostics (for metrics/debug)
    cfg_key_used: str = ""
    cfg_source: str = ""        # champion|challenger
    cfg_raw_len: int = 0
    cfg_parse_err: str = ""

    # per-symbol mode resolution (for observability)
    effective_mode: str = ""    # resolved mode after per-symbol overrides
    mode_source: str = ""      # global|cfg_per_symbol|env_per_symbol|canary|cfg_per_symbol_canary

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode
            "kind": self.kind
            "allow": bool(self.allow)
            "p_edge": float(self.p_edge)
            "p_min": float(self.p_min)
            "best_h_ms": int(self.best_h_ms)
            "score": float(self.score)
            "floor": float(self.floor)
            "bucket": str(self.bucket)
            "util_pred": self.util_pred or {}
            "unc": self.unc or {}
            "missing": self.missing or []
            "model_run_id": self.model_run_id
            "model_path": self.model_path
            "reason": self.reason
            "error": self.error
            "latency_us": int(self.latency_us)
            "abstain": int(bool(self.abstain))
            "conf": float(self.conf)
            "p_margin": float(self.p_margin)
            "status": str(self.status)
            "p_edge_raw": float(self.p_edge_raw)
            "p_edge_cal": float(self.p_edge_cal)
            "calib_type": str(self.calib_type or "")
            "cfg_key_used": str(self.cfg_key_used or "")
            "cfg_source": str(self.cfg_source or "")
            "cfg_raw_len": int(self.cfg_raw_len)
            "cfg_parse_err": str(self.cfg_parse_err or "")
            "effective_mode": str(self.effective_mode or self.mode)
            "mode_source": str(self.mode_source or "global")

            # P74+
            "exec_risk_ref_bps": float(self.exec_risk_ref_bps)
            "exec_risk_bps": float(self.exec_risk_bps)
            "exec_risk_norm": float(self.exec_risk_norm)
            "exec_pen": float(self.exec_pen)
            "score_breakdown_small": self.score_breakdown_small or {}
            "score_breakdown_json": str(self.score_breakdown_json or "")
        }
