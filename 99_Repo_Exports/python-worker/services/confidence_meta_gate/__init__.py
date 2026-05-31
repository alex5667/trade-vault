"""confidence_meta_gate — calibrated replacement for the legacy confidence gate.

Plan 1 (2026-05-30). Replaces ONLY the confidence threshold check inside
signal_pipeline; hard-safety gates (data quality, kill-switch, edge cost,
exposure caps) stay above and below meta-gate untouched.

Modes (driven by CONF_META_GATE_MODE):
  OFF          — gate is not computed.
  LEGACY_ONLY  — only the old confidence threshold acts.
  SHADOW       — meta-gate computed and logged but legacy still decides.
  CANARY       — meta-gate replaces legacy ONLY for deterministic share by sid.
  ENFORCE      — meta-gate replaces legacy for every sample.
  KILL_SWITCH  — meta-gate explicitly disabled, fallback to legacy.

Entry points:
  - decide_meta_gate(input) -> ConfidenceMetaGateOutput   (pure)
  - emit_decision(input, output, ...)                     (metrics + stream)
  - get_runtime() -> MetaGateRuntime                       (singleton accessor)
"""
from __future__ import annotations

from .canary import canary_bucket, is_canary_selected
from .config import MetaGateConfig, MetaGateMode, get_config
from .dto import ConfidenceMetaGateInput, ConfidenceMetaGateOutput, MetaGateDecisionT
from .gate import decide_meta_gate, risk_multiplier_from_p_win
from .reason_codes import MetaGateReason
from .runtime import MetaGateRuntime, get_runtime

__all__ = [
    "ConfidenceMetaGateInput",
    "ConfidenceMetaGateOutput",
    "MetaGateConfig",
    "MetaGateDecisionT",
    "MetaGateMode",
    "MetaGateReason",
    "MetaGateRuntime",
    "canary_bucket",
    "decide_meta_gate",
    "emit_decision",
    "get_config",
    "get_runtime",
    "is_canary_selected",
    "risk_multiplier_from_p_win",
]


def emit_decision(*args, **kwargs):
    """Lazy re-export to avoid importing prometheus_client at package load."""
    from .metrics import emit_decision as _emit

    return _emit(*args, **kwargs)
