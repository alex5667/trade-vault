"""Runtime configuration for the confidence meta-gate.

ENV-driven only; no Redis/Consul reads at decision time. Resolution happens
once per process (cached), with an explicit `reload_config()` for tests.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum


class MetaGateMode(str, Enum):
    OFF = "OFF"
    LEGACY_ONLY = "LEGACY_ONLY"
    SHADOW = "SHADOW"
    CANARY = "CANARY"
    ENFORCE = "ENFORCE"
    KILL_SWITCH = "KILL_SWITCH"


_DEFAULT_MODEL_PATH = "/app/ml_models/conf_meta_gate_v1.json"
_DEFAULT_CALIBRATOR_PATH = "/app/ml_models/conf_meta_gate_calibrator_v1.json"
_DEFAULT_SALT = "conf_meta_gate_v1_20260530"


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw


def _parse_mode(raw: str) -> MetaGateMode:
    raw = (raw or "").strip().upper()
    try:
        return MetaGateMode(raw)
    except ValueError:
        return MetaGateMode.SHADOW


@dataclass(frozen=True)
class MetaGateConfig:
    enabled: bool
    mode: MetaGateMode

    model_path: str
    calibrator_path: str

    canary_share: float
    canary_salt: str

    fail_mode: str  # "LEGACY" or "DENY"
    max_model_age_hours: float
    max_calibration_ece: float

    min_p_win: float
    min_expected_r: float
    min_expected_edge_bps: float

    # Soft-cap thresholds — translate to tightened reason codes, not DENY.
    dq_soft_cap: float
    spread_soft_cap_bps: float
    slippage_soft_cap_bps: float

    # Sizing hint (off by default during rollout).
    risk_mult_enabled: bool

    # Observability.
    metrics_stream: str
    decision_stream: str
    sample_features_in_stream: bool


def _load_from_env() -> MetaGateConfig:
    return MetaGateConfig(
        enabled=_env_bool("CONF_META_GATE_ENABLED", False),
        mode=_parse_mode(_env_str("CONF_META_GATE_MODE", "SHADOW")),
        model_path=_env_str("CONF_META_GATE_MODEL_PATH", _DEFAULT_MODEL_PATH),
        calibrator_path=_env_str("CONF_META_GATE_CALIBRATOR_PATH", _DEFAULT_CALIBRATOR_PATH),
        canary_share=max(0.0, min(1.0, _env_float("CONF_META_GATE_CANARY_SHARE", 0.0))),
        canary_salt=_env_str("CONF_META_GATE_CANARY_SALT", _DEFAULT_SALT),
        fail_mode=_env_str("CONF_META_GATE_FAIL_MODE", "LEGACY").upper(),
        max_model_age_hours=_env_float("CONF_META_GATE_MAX_MODEL_AGE_HOURS", 72.0),
        max_calibration_ece=_env_float("CONF_META_GATE_MAX_CALIBRATION_ECE", 0.07),
        min_p_win=_env_float("CONF_META_GATE_MIN_P_WIN", 0.56),
        min_expected_r=_env_float("CONF_META_GATE_MIN_EXPECTED_R", 0.02),
        min_expected_edge_bps=_env_float("CONF_META_GATE_MIN_EXPECTED_EDGE_BPS", 1.5),
        dq_soft_cap=_env_float("CONF_META_GATE_DQ_SOFT_CAP", 0.7),
        spread_soft_cap_bps=_env_float("CONF_META_GATE_SPREAD_SOFT_CAP_BPS", 6.0),
        slippage_soft_cap_bps=_env_float("CONF_META_GATE_SLIPPAGE_SOFT_CAP_BPS", 6.0),
        risk_mult_enabled=_env_bool("CONF_META_GATE_RISK_MULT_ENABLED", False),
        metrics_stream=_env_str("CONF_META_GATE_METRICS_STREAM", "metrics:conf_meta_gate"),
        decision_stream=_env_str("CONF_META_GATE_DECISION_STREAM", "stream:decisions:conf_meta_gate"),
        sample_features_in_stream=_env_bool("CONF_META_GATE_FEATURES_IN_STREAM", False),
    )


_CONFIG: MetaGateConfig | None = None


def get_config() -> MetaGateConfig:
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = _load_from_env()
    return _CONFIG


def reload_config() -> MetaGateConfig:
    """Re-read ENV. Tests call this after monkeypatching os.environ."""
    global _CONFIG
    _CONFIG = _load_from_env()
    return _CONFIG
