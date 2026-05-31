"""Plan 1 — config parsing tests.

Cover the ENV cast helpers, default values, and mode parsing so that a
mistyped ENV (e.g. CONF_META_GATE_MODE=enforce vs ENFORCE) does not
accidentally promote the gate to active.
"""
from __future__ import annotations

import pytest

from services.confidence_meta_gate.config import (
    MetaGateMode,
    _parse_mode,
    reload_config,
)


def test_default_config_is_shadow_and_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in list(__import__("os").environ.keys()):
        if k.startswith("CONF_META_GATE_"):
            monkeypatch.delenv(k, raising=False)
    cfg = reload_config()
    assert cfg.enabled is False
    assert cfg.mode is MetaGateMode.SHADOW
    assert cfg.canary_share == 0.0
    assert cfg.risk_mult_enabled is False


@pytest.mark.parametrize("raw,expected", [
    ("OFF", MetaGateMode.OFF),
    ("off", MetaGateMode.OFF),
    ("Shadow", MetaGateMode.SHADOW),
    ("CANARY", MetaGateMode.CANARY),
    ("enforce", MetaGateMode.ENFORCE),
    ("KILL_SWITCH", MetaGateMode.KILL_SWITCH),
    ("LEGACY_ONLY", MetaGateMode.LEGACY_ONLY),
])
def test_parse_mode_known_values(raw: str, expected: MetaGateMode) -> None:
    assert _parse_mode(raw) is expected


def test_parse_mode_unknown_falls_back_to_shadow() -> None:
    assert _parse_mode("ENFROCE") is MetaGateMode.SHADOW
    assert _parse_mode("") is MetaGateMode.SHADOW
    assert _parse_mode("   ") is MetaGateMode.SHADOW


def test_canary_share_clamped_to_unit_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONF_META_GATE_ENABLED", "1")
    monkeypatch.setenv("CONF_META_GATE_CANARY_SHARE", "5.0")
    cfg = reload_config()
    assert cfg.canary_share == 1.0

    monkeypatch.setenv("CONF_META_GATE_CANARY_SHARE", "-0.5")
    cfg = reload_config()
    assert cfg.canary_share == 0.0


def test_env_float_invalid_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONF_META_GATE_MIN_P_WIN", "not-a-number")
    cfg = reload_config()
    # Default value as declared in config.py
    assert cfg.min_p_win == 0.56


def test_env_bool_recognizes_truthy(monkeypatch: pytest.MonkeyPatch) -> None:
    for v in ("1", "true", "True", "YES", "on"):
        monkeypatch.setenv("CONF_META_GATE_ENABLED", v)
        cfg = reload_config()
        assert cfg.enabled is True, v


def test_env_bool_recognizes_falsy(monkeypatch: pytest.MonkeyPatch) -> None:
    for v in ("0", "false", "no", "off"):
        monkeypatch.setenv("CONF_META_GATE_ENABLED", v)
        cfg = reload_config()
        assert cfg.enabled is False, v
