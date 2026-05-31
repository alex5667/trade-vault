"""Unit tests for domain/handlers edge_directional_bias extraction helpers.

Covers:
  _parse_boolish_optional  — bool/int/str → bool|None
  _extract_edge_directional_bias_from_payload — signal_payload → (float, bool, str)|None
  _enrich_closed_from_pos  — transfer from PositionState.signal_payload → TradeClosed
"""
from __future__ import annotations

import pytest

from domain.handlers import (
    _extract_edge_directional_bias_from_payload,
    _parse_boolish_optional,
)
from domain.models import TradeClosed


# ---------------------------------------------------------------------------
# _parse_boolish_optional
# ---------------------------------------------------------------------------


def test_parse_boolish_optional_true_variants() -> None:
    for v in (True, 1, "1", "true", "True", "yes", "on"):
        assert _parse_boolish_optional(v) is True, f"expected True for {v!r}"


def test_parse_boolish_optional_false_variants() -> None:
    for v in (False, 0, "0", "false", "False", "no", "off"):
        assert _parse_boolish_optional(v) is False, f"expected False for {v!r}"


def test_parse_boolish_optional_none_on_missing() -> None:
    assert _parse_boolish_optional(None) is None


def test_parse_boolish_optional_does_not_raise_on_bad_input() -> None:
    result = _parse_boolish_optional(object())
    assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# _extract_edge_directional_bias_from_payload
# ---------------------------------------------------------------------------


def test_extract_from_indicators() -> None:
    payload = {
        "indicators": {
            "edge_directional_bias_value": 0.06,
            "edge_directional_bias_countertrend": True,
            "edge_directional_bias_source": "env",
        }
    }

    value, countertrend, source = _extract_edge_directional_bias_from_payload(payload)

    assert value == pytest.approx(0.06)
    assert countertrend is True
    assert source == "env"


def test_extract_config_snapshot_fallback() -> None:
    payload = {
        "config_snapshot": {
            "indicators": {
                "edge_directional_bias_value": "0.03",
                "edge_directional_bias_countertrend": "1",
                "edge_directional_bias_source": "autocal",
            }
        }
    }

    value, countertrend, source = _extract_edge_directional_bias_from_payload(payload)

    assert value == pytest.approx(0.03)
    assert countertrend is True
    assert source == "autocal"


def test_extract_string_payload_json_parsed() -> None:
    import json

    payload = json.dumps({
        "indicators": {
            "edge_directional_bias_value": 0.04,
            "edge_directional_bias_countertrend": False,
            "edge_directional_bias_source": "autocal",
        }
    })

    value, countertrend, source = _extract_edge_directional_bias_from_payload(payload)

    assert value == pytest.approx(0.04)
    assert countertrend is False
    assert source == "autocal"


def test_extract_returns_none_when_no_indicators() -> None:
    value, countertrend, source = _extract_edge_directional_bias_from_payload({})

    assert value is None
    assert countertrend is None
    assert source is None


def test_extract_returns_none_on_empty_input() -> None:
    value, countertrend, source = _extract_edge_directional_bias_from_payload(None)

    assert value is None
    assert countertrend is None
    assert source is None


def test_extract_ignores_nonfinite_value() -> None:
    payload = {
        "indicators": {
            "edge_directional_bias_value": float("inf"),
            "edge_directional_bias_countertrend": True,
            "edge_directional_bias_source": "env",
        }
    }

    value, countertrend, source = _extract_edge_directional_bias_from_payload(payload)

    assert value is None
    assert countertrend is True
    assert source == "env"


def test_extract_source_truncated_to_16_chars() -> None:
    payload = {
        "indicators": {
            "edge_directional_bias_value": 0.05,
            "edge_directional_bias_countertrend": False,
            "edge_directional_bias_source": "x" * 40,
        }
    }

    _, _, source = _extract_edge_directional_bias_from_payload(payload)

    assert source is not None
    assert len(source) <= 16


# ---------------------------------------------------------------------------
# _enrich_closed_from_pos copies bias from signal_payload → TradeClosed
# ---------------------------------------------------------------------------


def _make_pos(signal_payload: dict | None = None):
    """Build a minimal PositionState for handler tests."""
    from domain.models import PositionState

    pos = PositionState(
        id="p1",
        sid="s1",
        strategy="test",
        source="test",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=1_000_000,
        lot=1.0,
        qty=1.0,
        quantity=1.0,
        remaining_qty=0.0,
        sl=99.0,
        tp_levels=[101.0],
    )
    if signal_payload is not None:
        pos.signal_payload = signal_payload
    return pos


def test_enrich_closed_copies_bias_from_payload() -> None:
    """signal_payload.indicators values reach TradeClosed even when pos attrs are default."""
    from domain.handlers import _enrich_closed_from_pos

    pos = _make_pos(
        signal_payload={
            "indicators": {
                "edge_directional_bias_value": 0.06,
                "edge_directional_bias_countertrend": True,
                "edge_directional_bias_source": "env",
            }
        }
    )

    closed = _enrich_closed_from_pos(TradeClosed(), pos, exit_px=101.0, now_ms=2_000_000)

    assert closed.edge_directional_bias_value == pytest.approx(0.06)
    assert closed.edge_directional_bias_countertrend is True
    assert closed.edge_directional_bias_source == "env"


def test_enrich_closed_defaults_not_masked_by_payload_zero() -> None:
    """When payload carries 0-bias (baseline trade), closed stays at default 0.0."""
    from domain.handlers import _enrich_closed_from_pos

    pos = _make_pos(
        signal_payload={
            "indicators": {
                "edge_directional_bias_value": 0.0,
                "edge_directional_bias_countertrend": False,
                "edge_directional_bias_source": "none",
            }
        }
    )

    closed = _enrich_closed_from_pos(TradeClosed(), pos, exit_px=101.0, now_ms=2_000_000)

    assert closed.edge_directional_bias_value == pytest.approx(0.0)
    assert closed.edge_directional_bias_source == "none"


def test_enrich_closed_no_payload_leaves_defaults() -> None:
    """Position with no signal_payload → TradeClosed retains bias defaults."""
    from domain.handlers import _enrich_closed_from_pos

    pos = _make_pos(signal_payload=None)

    closed = _enrich_closed_from_pos(TradeClosed(), pos, exit_px=101.0, now_ms=2_000_000)

    assert closed.edge_directional_bias_value == pytest.approx(0.0)
    assert closed.edge_directional_bias_countertrend is False
    assert closed.edge_directional_bias_source == "none"


def test_enrich_closed_config_snapshot_fallback() -> None:
    """config_snapshot.indicators used when top-level indicators absent."""
    from domain.handlers import _enrich_closed_from_pos

    pos = _make_pos(
        signal_payload={
            "config_snapshot": {
                "indicators": {
                    "edge_directional_bias_value": 0.04,
                    "edge_directional_bias_countertrend": False,
                    "edge_directional_bias_source": "autocal",
                }
            }
        }
    )

    closed = _enrich_closed_from_pos(TradeClosed(), pos, exit_px=101.0, now_ms=2_000_000)

    assert closed.edge_directional_bias_value == pytest.approx(0.04)
    assert closed.edge_directional_bias_countertrend is False
    assert closed.edge_directional_bias_source == "autocal"
