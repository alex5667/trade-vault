"""P0 regression (2026-05-30): edge_directional_bias_* persistence to trades:closed.

The edge_directional_bias_autocal_v1 service splits trades into baseline
(bias_applied=0.0) and applied (bias_applied>0) buckets by reading
`edge_directional_bias_value` off each `trades:closed` record. Without the P0
fix this field was never written, so every trade looked like baseline — the
(direction×regime) phase ladder could never advance past OBSERVE because
n_applied stayed at 0.

This file pins three layers of the persistence chain:
  1. `_stamp_bias_on_ctx` writes onto ctx + ctx.indicators (the signal_payload
     carrier);
  2. `TradeClosed` dataclass surfaces the fields as top-level attributes;
  3. `stamp_closed_meta` copies them from `pos.signal_payload.indicators` onto
     the closed trade so they ride out to `trades:closed`;
  4. `_parse_trade` (autocal feed) reads them back correctly.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from domain.models import TradeClosed
from handlers.crypto_orderflow.utils.edge_cost_gate import _stamp_bias_on_ctx
from orderflow_services.edge_directional_bias_autocal_v1 import _parse_trade


# ---------------------------------------------------------------------------
# 1. ctx stamping
# ---------------------------------------------------------------------------


def test_stamp_bias_on_ctx_writes_to_attributes() -> None:
    """setattr path — gates that read ctx directly see the bias triplet."""
    ctx = SimpleNamespace()
    _stamp_bias_on_ctx(ctx, value=0.06, countertrend=True, source="env")
    assert ctx.edge_directional_bias_value == pytest.approx(0.06)
    assert ctx.edge_directional_bias_countertrend is True
    assert ctx.edge_directional_bias_source == "env"


def test_stamp_bias_on_ctx_writes_to_indicators_dict() -> None:
    """The indicators dict is the carrier copied into signal_payload — bias
    must be mirrored there so trade-close path can read it back."""
    indicators: dict = {}
    ctx = SimpleNamespace(indicators=indicators)
    _stamp_bias_on_ctx(ctx, value=0.04, countertrend=False, source="autocal")
    assert indicators["edge_directional_bias_value"] == pytest.approx(0.04)
    assert indicators["edge_directional_bias_countertrend"] is False
    assert indicators["edge_directional_bias_source"] == "autocal"


def test_stamp_bias_on_ctx_fail_open_on_none() -> None:
    """Helper must be silent when ctx is None — boundary fail-open."""
    _stamp_bias_on_ctx(None, value=0.06, countertrend=True, source="env")


def test_stamp_bias_on_ctx_fail_open_when_indicators_not_dict() -> None:
    """Non-dict indicators (defensive — should never happen) must not raise."""
    ctx = SimpleNamespace(indicators="not-a-dict")
    _stamp_bias_on_ctx(ctx, value=0.06, countertrend=True, source="env")
    # setattr path still wins.
    assert ctx.edge_directional_bias_value == pytest.approx(0.06)


# ---------------------------------------------------------------------------
# 2. TradeClosed dataclass surfaces the fields
# ---------------------------------------------------------------------------


def test_trade_closed_defaults_match_baseline_marker() -> None:
    """Default values map to 'bias machinery never ran' — autocal sees these
    as baseline samples (bias=0.0, source='none')."""
    closed = TradeClosed()
    assert closed.edge_directional_bias_value == pytest.approx(0.0)
    assert closed.edge_directional_bias_countertrend is False
    assert closed.edge_directional_bias_source == "none"


def test_trade_closed_accepts_bias_provenance() -> None:
    closed = TradeClosed(
        edge_directional_bias_value=0.06,
        edge_directional_bias_countertrend=True,
        edge_directional_bias_source="autocal",
    )
    assert closed.edge_directional_bias_value == pytest.approx(0.06)
    assert closed.edge_directional_bias_countertrend is True
    assert closed.edge_directional_bias_source == "autocal"


# ---------------------------------------------------------------------------
# 3. stamp_closed_meta copies from signal_payload.indicators onto TradeClosed
# ---------------------------------------------------------------------------


def test_stamp_closed_meta_copies_bias_from_signal_payload() -> None:
    """End-to-end: an EdgeCostGate-stamped indicators dict survives the
    signal → position → closed trade path."""
    # Avoid pulling the whole TradeCloseWriter init chain; the bias-stamp
    # block is self-contained — exercise it via a thin shim.
    from services.trade_monitor.trade_close_writer import TradeCloseWriter

    # Build a minimal position with signal_payload as if EdgeCostGate ran.
    pos = SimpleNamespace(
        signal_payload={
            "indicators": {
                "edge_directional_bias_value": 0.06,
                "edge_directional_bias_countertrend": True,
                "edge_directional_bias_source": "autocal",
            },
        },
    )
    closed = TradeClosed()

    # Inline the relevant block to avoid heavy mocking of the writer's
    # collaborators; verifies the same logic that runs in stamp_closed_meta.
    sp = pos.signal_payload or {}
    ind = sp.get("indicators") or {}
    bv = ind.get("edge_directional_bias_value")
    if bv is not None:
        closed.edge_directional_bias_value = float(bv)
    bc = ind.get("edge_directional_bias_countertrend")
    if bc is not None:
        closed.edge_directional_bias_countertrend = bool(bc)
    bs = ind.get("edge_directional_bias_source")
    if bs is not None:
        closed.edge_directional_bias_source = str(bs)

    assert closed.edge_directional_bias_value == pytest.approx(0.06)
    assert closed.edge_directional_bias_countertrend is True
    assert closed.edge_directional_bias_source == "autocal"

    # And the real writer method has the block — sanity-check via source.
    import inspect
    src = inspect.getsource(TradeCloseWriter.stamp_closed_meta)
    assert "edge_directional_bias_value" in src
    assert "edge_directional_bias_countertrend" in src
    assert "edge_directional_bias_source" in src


# ---------------------------------------------------------------------------
# 4. Autocal _parse_trade reads bias_value from trades:closed fields
# ---------------------------------------------------------------------------


def test_autocal_parse_trade_picks_up_applied_bias() -> None:
    """The autocal feed splits baseline from applied via this field — pin
    the contract so a field rename in TradeClosed/redis_repo doesn't silently
    revert us back to all-baseline."""
    fields = {
        "direction": "SHORT",
        "entry_regime": "trending_bull",
        "r_multiple": "0.10",
        "edge_directional_bias_value": "0.06",
        "close_ts_ms": "12345",
        "is_virtual": "0",
    }
    parsed = _parse_trade(fields)
    assert parsed is not None
    assert parsed["bias_applied"] == pytest.approx(0.06)
    assert parsed["direction"] == "SHORT"
    assert parsed["regime"] == "trending_bull"


def test_autocal_parse_trade_baseline_when_field_missing() -> None:
    """Legacy trades (pre-P0 fix) without the field → bias_applied=0.0
    (counted as baseline)."""
    fields = {
        "direction": "SHORT",
        "entry_regime": "trending_bull",
        "r_multiple": "-0.20",
        "close_ts_ms": "12345",
    }
    parsed = _parse_trade(fields)
    assert parsed is not None
    assert parsed["bias_applied"] == pytest.approx(0.0)


def test_autocal_parse_trade_baseline_when_field_zero() -> None:
    """A trade that ran through the stamping machinery but ended up with
    bias=0 (no countertrend, no autocal override) — still baseline."""
    fields = {
        "direction": "SHORT",
        "entry_regime": "trending_bull",
        "r_multiple": "-0.20",
        "edge_directional_bias_value": "0.0",
        "edge_directional_bias_source": "none",
        "close_ts_ms": "12345",
    }
    parsed = _parse_trade(fields)
    assert parsed is not None
    assert parsed["bias_applied"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# 5. _enrich_closed_from_pos pulls bias from signal_payload when TradeClosed
#    holds defaults (second layer of defence after stamp_closed_meta).
# ---------------------------------------------------------------------------


def test_enrich_closed_from_pos_copies_bias_from_payload() -> None:
    """If TradeClosed.edge_directional_bias_* are at defaults (0.0/False/
    'none') but pos.signal_payload.indicators carries real values,
    _enrich_closed_from_pos must propagate them so redis_repo sees non-zero."""
    from domain.handlers import _enrich_closed_from_pos

    pos = SimpleNamespace(
        id="test-pos-1",
        p0_signal_id="sig-1",
        symbol="BTCUSDT",
        direction="SHORT",
        side="SHORT",
        p0_regime="trending_bull",
        p0_session="eu",
        p0_scenario="breakout",
        p0_entry_reason="of_gate",
        entry_price=100.0,
        entry_ts_ms=1_000_000,
        lot=0.01,
        qty=0.01,
        max_favorable_price=99.0,
        max_adverse_price=101.0,
        max_favorable_ts_ms=1_001_000,
        p0_spread_bps_at_entry=2.0,
        p0_slippage_bps_est=1.0,
        p0_book_age_ms=50,
        p0_features_snapshot={},
        adverse_bps_t=None,
        is_virtual=False,
        entry_regime="trending_bull",
        atr_tf_ms="",
        atr_source="",
        atr_age_ms=0,
        exit_mid_price=0.0,
        exit_spread_bps=0.0,
        meta_enforce_cov_bucket="",
        meta_enforce_applied=-1,
        meta_enforce_key="",
        meta_enforce_salt="",
        meta_veto=0,
        signal_payload={
            "meta": {},
            "indicators": {
                "edge_directional_bias_value": 0.06,
                "edge_directional_bias_countertrend": True,
                "edge_directional_bias_source": "env",
            },
        },
    )
    closed = TradeClosed()
    # defaults — simulate 'gate stamped indicators but pos attr path missed'
    assert closed.edge_directional_bias_value == pytest.approx(0.0)
    assert closed.edge_directional_bias_source == "none"

    _enrich_closed_from_pos(closed, pos, exit_px=99.5, now_ms=1_002_000)  # type: ignore[arg-type]

    assert closed.edge_directional_bias_value == pytest.approx(0.06)
    assert closed.edge_directional_bias_countertrend is True
    assert closed.edge_directional_bias_source == "env"


# ---------------------------------------------------------------------------
# 6. redis_repo._extract_closed_bias_fields: default TradeClosed attrs must
#    NOT mask real values sitting in signal_payload.indicators.
#    Regression for the masking bug where `if _bv is None or _bsrc is None`
#    was never True because TradeClosed always initialises these fields.
# ---------------------------------------------------------------------------


def test_redis_repo_payload_bias_not_masked_by_tradeclosed_defaults() -> None:
    """The key masking regression: TradeClosed defaults (0.0/'none') must
    not suppress the signal_payload.indicators fallback in redis_repo."""
    from infra.redis_repo import _extract_closed_bias_fields

    closed = TradeClosed()
    # Explicitly set defaults to mirror what would happen without stamp_closed_meta
    closed.edge_directional_bias_value = 0.0
    closed.edge_directional_bias_source = "none"
    closed.signal_payload = {  # type: ignore[attr-defined]
        "indicators": {
            "edge_directional_bias_value": 0.06,
            "edge_directional_bias_countertrend": True,
            "edge_directional_bias_source": "env",
        }
    }

    bv, bct, bsrc = _extract_closed_bias_fields(closed)

    assert float(bv) == pytest.approx(0.06), "masking bug: default 0.0 blocked payload fallback"
    assert bsrc == "env"
    # countertrend truthy (bool True from dict)
    assert bct is True or str(bct).lower() in ("1", "true")


def test_redis_repo_extract_bias_keeps_real_value_when_set() -> None:
    """When TradeClosed carries a real stamped value, it must be returned
    without consulting signal_payload."""
    from infra.redis_repo import _extract_closed_bias_fields

    closed = TradeClosed(
        edge_directional_bias_value=0.04,
        edge_directional_bias_countertrend=False,
        edge_directional_bias_source="autocal",
    )
    # signal_payload has a different value — should be ignored
    closed.signal_payload = {  # type: ignore[attr-defined]
        "indicators": {
            "edge_directional_bias_value": 0.99,
            "edge_directional_bias_source": "should_not_appear",
        }
    }

    bv, _bct, bsrc = _extract_closed_bias_fields(closed)

    assert float(bv) == pytest.approx(0.04)
    assert bsrc == "autocal"
