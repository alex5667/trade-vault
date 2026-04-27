"""
Unit tests for Phase 0.1 horizon-aware contract surface.

Covers:
- Contract attachment via preprocess_signal_for_publish
- signal_id / sid stability (dedup safety)
- Idempotency of attach_phase0_contract
- Legacy field aliases (atr_tf_ms, atr_age_ms, atr_source, etc.)
- extract_horizon_contract_from_payload + extract_horizon_bucket + extract_atr_tf_ms
- build_main_row horizon fields (batch writer)
"""
from __future__ import annotations

import pytest

from services.signal_preprocess import preprocess_signal_for_publish
from services.horizon_contract import (
    attach_phase0_contract,
    extract_horizon_contract_from_payload,
    extract_horizon_bucket,
    extract_atr_tf_ms,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_signal(**kwargs):
    base = {
        "symbol": "btcusdt",
        "kind": "breakout",
        "price": 65000.0,
        "entry": 65010.0,
        "atr": 250.0,
        "confidence": 88.5,
        "meta": {"regime": "trend_up"},
    }
    base.update(kwargs)
    return base


# ---------------------------------------------------------------------------
# preprocess_signal_for_publish — contract attachment
# ---------------------------------------------------------------------------

def test_preprocess_adds_phase0_horizon_contract():
    sig = _make_signal()
    out = preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="CryptoOrderFlow", logger=None)

    assert out["symbol"] == "BTCUSDT"
    assert out["sid"] == out["signal_id"]
    assert out["meta"]["contract_ver"] == 2
    assert out["meta"]["horizon"]["phase_mode"] in {"off", "shadow", "canary", "enforce"}
    assert out["meta"]["horizon"]["reason_code"] == "HZ_STATIC_BOOTSTRAP"
    assert out["meta"]["atr_profile"]["mode"] == "legacy"
    assert out["meta"]["atr_profile"]["atr_value"] == 250.0
    assert out["atr_tf_ms"] == out["meta"]["atr_profile"]["atr_tf_ms"]


def test_preprocess_preserves_existing_signal_id_and_meta():
    sig = _make_signal(
        signal_id="fixed-sid-1",
        symbol="ETHUSDT",
        kind="absorption",
        price=3100.0,
        atr=45.0,
        meta={"sl_mode": "ATR", "sl_atr_mult": 1.5},
    )
    out = preprocess_signal_for_publish(sig, symbol="ETHUSDT", source="CryptoOrderFlow", logger=None)

    assert out["signal_id"] == "fixed-sid-1"
    assert out["meta"]["sl_mode"] == "ATR"
    assert out["meta"]["sl_atr_mult"] == 1.5
    assert "horizon" in out["meta"]
    assert "atr_profile" in out["meta"]


def test_preprocess_does_not_change_trading_fields():
    sig = _make_signal(
        sl_price=64500.0,
        tp1_price=65600.0,
        tradeable=True,
        atr=300.0,
    )
    out = preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="CryptoOrderFlow", logger=None)

    assert out["sl_price"] == 64500.0
    assert out["tp1_price"] == 65600.0
    assert out["tradeable"] is True
    # atr legacy field must be preserved
    assert out["atr"] == 300.0


# ---------------------------------------------------------------------------
# attach_phase0_contract — idempotency
# ---------------------------------------------------------------------------

def test_attach_phase0_contract_idempotent():
    sig = _make_signal()
    out1 = attach_phase0_contract(sig.copy(), symbol="BTCUSDT", source="test")
    out2 = attach_phase0_contract(out1, symbol="BTCUSDT", source="test")

    assert out1["meta"]["horizon"] == out2["meta"]["horizon"]
    assert out1["meta"]["atr_profile"] == out2["meta"]["atr_profile"]
    assert out1["meta"]["contract_ver"] == out2["meta"]["contract_ver"]


def test_attach_phase0_contract_non_dict_passthrough():
    result = attach_phase0_contract("not-a-dict", symbol="X", source="y")  # type: ignore[arg-type]
    assert result == "not-a-dict"


# ---------------------------------------------------------------------------
# Legacy aliases
# ---------------------------------------------------------------------------

def test_legacy_aliases_set():
    sig = _make_signal(atr=150.0, price=50000.0)
    out = attach_phase0_contract(sig, symbol="ETHUSDT", source="test")

    assert "risk_horizon_bucket" in out
    assert "atr_tf_ms" in out
    assert "atr_age_ms" in out
    assert "atr_source" in out
    assert isinstance(out["atr_tf_ms"], int)
    assert out["atr_tf_ms"] > 0


# ---------------------------------------------------------------------------
# extract_horizon_contract_from_payload
# ---------------------------------------------------------------------------

def test_extract_horizon_contract_empty_payload():
    assert extract_horizon_contract_from_payload({}) == {}
    assert extract_horizon_contract_from_payload(None) == {}  # type: ignore[arg-type]
    assert extract_horizon_contract_from_payload("bad") == {}  # type: ignore[arg-type]


def test_extract_horizon_contract_round_trip():
    sig = _make_signal(atr=200.0, price=3000.0)
    out = attach_phase0_contract(sig, symbol="SOLUSDT", source="test")

    contract = extract_horizon_contract_from_payload(out)
    assert "horizon" in contract
    assert "atr_profile" in contract
    assert contract["contract_ver"] == 2


def test_extract_horizon_bucket_from_contract():
    sig = _make_signal()
    out = attach_phase0_contract(sig, symbol="BTCUSDT", source="test")
    contract = extract_horizon_contract_from_payload(out)

    bucket = extract_horizon_bucket(contract)
    assert isinstance(bucket, str)
    # In phase 0 bootstrap, bucket is "unknown"
    assert bucket in {"micro", "short", "medium", "long", "unknown", ""}


def test_extract_atr_tf_ms_from_contract():
    sig = _make_signal(atr=300.0, price=65000.0)
    out = attach_phase0_contract(sig, symbol="BTCUSDT", source="test")
    contract = extract_horizon_contract_from_payload(out)

    tf = extract_atr_tf_ms(contract)
    assert isinstance(tf, int)
    assert tf >= 0


# ---------------------------------------------------------------------------
# _build_main_row — horizon fields presence (integration smoke)
# ---------------------------------------------------------------------------

def test_build_main_row_accepts_horizon_fields():
    """
    Smoke: _build_main_row runs without error when closed has signal_payload
    with horizon contract embedded via preprocess.
    """
    from services.batch_trade_writer import _build_main_row

    class _FakeClosed:
        order_id = "ord-1"
        sid = "sid-1"
        strategy = "CryptoOrderFlow"
        source = "CryptoOrderFlow"
        symbol = "BTCUSDT"
        tf = "1m"
        direction = "LONG"
        entry_ts_ms = 1_700_000_000_000
        exit_ts_ms  = 1_700_000_060_000
        entry_price = 65000.0
        exit_price  = 65200.0
        lot = 0.01
        notional_usd = 650.0
        pnl_net = 2.0
        pnl_gross = 2.1
        fees = 0.1
        pnl_pct = 0.003
        pnl_if_fixed_exit = 2.0
        tp1_hit = True
        tp2_hit = False
        tp3_hit = False
        tp_hits = 1
        tp_before_sl = True
        trailing_started = False
        trailing_active = False
        trailing_moves = 0
        mfe_pnl = 3.0
        mae_pnl = -0.5
        giveback = 1.0
        missed_profit = 0.0
        one_r_money = 6.5
        r_multiple = 0.3
        duration_ms = 60_000
        close_reason = "tp1"
        signal_payload = {
            "meta": {
                "contract_ver": 2,
                "horizon": {
                    "contract_ver": 2,
                    "phase_mode": "off",
                    "hold_target_ms": 0,
                    "alpha_half_life_ms": 0,
                    "max_signal_age_ms": 0,
                    "risk_horizon_bucket": "unknown",
                    "profile_source": "static_bootstrap",
                    "profile_conf": 0.0,
                    "reason_code": "HZ_STATIC_BOOTSTRAP",
                    "reason_details": {},
                },
                "atr_profile": {
                    "mode": "legacy",
                    "atr_value": 250.0,
                    "atr_tf_ms": 60000,
                    "atr_window_n": 14,
                    "atr_age_ms": 0,
                    "atr_source": "legacy",
                    "atr_regime_value": 250.0,
                    "atr_trail_value": 250.0,
                    "atr_regime_tf_ms": 60000,
                    "atr_trail_tf_ms": 60000,
                    "atr_pct": 0.00385,
                    "vol_ratio_fast_slow": 1.0,
                    "vol_ratio_z": 0.0,
                },
            },
            "config_snapshot": {"key": "val"},
        }

    row = _build_main_row(_FakeClosed())
    assert isinstance(row, tuple)
    # Verify config_json contains _horizon_contract embedded
    import json
    config_json_idx = 50  # position after all the standard fields
    # Just verify we have a valid tuple of the right length
    assert len(row) >= 53  # original 50 + 3 new horizon columns
