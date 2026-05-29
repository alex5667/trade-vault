"""Tests for Phase 0 infra hardening of the v15_of P1 feature rollout.

Covers:
  - Phase 0.2 shape guard `_validate_edge_stack_shape`
  - Phase 0.3 liquidation `_SymbolWindow.quality_status` derivation
"""

from __future__ import annotations

import pytest


# ─── Phase 0.2 — model shape guard ─────────────────────────────────────────────

def test_shape_guard_accepts_subset_for_known_schema():
    from services.ml_confirm.model_loader import _validate_edge_stack_shape

    # Subset of registry size — training routinely drops low-coverage cols.
    pack = {"feature_schema_ver": "v15_of", "feature_cols": ["f_x"] * 100}
    assert _validate_edge_stack_shape(pack, "/tmp/model.joblib") is True


def test_shape_guard_rejects_oversized_known_schema():
    from services.ml_confirm.model_loader import _validate_edge_stack_shape

    # Too many cols → fail-closed on a registry-known schema.
    pack = {"feature_schema_ver": "v15_of", "feature_cols": ["f_x"] * 99999}
    assert _validate_edge_stack_shape(pack, "/tmp/model.joblib") is False


def test_shape_guard_passes_unknown_schema_with_warning():
    from services.ml_confirm.model_loader import _validate_edge_stack_shape

    # Trainer-only naming like v15_lgbm is not in the registry; we accept and
    # log a warning so new trainers aren't blocked.
    pack = {"feature_schema_ver": "v15_lgbm", "feature_cols": ["f_x"] * 100}
    assert _validate_edge_stack_shape(pack, "/tmp/model.joblib") is True


def test_shape_guard_handles_missing_schema_ver():
    from services.ml_confirm.model_loader import _validate_edge_stack_shape

    pack = {"feature_cols": ["f_x"] * 100}
    # No schema_ver → behaves like unknown, accepts.
    assert _validate_edge_stack_shape(pack, "/tmp/model.joblib") is True


def test_shape_guard_emits_mismatch_metric_on_failure():
    from services.ml_confirm.model_loader import _validate_edge_stack_shape
    from services.orderflow.metrics import ml_feature_schema_hash_mismatch_total

    # Read counter sample value before / after to verify increment.
    def _sample(ver: str, expected: str, got: str) -> float:
        for m in ml_feature_schema_hash_mismatch_total.collect():
            for s in m.samples:
                if s.name.endswith("_total") and s.labels == {
                    "ver": ver, "expected": expected, "got": got
                }:
                    return s.value
        return 0.0

    pack = {"feature_schema_ver": "v15_of", "feature_cols": ["f_x"] * 99999}
    expected_n = None
    try:
        from core.feature_registry import get_schema_info
        expected_n = str(len(get_schema_info("v15_of").feature_names))
    except Exception:
        pytest.skip("v15_of not in registry; skip metric assertion")

    before = _sample("v15_of", expected_n, "99999")
    _validate_edge_stack_shape(pack, "/tmp/model.joblib")
    after = _sample("v15_of", expected_n, "99999")
    assert after == before + 1


# ─── Phase 0.3 — liquidation quality_status derivation ─────────────────────────

def test_liquidation_quality_absent_when_no_events():
    from services.orderflow.liquidation_context_worker import _SymbolWindow

    w = _SymbolWindow(window_ms=60_000, history_max=600, stress_z_thr=2.0)
    snap = w.build_snapshot("BTCUSDT", now_ms=1_000_000)
    assert snap.quality_status == "absent"
    assert snap.liq_event_count_1m == 0


def test_liquidation_quality_ok_when_event_in_window():
    from services.orderflow.liquidation_context_worker import LiqEvent, _SymbolWindow

    w = _SymbolWindow(window_ms=60_000, history_max=600, stress_z_thr=2.0)
    w.push(LiqEvent(ts_ms=1_000_000, symbol="BTCUSDT", order_side="BUY", notional_usd=10_000.0))
    snap = w.build_snapshot("BTCUSDT", now_ms=1_010_000)
    assert snap.quality_status == "OK"


def test_liquidation_quality_stale_after_window_expired():
    from services.orderflow.liquidation_context_worker import LiqEvent, _SymbolWindow

    w = _SymbolWindow(window_ms=60_000, history_max=600, stress_z_thr=2.0)
    w.push(LiqEvent(ts_ms=1_000_000, symbol="BTCUSDT", order_side="SELL", notional_usd=5_000.0))
    # 120s later — event has been evicted from the rolling window.
    snap = w.build_snapshot("BTCUSDT", now_ms=1_000_000 + 120_000)
    assert snap.quality_status == "stale"
    assert snap.liq_event_count_1m == 0
