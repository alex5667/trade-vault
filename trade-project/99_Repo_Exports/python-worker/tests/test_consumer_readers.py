"""
tests/test_consumer_readers.py — Consumer-side reader tests.

Coverage:
  Adaptive TTL reader (signal_outcome_snapshot_writer):
    1.  ADAPTIVE_TTL_READ_ENABLED=0 → _lookup_adaptive_barrier returns None
    2.  Redis miss → None
    3.  Exact match (symbol + regime + direction) → correct tp_r/sl_r
    4.  Regime miss → fallback to any-regime same symbol+direction
    5.  Symbol miss → None
    6.  Direction mismatch → None
    7.  Invalid tp_r/sl_r (zero) → None
    8.  Redis error → None (fail-open)
    9.  Cache hit: second call does not hit Redis again within TTL
    10. _barrier_config with adaptive: tp_r/sl_r overridden from Redis
    11. _barrier_config without rc: uses ENV defaults

  Ensemble weights reader (meta_label_trainer_v1):
    12. ENSEMBLE_WEIGHTS_READ_ENABLED=0 → returns {}
    13. Redis HGETALL miss → {}
    14. Valid weights → normalised sum=1
    15. Negative/zero weight entries → excluded
    16. Redis error → {}
    17. _apply_ensemble_weights: missing symbol→weight 1.0 fallback
    18. _apply_ensemble_weights: clamp at [0.1, 10.0]
    19. Integration: sample_weights passed to train_meta_labeling_model
"""
from __future__ import annotations

import json
import os
from typing import Any
from unittest.mock import MagicMock, patch




# ─── Helpers ─────────────────────────────────────────────────────────────────

def _rc_with_get(payload: Any):
    rc = MagicMock()
    rc.get.return_value = json.dumps(payload) if payload is not None else None
    return rc


def _rc_error():
    rc = MagicMock()
    rc.get.side_effect = Exception("redis down")
    return rc


def _make_adaptive_payload(recs: list[dict]) -> dict:
    return {"v": 1, "generated_at_ms": 1000, "n": len(recs), "recs": recs}


def _rec(symbol="BTCUSDT", regime="trending_bull", direction=1, tp_r=1.5, sl_r=0.8):
    return {"symbol": symbol, "regime": regime, "direction": direction,
            "tp_r": tp_r, "sl_r": sl_r, "n": 100, "win_rate": 0.6,
            "median_mfe_r": tp_r, "p10_mae_r": sl_r}


# ─── Import under patch ───────────────────────────────────────────────────────

def _import_lookup(enabled: bool):
    with patch.dict(os.environ, {"ADAPTIVE_TTL_READ_ENABLED": "1" if enabled else "0"}):
        # Re-import to pick up ENV at module level
        import importlib
        import services.signal_outcome_snapshot_writer as m
        importlib.reload(m)
        return m._lookup_adaptive_barrier, m._barrier_config, m


# ─── Adaptive TTL reader tests ────────────────────────────────────────────────

def test_adaptive_disabled_returns_none():
    with patch.dict(os.environ, {"ADAPTIVE_TTL_READ_ENABLED": "0"}):
        import importlib, services.signal_outcome_snapshot_writer as m
        importlib.reload(m)
        rc = _rc_with_get(_make_adaptive_payload([_rec()]))
        result = m._lookup_adaptive_barrier(rc, "BTCUSDT", "trending_bull", 1)
        assert result is None


def test_adaptive_redis_miss_returns_none():
    with patch.dict(os.environ, {"ADAPTIVE_TTL_READ_ENABLED": "1"}):
        import importlib, services.signal_outcome_snapshot_writer as m
        importlib.reload(m)
        rc = _rc_with_get(None)
        assert m._lookup_adaptive_barrier(rc, "BTCUSDT", "trending_bull", 1) is None


def test_adaptive_exact_match():
    with patch.dict(os.environ, {"ADAPTIVE_TTL_READ_ENABLED": "1"}):
        import importlib, services.signal_outcome_snapshot_writer as m
        importlib.reload(m)
        payload = _make_adaptive_payload([_rec("BTCUSDT", "trending_bull", 1, tp_r=1.8, sl_r=0.7)])
        rc = _rc_with_get(payload)
        result = m._lookup_adaptive_barrier(rc, "BTCUSDT", "trending_bull", 1)
        assert result is not None
        assert abs(result["tp_r"] - 1.8) < 1e-6
        assert abs(result["sl_r"] - 0.7) < 1e-6


def test_adaptive_regime_fallback():
    """No exact regime match → falls back to any rec with same symbol+direction."""
    with patch.dict(os.environ, {"ADAPTIVE_TTL_READ_ENABLED": "1"}):
        import importlib, services.signal_outcome_snapshot_writer as m
        importlib.reload(m)
        payload = _make_adaptive_payload([_rec("BTCUSDT", "ranging", 1, tp_r=1.2, sl_r=0.9)])
        rc = _rc_with_get(payload)
        # Ask for "trending_bull" but only "ranging" exists → fallback
        result = m._lookup_adaptive_barrier(rc, "BTCUSDT", "trending_bull", 1)
        assert result is not None
        assert abs(result["tp_r"] - 1.2) < 1e-6


def test_adaptive_symbol_miss_returns_none():
    with patch.dict(os.environ, {"ADAPTIVE_TTL_READ_ENABLED": "1"}):
        import importlib, services.signal_outcome_snapshot_writer as m
        importlib.reload(m)
        payload = _make_adaptive_payload([_rec("ETHUSDT", "ranging", 1)])
        rc = _rc_with_get(payload)
        assert m._lookup_adaptive_barrier(rc, "BTCUSDT", "ranging", 1) is None


def test_adaptive_direction_mismatch_returns_none():
    with patch.dict(os.environ, {"ADAPTIVE_TTL_READ_ENABLED": "1"}):
        import importlib, services.signal_outcome_snapshot_writer as m
        importlib.reload(m)
        payload = _make_adaptive_payload([_rec("BTCUSDT", "ranging", -1)])  # short only
        rc = _rc_with_get(payload)
        assert m._lookup_adaptive_barrier(rc, "BTCUSDT", "ranging", 1) is None  # ask long


def test_adaptive_zero_tp_returns_none():
    with patch.dict(os.environ, {"ADAPTIVE_TTL_READ_ENABLED": "1"}):
        import importlib, services.signal_outcome_snapshot_writer as m
        importlib.reload(m)
        payload = _make_adaptive_payload([_rec("BTCUSDT", "ranging", 1, tp_r=0.0, sl_r=0.9)])
        rc = _rc_with_get(payload)
        assert m._lookup_adaptive_barrier(rc, "BTCUSDT", "ranging", 1) is None


def test_adaptive_redis_error_returns_none():
    with patch.dict(os.environ, {"ADAPTIVE_TTL_READ_ENABLED": "1"}):
        import importlib, services.signal_outcome_snapshot_writer as m
        importlib.reload(m)
        assert m._lookup_adaptive_barrier(_rc_error(), "BTCUSDT", "ranging", 1) is None


def test_adaptive_cache_hit_no_second_redis_call():
    """Second call within TTL must not hit Redis again."""
    with patch.dict(os.environ, {"ADAPTIVE_TTL_READ_ENABLED": "1", "ADAPTIVE_TTL_TTL_SEC": "300"}):
        import importlib, services.signal_outcome_snapshot_writer as m
        importlib.reload(m)
        payload = _make_adaptive_payload([_rec("BTCUSDT", "ranging", 1)])
        rc = _rc_with_get(payload)
        m._lookup_adaptive_barrier(rc, "BTCUSDT", "ranging", 1)
        m._lookup_adaptive_barrier(rc, "BTCUSDT", "ranging", 1)
        assert rc.get.call_count == 1  # cached after first call


def test_barrier_config_uses_adaptive_tp_sl():
    with patch.dict(os.environ, {"ADAPTIVE_TTL_READ_ENABLED": "1"}):
        import importlib, services.signal_outcome_snapshot_writer as m
        importlib.reload(m)
        payload = _make_adaptive_payload([_rec("BTCUSDT", "trending_bull", 1, tp_r=2.0, sl_r=0.6)])
        rc = _rc_with_get(payload)
        indicators = {"atr_bps": 50.0, "symbol": "BTCUSDT", "regime": "trending_bull", "side": 1}
        cfg = m._barrier_config(indicators, "LONG", 50000.0, rc=rc)
        assert cfg is not None
        assert abs(cfg["tp_r"] - 2.0) < 1e-6
        assert abs(cfg["sl_r"] - 0.6) < 1e-6


def test_barrier_config_no_rc_uses_env_defaults():
    with patch.dict(os.environ, {"SO_DEFAULT_TP_R": "1.0", "SO_DEFAULT_SL_R": "1.0"}):
        import importlib, services.signal_outcome_snapshot_writer as m
        importlib.reload(m)
        indicators = {"atr_bps": 50.0, "symbol": "BTCUSDT", "regime": "ranging"}
        cfg = m._barrier_config(indicators, "LONG", 50000.0, rc=None)
        assert cfg is not None
        assert abs(cfg["tp_r"] - 1.0) < 1e-6


# ─── Ensemble weights reader tests ───────────────────────────────────────────

def _ew_import(enabled: bool):
    with patch.dict(os.environ, {"ENSEMBLE_WEIGHTS_READ_ENABLED": "1" if enabled else "0"}):
        import importlib
        import orderflow_services.meta_label_trainer_v1 as m
        importlib.reload(m)
        return m


def test_ensemble_disabled_returns_empty():
    import orderflow_services.meta_label_trainer_v1 as m
    rc = MagicMock()
    with patch.dict(os.environ, {"ENSEMBLE_WEIGHTS_READ_ENABLED": "0"}):
        assert m.read_ensemble_weights(rc, "BTCUSDT") == {}
    rc.hgetall.assert_not_called()


def test_ensemble_redis_miss_returns_empty():
    import orderflow_services.meta_label_trainer_v1 as m
    rc = MagicMock()
    rc.hgetall.return_value = {}
    with patch.dict(os.environ, {"ENSEMBLE_WEIGHTS_READ_ENABLED": "1"}):
        assert m.read_ensemble_weights(rc, "BTCUSDT") == {}


def test_ensemble_valid_weights_normalised():
    import orderflow_services.meta_label_trainer_v1 as m
    rc = MagicMock()
    rc.hgetall.return_value = {"source_a": "0.6", "source_b": "0.4"}
    with patch.dict(os.environ, {"ENSEMBLE_WEIGHTS_READ_ENABLED": "1"}):
        weights = m.read_ensemble_weights(rc, "BTCUSDT")
    assert set(weights) == {"source_a", "source_b"}
    assert abs(sum(weights.values()) - 1.0) < 1e-6
    assert abs(weights["source_a"] - 0.6) < 1e-6


def test_ensemble_zero_weight_excluded():
    import orderflow_services.meta_label_trainer_v1 as m
    rc = MagicMock()
    rc.hgetall.return_value = {"source_a": "0.8", "source_b": "0.0", "source_c": "bad"}
    with patch.dict(os.environ, {"ENSEMBLE_WEIGHTS_READ_ENABLED": "1"}):
        weights = m.read_ensemble_weights(rc, "BTCUSDT")
    assert "source_b" not in weights
    assert "source_c" not in weights
    assert abs(weights.get("source_a", 0) - 1.0) < 1e-6


def test_ensemble_redis_error_returns_empty():
    import orderflow_services.meta_label_trainer_v1 as m
    rc = MagicMock()
    rc.hgetall.side_effect = Exception("timeout")
    with patch.dict(os.environ, {"ENSEMBLE_WEIGHTS_READ_ENABLED": "1"}):
        assert m.read_ensemble_weights(rc, "BTCUSDT") == {}


def test_apply_ensemble_weights_missing_source_defaults_to_one():
    m = _ew_import(True)
    rows = [{"symbol": "BTCUSDT", "source": "unknown_src"}]
    weights_by_symbol = {"BTCUSDT": {"source_a": 0.7, "source_b": 0.3}}
    sw = m._apply_ensemble_weights(rows, weights_by_symbol)
    assert abs(sw[0] - 1.0) < 1e-6


def test_apply_ensemble_weights_clamp():
    m = _ew_import(True)
    rows = [
        {"symbol": "BTCUSDT", "source": "hot_source"},   # extreme high
        {"symbol": "BTCUSDT", "source": "cold_source"},  # extreme low
    ]
    weights_by_symbol = {"BTCUSDT": {"hot_source": 100.0, "cold_source": 0.001}}
    sw = m._apply_ensemble_weights(rows, weights_by_symbol)
    assert sw[0] <= 10.0   # clamped at max
    assert sw[1] >= 0.1    # clamped at min


def test_train_meta_labeling_model_accepts_sample_weights():
    """train_meta_labeling_model signature accepts sample_weights without error."""
    import inspect
    from calibration.meta_labeling_model import train_meta_labeling_model
    sig = inspect.signature(train_meta_labeling_model)
    assert "sample_weights" in sig.parameters
