from __future__ import annotations

"""
Regression pack — Squeeze-режим: publish_signal должен вернуться без публикации.

Контракт (2026-04-18 wave):
  - Если runtime.last_regime содержит "squeeze" → вето, return без publish
  - Метрика strong_gate_veto_total{reason="veto_squeeze", mode="ENFORCE"} инкрементируется
  - Ни один метод publisher-а не вызывается с payload-ом сигнала
  - range-режим и trending-режим НЕ блокируются (проверка false-positive)
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_runtime(regime: str = "squeeze") -> MagicMock:
    rt = MagicMock()
    rt.symbol = "BTCUSDT"
    rt.last_regime = regime
    rt.ready = True
    rt.is_active = True
    rt.book_churn_hi = 0
    rt.liq_regime = "na"
    rt.dynamic_cfg = {}
    rt.last_tick_ts = int(time.time() * 1000) - 50
    rt.last_book_ts = int(time.time() * 1000) - 50
    rt.calibrated_specs = {}
    rt.trail_calib_params = None
    rt.tick_integrity = None
    rt.book_integrity = None
    # Config: all gates disabled (zero thresholds → pass-through)
    cfg = MagicMock()
    cfg.get = lambda key, default=None: default
    rt.config = cfg
    return rt


def _make_signal(direction: str = "LONG") -> dict:
    now_ms = int(time.time() * 1000)
    return {
        "direction": direction,
        "entry": 50000.0,
        "sl": 49500.0,
        "tp_levels": [50500.0, 51000.0],
        "atr": 500.0,
        "confidence": 0.85,
        "ts_ms": now_ms,
        "tick_ts": now_ms,
        "delta": 100.0,
        "delta_z": 1.5,
        "confirmations": ["A=1"],
        "indicators": {"regime": "squeeze"},
    }


def _make_pipeline():
    from services.orderflow.signal_pipeline import SignalPipeline

    mock_publisher = MagicMock()
    mock_publisher.publish = AsyncMock()
    mock_publisher.xadd = AsyncMock()

    mock_atr_cache = MagicMock()
    mock_atr_cache.get.return_value = 500.0

    return SignalPipeline(publisher=mock_publisher, atr_cache=mock_atr_cache)


# ---------------------------------------------------------------------------
# Core veto tests
# ---------------------------------------------------------------------------

_VETO_PATCH = "services.orderflow.metrics.strong_gate_veto_total"


@pytest.mark.asyncio
async def test_squeeze_regime_veto_no_publish():
    """publish_signal не должен публиковать сигнал в squeeze-режиме."""
    pipeline = _make_pipeline()
    runtime = _make_runtime("squeeze")

    with patch(_VETO_PATCH) as mock_veto_ctr:
        mock_veto_ctr.labels.return_value = MagicMock()
        await pipeline.publish_signal(runtime, _make_signal())

    pipeline.publisher.publish.assert_not_called()
    pipeline.publisher.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_squeeze_veto_increments_counter():
    """strong_gate_veto_total{reason=veto_squeeze, mode=ENFORCE} инкрементируется."""
    pipeline = _make_pipeline()
    runtime = _make_runtime("squeeze")

    with patch(_VETO_PATCH) as mock_veto_ctr:
        inc_mock = MagicMock()
        mock_veto_ctr.labels.return_value = inc_mock
        await pipeline.publish_signal(runtime, _make_signal())

    # Проверяем, что среди вызовов есть именно squeeze-veto
    squeeze_calls = [
        c for c in mock_veto_ctr.labels.call_args_list
        if c.kwargs.get("reason") == "veto_squeeze"
    ]
    assert squeeze_calls, (
        f"Ожидали вызов labels(reason='veto_squeeze'), вызовы: {mock_veto_ctr.labels.call_args_list}"
    )
    assert squeeze_calls[0].kwargs == {
        "symbol": "BTCUSDT",
        "scenario": "regime",
        "reason": "veto_squeeze",
        "mode": "ENFORCE",
    }
    inc_mock.inc.assert_called()


@pytest.mark.asyncio
async def test_squeeze_veto_short_direction():
    """SHORT direction также блокируется в squeeze-режиме."""
    pipeline = _make_pipeline()
    runtime = _make_runtime("squeeze")

    with patch(_VETO_PATCH) as mock_veto_ctr:
        mock_veto_ctr.labels.return_value = MagicMock()
        await pipeline.publish_signal(runtime, _make_signal(direction="SHORT"))

    pipeline.publisher.publish.assert_not_called()


@pytest.mark.asyncio
async def test_squeeze_veto_case_insensitive():
    """Детектор нечувствителен к регистру: 'Squeeze', 'SQUEEZE', 'squeeze_tight'."""
    # "range_squeeze" содержит "range" → range-ветка выигрывает над squeeze (if/elif приоритет)
    for regime in ("Squeeze", "SQUEEZE", "squeeze_tight", "pure_squeeze"):
        pipeline = _make_pipeline()
        runtime = _make_runtime(regime)

        with patch(_VETO_PATCH) as mock_veto_ctr:
            mock_veto_ctr.labels.return_value = MagicMock()
            await pipeline.publish_signal(runtime, _make_signal())

        squeeze_calls = [
            c for c in mock_veto_ctr.labels.call_args_list
            if c.kwargs.get("reason") == "veto_squeeze"
        ]
        assert squeeze_calls, f"режим {regime!r}: ожидали veto_squeeze, вызовы={mock_veto_ctr.labels.call_args_list}"
        pipeline.publisher.publish.assert_not_called()


# ---------------------------------------------------------------------------
# False-positive guard: другие режимы не должны попадать в squeeze-veto
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_trending_regime_not_vetoed_by_squeeze_logic():
    """trending-режим не должен попадать в squeeze-veto."""
    pipeline = _make_pipeline()
    runtime = _make_runtime("trending")

    with patch(_VETO_PATCH) as mock_veto_ctr:
        mock_veto_ctr.labels.return_value = MagicMock()
        await pipeline.publish_signal(runtime, _make_signal())

    squeeze_calls = [
        c for c in mock_veto_ctr.labels.call_args_list
        if c.kwargs.get("reason") == "veto_squeeze"
    ]
    assert not squeeze_calls, (
        f"trending-режим не должен получать squeeze-veto, вызовы: {mock_veto_ctr.labels.call_args_list}"
    )


@pytest.mark.asyncio
async def test_range_regime_not_vetoed():
    """range-режим не должен попадать в squeeze-veto."""
    pipeline = _make_pipeline()
    runtime = _make_runtime("range")

    with patch(_VETO_PATCH) as mock_veto_ctr:
        mock_veto_ctr.labels.return_value = MagicMock()
        await pipeline.publish_signal(runtime, _make_signal())

    squeeze_calls = [
        c for c in mock_veto_ctr.labels.call_args_list
        if c.kwargs.get("reason") == "veto_squeeze"
    ]
    assert not squeeze_calls, (
        f"range-режим не должен получать squeeze-veto, вызовы: {mock_veto_ctr.labels.call_args_list}"
    )


# ---------------------------------------------------------------------------
# Classification unit tests (без запуска publish_signal)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("regime,expect_squeeze_flag,expect_veto", [
    # (режим, is_squeeze_regime_flag, фактически ли применяется veto)
    # Veto срабатывает только если !is_range_regime_flag — так как if/elif приоритет
    ("squeeze",         True,  True),
    ("SQUEEZE",         True,  True),
    ("squeeze_tight",   True,  True),
    ("pure_squeeze",    True,  True),
    ("range_squeeze",   True,  False),  # range выигрывает в if/elif, veto не срабатывает
    ("range",           False, False),
    ("trending",        False, False),
    ("expansion",       False, False),
    ("na",              False, False),
    ("",                False, False),
])
def test_squeeze_classification(regime: str, expect_squeeze_flag: bool, expect_veto: bool):
    """is_squeeze_regime_flag классификация и фактический veto-приоритет (if/elif)."""
    rg = (regime or "na").lower()
    is_range = "range" in rg
    is_squeeze = "squeeze" in rg
    # Veto применяется только если squeeze=True И range=False (if/elif порядок)
    actual_veto = is_squeeze and not is_range
    assert is_squeeze == expect_squeeze_flag, (
        f"regime={regime!r}: is_squeeze_flag: ожидали {expect_squeeze_flag}, получили {is_squeeze}"
    )
    assert actual_veto == expect_veto, (
        f"regime={regime!r}: veto: ожидали {expect_veto}, получили {actual_veto}"
    )


@pytest.mark.parametrize("regime,expect_range", [
    ("range",           True),
    ("range_tight",     True),
    ("range_squeeze",   True),
    ("squeeze",         False),
    ("trending",        False),
    ("na",              False),
])
def test_range_classification(regime: str, expect_range: bool):
    """is_range_regime_flag логика корректна."""
    rg = (regime or "na").lower()
    is_range = "range" in rg
    assert is_range == expect_range, (
        f"regime={regime!r}: ожидали is_range={expect_range}, получили {is_range}"
    )
