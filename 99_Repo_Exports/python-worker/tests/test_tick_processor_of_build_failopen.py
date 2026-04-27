from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _reset_of_build_runtime(monkeypatch):
    from services import ml_confirm_gate as mg

    monkeypatch.setenv("ML_CONFIRM_THREADS", "1")
    monkeypatch.setenv("OF_BUILD_MAX_INFLIGHT", "1")
    monkeypatch.setenv("OF_BUILD_TIMEOUT_S", "0.05")
    monkeypatch.setenv("OF_SYNC_BUILD", "0")
    mg._shutdown_ml_executor()
    mg._OF_BUILD_SEMAPHORE = None
    yield
    mg._shutdown_ml_executor()
    mg._OF_BUILD_SEMAPHORE = None


def _make_runtime() -> SimpleNamespace:
    return SimpleNamespace(
        symbol="BTCUSDT",
        config={
            "micro_tf": "1s",
            "require_strong_confirmation": True,
            "strong_gate_shadow": False,
            "delta_tier_min": 0,
        },
        dynamic_cfg={},
        tick_count=0,
        last_regime="na",
        last_book=None,
        last_bar=None,
        last_sweep=None,
        last_wp=None,
        last_obi_event=None,
        last_iceberg_event=None,
        last_fp_edge=None,
        last_div=None,
        last_swing_high=None,
        last_swing_low=None,
        last_book_ts_ms=0,
        last_atr=1.0,
        book_churn_score=0.0,
        book_rate_z=0.0,
        pressure_sps=0.0,
        cvd_state=None,
        cvd_quarantine_active=0,
        delta_detector=SimpleNamespace(push=lambda tick: {"delta": 1.0, "z": 2.0}),
        tick_dn_calib=SimpleNamespace(
            tiers=lambda **kwargs: SimpleNamespace(
                tier0_usd=10.0,
                tier1_usd=20.0,
                tier2_usd=30.0,
                src="test",
                scale=1.0,
            ),
            update=MagicMock(),
        ),
        dn_passrate=SimpleNamespace(update=MagicMock()),
        pressure=SimpleNamespace(snapshot=lambda now_ms: SimpleNamespace(per_min_ema=0.0, cd_rate_ema=0.0)),
        absorption_detector=SimpleNamespace(push=lambda *args, **kwargs: None),
        delta_log_sampler=SimpleNamespace(should_log=lambda key: False),
    )


@pytest.mark.asyncio
async def test_process_tick_of_build_timeout_fail_open_and_veto(caplog):
    from confidence_calculation.tick_processor import TickProcessor

    class SlowEngine:
        def build(self, **kwargs):
            time.sleep(0.15)
            return {"unexpected": "late"}, None

    tp = TickProcessor(
        redis=AsyncMock(),
        ticks=AsyncMock(),
        publisher=MagicMock(),
        of_engine=SlowEngine(),
        calib_svc=MagicMock(symbol="BTCUSDT"),
        atr_cache=MagicMock(get_with_meta=MagicMock(return_value=(1.0, {"picked_src": "cache", "picked_tf": "1m", "age_ms": 0}))),
        atr_sanity=MagicMock(update=MagicMock(return_value=SimpleNamespace(atr_used=1.0, bad=0))),
        conf_scorer=None,
    )

    runtime = _make_runtime()
    tick = {"price": 100.0, "is_buyer_maker": False, "ts_ms": 1234567890000}

    tp._apply_tick_time_guard = AsyncMock(return_value={"tick_ts_ms": 1234567890000, "decision": "ok", "meta": {}})
    tp.cb_state.update = AsyncMock(return_value=("ok", {"switched": False}))
    tp._emit_gate_metrics = MagicMock()
    tp._emit_early_veto_decision_record = AsyncMock()

    def _close_task(coro, name=None):
        try:
            coro.close()
        except Exception:
            pass
        return None

    raw_timeout_metric = MagicMock()
    raw_hist_metric = MagicMock()
    raw_veto_metric = MagicMock()
    timeout_labels = MagicMock()
    hist_labels = MagicMock()
    veto_labels = MagicMock()
    raw_timeout_metric.labels.return_value = timeout_labels
    raw_hist_metric.labels.return_value = hist_labels
    raw_veto_metric.labels.return_value = veto_labels

    with patch("confidence_calculation.tick_processor.safe_create_task", side_effect=_close_task), \
         patch("confidence_calculation.tick_processor.decide_circuit_breaker", return_value=SimpleNamespace(regime="ok")), \
         patch("confidence_calculation.tick_processor.enforce_circuit_breaker_regime", side_effect=lambda raw, effective, cfg: raw), \
         patch("confidence_calculation.tick_processor.apply_circuit_breaker_overrides", return_value=({}, {})), \
         patch("services.orderflow.metrics.of_confirm_build_timeout_total", raw_timeout_metric), \
         patch("confidence_calculation.tick_processor.of_confirm_build_ms_hist", raw_hist_metric), \
         patch("services.orderflow.metrics.strong_gate_veto_total", raw_veto_metric), \
         caplog.at_level("WARNING", logger="orderflow_tick_processor"):
        result = await tp.process_tick(runtime, tick)

    assert result is None
    raw_timeout_metric.labels.assert_called_once_with(symbol="BTCUSDT", tf="1s")
    timeout_labels.inc.assert_called_once()
    raw_hist_metric.labels.assert_called_once_with(symbol="BTCUSDT", tf="1s")
    hist_labels.observe.assert_called_once()
    raw_veto_metric.labels.assert_called_once_with(
        symbol="BTCUSDT",
        scenario="none",
        reason="engine_veto",
        mode="ENFORCE",
    )
    veto_labels.inc.assert_called_once()
    assert any("OFConfirmEngine.build timeout" in rec.message for rec in caplog.records)
