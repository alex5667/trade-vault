from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.dyn_cfg_keys import DynCfgKeys as DK
from core.gates.decision import GateDecisionV1
from services.orderflow.signal_pipeline import SignalPipeline
from core.redis_keys import RedisStreams as RS


def _make_pipeline(strict: bool) -> SignalPipeline:
    publisher = MagicMock()
    publisher.xadd_json = AsyncMock(side_effect=RuntimeError("redis xadd failed"))
    publisher.r = MagicMock()
    atr_cache = MagicMock()
    atr_cache.get.return_value = 100.0

    with patch.dict(
        "os.environ",
        {
            "OF_INPUTS_PUBLISH_STRICT": "1" if strict else "0",
            "OF_INPUTS_STREAM": RS.OF_INPUTS,
            "OF_INPUTS_STREAM_MAXLEN": "5000",
        },
        clear=False,
    ):
        return SignalPipeline(publisher=publisher, atr_cache=atr_cache)


@pytest.mark.asyncio
async def test_publish_of_inputs_logs_metric_and_swallow_when_not_strict():
    pipeline = _make_pipeline(strict=False)

    with patch("services.orderflow.signal_pipeline.of_inputs_publish_error_total") as metric:
        await pipeline._publish_of_inputs(
            publisher=pipeline.publisher,
            enriched_signal={"sid": "s1"},
            symbol="BTCUSDT",
            path="direct",
        )

    metric.labels.assert_called_once_with(
        symbol="BTCUSDT",
        stream=RS.OF_INPUTS,
        path="direct",
    )


@pytest.mark.asyncio
async def test_publish_of_inputs_raises_in_strict_mode():
    pipeline = _make_pipeline(strict=True)

    with patch("services.orderflow.signal_pipeline.of_inputs_publish_error_total") as metric:
        with pytest.raises(RuntimeError, match="redis xadd failed"):
            await pipeline._publish_of_inputs(
                publisher=pipeline.publisher,
                enriched_signal={"sid": "s1"},
                symbol="BTCUSDT",
                path="outbox",
            )

    metric.labels.assert_called_once_with(
        symbol="BTCUSDT",
        stream=RS.OF_INPUTS,
        path="outbox",
    )


@pytest.mark.asyncio
async def test_publish_of_inputs_mirrors_runtime_volatility_features():
    publisher = MagicMock()
    publisher.xadd_json = AsyncMock()
    publisher.r = MagicMock()
    atr_cache = MagicMock()
    atr_cache.get.return_value = 100.0

    with patch.dict(
        "os.environ",
        {
            "OF_INPUTS_PUBLISH_STRICT": "0",
            "OF_INPUTS_STREAM": RS.OF_INPUTS,
            "OF_INPUTS_STREAM_MAXLEN": "5000",
        },
        clear=False,
    ):
        pipeline = SignalPipeline(publisher=publisher, atr_cache=atr_cache)

    sync_redis = MagicMock()
    sync_redis.mget.return_value = [None] * 11
    sync_redis.hgetall.return_value = {}
    pipeline._sync_redis_client = sync_redis

    runtime = SimpleNamespace(
        dynamic_cfg={
            DK.VOL_FAST_BPS: 42.0,
            DK.VOL_SLOW_BPS: 38.0,
            DK.VOL_RATIO: 1.105,
            DK.VOL_RATIO_Z: 0.55,
            DK.VOL_REGIME_LABEL: "shock",
        },
        v13_tracker=SimpleNamespace(
            snapshot=lambda: {
                "garman_klass_vol": 0.012,
                "parkinson_vol": 0.013,
                "yang_zhang_vol": 0.014,
                "vol_of_vol": 0.33,
            }
        ),
        last_regime="trending_bear",
    )

    enriched_signal = {
        "sid": "s-vol-1",
        "indicators": {
            "obi_avg": 0.2,
            "pressure_per_min_ema": 1.5,
        },
    }

    await pipeline._publish_of_inputs(
        publisher=publisher,
        enriched_signal=enriched_signal,
        symbol="BTCUSDT",
        path="direct",
        runtime=runtime,
    )

    payload = publisher.xadd_json.await_args.kwargs["payload"]
    inds = payload["indicators"]

    assert inds["vol_fast_bps"] == 42.0
    assert inds["vol_slow_bps"] == 38.0
    assert inds["vol_ratio"] == 1.105
    assert inds["vol_ratio_z"] == 0.55
    assert inds["vol_regime_label"] == "shock"
    assert inds["vol_regime_code"] == 1.0
    assert inds["garman_klass_vol"] == 0.012
    assert inds["parkinson_vol"] == 0.013
    assert inds["yang_zhang_vol"] == 0.014
    assert inds["vol_of_vol"] == 0.33


def _make_pipeline_simple() -> SignalPipeline:
    publisher = MagicMock()
    publisher.xadd_json = AsyncMock()
    publisher.r = MagicMock()
    atr_cache = MagicMock()
    atr_cache.get.return_value = 100.0
    with patch.dict(
        "os.environ",
        {"OF_INPUTS_PUBLISH_STRICT": "0", "OF_INPUTS_STREAM": RS.OF_INPUTS},
        clear=False,
    ):
        p = SignalPipeline(publisher=publisher, atr_cache=atr_cache)
    sync_redis = MagicMock()
    sync_redis.mget.return_value = [None] * 11
    sync_redis.hgetall.return_value = {}
    p._sync_redis_client = sync_redis
    return p


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_tq,expected", [
    ("absent", 0.0),  # key absent → must become 0.0
    ("null",   0.0),  # key present as None → must become 0.0
    (2.5,      2.5),  # key present with real value → preserved
])
async def test_publish_of_inputs_tick_qty_none_override(initial_tq, expected):
    pipeline = _make_pipeline_simple()
    inds: dict = {}
    if initial_tq == "null":
        inds["tick_qty"] = None
    elif initial_tq != "absent":
        inds["tick_qty"] = initial_tq

    await pipeline._publish_of_inputs(
        publisher=pipeline.publisher,
        enriched_signal={"sid": "s-tq", "indicators": inds},
        symbol="BTCUSDT",
        path="direct",
    )

    payload = pipeline.publisher.xadd_json.await_args.kwargs["payload"]
    assert payload["indicators"]["tick_qty"] == expected
    assert payload["indicators"]["tick_qty"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_sgo,expected", [
    ("absent", 0.0),  # key absent → must become 0.0
    ("null",   0.0),  # key present as None → must become 0.0
    (1,        1.0),  # set by tick_decision_engine → preserved
    (0,        0.0),  # explicit fail from tick_decision_engine → preserved
])
async def test_publish_of_inputs_strong_gate_ok_none_override(initial_sgo, expected):
    pipeline = _make_pipeline_simple()
    inds: dict = {}
    if initial_sgo == "null":
        inds["strong_gate_ok"] = None
    elif initial_sgo != "absent":
        inds["strong_gate_ok"] = initial_sgo

    await pipeline._publish_of_inputs(
        publisher=pipeline.publisher,
        enriched_signal={"sid": "s-sgo", "indicators": inds},
        symbol="ETHUSDT",
        path="veto",
    )

    payload = pipeline.publisher.xadd_json.await_args.kwargs["payload"]
    assert payload["indicators"]["strong_gate_ok"] == expected
    assert payload["indicators"]["strong_gate_ok"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("spread_z,initial,expected", [
    (0.3,  "absent", 0.3),  # runtime provides non-zero → bridged into veto-path signal
    (0.0,  "absent", 0.0),  # runtime has 0.0 → not bridged (only_nonzero policy)
    (0.3,  0.5,      0.5),  # indicators already populated → setdefault preserves it
])
async def test_publish_of_inputs_spread_bps_z_bridge(spread_z, initial, expected):
    """spread_bps_z bridge from runtime.last_spread_z works for veto-path signals."""
    pipeline = _make_pipeline_simple()
    inds: dict = {}
    if initial != "absent":
        inds["spread_bps_z"] = initial

    runtime = SimpleNamespace(
        last_spread_z=spread_z,
        dynamic_cfg={},
        last_regime="trending_bear",
    )

    await pipeline._publish_of_inputs(
        publisher=pipeline.publisher,
        enriched_signal={"sid": "s-sz", "indicators": inds},
        symbol="BTCUSDT",
        path="veto",
        runtime=runtime,  # type: ignore[arg-type]
    )

    payload = pipeline.publisher.xadd_json.await_args.kwargs["payload"]  # type: ignore[union-attr]
    assert payload["indicators"].get("spread_bps_z", 0.0) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("reader_val,initial,expected", [
    (0.27, 0.0, 0.27),  # reader returns real value, inject_v12_of pre-set 0.0 → overridden
    (0.0,  0.0, 0.0),   # reader returns 0.0 → not updated (stays 0.0)
    (0.27, 0.5, 0.5),   # indicators already populated non-zero → not entered (truthy guard)
])
async def test_publish_of_inputs_eth_btc_corr_5m_bridge(reader_val, initial, expected):
    """eth_btc_corr_5m is re-read for all paths (including veto), overriding inject_v12_of 0.0."""
    pipeline = _make_pipeline_simple()
    inds = {"eth_btc_corr_5m": initial}

    with patch("core.cross_asset_corr_reader.get_eth_btc_corr_5m", return_value=reader_val):
        await pipeline._publish_of_inputs(
            publisher=pipeline.publisher,
            enriched_signal={"sid": "s-corr", "indicators": inds},
            symbol="ETHUSDT",
            path="veto",
        )
        payload = pipeline.publisher.xadd_json.await_args.kwargs["payload"]  # type: ignore[union-attr]

    assert payload["indicators"]["eth_btc_corr_5m"] == expected


# ---------------------------------------------------------------------------
# signal_age_ms bridge
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("scenario,expected_positive", [
    ("ts_ms_present", True),   # ts_ms in signal → age computed > 0
    ("already_set",   False),  # signal_age_ms already 500.0 → preserved (not recomputed)
    ("ts_ms_absent",  False),  # no ts_ms → signal_age_ms stays absent (returns 0.0 default)
])
async def test_publish_of_inputs_signal_age_ms_bridge(scenario, expected_positive):
    """signal_age_ms is computed from ts_ms for veto-path signals that bypass
    of_confirm_engine.build() where it is normally set."""
    pipeline = _make_pipeline_simple()

    now_ms = int(time.time() * 1000)
    inds: dict = {}
    sig: dict = {"sid": "s-age"}

    if scenario == "ts_ms_present":
        sig["ts_ms"] = now_ms - 200  # 200 ms ago
    elif scenario == "already_set":
        sig["ts_ms"] = now_ms - 200
        inds["signal_age_ms"] = 500.0  # pre-populated — must survive
    # "ts_ms_absent": sig has no ts_ms key

    sig["indicators"] = inds

    await pipeline._publish_of_inputs(
        publisher=pipeline.publisher,
        enriched_signal=sig,
        symbol="BTCUSDT",
        path="veto",
    )

    payload = pipeline.publisher.xadd_json.await_args.kwargs["payload"]  # type: ignore[union-attr]
    age = payload["indicators"].get("signal_age_ms", 0.0)

    if scenario == "already_set":
        assert age == 500.0, f"pre-set value must be preserved, got {age}"
    elif expected_positive:
        assert age > 0, f"expected positive signal_age_ms, got {age}"
    else:
        assert age == 0.0 or age is None or "signal_age_ms" not in payload["indicators"], (
            f"expected absent/zero signal_age_ms, got {age}"
        )


# ---------------------------------------------------------------------------
# SMT field propagation from ctx to indicators before veto
# ---------------------------------------------------------------------------

def _make_veto_runtime() -> MagicMock:
    """Minimal MagicMock runtime for publish_signal veto-path tests."""
    rt = MagicMock()
    rt.symbol = "BTCUSDT"
    rt.last_regime = "trending_bear"
    rt.ready = True
    rt.is_active = True
    rt.dynamic_cfg = {}
    rt.config = MagicMock()
    rt.config.get = lambda k, d=None: d
    return rt


def _make_veto_signal() -> dict:
    now_ms = int(time.time() * 1000)
    return {
        "direction": "LONG",
        "entry": 50000.0,
        "sl": 49500.0,
        "tp_levels": [50500.0],
        "confidence": 0.85,
        "ts_ms": now_ms,
        "tick_ts": now_ms,
        "_barrier_resolved": True,  # bypass ConfirmationBarrier to reach gate checks
        "indicators": {},
    }


@pytest.mark.asyncio
async def test_publish_signal_veto_smt_fields_propagated():
    """Before _handle_pipeline_veto is called, SMT fields from ctx are copied
    into indicators.  This ensures veto-path of:inputs records have them for
    ML training (pass-path propagates at L2897 after all gates; veto-path
    never reaches that point without this fix)."""
    pipeline = _make_pipeline_simple()

    # Simulate ctx state after SmtCoherenceGate ran and blocked the signal.
    mock_ctx = SimpleNamespace(
        smt_blocked=1,
        smt_leader_confirm=0,
        smt_leader_dir="DOWN",
        smt_coh=0.42,
        smt_align=0,
        smt_state_stale=0,
        smt_bundle_id="bndl-test",
    )

    deny = GateDecisionV1(
        stage="dq_integrity",
        gate="TestGate",
        decision="DENY",
        reason_code="TEST_VETO",
        severity="CRITICAL",
        profile="hard",
        fail_policy="CLOSED",
        ts_event_ms=0,
        ts_decision_ms=0,
        latency_us=0,
        inputs_hash="",
    )

    captured: dict = {}

    def _capture_veto(**kw: object) -> None:
        captured.update(kw.get("indicators", {}))  # type: ignore[arg-type]

    with (
        patch.object(pipeline, "_build_gate_ctx", return_value=mock_ctx),
        patch.object(pipeline.orchestrator, "check_dq_integrity", return_value=deny),
        patch.object(pipeline, "_handle_pipeline_veto", side_effect=_capture_veto),
    ):
        await pipeline.publish_signal(_make_veto_runtime(), _make_veto_signal())

    assert captured.get("smt_blocked") == 1, (
        f"smt_blocked must be 1 (propagated from ctx); got {captured.get('smt_blocked')!r}"
    )
    assert captured.get("smt_leader_dir") == "DOWN", (
        f"smt_leader_dir must be 'DOWN'; got {captured.get('smt_leader_dir')!r}"
    )
    assert "smt_coh" in captured, "smt_coh must be propagated from ctx"


@pytest.mark.asyncio
async def test_publish_signal_veto_smt_fields_not_overwritten():
    """If indicators already have smt_* fields (e.g. set upstream), the
    propagation block must NOT overwrite them — only fill missing keys."""
    pipeline = _make_pipeline_simple()

    mock_ctx = SimpleNamespace(
        smt_blocked=0,
        smt_leader_confirm=1,
        smt_leader_dir="UP",
        smt_coh=0.9,
        smt_align=1,
        smt_state_stale=0,
        smt_bundle_id="bndl-x",
    )

    deny = GateDecisionV1(
        stage="dq_integrity",
        gate="TestGate",
        decision="DENY",
        reason_code="TEST_VETO",
        severity="CRITICAL",
        profile="hard",
        fail_policy="CLOSED",
        ts_event_ms=0,
        ts_decision_ms=0,
        latency_us=0,
        inputs_hash="",
    )

    sig = _make_veto_signal()
    sig["indicators"]["smt_blocked"] = 1  # upstream already set a different value

    captured: dict = {}

    def _capture_veto(**kw: object) -> None:
        captured.update(kw.get("indicators", {}))  # type: ignore[arg-type]

    with (
        patch.object(pipeline, "_build_gate_ctx", return_value=mock_ctx),
        patch.object(pipeline.orchestrator, "check_dq_integrity", return_value=deny),
        patch.object(pipeline, "_handle_pipeline_veto", side_effect=_capture_veto),
    ):
        await pipeline.publish_signal(_make_veto_runtime(), sig)

    # upstream value (1) must survive — ctx value (0) must NOT overwrite it
    assert captured.get("smt_blocked") == 1, (
        f"pre-set smt_blocked=1 must not be overwritten by ctx.smt_blocked=0; "
        f"got {captured.get('smt_blocked')!r}"
    )

# ---------------------------------------------------------------------------
# fp_edge_absorb bridge
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_publish_of_inputs_fp_edge_absorb_from_runtime():
    """fp_edge_absorb is computed via compute_fp_edge_absorb(last_edge=runtime.last_fp_edge)
    for veto-path signals that bypass of_confirm_engine.build()."""
    pipeline = _make_pipeline_simple()

    # runtime with a live FP-edge event that should trigger absorb
    mock_edge = {"ts_ms": int(time.time() * 1000) - 5000, "value": 3.0, "p90": 1.0,
                 "strength": 2.5, "bias": "LONG", "range_expansion": 0}
    runtime = SimpleNamespace(
        last_fp_edge=mock_edge,
        config={},
        dynamic_cfg={},
        last_regime="trending_bear",
    )

    await pipeline._publish_of_inputs(
        publisher=pipeline.publisher,
        enriched_signal={"sid": "s-fp", "direction": "LONG", "indicators": {}},
        symbol="BTCUSDT",
        path="veto",
        runtime=runtime,  # type: ignore[arg-type]
    )

    payload = pipeline.publisher.xadd_json.await_args.kwargs["payload"]  # type: ignore[union-attr]
    inds = payload["indicators"]

    # absorb should fire (bias=LONG, direction=LONG, strength=2.5>1.0, no range expansion)
    assert inds.get("fp_edge_absorb") == 1, f"expected fp_edge_absorb=1, got {inds.get('fp_edge_absorb')!r}"


@pytest.mark.asyncio
async def test_publish_of_inputs_fp_edge_absorb_no_edge():
    """When runtime.last_fp_edge is None, fp_edge_absorb defaults to 0."""
    pipeline = _make_pipeline_simple()

    runtime = SimpleNamespace(last_fp_edge=None, config={}, dynamic_cfg={}, last_regime="trending_bear")

    await pipeline._publish_of_inputs(
        publisher=pipeline.publisher,
        enriched_signal={"sid": "s-fp2", "direction": "LONG", "indicators": {}},
        symbol="BTCUSDT",
        path="veto",
        runtime=runtime,  # type: ignore[arg-type]
    )

    payload = pipeline.publisher.xadd_json.await_args.kwargs["payload"]  # type: ignore[union-attr]
    assert payload["indicators"].get("fp_edge_absorb") == 0


@pytest.mark.asyncio
async def test_publish_of_inputs_fp_edge_absorb_pre_set_preserved():
    """If fp_edge_absorb is already in indicators (set upstream), the bridge must not overwrite it."""
    pipeline = _make_pipeline_simple()

    runtime = SimpleNamespace(last_fp_edge=None, config={}, dynamic_cfg={}, last_regime="trending_bear")

    await pipeline._publish_of_inputs(
        publisher=pipeline.publisher,
        enriched_signal={"sid": "s-fp3", "direction": "LONG", "indicators": {"fp_edge_absorb": 1}},
        symbol="BTCUSDT",
        path="veto",
        runtime=runtime,  # type: ignore[arg-type]
    )

    payload = pipeline.publisher.xadd_json.await_args.kwargs["payload"]  # type: ignore[union-attr]
    assert payload["indicators"].get("fp_edge_absorb") == 1


# ---------------------------------------------------------------------------
# dq_* bridge from data_health
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("data_health,reasons,exp_score,exp_flags", [
    (1.0, "",              1.0, 0.0),   # fully healthy → dq_score=1.0, no flags
    (0.8, "spread,stale", 0.8, 2.0),   # two reasons → flag_count=2
    (0.5, "stale",        0.5, 1.0),   # one reason → flag_count=1
])
async def test_publish_of_inputs_dq_bridge_from_data_health(data_health, reasons, exp_score, exp_flags):
    """dq_score and dq_flag_count are bridged from data_health/data_health_reasons
    for veto-path signals where of_confirm_engine.build() never ran."""
    pipeline = _make_pipeline_simple()

    inds = {"data_health": data_health, "data_health_reasons": reasons}

    await pipeline._publish_of_inputs(
        publisher=pipeline.publisher,
        enriched_signal={"sid": "s-dq", "indicators": inds},
        symbol="BTCUSDT",
        path="veto",
    )

    payload = pipeline.publisher.xadd_json.await_args.kwargs["payload"]  # type: ignore[union-attr]
    out = payload["indicators"]

    assert out.get("dq_score") == exp_score, f"dq_score: expected {exp_score}, got {out.get('dq_score')!r}"
    assert out.get("dq_flag_count") == exp_flags, f"dq_flag_count: expected {exp_flags}, got {out.get('dq_flag_count')!r}"
    assert out.get("dq_level") == 0, "dq_level must default to 0 for veto-path"
    assert out.get("dq_pen") == 0.0, "dq_pen must default to 0.0 for veto-path"


@pytest.mark.asyncio
async def test_publish_of_inputs_dq_pre_set_preserved():
    """dq_score already set by of_confirm_engine (e.g. pass-path replay) must not be overwritten."""
    pipeline = _make_pipeline_simple()

    inds = {"data_health": 0.6, "dq_score": 0.95, "dq_flag_count": 0.0}

    await pipeline._publish_of_inputs(
        publisher=pipeline.publisher,
        enriched_signal={"sid": "s-dq2", "indicators": inds},
        symbol="BTCUSDT",
        path="direct",
    )

    payload = pipeline.publisher.xadd_json.await_args.kwargs["payload"]  # type: ignore[union-attr]
    assert payload["indicators"]["dq_score"] == 0.95, "pre-set dq_score must survive"
