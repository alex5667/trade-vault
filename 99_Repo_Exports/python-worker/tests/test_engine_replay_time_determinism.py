from __future__ import annotations

from core.of_confirm_engine import OFConfirmEngine
from tests.test_of_confirm_engine_ok_soft import _base_cfg, _Runtime


def _run_once(replay_ts_ms: int):
    rt = _Runtime(replay_ts_ms)
    cfg = _base_cfg()
    indicators = {
        "bucket_id": 1,
        "exec_risk_bps": 0.5,
        "exec_risk_norm": 0.10,
        "confidence_pct": 0.0,
    }

    eng = OFConfirmEngine(version=3)
    eng.set_replay_time_ms(replay_ts_ms)
    ofc, _gd = eng.build(
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        tick_ts_ms=0,  # force engine to use replay time
        price=100.0,
        delta_z=3.2,
        runtime=rt,
        cfg=cfg,
        indicators=indicators,
        absorption=None,
    )
    return ofc.to_dict(), dict(indicators)


def test_replay_time_makes_now_ts_deterministic_when_tick_ts_missing():
    replay_ts_ms = 1_700_000_123_456
    a1, ind1 = _run_once(replay_ts_ms)
    a2, ind2 = _run_once(replay_ts_ms)

    assert a1 == a2
    assert ind1["ok_soft"] == ind2["ok_soft"]
    assert ind1["ok_soft_reason"] == ind2["ok_soft_reason"]











