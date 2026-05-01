from core.taker_flow_gate_v1 import eval_taker_flow_gate


def _ind(bucket: str):
    return {
        'exec_regime_bucket': bucket,
        'taker_buy_rate_ema': 10.0,
        'taker_sell_rate_ema': 1.0,
        'taker_flow_imb': -0.5,
        'taker_flow_imb_z': -3.0,
    }


def test_taker_flow_gate_enforce_default_bucket_only():
    cfg = {
        'taker_flow_gate_mode': 'enforce',
        'taker_flow_contra_z_hard': 2.5,
        'taker_flow_contra_imb_hard': 0.25,
    }
    r_norm = eval_taker_flow_gate('LONG', _ind('NORMAL'), cfg)
    assert r_norm.veto == 0 and r_norm.shadow_veto == 1

    r_hvll = eval_taker_flow_gate('LONG', _ind('HIGH_VOL_LOW_LIQ'), cfg)
    assert r_hvll.veto == 1 and r_hvll.shadow_veto == 0
