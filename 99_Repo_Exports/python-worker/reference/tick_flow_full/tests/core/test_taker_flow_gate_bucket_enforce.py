from core.taker_flow_gate_v1 import eval_taker_flow_gate


def _ind(bucket: str):
    return {
        'exec_regime_bucket': bucket
        'taker_buy_rate_ema': 10.0
        'taker_sell_rate_ema': 1.0
        'taker_flow_imb': -0.5
        'taker_flow_imb_z': -3.0
    }


def test_taker_flow_gate_enforce_only_in_high_vol_low_liq_default():
    cfg = {
        'taker_flow_gate_mode': 'enforce'
        # default allowlist is HIGH_VOL_LOW_LIQ
        'taker_flow_contra_z_hard': 2.5
        'taker_flow_contra_imb_hard': 0.25
    }

    r1 = eval_taker_flow_gate('LONG', _ind('NORMAL'), cfg)
    assert r1.veto == 0
    assert r1.shadow_veto == 1

    r2 = eval_taker_flow_gate('LONG', _ind('HIGH_VOL_LOW_LIQ'), cfg)
    assert r2.veto == 1
    assert r2.shadow_veto == 0


def test_taker_flow_gate_enforce_all():
    cfg = {
        'taker_flow_gate_mode': 'enforce'
        'taker_flow_gate_enforce_buckets': 'all'
        'taker_flow_contra_z_hard': 2.5
        'taker_flow_contra_imb_hard': 0.25
    }
    r = eval_taker_flow_gate('LONG', _ind('NORMAL'), cfg)
    assert r.veto == 1
