from __future__ import annotations


class DummyCfg:
    delta_window_ticks = 200
    delta_z_threshold = 2.5
    weak_progress_atr = None
    tp_rr = 1.8


def test_build_config_params_filters_none():
    from handlers.crypto_orderflow.crypto_orderflow_handler import CryptoOrderFlowHandler

    out = CryptoOrderFlowHandler._build_config_params_from_cfg(DummyCfg())
    assert out["delta_window_ticks"] == 200
    assert out["delta_z_threshold"] == 2.5
    assert out["tp_rr"] == 1.8
    assert "weak_progress_atr" not in out  # None должен быть выкинут
