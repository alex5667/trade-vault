from __future__ import annotations

import inspect


def test_no_append_flag_one_arg_regression():
    # Regression guard: historical bug where _append_flag was called with one arg.
    import handlers.crypto_orderflow_handler as m

    src = inspect.getsource(m)
    assert "_append_flag(f\"cons:" not in src
    assert "_append_flag(ctx, f\"cons:" in src


def test_no_math_isfinite_none_regression():
    # Regression guard: math.isfinite(dec.spread_ema_bps) crashes when spread_ema_bps is None.
    import handlers.crypto_orderflow_handler as m

    src = inspect.getsource(m)
    assert "math.isfinite(dec.spread_ema_bps)" not in src
