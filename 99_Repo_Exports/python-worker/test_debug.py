import os
from unittest.mock import patch
from core.tick_cvd import TickCVDState
with patch.dict(os.environ, {"CVD_QUARANTINE_ENABLE": "1", "CVD_JUMP_ABS_QTY": "100", "CVD_JUMP_REL_K": "5.0", "CVD_JUMP_WINDOW_MS": "10000", "CVD_JUMP_K_EVENTS": "2", "CVD_QUARANTINE_TTL_MS": "5000"}):
    state = TickCVDState("BTCUSDT", ema_period_delta=10)
    now_ms = 1000000000
    for i in range(10): state.update({"ts": now_ms + i * 100, "qty": 10, "side": "BUY"})
    state.update({"ts": now_ms + 2000, "qty": 1000, "side": "BUY"})
    print("ema_abs_qty after 1:", state._ema_abs_delta_qty, "thr_qty:", max(state._jump_abs_qty, state._jump_rel_k * max(1e-9, state._ema_abs_delta_qty)))
    state.update({"ts": now_ms + 2100, "qty": 1000, "side": "BUY"})
    print("ema_abs_qty after 2:", state._ema_abs_delta_qty)
