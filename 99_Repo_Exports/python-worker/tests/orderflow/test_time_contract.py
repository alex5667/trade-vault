import time
from services.crypto_orderflow_service import _utc_epoch_ms

def test_utc_epoch_ms():
    t_start = int(time.time() * 1000)
    t_val = _utc_epoch_ms()
    t_end = int(time.time() * 1000)
    assert t_start <= t_val <= t_end + 1000
