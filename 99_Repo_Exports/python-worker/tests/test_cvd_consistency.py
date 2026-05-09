
from core.tick_cvd import TickCVDState
from utils.time_utils import get_ny_time_millis


def test_quarantine_on_external_cvd_jump(monkeypatch):
    monkeypatch.setenv("CVD_QUARANTINE_ENABLE", "1")
    monkeypatch.setenv("CVD_JUMP_ABS_USD", "100")
    monkeypatch.setenv("CVD_JUMP_REL_K", "2.0")
    monkeypatch.setenv("CVD_JUMP_K_EVENTS", "2")
    monkeypatch.setenv("CVD_JUMP_WINDOW_MS", "180000")
    monkeypatch.setenv("CVD_QUARANTINE_TTL_MS", "60000")

    s = TickCVDState(symbol="BTCUSDT", robust_window=50)
    t0 = get_ny_time_millis()

    # seed deltas with small CVD values to establish baseline
    for i in range(20):
        s.update({"ts_ms": t0 + i * 100, "price": 1000.0, "qty": 1.0, "is_buyer_maker": False, "cvd_usd": float(i * 10)})

    # 1st jump (no quarantine yet, but event recorded)
    s.update({"ts_ms": t0 + 3000, "price": 1000.0, "qty": 1.0, "is_buyer_maker": False, "cvd_usd": 10000.0})
    ind1 = s.indicators_light()
    assert int(ind1.get("cvd_quarantine_active", 0)) == 0
    assert int(ind1.get("cvd_jump_events_total", 0)) >= 1

    # 2nd jump within window -> quarantine (jump from 10000 to 0 = 10000 USD jump > 100 threshold)
    s.update({"ts_ms": t0 + 4000, "price": 1000.0, "qty": 1.0, "is_buyer_maker": False, "cvd_usd": 0.0})
    ind2 = s.indicators_light()
    assert int(ind2.get("cvd_quarantine_active", 0)) == 1


def test_quarantine_on_out_of_order(monkeypatch):
    monkeypatch.setenv("CVD_QUARANTINE_ENABLE", "1")
    monkeypatch.setenv("CVD_OOO_MAX_LAG_MS", "10")
    monkeypatch.setenv("CVD_QUARANTINE_TTL_MS", "60000")
    s = TickCVDState(symbol="BTCUSDT", robust_window=50)
    t0 = get_ny_time_millis()
    s.update({"ts_ms": t0 + 1000, "price": 1000.0, "qty": 1.0, "is_buyer_maker": False})
    s.update({"ts_ms": t0 + 2000, "price": 1000.0, "qty": 1.0, "is_buyer_maker": False})
    # out-of-order
    s.update({"ts_ms": t0 + 1500, "price": 1000.0, "qty": 1.0, "is_buyer_maker": False})
    ind = s.indicators_light()
    assert int(ind.get("cvd_quarantine_active", 0)) == 1
