from core.weak_progress_detector import WeakProgressDetector


class _WP:
    def __init__(self, weak_any: bool):
        self.weak_any = bool(weak_any)
        self.range_atr = 0.2
        self.body_atr = 0.2
        self.eff = 0.01


def test_recent_weak_count_and_streak():
    det = WeakProgressDetector(maxlen=50, recent_window=5)
    ts = 0

    for w in [1, 0, 1, 1, 0, 1]:
        det.push(_WP(bool(w)), ts_ms=ts)
        ts += 1000

    # last 5: 0,1,1,0,1 => 3
    assert det.recent_weak_count() == 3
    assert abs(det.recent_weak_frac() - (3 / 5)) < 1e-9
    assert det.weak_streak() == 1
