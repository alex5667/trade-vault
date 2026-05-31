import json
import time
from types import SimpleNamespace

from orderflow_services.confidence_calibrator import get_cached_calibrator


def test_get_cached_calibrator_basic(tmp_path):
    cal_file = tmp_path / "cal.json"
    cal_data = {
        "schema_version": 1,
        "type": "temp_logit",
        "t": 1.5
    }
    cal_file.write_text(json.dumps(cal_data))

    runtime = SimpleNamespace()

    # 1. Load first time
    cal1 = get_cached_calibrator(runtime, str(cal_file))
    assert cal1 is not None
    assert cal1.t == 1.5
    assert hasattr(runtime, "_confidence_cal_cache")

    # 2. Sequential call within check_every_ms (should be same object)
    cal2 = get_cached_calibrator(runtime, str(cal_file), check_every_ms=5000)
    assert cal1 is cal2

    # 3. Modify file, but call again within check_every_ms (should still be old cached object)
    cal_data["t"] = 2.0
    cal_file.write_text(json.dumps(cal_data))
    cal3 = get_cached_calibrator(runtime, str(cal_file), check_every_ms=5000)
    assert cal3.t == 1.5
    assert cal3 is cal1

    # 4. Force check by using a very small/negative check_every_ms
    # To handle mtime precision, we might need to sleep or manually adjust mtime
    # but let's try just setting check_every_ms to -1 first.
    # Note: os.stat mtime might not change if we write too fast.
    time.sleep(0.1) # ensure mtime change if possible
    cal_file.write_text(json.dumps(cal_data))

    cal4 = get_cached_calibrator(runtime, str(cal_file), check_every_ms=-1)
    assert cal4 is not cal1
    assert cal4.t == 2.0

def test_get_cached_calibrator_missing_file(tmp_path):
    runtime = SimpleNamespace()
    p = str(tmp_path / "nonexistent.json")

    cal = get_cached_calibrator(runtime, p)
    assert cal is None
    assert runtime._confidence_cal_cache["cal"] is None

def test_get_cached_calibrator_invalid_path(tmp_path):
    runtime = SimpleNamespace()
    assert get_cached_calibrator(runtime, "") is None
    assert get_cached_calibrator(runtime, None) is None
