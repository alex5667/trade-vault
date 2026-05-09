import json
import time


def test_norm_side_no_pass_and_bool_guard():
    from common.calibration_store import _norm_side
    assert _norm_side(None) == "*"
    assert _norm_side(True) == "*"     # bool must not map to LONG
    assert _norm_side(False) == "*"
    assert _norm_side(1) == "LONG"
    assert _norm_side(-1) == "SHORT"
    assert _norm_side("buy") == "LONG"
    assert _norm_side("SELL") == "SHORT"
    assert _norm_side("weird") == "*"


def _mk_cal():
    from common.isotonic_calibration import IsotonicCalibrator
    return IsotonicCalibrator(x=[0.0, 1.0], p=[0.0, 1.0], mode="linear")


def test_get_group_side_aware_priority_and_legacy_fallback():
    from common.calibration_store import CalibGroup, CalibStore

    # path="" -> load() keeps empty groups (no FS)
    s = CalibStore(path="", min_samples=10, reload_sec=0)
    s._groups = {
        "kind:K|symbol:S|side:LONG": CalibGroup(calibrator=_mk_cal(), n=100),
        "kind:K|symbol:S|side:*": CalibGroup(calibrator=_mk_cal(), n=100),
        "kind:K|symbol:S": CalibGroup(calibrator=_mk_cal(), n=100),
        "global": CalibGroup(calibrator=_mk_cal(), n=100),
    }

    g, k = s.get_group(kind="K", symbol="S", side="LONG")
    assert g is not None
    assert k == "kind:K|symbol:S|side:LONG"

    g2, k2 = s.get_group(kind="K", symbol="S")  # no side -> should prefer side:* in new format
    assert g2 is not None
    assert k2 == "kind:K|symbol:S|side:*"

    # If side:* missing -> legacy fallback should still work
    s2 = CalibStore(path="", min_samples=10, reload_sec=0)
    s2._groups = {
        "kind:K|symbol:S": CalibGroup(calibrator=_mk_cal(), n=100),
        "global": CalibGroup(calibrator=_mk_cal(), n=100),
    }
    g3, k3 = s2.get_group(kind="K", symbol="S")
    assert g3 is not None
    assert k3 == "kind:K|symbol:S"


def test_min_samples_gate_skips_small_groups():
    from common.calibration_store import CalibGroup, CalibStore
    s = CalibStore(path="", min_samples=300, reload_sec=0)
    s._groups = {
        "kind:K|symbol:S|side:*": CalibGroup(calibrator=_mk_cal(), n=10),   # too small
        "global": CalibGroup(calibrator=_mk_cal(), n=500),
    }
    g, k = s.get_group(kind="K", symbol="S")
    assert g is not None
    assert k == "global"


def test_load_and_maybe_reload_and_corrupt_file_keeps_old_groups(tmp_path):
    from common.calibration_store import CalibStore

    p = tmp_path / "calib.json"
    obj1 = {
        "groups": {
            "kind:K|symbol:S|side:LONG": {"type": "isotonic", "x": [0, 1], "p": [0, 1], "mode": "linear", "n": 500},
            "global": {"type": "isotonic", "x": [0, 1], "p": [0.2, 0.8], "mode": "linear", "n": 500},
        }
    }
    p.write_text(json.dumps(obj1), encoding="utf-8")

    s = CalibStore(path=str(p), min_samples=10, reload_sec=0)
    g, k = s.get_group(kind="K", symbol="S", side="LONG")
    assert g is not None
    assert k == "kind:K|symbol:S|side:LONG"

    # Update file -> maybe_reload should pick it up (reload_sec=0)
    obj2 = {
        "groups": {
            "kind:K|symbol:S|side:LONG": {"type": "isotonic", "x": [0, 1], "p": [0.1, 0.9], "mode": "linear", "n": 500},
            "global": {"type": "isotonic", "x": [0, 1], "p": [0.3, 0.7], "mode": "linear", "n": 500},
        }
    }
    time.sleep(0.01)  # ensure mtime changes on some filesystems
    p.write_text(json.dumps(obj2), encoding="utf-8")
    s.maybe_reload(now_ts=time.time())
    g2, k2 = s.get_group(kind="K", symbol="S", side="LONG")
    assert g2 is not None
    assert k2 == "kind:K|symbol:S|side:LONG"
    assert list(g2.calibrator.p) == [0.1, 0.9]

    # Corrupt file -> reload attempt should NOT wipe current groups
    time.sleep(0.01)
    p.write_text("{broken json", encoding="utf-8")
    s.maybe_reload(now_ts=time.time())
    g3, k3 = s.get_group(kind="K", symbol="S", side="LONG")
    assert g3 is not None
    assert k3 == "kind:K|symbol:S|side:LONG"
    assert list(g3.calibrator.p) == [0.1, 0.9]
