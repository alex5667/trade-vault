import json
import os
import time

from ml_analysis.tools.confidence_cal_live_health_loop_v1 import _atomic_replace, _pick_rollback_version


def test_pick_rollback_previous_version(tmp_path):
    out_dir = tmp_path / "cal"
    ver_dir = out_dir / "versions"
    ver_dir.mkdir(parents=True)

    v1 = ver_dir / "conf_cal_1.json"
    v2 = ver_dir / "conf_cal_2.json"
    latest = out_dir / "conf_cal_latest.json"

    v1.write_text(json.dumps({"schema_version": 1, "type": "temp_logit", "t": 1.0}), encoding="utf-8")
    v2.write_text(json.dumps({"schema_version": 1, "type": "temp_logit", "t": 2.0}), encoding="utf-8")

    # ensure mtimes ordered
    os.utime(v1, (time.time() - 10, time.time() - 10))
    os.utime(v2, (time.time() - 5, time.time() - 5))

    latest.write_text(v2.read_text(encoding="utf-8"), encoding="utf-8")

    cand = _pick_rollback_version(str(out_dir), str(latest))
    assert cand is not None
    assert os.path.basename(cand) == "conf_cal_1.json"


def test_atomic_replace_rolls_back(tmp_path):
    out_dir = tmp_path / "cal"
    out_dir.mkdir(parents=True)
    src = out_dir / "src.json"
    dst = out_dir / "dst.json"

    src.write_text('{"a":1}', encoding="utf-8")
    dst.write_text('{"a":2}', encoding="utf-8")

    _atomic_replace(str(src), str(dst))
    assert dst.read_text(encoding="utf-8") == '{"a":1}'
