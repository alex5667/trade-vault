import json
import sys
from pathlib import Path

import pytest

# Mirror existing tests' import contract:
# add tick_flow_full to sys.path so we can import as `core.*`
ROOT = Path(__file__).resolve().parents[2]
TICK_FLOW_FULL = ROOT / "tick_flow_full"
if str(TICK_FLOW_FULL) not in sys.path:
    sys.path.insert(0, str(TICK_FLOW_FULL))


def _pick_v5_extra_keys():
    from core.feature_registry import get_schema_info

    v4 = set(get_schema_info("v4_of").feature_names)
    v5 = set(get_schema_info("v5_of").feature_names)
    extras = sorted(v5 - v4)

    num = None
    boo = None
    for f in extras:
        if f.startswith("n:") and num is None:
            num = f[2:]
        if f.startswith("b:") and boo is None:
            boo = f[2:]
        if num and boo:
            break

    if num is None and extras:
        f = extras[0]
        num = f.split(":", 1)[-1]

    return num, boo


def test_feature_registry_v5_of_stable_filters_denylist(tmp_path, monkeypatch):
    import core.feature_registry as fr

    num_key, bool_key = _pick_v5_extra_keys()
    assert num_key is not None

    deny = {
        "ver": "v1"
        "updated_utc": ""
        "deny_num": [num_key]
        "deny_bool": [bool_key] if bool_key else []
        "notes": "test"
    }
    deny_path = tmp_path / "feature_denylist_v1.json"
    deny_path.write_text(json.dumps(deny, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    monkeypatch.setenv("ML_FEATURE_DENYLIST_PATH", str(deny_path))
    fr._DENYLIST_CACHE = None

    v5 = set(fr.get_schema_info("v5_of").feature_names)
    stable = set(fr.get_schema_info("v5_of_stable").feature_names)

    assert f"n:{num_key}" in v5
    assert f"n:{num_key}" not in stable

    if bool_key:
        assert f"b:{bool_key}" in v5
        assert f"b:{bool_key}" not in stable


def test_autogen_denylist_proposal_writes_diff_and_dedups(tmp_path, monkeypatch):
    # Ensure tool can import `core` too
    if str(TICK_FLOW_FULL) not in sys.path:
        sys.path.insert(0, str(TICK_FLOW_FULL))

    from ml_analysis.tools import autogen_feature_denylist_proposal_v1 as tool

    num_key, _ = _pick_v5_extra_keys()
    assert num_key is not None

    fs_dir = tmp_path / "fs_run"
    fs_dir.mkdir(parents=True)

    st = fs_dir / "stability_table.csv"
    st.write_text("feature,flag_noise\n" f"n:{num_key},1\n", encoding="utf-8")

    deny_path = tmp_path / "feature_denylist_v1.json"
    deny_path.write_text(
        json.dumps({"ver": "v1", "updated_utc": "", "deny_num": [], "deny_bool": [], "notes": ""}, indent=2)
        + "\n"
        encoding="utf-8"
    )

    proposals_dir = tmp_path / "proposals"
    monkeypatch.chdir(tmp_path)

    argv = [
        "--fs-run-dir"
        str(fs_dir)
        "--denylist-path"
        str(deny_path)
        "--proposals-dir"
        str(proposals_dir)
        "--dedup-days"
        "30"
        "--max-features"
        "5"
    ]

    import sys as _sys

    old = _sys.argv
    try:
        _sys.argv = ["autogen"] + argv
        assert tool.main() == 0
    finally:
        _sys.argv = old

    diffs = list(proposals_dir.glob("denylist_proposal_*.diff"))
    manifests = list(proposals_dir.glob("denylist_proposal_*.manifest.json"))
    assert len(diffs) == 1
    assert len(manifests) == 1

    diff_txt = diffs[0].read_text(encoding="utf-8")
    assert num_key in diff_txt

    m = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert m["status"] == "pending_ab"
    assert num_key in m["adds"]["deny_num"]

    # rerun -> dedup
    old = _sys.argv
    try:
        _sys.argv = ["autogen"] + argv
        assert tool.main() == 0
    finally:
        _sys.argv = old

    diffs2 = list(proposals_dir.glob("denylist_proposal_*.diff"))
    manifests2 = list(proposals_dir.glob("denylist_proposal_*.manifest.json"))
    assert len(diffs2) == 1
    assert len(manifests2) == 1
