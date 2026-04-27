from pathlib import Path

from orderflow_services.ofc_contextual_runtime_reloader_v1 import (
    _reason_kind,
    fingerprint_overlay,
    load_env_file,
    merge_child_env,
)


def test_load_env_file_parses_basic_pairs(tmp_path: Path):
    p = tmp_path / "overlay.env"
    p.write_text(
        "# comment\n"
        "OFC_CTX_ENABLE=1\n"
        "export OFC_CTX_MODE=shadow\n"
        "QUOTED='abc def'\n",
        encoding="utf-8",
    )
    got = load_env_file(str(p))
    assert got["OFC_CTX_ENABLE"] == "1"
    assert got["OFC_CTX_MODE"] == "shadow"
    assert got["QUOTED"] == "abc def"


def test_merge_child_env_overlay_wins():
    got = merge_child_env({"A": "1", "B": "2"}, {"B": "9", "C": "3"})
    assert got["A"] == "1"
    assert got["B"] == "9"
    assert got["C"] == "3"


def test_fingerprint_changes_on_rollback_flag():
    fp1 = fingerprint_overlay({"OFC_CTX_MODE": "shadow"}, False)
    fp2 = fingerprint_overlay({"OFC_CTX_MODE": "shadow"}, True)
    assert fp1 != fp2


def test_reason_kind_maps_expected_prefixes():
    assert _reason_kind("child_exit:143") == "child_exit"
    assert _reason_kind("overlay_changed:old_pid=10") == "overlay_changed"
    assert _reason_kind("signal:15") == "signal"
    assert _reason_kind("cooldown") == "cooldown"
