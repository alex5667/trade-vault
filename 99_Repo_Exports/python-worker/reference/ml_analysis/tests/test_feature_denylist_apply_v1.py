"""Tests for apply_feature_denylist_proposal_v1 and the updated approve gate (P106)."""
import json


def _write_json(p, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def test_approve_requires_ab_done_and_gate_pass(tmp_path, monkeypatch):
    """approve must reject if status != ab_done, or if ab.gate_pass != 1."""
    from ml_analysis.tools import approve_feature_denylist_proposal_v1 as approve

    proposals_dir = tmp_path / "proposals"
    proposals_dir.mkdir(parents=True)

    manifest = proposals_dir / "denylist_proposal_test.manifest.json"
    report = proposals_dir / "ab_report.json"
    _write_json(report, {"gate_pass": 1, "manifest": str(manifest)})

    m = {
        "kind": "feature_denylist_proposal_v1"
        "proposal_hash": "ph"
        "created_utc": "2026-02-25T00:00:00+00:00"
        "status": "pending_ab",  # wrong status – must be ab_done
        "ab": {"gate_pass": 1, "report_json": str(report)}
        "denylist_after": {"deny_num": ["n:x"], "deny_bool": []}
    }
    _write_json(manifest, m)

    import sys as _sys

    old = _sys.argv
    try:
        _sys.argv = ["approve", "--manifest", str(manifest), "--approve", "1"]
        assert approve.main() == 2  # must reject: status is pending_ab
    finally:
        _sys.argv = old

    # ab_done but gate_pass=0 in manifest
    m["status"] = "ab_done"
    m["ab"]["gate_pass"] = 0
    _write_json(manifest, m)
    old = _sys.argv
    try:
        _sys.argv = ["approve", "--manifest", str(manifest), "--approve", "1"]
        assert approve.main() == 2  # must reject: gate_pass=0
    finally:
        _sys.argv = old

    # ab_done, gate_pass=1 in manifest, but report gate_pass=0
    m["ab"]["gate_pass"] = 1
    _write_json(manifest, m)
    _write_json(report, {"gate_pass": 0})  # report says NOT passed
    old = _sys.argv
    try:
        _sys.argv = ["approve", "--manifest", str(manifest), "--approve", "1"]
        assert approve.main() == 2  # must reject: report gate_pass=0
    finally:
        _sys.argv = old


def test_approve_then_apply_happy_path(tmp_path, monkeypatch):
    """Full happy path: approve succeeds with ab_done+gate_pass=1, then apply transitions to applied."""
    from ml_analysis.tools import approve_feature_denylist_proposal_v1 as approve
    from ml_analysis.tools import apply_feature_denylist_proposal_v1 as apply_tool

    proposals_dir = tmp_path / "proposals"
    proposals_dir.mkdir(parents=True)

    deny = tmp_path / "feature_denylist_v1.json"
    _write_json(deny, {"deny_num": ["n:keep"], "deny_bool": ["b:keep"], "updated_utc": ""})

    manifest = proposals_dir / "denylist_proposal_test.manifest.json"
    report = proposals_dir / "ab_report.json"
    _write_json(report, {"gate_pass": 1, "manifest": str(manifest)})

    m = {
        "kind": "feature_denylist_proposal_v1"
        "proposal_hash": "ph"
        "created_utc": "2026-02-25T00:00:00+00:00"
        "status": "ab_done"
        "ab": {"gate_pass": 1, "report_json": str(report)}
        "denylist_after": {"deny_num": ["n:keep", "n:add1"], "deny_bool": ["b:keep", "b:add1"]}
    }
    _write_json(manifest, m)

    import sys as _sys

    old = _sys.argv
    try:
        _sys.argv = ["approve", "--manifest", str(manifest), "--approve", "1"]
        assert approve.main() == 0
    finally:
        _sys.argv = old

    m2 = json.loads(manifest.read_text(encoding="utf-8"))
    assert m2["status"] == "approved"
    assert m2.get("approved_gate_pass") == 1

    old = _sys.argv
    try:
        _sys.argv = [
            "apply"
            "--manifest"
            str(manifest)
            "--denylist-path"
            str(deny)
            "--apply"
            "1"
        ]
        assert apply_tool.main() == 0
    finally:
        _sys.argv = old

    active = json.loads(deny.read_text(encoding="utf-8"))
    assert "n:keep" in active["deny_num"]
    assert "n:add1" in active["deny_num"]
    assert "b:keep" in active["deny_bool"]
    assert "b:add1" in active["deny_bool"]

    m3 = json.loads(manifest.read_text(encoding="utf-8"))
    assert m3["status"] == "applied"
    assert m3.get("applied_denylist_path")
    assert m3.get("applied_audit_record")


def test_apply_rejects_non_approved_status(tmp_path):
    """apply_feature_denylist_proposal_v1 must reject if status != approved."""
    from ml_analysis.tools import apply_feature_denylist_proposal_v1 as apply_tool

    proposals_dir = tmp_path / "proposals"
    proposals_dir.mkdir(parents=True)
    deny = tmp_path / "feature_denylist_v1.json"
    _write_json(deny, {"deny_num": [], "deny_bool": []})

    manifest = proposals_dir / "denylist_proposal_test.manifest.json"
    _write_json(manifest, {
        "kind": "feature_denylist_proposal_v1"
        "status": "ab_done",  # not approved
        "denylist_after": {"deny_num": [], "deny_bool": []}
    })

    import sys as _sys

    old = _sys.argv
    try:
        _sys.argv = ["apply", "--manifest", str(manifest), "--denylist-path", str(deny), "--apply", "1"]
        assert apply_tool.main() == 2  # must reject
    finally:
        _sys.argv = old


def test_apply_dry_run(tmp_path):
    """Dry-run (--apply 0) must not modify the denylist file."""
    from ml_analysis.tools import apply_feature_denylist_proposal_v1 as apply_tool

    proposals_dir = tmp_path / "proposals"
    proposals_dir.mkdir(parents=True)
    deny = tmp_path / "feature_denylist_v1.json"
    _write_json(deny, {"deny_num": ["n:old"], "deny_bool": []})
    original_content = deny.read_text(encoding="utf-8")

    manifest = proposals_dir / "denylist_proposal_test.manifest.json"
    _write_json(manifest, {
        "kind": "feature_denylist_proposal_v1"
        "status": "approved"
        "denylist_after": {"deny_num": ["n:old", "n:new"], "deny_bool": ["b:new"]}
    })

    import sys as _sys

    old = _sys.argv
    try:
        _sys.argv = ["apply", "--manifest", str(manifest), "--denylist-path", str(deny), "--apply", "0"]
        assert apply_tool.main() == 0  # dry-run succeeds
    finally:
        _sys.argv = old

    # File must be unchanged
    assert deny.read_text(encoding="utf-8") == original_content


def test_approve_optional_ab_report_json_uses_manifest(tmp_path):
    """--ab-report-json is optional: if omitted, manifest ab.report_json is used."""
    from ml_analysis.tools import approve_feature_denylist_proposal_v1 as approve

    proposals_dir = tmp_path / "proposals"
    proposals_dir.mkdir(parents=True)

    manifest = proposals_dir / "denylist_proposal_test.manifest.json"
    report = proposals_dir / "ab_report.json"
    _write_json(report, {"gate_pass": 1})

    m = {
        "kind": "feature_denylist_proposal_v1"
        "proposal_hash": "ph"
        "created_utc": "2026-02-25T00:00:00+00:00"
        "status": "ab_done"
        "ab": {"gate_pass": 1, "report_json": str(report)}
        "denylist_after": {"deny_num": [], "deny_bool": []}
    }
    _write_json(manifest, m)

    import sys as _sys

    old = _sys.argv
    try:
        # No --ab-report-json: should fall back to manifest ab.report_json
        _sys.argv = ["approve", "--manifest", str(manifest), "--approve", "1"]
        assert approve.main() == 0
    finally:
        _sys.argv = old

    m2 = json.loads(manifest.read_text(encoding="utf-8"))
    assert m2["status"] == "approved"
    assert m2["approved_gate_pass"] == 1
