from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def _read_yaml(rel_path: str):
    return yaml.safe_load((ROOT / rel_path).read_text(encoding="utf-8"))


def test_rollout_workflow_exists_and_has_dispatch_inputs():
    wf = _read_yaml(".github/workflows/news-notification-schema-rollout.yml")
    assert "on" in wf
    assert "workflow_dispatch" in wf["on"]
    inputs = wf["on"]["workflow_dispatch"]["inputs"]
    for key in ("current_phase", "dwell_minutes", "approved", "apply", "env_path"):
        assert key in inputs


def test_rollout_workflow_has_expected_jobs():
    wf = _read_yaml(".github/workflows/news-notification-schema-rollout.yml")
    jobs = wf["jobs"]
    assert "decision" in jobs
    assert "proposal" in jobs
    assert "apply" in jobs
    assert "verify" in jobs


def test_rollout_workflow_references_tools():
    text = (ROOT / ".github/workflows/news-notification-schema-rollout.yml").read_text(encoding="utf-8")
    assert "tools.notification_rollout_decision" in text
    assert "tools.notification_rollout_proposal" in text
    assert "tools.notification_rollout_apply" in text
    assert "notification-rollout-decision" in text
    assert "notification-rollout-proposal" in text
    assert "notification-rollout-apply" in text


def test_rollout_workflow_apply_is_gated_by_approval():
    text = (ROOT / ".github/workflows/news-notification-schema-rollout.yml").read_text(encoding="utf-8")
    assert "if: ${{ github.event.inputs.approved == 'true' }}" in text
