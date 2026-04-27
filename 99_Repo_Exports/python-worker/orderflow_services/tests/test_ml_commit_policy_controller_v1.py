from orderflow_services.ml_commit_policy_controller_v1 import evaluate_commit_policy


def test_global_kill_switch_blocks():
    d = evaluate_commit_policy(
        recommendation_id="r1",
        action_type="propose_threshold_canary",
        replay_status="PASS",
        risk_level="low",
        approvals=2,
        min_approvals=1,
        last_commit_ts_ms=0,
        commits_last_hour=0,
        now_ms=1_000_000,
        global_cfg={"commit_enabled": "1", "kill_switch": "1", "kill_reason": "ops"},
        action_cfg={"enabled": "1", "executor_mode": "COMMIT"},
    )
    assert d.allow_commit is False
    assert d.reason.startswith("global_kill_switch")


def test_cooldown_blocks_commit():
    d = evaluate_commit_policy(
        recommendation_id="r1",
        action_type="propose_threshold_canary",
        replay_status="PASS",
        risk_level="low",
        approvals=2,
        min_approvals=1,
        last_commit_ts_ms=950_000,
        commits_last_hour=0,
        now_ms=1_000_000,
        global_cfg={"commit_enabled": "1", "kill_switch": "0", "default_cooldown_sec": "60"},
        action_cfg={"enabled": "1", "executor_mode": "COMMIT", "cooldown_sec": "120"},
    )
    assert d.allow_commit is False
    assert d.reason == "cooldown_active"
    assert d.cooldown_remaining_sec > 0


def test_replay_required_blocks():
    d = evaluate_commit_policy(
        recommendation_id="r1",
        action_type="propose_threshold_canary",
        replay_status="FAIL",
        risk_level="low",
        approvals=2,
        min_approvals=1,
        last_commit_ts_ms=0,
        commits_last_hour=0,
        now_ms=1_000_000,
        global_cfg={"commit_enabled": "1", "kill_switch": "0"},
        action_cfg={"enabled": "1", "executor_mode": "COMMIT", "require_replay_pass": "1"},
    )
    assert d.allow_commit is False
    assert d.reason == "replay_required"


def test_approved_commit_path():
    d = evaluate_commit_policy(
        recommendation_id="r1",
        action_type="freeze_candidate",
        replay_status="PASS",
        risk_level="low",
        approvals=2,
        min_approvals=1,
        last_commit_ts_ms=0,
        commits_last_hour=0,
        now_ms=1_000_000,
        global_cfg={"commit_enabled": "1", "kill_switch": "0"},
        action_cfg={"enabled": "1", "executor_mode": "COMMIT", "canary_share": "1.0"},
    )
    assert d.allow_commit is True
    assert d.reason == "approved"
    assert d.policy_mode == "COMMIT"
    assert d.dry_run_only is False

