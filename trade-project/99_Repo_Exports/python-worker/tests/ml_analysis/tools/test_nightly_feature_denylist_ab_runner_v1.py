import json
import os
from unittest import mock

import pytest

from ml_analysis.tools import nightly_feature_denylist_ab_runner_v1


@pytest.fixture
def proposals_dir(tmp_path):
    d = tmp_path / "proposals"
    d.mkdir()

    # Create an old pending one
    p1 = d / "denylist_proposal_1.manifest.json"
    p1.write_text(json.dumps({"status": "pending_ab"}), encoding="utf-8")
    os.utime(p1, (0, 0))

    # Create a newer pending one
    p2 = d / "denylist_proposal_2.manifest.json"
    p2.write_text(json.dumps({"status": "pending_ab"}), encoding="utf-8")

    # Create an approved one
    p3 = d / "denylist_proposal_3.manifest.json"
    p3.write_text(json.dumps({"status": "approved"}), encoding="utf-8")

    return d


def test_list_pending(proposals_dir):
    pending = nightly_feature_denylist_ab_runner_v1._list_pending(proposals_dir)
    assert len(pending) == 2
    # Oldest first
    assert pending[0].name == "denylist_proposal_1.manifest.json"
    assert pending[1].name == "denylist_proposal_2.manifest.json"


@mock.patch("ml_analysis.tools.nightly_feature_denylist_ab_runner_v1._get_redis")
@mock.patch("ml_analysis.tools.nightly_feature_denylist_ab_runner_v1._run_replay_ab")
def test_nightly_runner_main(mock_run_replay, mock_get_redis, proposals_dir, monkeypatch):
    mock_redis = mock.Mock()
    mock_get_redis.return_value = mock_redis

    mock_run_replay.return_value = (0, "ok", "")

    monkeypatch.setenv("FEATURE_DENYLIST_PROPOSALS_DIR", str(proposals_dir))
    monkeypatch.setenv("FEATURE_DENYLIST_AB_MAX_PENDING", "1")

    # Dry run check
    with mock.patch("sys.argv", ["runner", "--dry-run", "1"]):
        assert nightly_feature_denylist_ab_runner_v1.main() == 0
        mock_run_replay.assert_not_called()

    # Normal run check
    with mock.patch("sys.argv", ["runner"]):
        rc = nightly_feature_denylist_ab_runner_v1.main()
        assert rc == 0
        mock_run_replay.assert_called_once()

        args, _ = mock_run_replay.call_args
        assert args[0].name == "denylist_proposal_1.manifest.json"

        # Check metrics written
        assert mock_redis.hset.called

@mock.patch("ml_analysis.tools.nightly_feature_denylist_ab_runner_v1._get_redis")
@mock.patch("ml_analysis.tools.nightly_feature_denylist_ab_runner_v1._run_replay_ab")
def test_nightly_runner_main_fail(mock_run_replay, mock_get_redis, proposals_dir, monkeypatch):
    mock_redis = mock.Mock()
    mock_get_redis.return_value = mock_redis

    # Simulate a failure in replay tool
    mock_run_replay.return_value = (1, "fail", "error")

    monkeypatch.setenv("FEATURE_DENYLIST_PROPOSALS_DIR", str(proposals_dir))
    monkeypatch.setenv("FEATURE_DENYLIST_AB_MAX_PENDING", "2")

    with mock.patch("sys.argv", ["runner"]):
        rc = nightly_feature_denylist_ab_runner_v1.main()
        assert rc == 2
        assert mock_run_replay.call_count == 2
        assert mock_redis.hset.called
