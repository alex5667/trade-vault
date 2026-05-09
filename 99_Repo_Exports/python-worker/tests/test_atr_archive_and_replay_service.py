
# Adjusting python path in case it is needed:
from unittest.mock import MagicMock

import pytest

# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from services.atr_archive_and_replay_service import ATRArchiveAndReplayService
from core.redis_keys import RedisStreams as RS


@pytest.fixture
def db_conn_mock():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn

@pytest.fixture
def service(db_conn_mock):
    return ATRArchiveAndReplayService(db_conn_mock)

def test_classify_artifact(service):
    assert service.classify_artifact(RS.CRYPTO_RAW) == "signal"
    assert service.classify_artifact("orders:queue:mt5") == "dispatch"
    assert service.classify_artifact("closed_trades") == "post_trade"
    assert service.classify_artifact("control_plane") == "governance"
    assert service.classify_artifact("unknown_stream") == "unknown"


def test_archive_artifact_class(service, db_conn_mock):
    success = service.archive_artifact_class("signal", "warm")
    assert success is True
    cursor_mock = db_conn_mock.cursor.return_value.__enter__.return_value
    assert cursor_mock.execute.called

    with pytest.raises(ValueError):
        service.archive_artifact_class("invalid_class", "warm")


def test_build_replay_bundle(service, db_conn_mock):
    scope = {"symbols": ["BTCUSDT", "ETHUSDT"], "layers": ["signal", "execution", "post_trade"]}
    time_range = {"start": "2026-04-17T00:00:00Z", "end": "2026-04-17T23:59:59Z"}

    manifest = service.build_replay_bundle("bundle_123", time_range, scope)
    assert manifest["bundle_id"] == "bundle_123"
    assert len(manifest["files"]) == 3
    for f in manifest["files"]:
        assert "sha256" in f

    cursor_mock = db_conn_mock.cursor.return_value.__enter__.return_value
    assert cursor_mock.execute.call_count == 1
    args, _ = cursor_mock.execute.call_args
    assert "INSERT INTO atr_replay_bundles" in args[0]


def test_verify_bundle_integrity_success(service, db_conn_mock):
    scope = {"layers": ["signal"]}
    time_range = {"start": "2026-04-17", "end": "2026-04-17"}
    manifest = service.build_replay_bundle("bundle_456", time_range, scope)

    assert service.verify_bundle_integrity("bundle_456", manifest) is True


def test_verify_bundle_integrity_missing_checksum(service):
    # Invalid missing sha256
    manifest = {"files": [{"name": "signal.ndjson"}]}
    assert service.verify_bundle_integrity("bundle_789", manifest) is False


def test_purge_expired_hot_data(service):
    # Should pass
    assert service.purge_expired_hot_data("signal", incident_linked=False, archive_ready=True) is True

    # Blocked because linked to incident
    assert service.purge_expired_hot_data("signal", incident_linked=True, archive_ready=True) is False

    # Blocked because archive is not ready
    assert service.purge_expired_hot_data("signal", incident_linked=False, archive_ready=False) is False


def test_run_restore_sample(service, db_conn_mock):
    success = service.run_restore_sample("bundle_123")
    assert success is True

    cursor_mock = db_conn_mock.cursor.return_value.__enter__.return_value
    calls = cursor_mock.execute.mock_calls

    # Calls include the insert to checking table, and the update for restored_at
    found_update = any("UPDATE atr_replay_bundles SET restored_at = %s WHERE bundle_id = %s" in str(c) for c in calls)
    assert found_update is True
