from unittest.mock import MagicMock, patch

from services.of_timers_worker import run_of_gate_dlq_db_archive_nightly


@patch("services.of_timers_worker.run_tool")
@patch("os.getenv")
@patch("redis.Redis.from_url")
def test_run_of_gate_dlq_db_archive_nightly_disabled(mock_redis, mock_getenv, mock_run_tool):
    def env_mock(key, default=None):
        if key == "ENABLE_OF_GATE_DLQ_DB_ARCHIVE_NIGHTLY": return "0"
        return default
    mock_getenv.side_effect = env_mock

    assert run_of_gate_dlq_db_archive_nightly() is False
    mock_run_tool.assert_not_called()

@patch("services.of_timers_worker.run_tool")
@patch("os.getenv")
@patch("redis.Redis.from_url")
def test_run_of_gate_dlq_db_archive_nightly_lock_fail(mock_redis_url, mock_getenv, mock_run_tool):
    def env_mock(key, default=None):
        if key == "ENABLE_OF_GATE_DLQ_DB_ARCHIVE_NIGHTLY": return "1"
        return default
    mock_getenv.side_effect = env_mock

    mock_r = MagicMock()
    mock_redis_url.return_value = mock_r
    mock_r.set.return_value = False # fail to acquire lock

    assert run_of_gate_dlq_db_archive_nightly() is False
    mock_run_tool.assert_not_called()

@patch("services.of_timers_worker.run_tool")
@patch("os.getenv")
@patch("redis.Redis.from_url")
def test_run_of_gate_dlq_db_archive_nightly_success(mock_redis_url, mock_getenv, mock_run_tool):
    def env_mock(key, default=None):
        if key == "ENABLE_OF_GATE_DLQ_DB_ARCHIVE_NIGHTLY": return "1"
        if key == "OF_GATE_DLQ_DB_ARCHIVE_STREAMS": return "stream:1,stream:2"
        if key == "OF_GATE_DLQ_DB_ARCHIVE_BATCH": return "5000"
        if key == "OF_GATE_DLQ_DB_ARCHIVE_TIMEOUT_S": return "1800"
        return default
    mock_getenv.side_effect = env_mock

    mock_r = MagicMock()
    mock_redis_url.return_value = mock_r
    mock_r.set.return_value = True # acquire lock

    mock_run_tool.return_value = True

    assert run_of_gate_dlq_db_archive_nightly() is True
    mock_run_tool.assert_called_once_with(
        "orderflow_services.of_gate_dlq_archive_to_db_v1",
        ["--streams", "stream:1,stream:2", "--batch", "5000", "--once"],
        timeout=1800
    )

