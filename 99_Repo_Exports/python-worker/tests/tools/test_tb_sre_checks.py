from unittest.mock import MagicMock, patch
import pytest
import redis
from tools.tb_sre_checks import check_tb_health, TBHealth

class TestTBSREChecks:
    @patch("redis.Redis.from_url")
    def test_check_tb_health_busy_loading_error(self, mock_from_url):
        # Setup mock to raise BusyLoadingError
        mock_redis = MagicMock()
        mock_redis.get.side_effect = redis.exceptions.BusyLoadingError("Redis is loading the dataset in memory")
        mock_from_url.return_value = mock_redis

        # Call the function and assert it handles the error gracefully
        result = check_tb_health(redis_url="redis://localhost:6379/0")
        
        assert result.ok is False
        assert result.reason == "redis_loading"

    @patch("redis.Redis.from_url")
    def test_check_tb_health_connection_error(self, mock_from_url):
        # Setup mock to raise ConnectionError
        mock_from_url.side_effect = redis.exceptions.ConnectionError("Connection refused")

        # Call the function and assert it handles the error gracefully
        result = check_tb_health(redis_url="redis://localhost:6379/0")
        
        assert result.ok is False
        assert result.reason == "redis_connection_error"
