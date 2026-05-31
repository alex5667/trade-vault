import json
from unittest.mock import MagicMock, patch

import pytest
import redis

from tools.deploy_calibrator_v1 import main


class TestDeployCalibrator:
    # mock redis connection
    @pytest.fixture
    def mock_redis(self):
        with patch("redis.Redis.from_url") as mock:
             r = MagicMock()
             mock.return_value = r
             # hgetall returns empty dict by default
             r.hgetall.return_value = {}
             yield r

    def test_dry_run(self, mock_redis):
        with patch("sys.argv", ["deploy_calibrator_v1.py", "--a", "2.5"]):
            main()
        mock_redis.hset.assert_not_called()

    def test_apply_redis(self, mock_redis):
        with patch("sys.argv", ["deploy_calibrator_v1.py", "--a", "3.0", "--b", "1.0", "--apply", "--key", "cfg:test"]):
            main()

        # Verify hset call
        # args: key, field, value
        mock_redis.hset.assert_any_call("cfg:test", "calibrator", json.dumps({"type": "platt_logit", "a": 3.0, "b": 1.0}))
        mock_redis.hset.assert_any_call("cfg:test", "calibrate_p_edge", "1")

    def test_connection_error(self):
        with patch("redis.Redis.from_url", side_effect=redis.ConnectionError("fail")):
            with patch("sys.argv", ["deploy_calibrator_v1.py", "--apply"]):
                 # Should handle error gracefully
                 ret = main()
                 assert ret == 1
