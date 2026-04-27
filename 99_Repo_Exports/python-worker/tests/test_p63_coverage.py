from utils.time_utils import get_ny_time_millis

import os
import time
import json
import pytest
import fakeredis
from unittest.mock import MagicMock, patch
from tools.decision_coverage_kpi_worker_v1 import DecisionCoverageWorker
from tools.archive_decisions_final_v1 import DecisionsArchiver


@pytest.fixture
def mock_redis():
    r = MagicMock()
    return r

class TestDecisionCoverageWorker:
    def test_logic_flow(self, mock_redis):
        # Setup
        with patch('tools.decision_coverage_kpi_worker_v1.redis.Redis.from_url', return_value=mock_redis):
            worker = DecisionCoverageWorker()
            
            # Seed data via MagicMock return
            # xrange returns list of (id, fields)
            mock_redis.xrange.return_value = [
                ("1-0", {"payload": json.dumps({
                    "decision": "allow", 
                    "dq_state": "ok", 
                    "drift_state": "ok", 
                    "ts": get_ny_time_millis()
                })}),
                ("2-0", {"payload": json.dumps({
                    "decision": "veto", 
                    "dq_state": "warn", 
                    "drift_state": "ok", 
                    "ts": get_ny_time_millis()
                })}),
                ("3-0", {"payload": json.dumps({
                    "decision": "veto", 
                    "dq_state": "block", 
                    "drift_state": "ok", 
                    "ts": get_ny_time_millis()
                })})
            ]
            
            # Pipeline
            pipeline_mock = MagicMock()
            mock_redis.pipeline.return_value = pipeline_mock

            # Run
            worker.run_once()
            
            # hset called on pipeline
            call_args = pipeline_mock.hset.call_args_list
            found = False
            for args, kwargs in call_args:
                if args and args[0] == worker.out_key:
                    found = True
                    metrics = kwargs.get("mapping", {})
                    if not metrics and len(args) > 1:
                         metrics = args[1] 
                    
                    if "mapping" in kwargs:
                        metrics = kwargs["mapping"]
                    
                    assert int(metrics["decision_n_24h"]) == 3
                    assert float(metrics["decision_allow_rate_24h"]) == 0.3333
                    assert float(metrics["decision_veto_rate_24h"]) == 0.6667
                    
                    regimes = json.loads(metrics["decision_regimes_24h_json"])
                    assert regimes["ok"]["allow"] == 1
                    assert regimes["warn"]["veto"] == 1
                    assert regimes["block"]["veto"] == 1
                    
                    # P65: Check flat metrics
                    assert int(metrics["decision_regime_n_24h_ok"]) == 1
                    assert int(metrics["decision_regime_n_24h_warn"]) == 1
                    assert int(metrics["decision_regime_n_24h_block"]) == 1
                    
                    assert float(metrics["decision_regime_share_24h_ok"]) == 0.3333
                    assert float(metrics["decision_regime_share_24h_warn"]) == 0.3333
                    assert float(metrics["decision_regime_share_24h_block"]) == 0.3333
            
            assert found
            pipeline_mock.execute.assert_called_once()

class TestDecisionsArchiver:
    def test_archive_logic(self, mock_redis, tmp_path):
        with patch('tools.archive_decisions_final_v1.redis.Redis.from_url', return_value=mock_redis):
            with patch.dict(os.environ, {"DECISIONS_FINAL_ARCHIVE_DIR": str(tmp_path)}):
                archiver = DecisionsArchiver()
                archiver.archive_dir = str(tmp_path)
                
                # Mock xread
                # xread returns [[stream, [(id, fields), ...]]]
                ts_now = get_ny_time_millis()
                mock_redis.xread.side_effect = [
                    [[archiver.stream_key, [("1-0", {"payload": json.dumps({"decision": "allow", "ts": ts_now})})]]],
                    [] # Second call returns empty to stop loop
                ]
                mock_redis.get.return_value = "0-0"
                
                # Run
                archiver.run_once()
                
                # Verify file
                import datetime
                dt = datetime.datetime.fromtimestamp(ts_now/1000.0, tz=datetime.timezone.utc)
                date_str = dt.strftime("%Y-%m-%d")
                expected_file = tmp_path / f"{date_str}.ndjson"
                
                assert expected_file.exists()
                lines = expected_file.read_text().strip().split('\n')
                assert len(lines) == 1
                data = json.loads(lines[0])
                assert data["decision"] == "allow"
                
                # Check state set
                mock_redis.set.assert_called_with(archiver.state_key, "1-0")
