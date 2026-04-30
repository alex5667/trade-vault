import pytest
from unittest.mock import patch, MagicMock

# Import the module so we can check gauge values
from services.orderflow.tools import signal_quality_exporter_v3

def test_signal_quality_exporter_v3_p71():
    mock_redis = MagicMock()
    mock_cli = MagicMock()
    mock_redis.from_url.return_value = mock_cli
    
    # Setup mock data for P71 keys
    mock_cli.hgetall.side_effect = [
        # Call 1: cfg
        {
            "signal_quality_n_24h": "100"
            "signal_quality_last_ts_ms": "1000"
            "policy_effectiveness_last_ts_ms": "2000"
            "policy_effectiveness_input_last_ts_ms": "1500"
            "policy_effectiveness_total_n_24h": "300"
            "policy_effectiveness_baseline_ok_present": "1"
            "policy_effectiveness_share_24h_warn": "0.1"
            "policy_effectiveness_expectancy_r_delta_24h_warn": "-0.5"
        }
        # Call 2: by_mode
        {}
        # Call 3: by_bucket
        {}
    ]
    
    # break the while loop after one iteration by raising KeyboardInterrupt in sleep()
    with patch("services.orderflow.tools.signal_quality_exporter_v3.redis.Redis", mock_redis), \
         patch("services.orderflow.tools.signal_quality_exporter_v3.start_http_server"), \
         patch("services.orderflow.tools.signal_quality_exporter_v3.time.sleep", side_effect=KeyboardInterrupt):
        try:
            signal_quality_exporter_v3.main()
        except KeyboardInterrupt:
            pass
            
    # Verify Gauges were set based on Mock Redis Data
    assert signal_quality_exporter_v3.g_pe_last_ts_ms._value.get() == 2000
    assert signal_quality_exporter_v3.g_pe_total_n_24h._value.get() == 300
    
    # check share
    val_share = signal_quality_exporter_v3.g_pe_share_24h.labels(mode="warn")._value.get()
    assert val_share == 0.1
    
    # check expectancy delta
    val_exp = signal_quality_exporter_v3.g_pe_expectancy_r_delta_24h.labels(mode="warn")._value.get()
    assert val_exp == -0.5

    # check default empty for block
    val_block_share = signal_quality_exporter_v3.g_pe_share_24h.labels(mode="block")._value.get()
    assert val_block_share == 0.0
