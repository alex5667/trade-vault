from utils.time_utils import get_ny_time_millis
import json
import time
from services.orderflow.liqmap_features import try_parse_liqmap_snapshot_json, compute_liqmap_features_from_snapshot

def test_parse_json():
    raw = json.dumps({"ts_ms": 123, "levels": []})
    res = try_parse_liqmap_snapshot_json(raw)
    assert res is not None
    assert res["ts_ms"] == 123

def test_compute_features_basic():
    now_ms = get_ny_time_millis()
    # Price is 100.
    # Longs liquidated at 90.
    # Shorts liquidated at 110.
    payload = {
        "ts_ms": now_ms
        "levels": [
            {"price": "90", "long_usd": "1000", "short_usd": "0"}
            {"price": "110", "long_usd": "0", "short_usd": "2000"}
        ]
    }
    feats = compute_liqmap_features_from_snapshot(
        payload=payload
        mid_px=100.0
        now_ms=now_ms + 100
        max_stale_ms=3500
        peak_range_bps=2000.0, # 20%
        front_run_bps=20.0
        sl_buffer_bps=15.0
    )
    
    # stale_ms should be 100
    assert feats["stale_ms"] == 100
    assert feats["is_stale"] == 0
    assert feats["levels_n"] == 2
    
    # 2000 short / 3000 total = 0.666
    assert abs(feats["squeeze_bias"] - (2000/3000)) < 0.01

    # For LONG: TP anchored to short peak minus front_run_bps.
    # Peak at 110. Dist to 100 is 10%. 10% = 1000 bps.
    # Subtract 20 bps = 980 bps.
    assert abs(feats["tp1_anchor_bps_long"] - 980.0) < 0.1

    # For LONG: SL anchored to long peak plus sl_buffer_bps.
    # Peak at 90. Dist to 100 is 10%. 10% = 1000 bps.
    # Add 15 bps = 1015 bps.
    assert abs(feats["sl_reco_bps_long"] - 1015.0) < 0.1

    # For SHORT: TP anchored to long peak minus front_run_bps.
    assert abs(feats["tp1_anchor_bps_short"] - 980.0) < 0.1
    # For SHORT: SL anchored to short peak plus sl_buffer_bps.
    assert abs(feats["sl_reco_bps_short"] - 1015.0) < 0.1

if __name__ == "__main__":
    test_parse_json()
    test_compute_features_basic()
    print("test_liqmap_features_unit_v1.py OK")
