import json
import os
from calibration.adaptive_ttl import to_redis_payload as ttl_payload
from calibration.ensemble_weights import to_redis_payload as ew_payload

os.makedirs('/home/alex/front/trade/scanner_infra/python-worker/tests/fixtures', exist_ok=True)

# Adaptive TTL
ttl_recs = [
    {
        "symbol": "BTCUSDT",
        "regime": "bull",
        "direction": "LONG",
        "n": 150,
        "win_rate": 0.65,
        "mfe_med": 0.02,
        "mae_med": -0.01,
        "mfe_mad": 0.005,
        "mae_mad": 0.002,
        "tp_r": 1.5,
        "sl_r": 1.0,
        "ev": 0.005,
    }
]
ttl_data = ttl_payload(ttl_recs, generated_at_ms=1700000000000)
with open('/home/alex/front/trade/scanner_infra/python-worker/tests/fixtures/adaptive_ttl_golden.json', 'w') as f:
    json.dump(ttl_data, f, indent=2)

# Ensemble Weights
ew_recs = {
    "BTCUSDT": {
        "sourceA": 0.6,
        "sourceB": 0.4
    }
}
ew_data = ew_payload(ew_recs)
with open('/home/alex/front/trade/scanner_infra/python-worker/tests/fixtures/ensemble_weights_golden.json', 'w') as f:
    json.dump(ew_data, f, indent=2)

print("Golden fixtures generated.")
