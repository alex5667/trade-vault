import urllib.request
import json
import urllib.parse
import os

PROMETHEUS_URL = "http://127.0.0.1:19090/api/v1/query"

QUERIES = {
    # Feature Drift & Distribution Shift
    "psi_drift":         'psi_max_24h',
    "feature_drift_z":   'feature_drift_max_z_24h',
    "dq_flag_rate":      'dq_flag_rate',
    "decision_n_24h":    'decision_n_24h',

    # Regime & Decision Health
    "decision_lag":      '(time() * 1000 - decision_last_ts_ms)',

    # Signal Quality & Calibration
    "dq_level2":         'avg_over_time((dq_level == bool 2)[1h:15s])',
    "slippage_age":      'of_slippage_calib_last_ok_age_sec',
        
    # Safety Registry
    "ts_missing":        'of_gate_timescale_policies_missing',
}

for name, query in QUERIES.items():
    url = f"{PROMETHEUS_URL}?query={urllib.parse.quote(query)}"
    try:
        req = urllib.request.urlopen(url, timeout=5)
        resp = json.loads(req.read())
        if resp.get('status') == 'success':
            data = resp.get('data', {}).get('result', [])
            if data:
                print(f"{name}: {data[0]['value'][1]}")
            else:
                print(f"{name}: NO DATA")
        else:
            print(f"{name}: ERROR {resp}")
    except Exception as e:
        print(f"{name}: REQUEST FAILED - {e}")

