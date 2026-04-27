from utils.time_utils import get_ny_time_millis
import json
import time
import os

def generate_golden_fixture():
    # Мок-данные, имитирующие реальный выход из python-worker'a
    ts_event_ms = get_ny_time_millis()
    
    golden_signal = {
        "sid": "crypto-of:BTCUSDT:123456789",
        "signal_id": "crypto-of:BTCUSDT:123456789",
        "symbol": "BTCUSDT",
        "venue": "binance",
        "session": "NY",
        "tf": "1m",
        "direction": "LONG",
        "side": "LONG",
        "entry": 65000.50,
        "sl": 64800.00,
        "tp_levels": [65500.0, 66000.0, 66500.0],
        "lot": 0.05,
        "position_size_usd": 3250.0,
        "deposit": 10000.0,
        "leverage": 10.0,
        "atr": 45.0,
        "confidence": 0.85,
        "confidence_raw": 0.82,
        "confidence_final": 0.85,
        "ts_emit_ms": ts_event_ms,
        "tick_ts": ts_event_ms,
        "decision_mid_at_emit": 65000.00,
        "expected_slippage_bps_at_emit": 2.5,
        "decision_expected_slippage_bps": 2.5,
        "source": "OrderFlowStrategy",
        "producer": "python-worker",
        "schema_version": 1,
        
        # Индикаторы и evidence map (как будет передано на бекэнд)
        "evidence_map": {
            "book_health_ok": 1.0,
            "liq_geom_monitor_hit": 1.0,
            "book_stale_ms": 150.0,
            "spread_bps": 3.2,
            "rsi_agree": 1.0,
            "div_strength": 0.75,
            "market_mode": 1.0
        },
        "data_quality_flags": ["wide_spread", "stale_l2"],
        "liquidity_regime": "tight",
        
        "trail_profile": "rocket_v1",
        "trail_after_tp1": True
    }
    
    os.makedirs("python-worker/tests/fixtures", exist_ok=True)
    with open("python-worker/tests/fixtures/golden_signal_crypto_raw_v1.json", "w", encoding="utf-8") as f:
        json.dump(golden_signal, f, indent=2, ensure_ascii=False)
        
    print("Golden fixture saved to python-worker/tests/fixtures/golden_signal_crypto_raw_v1.json")

if __name__ == "__main__":
    generate_golden_fixture()
