#!/usr/bin/env python3

import json
import time
import sys
sys.path.insert(0, '/home/alex/front/trade/scanner_infra/python-worker')
from services.outbox.envelope_builder import build_outbox_envelope, dumps_env

# Воспроизведем код из signal_pipeline.py

symbol = "BTCUSDT"
sid = "test-sid-123"
enriched_signal = {
    "signal_id": sid,
    "direction": "LONG",
    "entry": 50000.0,
    "indicators": {"delta_z": 2.5},
    "lot": 0.001,
    "position_size_usd": 50.0,
    "deposit": 1000.0,
    "leverage": 1.0
}

# Из signal_pipeline.py строка 575
audit_payload = {"payload": json.dumps(enriched_signal, ensure_ascii=False)}

# Из signal_pipeline.py строка 531-541
telegram_payload = {
    "text": "Test signal",
    "symbol": symbol,
    "direction": "LONG",
    "entry": "50000.00"
}

# Из signal_pipeline.py строка 546-568
audit_payload_for_stream = {
    "sid": "test-sid-123",
    "signal_id": "test-sid-123",
    "symbol": symbol,
    "side": "LONG",
    "entry": 50000.0,
    "sl": 49500.0,
    "tp_levels": [51000.0],
    "lot": 0.001,
    "source": "CryptoOrderFlow",
    "reason": "delta_spike",
    "confidence": 0.8,
    "confidence01": 0.8,
    "confidence_pct": 80.0,
    "atr": 500.0,
    "ts": 1768968845148,
    "ts_ms": 1768968845148,
    "trail_after_tp1": False,
    "trail_profile": "rocket_v1",
    "indicators": {"delta_z": 2.5},
    "strategy": "cryptoorderflow",
    "tf": "tick",
}

signal_stream = f"signals:cryptoorderflow:{symbol}"
audit_stream = "signals:crypto:raw"

print("=== Input data ===")
print(f"audit_payload: {audit_payload}")
print()

# Создаем envelope как в signal_pipeline.py
env = build_outbox_envelope(
    sid=sid,
    symbol=symbol,
    kind="crypto_orderflow",
    notify_payload=telegram_payload,
    audit_payload=audit_payload,
    signal_stream_payload={"data": json.dumps(audit_payload_for_stream, ensure_ascii=False)},
    audit_stream=audit_stream,
    signal_stream=signal_stream,
)

print("=== Built envelope ===")
print(json.dumps(env, indent=2, ensure_ascii=False))
print()

# Сериализуем как в signal_pipeline.py
env_json = dumps_env(env)
print("=== Serialized envelope ===")
print(env_json)
print()

# Разбираем обратно как в signal_dispatcher.py
parsed_env = json.loads(env_json)
print("=== Parsed envelope ===")
print(json.dumps(parsed_env, indent=2, ensure_ascii=False))
print()

print("=== Checking for audit_payload and meta on top level ===")
if "audit_payload" in parsed_env:
    print("❌ FOUND audit_payload on top level!")
    print(f"audit_payload: {parsed_env['audit_payload']}")
else:
    print("✅ audit_payload not on top level")

if "meta" in parsed_env:
    print("✅ meta found on top level (expected)")
    print(f"meta keys: {list(parsed_env['meta'].keys())}")
else:
    print("❌ meta not found on top level")

if "targets" in parsed_env:
    print("✅ targets found on top level (expected)")
    print(f"targets keys: {list(parsed_env['targets'].keys())}")
    if "audit_payload" in parsed_env["targets"]:
        print("✅ audit_payload found in targets (expected)")
        print(f"targets.audit_payload: {parsed_env['targets']['audit_payload']}")
    else:
        print("❌ audit_payload not found in targets")
