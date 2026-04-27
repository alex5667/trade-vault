import json
import os
import sys

# Add python-worker to sys.path
worker_path = os.path.join(os.getcwd(), 'python-worker')
sys.path.insert(0, worker_path)

import logging
logging.basicConfig(level=logging.DEBUG)

try:
    from services.outbox.envelope_builder import build_outbox_envelope
    print("✅ Import successful")
except Exception as e:
    import traceback
    print(f"❌ Import failed: {e}")
    traceback.print_exc()
    sys.exit(1)

enriched_signal = {
    "symbol": "BTCUSDT",
    "direction": "LONG",
    "entry": 40000.0,
    "indicators": {"foo": "bar"}
}

telegram_payload = {"text": "test"}
audit_payload_inner = {"sid": "test-sid", "foo": "bar"}

env = build_outbox_envelope(
    sid="test-sid",
    symbol="BTCUSDT",
    kind="crypto_orderflow",
    notify_payload=telegram_payload,
    audit_payload={"payload": json.dumps(enriched_signal)},
    signal_stream_payload={"data": json.dumps(audit_payload_inner)},
    audit_stream="raw_stream",
    signal_stream="sig_stream"
)

print("ENVELOPE TARGETS:", json.dumps(env['targets'], indent=2))
