import json
import uuid

from services.trade_monitor import TradeMonitorService


class MockRedis:
    def __init__(self):
        self.data = {}
    def get(self, k): return self.data.get(k)
    def set(self, k, v, **kwargs):
        if kwargs.get('nx') and k in self.data:
            return False
        self.data[k] = v
        return True
    def hset(self, k, mapping=None, **kwargs):
        if k not in self.data: self.data[k] = {}
        if mapping: self.data[k].update(mapping)
        if kwargs: self.data[k].update(kwargs)
    def hgetall(self, k): return self.data.get(k, {})
    def sadd(self, k, v): pass
    def xadd(self, stream, fields, **kwargs): pass
    def delete(self, k): self.data.pop(k, None)
    def exists(self, k): return k in self.data
    def expire(self, k, ttl): pass
    def pipeline(self, transaction=False):
        return MockPipeline(self)

class MockPipeline:
    def __init__(self, r): self.r = r
    def __enter__(self): return self
    def __exit__(self, *a): pass
    def hset(self, k, mapping=None, **kwargs): self.r.hset(k, mapping, **kwargs); return self
    def sadd(self, k, v): self.r.sadd(k, v); return self
    def execute(self): return []

def test_shadow_tracking():
    print("Testing Shadow Tracking...")
    redis = MockRedis()
    monitor = TradeMonitorService(redis_client=redis)
    monitor.shadow_conf_threshold = 70.0

    sid = str(uuid.uuid4())

    # 1. Receive RAW signal (conf 85)
    print("\n1. Processing RAW signal...")
    raw_sig = {
        "sid": sid,
        "symbol": "BTCUSDT",
        "side": "LONG",
        "price": 50000.0,
        "confidence": 0.85,
        "source": "OrderFlow"
    }
    monitor.on_signal(raw_sig)

    pos_id = monitor.pos_by_sid.get(sid)
    assert pos_id is not None
    pos = monitor.open_positions[pos_id]
    print(f"Position created: {pos_id}, is_virtual={pos.is_virtual}")
    assert pos.is_virtual is True
    assert pos.v_gate_status == "na"

    # 2. Receive Audit (FAILED)
    print("\n2. Processing Audit (FAILED)...")
    audit_data = {
        "data": json.dumps({
            "sid": sid,
            "ok": False,
            "reason_code": "LIQUIDITY_VETO",
            "notes": "Spread too high"
        })
    }
    monitor.on_audit(audit_data)
    assert pos.v_gate_status == "failed"
    assert pos.v_gate_reason == "LIQUIDITY_VETO"
    print(f"Gate status updated: {pos.v_gate_status} (reason: {pos.v_gate_reason})")

    # 3. Another signal with low confidence
    print("\n3. Processing Low Confidence signal...")
    sid_low = str(uuid.uuid4())
    raw_sig_low = {
        "sid": sid_low,
        "symbol": "BTCUSDT",
        "side": "LONG",
        "price": 50000.0,
        "confidence": 0.50,
        "source": "OrderFlow"
    }
    monitor.on_signal(raw_sig_low)
    assert monitor.pos_by_sid.get(sid_low) is None
    print("Low confidence signal ignored as expected.")

    # 4. Receive Real Entry for existing virtual
    print("\n4. Upgrading Virtual to REAL...")
    real_entry = {
        "sid": sid,
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "entry": 50000.0,
        "source": "smt_entry_policy"
    }
    monitor.on_signal(real_entry)
    assert pos.is_virtual is False
    assert pos.v_gate_status == "passed"
    print(f"Upgraded to REAL: is_virtual={pos.is_virtual}, status={pos.v_gate_status}")

    print("\n✅ Shadow tracking tests passed!")

if __name__ == "__main__":
    test_shadow_tracking()
