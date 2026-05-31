import redis
import json
import os
import sys

# Mocking enough to run OFConfirmEngine
sys.path.append('python-worker')
from core.of_confirm_engine import OFConfirmEngine

class MockRuntime:
    def __init__(self):
        self.symbol = "ETHUSDT"
        self.config = {"exec_risk_ref_bps": 10.0}
        self.pressure_sps = 0.0
        self.last_regime = "normal"

engine = OFConfirmEngine()
indicators = {"spread_bps": 1.5, "expected_slippage_bps": 4.0}
runtime = MockRuntime()

ofc, dec = engine.build(
    symbol="ETHUSDT",
    tf="1m",
    direction="LONG",
    tick_ts_ms=1000,
    price=3000.0,
    delta_z=3.5,
    runtime=runtime,
    cfg={},
    indicators=indicators
)

print(json.dumps(ofc.evidence, indent=2))
