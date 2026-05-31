import asyncio
import time
from services.ml_confirm_gate import MLConfirmGate

async def main():
    gate = MLConfirmGate.from_env()
    print("Initial config:", gate._cfg)
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=123,
        direction="LONG",
        scenario="continuation",
        indicators={},
        rule_score=0.5,
        rule_have=1,
        rule_need=1,
        cancel_spike_veto=0,
        ok_rule=1
    )
    print("Decision status:", dec.status, "Error:", dec.error)
    
if __name__ == "__main__":
    asyncio.run(main())
