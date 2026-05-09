
import os
from dataclasses import dataclass

# [AUTOGRAVITY CLEANUP] sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")
from handlers.crypto_orderflow.core.cost_edge_gate import CostEdgeGate


@dataclass
class MockContext:
    tp1: float = None
    rr: float = None
    atr: float = None
    spread_bps: float = None
    side: str = "LONG"

def simulate():
    print("="*20 + " EDGE GATE SIMULATION REPORT " + "="*20)

    # 1. Setup ENV mimic (Option A + Buffer)
    env = {
        "EDGE_COST_GATE_ENABLED": "1",
        "EDGE_COST_K": "4.0",
        "EDGE_COST_K_BTCUSDT": "3.0",
        "EDGE_COST_K_1000PEPEUSDT": "5.0", # Note: Uppercase override
        "EDGE_COST_K_weird_case": "2.0",   # lowercase override definition (should be normed)
        "EDGE_COST_BUFFER_BPS": "0.0",
        "EDGE_COST_BUFFER_BPS_TESTBUF": "2.5",
        "EDGE_FEES_BPS_DEFAULT": "4.0",
        "EDGE_SLIPPAGE_BPS_DEFAULT": "4.0"
    }

    # Apply env
    for k,v in env.items():
        os.environ[k] = v

    gate = CostEdgeGate.from_env()

    scenarios = [
        # Sym, Entry, TP1 (Edge), Note
        ("BTCUSDT", 100000, 100050, "Major (K=3.0), Edge 5bps (fail)"),
        ("BTCUSDT", 100000, 100400, "Major (K=3.0), Edge 40bps (pass)"),

        ("1000PEPEUSDT", 0.01, 0.010035, "Meme (K=5.0), Edge 35bps (req 40, fail)"),
        ("1000PEPEUSDT", 0.01, 0.010045, "Meme (K=5.0), Edge 45bps (pass)"),

        ("btcusdt", 100000, 100400, "Lower case input symbol (should use K=3.0)"),

        ("TESTBUF", 100, 100.41, "Buffer=2.5. Costs=4+4+2.5=10.5. K=4. Req=42. Edge=41 (fail)"),
        ("TESTBUF", 100, 100.43, "Buffer=2.5. Edge=43 (pass)"),

        ("weird_case", 100, 100.20, "Env Key 'weird_case'. Sym input 'WEIRD_CASE'. Check norm."),
    ]

    summary = {"PASS": 0, "VETO": 0}

    for sym, entry, tp1, note in scenarios:
        ctx = MockContext(tp1=tp1)
        res = gate.evaluate(ctx, sym, entry)

        status = "PASS" if res.passed else "VETO"
        summary[status] += 1

        print(f"\nScenario: {sym} | {note}")
        print(f"Log: {str(res)}")

    print("\n" + "="*20 + " ENV DUMP (Mocked) " + "="*20)
    for k, v in env.items():
        print(f"{k}={v}")

    print("\n" + "="*20 + " STATS " + "="*20)
    print(f"Total: {len(scenarios)}")
    print(f"PASS: {summary['PASS']}")
    print(f"VETO: {summary['VETO']}")

if __name__ == "__main__":
    simulate()
