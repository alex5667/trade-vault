
import json
import sys

# Add project root to sys.path
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.trade_metrics_service import TradeMetricsService


def test_ml_accumulation():
    tm = TradeMetricsService()
    m = tm.new_metrics()

    # Mock signal payload with ML data
    signal_payload = {
        "indicators": {
            "of_confirm": {
                "scenario": "reversal",
                "have": 2,
                "need": 1,
                "evidence": {
                    "ml": {
                        "allow": True,
                        "p_edge": 0.65
                    }
                }
            }
        }
    }

    # Mock trade 1: ML ALLOW, WIN
    trade1 = {
        "pnl_net": 100.0,
        "signal_payload": json.dumps(signal_payload),
        "close_reason": "TP"
    }

    # Mock trade 2: ML VETO, LOSS
    signal_payload_veto = json.loads(json.dumps(signal_payload))
    signal_payload_veto["indicators"]["of_confirm"]["evidence"]["ml"]["allow"] = False

    trade2 = {
        "pnl_net": -50.0,
        "signal_payload": json.dumps(signal_payload_veto),
        "close_reason": "SL"
    }

    print("Accumulating trade 1 (ML ALLOW, WIN)...")
    tm.accumulate_trade(m, trade1)

    print("Accumulating trade 2 (ML VETO, LOSS)...")
    tm.accumulate_trade(m, trade2)

    print("\nFinal ML Stats:")
    print(json.dumps(m["ml_stats"], indent=4))

    # Assertions
    assert m["ml_stats"]["pass"]["count"] == 1
    assert m["ml_stats"]["pass"]["wins"] == 1
    assert m["ml_stats"]["pass"]["pnl"] == 100.0

    assert m["ml_stats"]["veto"]["count"] == 1
    assert m["ml_stats"]["veto"]["wins"] == 0
    assert m["ml_stats"]["veto"]["pnl"] == -50.0

    print("\n✅ Verification SUCCESS!")

if __name__ == "__main__":
    try:
        test_ml_accumulation()
    except Exception as e:
        print(f"\n❌ Verification FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
