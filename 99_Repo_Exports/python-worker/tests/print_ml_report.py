import json

from services.trade_metrics_service import TradeMetricsService


def main():
    tm = TradeMetricsService()
    m = tm.new_metrics()

    t1 = {
        "id": "t1", "pnl_net": "55.5", "pnl_gross": "56.0", "mfe_pnl": "60.0", "close_reason": "TP1",
        "signal_payload": json.dumps({
            "version": 1,
            "rule": {"ok": 1, "score": 0.85, "scenario": "trend_pullback", "have": 2, "need": 2},
            "ml": {"state": "allow", "p_edge": 0.62}
        })
    }

    t2 = {
        "id": "t2", "pnl_net": "-10.0", "pnl_gross": "-10.0", "mfe_pnl": "5.0", "close_reason": "SL",
        "signal_payload": json.dumps({
            "indicators": {
                 "of_confirm": {"scenario": "reversal", "have": 1, "need": 2}
            }
        })
    }

    tm.accumulate_trade(m, t1)
    tm.accumulate_trade(m, t2)
    tm.finalize(m)

    # Simplified formatter matching PeriodicReporter's ML block
    lines = []
    lines.append("🤖 ML Performance (1 passed, 1 vetoed, 0 err):")
    lines.append("PASS: 1 trades | WR: 100.0% | PnL: +55.50$")
    lines.append("VETO: 0 trades | WR: 0.0% | PnL: +0.00$")
    lines.append("")
    lines.append("🧠 ML Condition Analysis:")

    mc = m.get("ml_condition_stats", {})
    if mc.get("total_evaluated", 0) > 0:
        lines.append(f"Tested: {mc['total_evaluated']} trades")

        # Thresholds
        lines.append("By Threshold (if edge >= X):")
        for thr in ["0.50", "0.55", "0.60"]:
            stats = mc["by_threshold"].get(thr, {})
            c = stats.get('count', 0)
            if c > 0:
                p = stats.get('pnl', 0.0)
                wr = (stats.get('wins', 0) / c) * 100 if c > 0 else 0
                lines.append(f"  >= {thr}: {c} trades, WR: {wr:.1f}%, PnL: {p:+.2f}$")

        # Scenarios
        lines.append("By Scenario:")
        for scn, stats in mc["by_scenario"].items():
            if scn == "none": continue
            c = stats.get('count', 0)
            if c > 0:
                avg_e = stats.get('sum_p_edge', 0) / c
                lines.append(f"  {scn[:15]}: {c}t | Avg Edge: {avg_e:.2f}")

    print("\n" + "\n".join(lines) + "\n")

main()
