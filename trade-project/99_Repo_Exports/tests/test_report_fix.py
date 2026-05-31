
import sys
import os
from typing import Dict, Any

# Mock setup
sys.path.insert(0, '/home/alex/front/trade/scanner_infra/python-worker')
from services.trade_metrics_service import TradeMetricsService
from domain.normalizers import bucket_close_reason
from analytics.tag_stats import TagStats, Trade

def test_metrics_consistency():
    tm = TradeMetricsService()
    m = tm.new_metrics()
    
    # Mock data with TRAIL_SL
    trades = [
        {
            "order_id": "1",
            "pnl_net": 10.0,
            "close_reason_raw": "TRAIL_SL",
            "trailing_started": "True",
            "one_r_money": 100.0
        },
        {
            "order_id": "2",
            "pnl_net": -5.0,
            "close_reason_raw": "INITIAL_SL",
            "trailing_started": "False",
            "one_r_money": 100.0
        },
        {
            "order_id": "3",
            "pnl_net": 20.0,
            "close_reason_raw": "TP1",
            "trailing_started": "True",
            "one_r_money": 100.0
        }
    ]
    
    # Emulate PeriodicReporter._accumulate_trade_metrics logic
    wins_strict = 0
    losses_strict = 0
    
    for t in trades:
        raw_reason = t.get("close_reason_raw")
        bucket = bucket_close_reason(raw_reason)
        pnl = t.get("pnl_net")
        eps = 1e-9
        
        if bucket in ("TP_LIMIT", "TP"):
            wins_strict += 1
        elif bucket == "TRAIL_SL":
            if pnl > eps:
                wins_strict += 1
            else:
                losses_strict += 1
        elif bucket == "INITIAL_SL":
            losses_strict += 1
            
        t["close_reason"] = bucket
        tm.accumulate_trade(m, t)
    
    print(f"Wins Strict: {wins_strict}")
    print(f"TM Wins Strict: {m['wins_strict']}") 
    print(f"Trailing Stop Hits: {m['trailing_stop_hits']}")
    print(f"Reasons: {m['reasons']}")
    print(f"Wins by Reason: {m['wins_by_reason']}")
    print(f"Losses by Reason: {m['losses_by_reason']}")
    
    assert m['wins_by_reason'].get('TP_LIMIT') == 1
    assert m['wins_by_reason'].get('TRAIL_SL') == 1
    assert m['losses_by_reason'].get('INITIAL_SL') == 1
    
    # Check TagStats
    stats = TagStats(tag="test")
    for t in trades:
        trade_obj = Trade(
            source="test", symbol="test", exit_ts_ms=0,
            pnl_net=t["pnl_net"], pnl_if_fixed_exit=0.0, one_r_money=t.get("one_r_money", 1.0),
            giveback=0.0, missed_profit=0.0, mfe_pnl=0.0, mae_pnl=0.0,
            trailing_started=t.get("trailing_started") == "True",
            trailing_active=False,
            close_reason=t["close_reason"], # normalized
            close_reason_raw=t["close_reason_raw"],
            close_reason_detail="",
            entry_tag=""
        )
        stats.add_trade(trade_obj)
    
    res = stats.finalize()
    print(f"TagStats WR Managed: {res['wr_managed']:.1%}")
    print(f"TagStats WR (alias): {res['wr']:.1%}")
    print(f"TagStats Trailing Close Share: {res['trailing_close_share']:.1%}")
    
    assert m['trailing_stop_hits'] == 1 # only TRAIL_SL is a trailing stop hit (others are TP1)
    assert res['wr'] == res['wr_managed']
    assert res['trailing_close_share'] > 0

if __name__ == "__main__":
    test_metrics_consistency()
