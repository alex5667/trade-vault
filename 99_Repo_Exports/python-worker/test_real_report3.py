import sys
import os
import json

from pathlib import Path

# Add python-worker to PYTHONPATH
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, str(Path("/home/alex/front/trade/scanner_infra/python-worker").resolve()))

# Force environment for the test
os.environ["PERIODIC_REPORT_SEND_VIRTUAL_ONLY"] = "false"
os.environ["PERIODIC_REPORT_SEND_EMPTY"] = "true"
os.environ["REDIS_URL"] = "redis://redis-worker-1:6379/0"

from services.periodic_reporter import get_reporter_instance
from domain.normalizers import canon_source, canon_symbol, strategy_from_source

def run():
    rep = get_reporter_instance()
    
    source = "CryptoOrderFlow"
    symbol = "ALL"
    window_seconds = 3600 * 24 # За последние сутки
    
    src = canon_source(source)
    sym = canon_symbol(symbol)
    strategy = strategy_from_source(src)
    
    print("Loading recent trades...")
    trades = rep._iter_recent_trades_window(
        strategy=strategy,
        symbol=sym,
        tf="tick",
        source=src,
        window_seconds=window_seconds,
    )
    
    print(f"Loaded {len(trades) if trades else 0} trades.")
    if not trades:
        print("No trades found. Sending empty report.")
        rep._send_report(src, sym, rep.tm.new_metrics(), window_seconds)
        return
        
    print("Compiling metrics (forcing virtual trades to be calculated as REAL)...")
    m_real = rep.tm.new_metrics()
    
    for t in trades:
        if isinstance(t, dict):
            # dict is mutable, so we can just pass it directly
            t2 = dict(t)
            t2["is_virtual"] = "0"
            t2["of_gate_mode"] = "ENFORCE"
            # we must call accumulate_trade
            rep.tm.accumulate_trade(m_real, t2)
        else:
            # dataclass / object: we need to construct a dict for accumulate_trade
            t2 = {
                "entry_ts_ms": getattr(t, "entry_time", 0) * 1000,
                "exit_ts_ms": getattr(t, "exit_time", 0) * 1000,
                "duration_ms": getattr(t, "duration_sec", 0) * 1000,
                "pnl_net": getattr(t, "pnl_usd", 0.0),
                "fees": getattr(t, "fees", 0.0),
                "bucket_close_reason": getattr(t, "close_reason", ""),
                "one_r_money": getattr(t, "risk_usd", 1.0) or 1.0, # Avoid missing risk error
                "is_virtual": "0",
                "of_gate_mode": "ENFORCE",
                "tp1_hit": getattr(t, "tp1_hit", 0),
                "tp2_hit": getattr(t, "tp2_hit", 0),
                "tp3_hit": getattr(t, "tp3_hit", 0)
            }
            if hasattr(t, "config") and isinstance(t.config, dict):
                t2.update(t.config)
            
            rep.tm.accumulate_trade(m_real, t2)
            
    rep.tm.finalize(m_real)
    
    # Needs shadow buckets attached
    m_real["shadow_passed"] = rep.tm.new_metrics()
    m_real["shadow_all"] = m_real
    m_real["smt_passed"] = rep.tm.new_metrics()
    m_real["shadow_all_gates"] = rep.tm.new_metrics()
    m_real["virtual_all"] = m_real
    m_real["report_virtual_only"] = False
    m_real["is_demo"] = False
    
    # Add health
    rep._add_health_metrics(m_real, src, sym)
    
    print(f"Metrics compiled: {m_real.get('total_trades', 0)} trades. Sending REAL report...")
    
    # Direct send_report
    rep._send_report(src, sym, m_real, window_seconds)
    
    print("REAL test report dispatched to Telegram!")

if __name__ == "__main__":
    run()
