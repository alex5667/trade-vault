#!/usr/bin/env python3
"""
Test script to verify that database save operations work correctly
"""

import os
import sys
sys.path.insert(0, 'python-worker')

# Mock the environment variables - use the container host
os.environ['TRADES_DB_DSN'] = 'postgresql://postgres:12345@postgres:5432/scanner_analytics'

from services.analytics_db import save_trade_closed

# Create a mock TradeClosed object
class MockTradeClosed:
    def __init__(self):
        self.order_id = 'test-order-123'
        self.sid = 'test-sid'
        self.strategy = 'test-strategy'
        self.source = 'CryptoOrderFlow'
        self.symbol = 'BTCUSDT'
        self.tf = 'tick'
        self.direction = 'LONG'
        self.entry_ts_ms = 1700000000000
        self.exit_ts_ms = 1700000001000
        self.entry_price = 50000.0
        self.exit_price = 50100.0
        self.lot = 0.001
        self.notional_usd = 50.0
        self.pnl_net = 0.1
        self.pnl_gross = 0.11
        self.fees = 0.01
        self.pnl_pct = 0.002
        # Baseline fields
        self.pnl_if_fixed_exit = None
        self.baseline_exit_reason = ''
        self.baseline_exit_ts_ms = 0
        self.baseline_exit_price = 0.0
        # TP fields
        self.tp1_hit = True
        self.tp2_hit = False
        self.tp3_hit = False
        self.tp_hits = 1
        self.tp_before_sl = 1
        # Trailing fields
        self.trailing_started = False
        self.trailing_active = False
        self.trailing_moves = 0
        self.trailing_profile = ''
        # Excursions
        self.mfe_pnl = 0.2
        self.mae_pnl = -0.05
        self.giveback = 0.05
        self.missed_profit = 0.0
        # Risk
        self.one_r_money = 0.05
        self.r_multiple = 2.0
        self.duration_ms = 1000
        self.close_reason = 'TP1'
        self.close_reason_raw = 'TP1'
        self.close_reason_detail = ''
        self.entry_tag = 'test-entry'
        self.max_favorable_price = 50150.0
        self.max_favorable_ts = 1700000000500
        self.status = 'CLOSED'
        self.is_final_close = True
        self.remaining_qty = 0.0
        # Health metrics
        self.health_l2_stale_ratio_tick = None
        self.health_l2_stale_ratio_now = None
        self.health_avg_l2_age_ms = None
        self.health_avg_l2_age_tick_ms = None
        self.health_signal_emit_rate = None
        self.health_dlq_rate = None

try:
    print("Testing database save operation...")
    trade = MockTradeClosed()
    save_trade_closed(trade)
    print("✅ Successfully saved trade to database!")
except Exception as e:
    print(f"❌ Failed to save trade: {e}")
    import traceback
    traceback.print_exc()
