from utils.time_utils import get_ny_time_millis
import asyncio
import os
import time
import json
import redis.asyncio as aioredis
from typing import AsyncGenerator, Dict, Any

from domain.models import TradeClosed
from services.trade_metrics_service import TradeMetricsService
from services.periodic_reporter import PeriodicReporter
import types
from services.trade_closed_hydrator import hydrate_trade_closed

class MockPublisher:
    async def publish_telegram(self, html_text: str, severity: str = "info"):
        print("\n\n=== TELEGRAM REPORT OUTPUT ===\n")
        print(html_text)
        print("\n==============================\n")

class MockRedisRepo:
    def __init__(self, trades):
        self.trades = trades
    async def iter_recent_trades_window(self, source: str, symbol: str, window_seconds: int) -> AsyncGenerator[Dict[str, Any], None]:
        for t in self.trades:
            yield t
    async def iter_recent_trades(self, source, symbol, count):
        for t in self.trades[:count]:
            yield t

import pytest

def test_report_generation():
    now = get_ny_time_millis()
    
    # 1. Trade with ML
    sp = {
        "version": 1,
        "sid": "test_sid_1",
        "rule": {
            "ok": 1,
            "score": 0.85,
            "scenario": "trend_pullback",
            "have": 2,
            "need": 2
        },
        "ml": {
            "state": "allow",
            "p_edge": 0.62
        }
    }
    
    t1 = {
        "id": "t1",
        "order_id": "t1",
        "symbol": "BTCUSDT",
        "source": "binance",
        "strategy": "cryptoorderflow",
        "status": "closed",
        "side": "LONG",
        "pnl_net": "55.5",
        "pnl_gross": "56.0",
        "mfe_pnl": "60.0",
        "close_reason": "TP1",
        "exit_ts_ms": str(now),
        "entry_ts_ms": str(now - 60000),
        "signal_payload": json.dumps(sp)
    }
    
    t2 = {
        "id": "t2",
        "order_id": "t2",
        "symbol": "BTCUSDT",
        "source": "binance",
        "strategy": "cryptoorderflow",
        "status": "closed",
        "side": "SHORT",
        "pnl_net": "-10.0",
        "pnl_gross": "-10.0",
        "mfe_pnl": "5.0",
        "close_reason": "SL",
        "exit_ts_ms": str(now - 10000),
        "entry_ts_ms": str(now - 70000),
        "signal_payload": json.dumps({
             "indicators": {
                 "of_confirm": {
                     "scenario": "reversal",
                     "have": 1, "need": 2
                 }
             }
        })
    }
    
    trades = [t1, t2]
    
    redis_repo = MockRedisRepo(trades)
    reporter = PeriodicReporter.__new__(PeriodicReporter)
    reporter.redis = None # mocked
    reporter.tm = TradeMetricsService()
    reporter.repo = redis_repo
    reporter.report_counter = {}
    reporter.trailing_vs_baseline_reports_interval = 1
    reporter.trailing_analysis_reports_interval = 1
    reporter.trailing_analyzer = types.SimpleNamespace()
    reporter.trailing_analyzer.analyze_last_trades = lambda **kwargs: None
    reporter._symbol_trailing_enabled = lambda s, f=None: False
    reporter.reporting = types.SimpleNamespace()
    reporter.reporting.publisher = MockPublisher()
    reporter.reporting.send_telegram_message = lambda msg: True
    
    # Override iter to bypass hydration logic that wants real redis
    def mock_iter(*args, **kwargs):
        return trades
            
    reporter._iter_recent_trades_window = mock_iter
    
    print("Generating report...")
    reporter.send_report_for_pair("binance", "BTCUSDT", 86400)
    print("Done")



