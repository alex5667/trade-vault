#!/usr/bin/env python3
"""
Analytics API Service - REST API for scanner_analytics database.

Provides endpoints for trade analytics, baseline vs managed comparisons,
and entry tag performance metrics.

Features:
    - GET /metrics/trades - Recent trades with filtering
    - GET /metrics/daily - Daily aggregated metrics
    - GET /metrics/entry-tags - Entry tag performance
    - GET /healthz - Health check

Usage:
    python3 -m services.analytics_api_service
    # Or:
    uvicorn services.analytics_api_service:app --host 127.0.0.1 --port 8091
"""

import os
from typing import List, Optional
from datetime import datetime, date

from fastapi import FastAPI, HTTPException, Query
from dataclasses import dataclass

from services import analytics_db


# ═══════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════

DEFAULT_PORT = int(os.getenv("ANALYTICS_API_PORT", "8091"))
DEFAULT_HOST = os.getenv("ANALYTICS_API_HOST", "127.0.0.1")

app = FastAPI(
    title="Scanner Analytics API",
    description="REST API for trade analytics from scanner_analytics database",
    version="1.0.0"
)


# ═══════════════════════════════════════════════════════════════
# Pydantic Models
# ═══════════════════════════════════════════════════════════════

@dataclass(slots=True)
class TradeResponse:
    order_id: str
    symbol: str
    source: str
    exit_ts_ms: int
    pnl_net: float
    pnl_if_fixed_exit: float
    entry_tag: str
    close_reason: str

@dataclass(slots=True)
class DailyMetricsResponse:
    date: date
    source: str
    symbol: str
    trades_count: int
    pnl_net_sum: float
    wr_managed: float
    wr_baseline: float
    expectancy_r: float
    delta_expectancy_r: float

@dataclass(slots=True)
class EntryTagMetricsResponse:
    date: date
    source: str
    symbol: str
    entry_tag: str
    trades_count: int
    pnl_net_sum: float
    wr_managed: float
    expectancy_r: float
    delta_expectancy_r: float
    giveback_avg_r: float
    missed_avg_r: float


# ═══════════════════════════════════════════════════════════════
# Endpoints
# ═══════════════════════════════════════════════════════════════

@app.get("/healthz")
async def health_check():
    """Health check endpoint."""
    try:
        # Simple query to test database connection
        analytics_db.get_conn().close()
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unhealthy: {e}")


@app.get("/metrics/trades", response_model=List[TradeResponse])
async def get_trades(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    source: Optional[str] = Query(None, description="Filter by source"),
    limit: int = Query(100, description="Max number of trades to return", ge=1, le=1000)
):
    """
    Get recent closed trades from analytics database.

    Filters:
    - symbol: Filter by trading symbol (e.g., ETHUSDT)
    - source: Filter by signal source (e.g., CryptoOrderFlow)
    - limit: Maximum number of trades to return (1-1000)
    """
    try:
        rows = analytics_db.fetch_trades_closed(
            symbol=symbol,
            source=source,
            limit=limit
        )

        # Convert to response format
        trades = []
        for row in rows:
            trades.append(TradeResponse(
                order_id=row['order_id'],
                symbol=row['symbol'],
                source=row['source'] or '',
                exit_ts_ms=row['exit_ts_ms'],
                pnl_net=row['pnl_net'],
                pnl_if_fixed_exit=row['pnl_if_fixed_exit'],
                entry_tag=row['entry_tag'] or '',
                close_reason=row['close_reason'] or ''
            ))

        return trades

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch trades: {e}")


@app.get("/metrics/daily", response_model=List[DailyMetricsResponse])
async def get_daily_metrics(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    source: Optional[str] = Query(None, description="Filter by source"),
    limit: int = Query(30, description="Max number of days to return", ge=1, le=365)
):
    """
    Get daily aggregated metrics from analytics database.

    Filters:
    - symbol: Filter by trading symbol
    - source: Filter by signal source
    - limit: Maximum number of days to return (1-365)
    """
    try:
        rows = analytics_db.fetch_daily_metrics(
            symbol=symbol,
            source=source,
            limit=limit
        )

        # Convert to response format
        metrics = []
        for row in rows:
            metrics.append(DailyMetricsResponse(
                date=row['date'],
                source=row['source'] or '',
                symbol=row['symbol'],
                trades_count=row['trades_count'],
                pnl_net_sum=row['pnl_net_sum'],
                wr_managed=row['wr_managed'],
                wr_baseline=row['wr_baseline'],
                expectancy_r=row['expectancy_r'],
                delta_expectancy_r=row['delta_expectancy_r']
            ))

        return metrics

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch daily metrics: {e}")


@app.get("/metrics/entry-tags", response_model=List[EntryTagMetricsResponse])
async def get_entry_tag_metrics(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    source: Optional[str] = Query(None, description="Filter by source"),
    entry_tag: Optional[str] = Query(None, description="Filter by entry tag"),
    limit: int = Query(30, description="Max number of entries to return", ge=1, le=365)
):
    """
    Get entry tag performance metrics from analytics database.

    Filters:
    - symbol: Filter by trading symbol
    - source: Filter by signal source
    - entry_tag: Filter by specific entry tag
    - limit: Maximum number of entries to return (1-365)
    """
    try:
        rows = analytics_db.fetch_entry_tag_metrics(
            symbol=symbol,
            source=source,
            entry_tag=entry_tag,
            limit=limit
        )

        # Convert to response format
        metrics = []
        for row in rows:
            metrics.append(EntryTagMetricsResponse(
                date=row['date'],
                source=row['source'] or '',
                symbol=row['symbol'],
                entry_tag=row['entry_tag'],
                trades_count=row['trades_count'],
                pnl_net_sum=row['pnl_net_sum'],
                wr_managed=row['wr_managed'],
                expectancy_r=row['expectancy_r'],
                delta_expectancy_r=row['delta_expectancy_r'],
                giveback_avg_r=row['giveback_avg_r'],
                missed_avg_r=row['missed_avg_r']
            ))

        return metrics

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch entry tag metrics: {e}")


@app.get("/metrics/summary")
async def get_summary_metrics(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    source: Optional[str] = Query(None, description="Filter by source")
):
    """
    Get summary metrics across all trades.

    This endpoint provides high-level overview of trading performance.
    """
    try:
        # Get recent trades for summary calculation
        trades = analytics_db.fetch_trades_closed(
            symbol=symbol,
            source=source,
            limit=1000  # Use last 1000 trades for summary
        )

        if not trades:
            return {
                "total_trades": 0,
                "total_pnl": 0.0,
                "win_rate": 0.0,
                "avg_trade_pnl": 0.0,
                "baseline_vs_managed_delta": 0.0
            }

        total_trades = len(trades)
        total_pnl = sum(row['pnl_net'] for row in trades)
        wins = sum(1 for row in trades if row['pnl_net'] > 0)
        win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0.0
        avg_trade_pnl = total_pnl / total_trades

        # Calculate baseline vs managed delta
        managed_pnl = total_pnl
        baseline_pnl = sum(row['pnl_if_fixed_exit'] for row in trades)
        baseline_vs_managed_delta = managed_pnl - baseline_pnl

        return {
            "total_trades": total_trades,
            "total_pnl": round(total_pnl, 2),
            "win_rate": round(win_rate, 2),
            "avg_trade_pnl": round(avg_trade_pnl, 4),
            "baseline_vs_managed_delta": round(baseline_vs_managed_delta, 2)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to calculate summary: {e}")


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Analytics API Service...")
    print(f"📡 Listening on http://{DEFAULT_HOST}:{DEFAULT_PORT}")
    print(f"📊 Analytics endpoints:")
    print(f"  GET /healthz - Health check")
    print(f"  GET /metrics/trades - Recent trades")
    print(f"  GET /metrics/daily - Daily metrics")
    print(f"  GET /metrics/entry-tags - Entry tag performance")
    print(f"  GET /metrics/summary - Summary overview")

    uvicorn.run(
        "services.analytics_api_service:app",
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        reload=False
    )
