#!/usr/bin/env python3
"""
Generate test trades CSV for aggregated_signal_hub_v2 offline replay.

Creates synthetic trade data with realistic patterns:
- Price movements
- Volume patterns
- Buy/sell aggressor distribution
- Bid/ask spreads
- ATR calculation
"""

import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_test_trades(
    output_path: str,
    rows: int = 10000,
    symbol: str = "XAUUSD",
    start_price: float = 2760.0,
    volatility: float = 3.0,
    spread_bps: float = 5.0,
):
    """Generate synthetic trade data for testing."""

    print(f"Generating {rows} test trades...")

    # Start timestamp (now - 1 hour)
    start_ts = int((datetime.now() - timedelta(hours=1)).timestamp() * 1000)

    # Generate timestamps (1 trade per ~360ms on average)
    timestamps = start_ts + np.cumsum(np.random.exponential(360, size=rows)).astype(int)

    # Generate price walk
    returns = np.random.normal(0, volatility / 100, size=rows)
    prices = start_price * np.exp(np.cumsum(returns))

    # Generate volumes (log-normal distribution)
    volumes = np.random.lognormal(mean=-0.5, sigma=0.8, size=rows)
    volumes = np.clip(volumes, 0.01, 5.0)

    # Generate sides (60% buy / 40% sell for slight uptrend)
    sides = np.random.choice(['buy', 'sell'], size=rows, p=[0.6, 0.4])

    # Calculate bid/ask with spread
    spread = start_price * (spread_bps / 10000)
    bids = prices - spread / 2
    asks = prices + spread / 2

    # Calculate rolling ATR (simplified)
    price_changes = np.abs(np.diff(prices, prepend=prices[0]))
    atr = pd.Series(price_changes).rolling(window=20, min_periods=1).mean()

    # Create DataFrame
    df = pd.DataFrame({
        'ts': timestamps,
        'price': prices.round(2),
        'qty': volumes.round(2),
        'side': sides,
        'bid': bids.round(2),
        'ask': asks.round(2),
        'atr': atr.fillna(volatility).round(2),
    })

    # Add some spike events (extreme moves)
    spike_indices = np.random.choice(rows, size=int(rows * 0.05), replace=False)
    df.loc[spike_indices, 'qty'] *= 3.0  # 3x volume on spikes

    # Save to CSV
    df.to_csv(output_path, index=False)

    print(f"✅ Generated {rows} trades")
    print(f"   Price range: {df['price'].min():.2f} - {df['price'].max():.2f}")
    print(f"   Avg volume: {df['qty'].mean():.2f}")
    print(f"   Buy/Sell: {(sides == 'buy').sum()}/{(sides == 'sell').sum()}")
    print(f"   Saved to: {output_path}")

    return df


def main():
    parser = argparse.ArgumentParser(description="Generate test trades CSV")
    parser.add_argument("--output", default="/tmp/test_trades.csv", help="Output CSV path")
    parser.add_argument("--rows", type=int, default=10000, help="Number of trades")
    parser.add_argument("--symbol", default="XAUUSD", help="Trading symbol")
    parser.add_argument("--start-price", type=float, default=2760.0, help="Starting price")
    parser.add_argument("--volatility", type=float, default=3.0, help="Price volatility")
    parser.add_argument("--spread-bps", type=float, default=5.0, help="Bid/ask spread in bps")

    args = parser.parse_args()

    generate_test_trades(
        output_path=args.output,
        rows=args.rows,
        symbol=args.symbol,
        start_price=args.start_price,
        volatility=args.volatility,
        spread_bps=args.spread_bps,
    )


if __name__ == "__main__":
    main()

