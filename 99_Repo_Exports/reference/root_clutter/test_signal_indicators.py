#!/usr/bin/env python3
"""
Test script for signal generator indicator calculations.
Tests the fixes for NaN/Inf handling.
"""

import sys
import os
import pandas as pd
import numpy as np

# Add signal-generator to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'signal-generator'))

from signal_generator import TechnicalIndicators, EMA_FAST, EMA_SLOW, RSI_PERIOD, ATR_PERIOD

def create_test_candles(num_candles=50):
    """Create test candle data"""
    np.random.seed(42)  # For reproducible results

    # Base price around 100.0
    base_price = 100.0

    # Generate OHLC data with realistic movements
    opens = [base_price]
    highs = []
    lows = []
    closes = []

    for i in range(num_candles):
        prev_close = opens[-1]

        # Generate realistic price movement
        change = np.random.normal(0, 0.5)  # Mean=0, std=0.5
        close = prev_close + change

        # Generate high/low around close
        volatility = abs(change) * 2 + 0.1
        high = close + np.random.uniform(0, volatility)
        low = close - np.random.uniform(0, volatility)

        highs.append(high)
        lows.append(low)
        closes.append(close)

        # Next open is previous close
        opens.append(close)

    # Create DataFrame
    candles = pd.DataFrame({
        'open': opens[:-1],  # Remove last open as we have one extra
        'high': highs,
        'low': lows,
        'close': closes
    })

    return candles

def test_indicators():
    """Test indicator calculations"""
    print("🧪 Testing Technical Indicators")

    # Create test data
    candles = create_test_candles(100)  # Plenty of data
    print(f"📊 Created {len(candles)} test candles")

    close = candles['close']
    high = candles['high']
    low = candles['low']

    # Test EMA
    print("📈 Testing EMA calculations...")
    ema_fast = TechnicalIndicators.ema(close, EMA_FAST)
    ema_slow = TechnicalIndicators.ema(close, EMA_SLOW)

    print(f"  EMA Fast ({EMA_FAST}): {ema_fast.iloc[-1]:.4f}")
    print(f"  EMA Slow ({EMA_SLOW}): {ema_slow.iloc[-1]:.4f}")

    # Check for NaN/Inf
    fast_nan = ema_fast.isna().sum()
    slow_nan = ema_slow.isna().sum()
    print(f"  NaN values - Fast: {fast_nan}, Slow: {slow_nan}")

    # Test RSI
    print("📊 Testing RSI calculation...")
    rsi = TechnicalIndicators.rsi(close, RSI_PERIOD)
    print(f"  RSI ({RSI_PERIOD}): {rsi.iloc[-1]:.2f}")

    rsi_nan = rsi.isna().sum()
    print(f"  NaN values: {rsi_nan}")

    # Test ATR
    print("📈 Testing ATR calculation...")
    atr = TechnicalIndicators.atr(high, low, close, ATR_PERIOD)
    print(f"  ATR ({ATR_PERIOD}): {atr.iloc[-1]:.4f}")

    atr_nan = atr.isna().sum()
    print(f"  NaN values: {atr_nan}")

    # Test MACD
    print("📊 Testing MACD calculation...")
    macd_line, signal_line, histogram = TechnicalIndicators.macd(close)
    print(f"  MACD: {macd_line.iloc[-1]:.4f}")
    print(f"  Signal: {signal_line.iloc[-1]:.4f}")
    print(f"  Histogram: {histogram.iloc[-1]:.4f}")

    macd_nan = macd_line.isna().sum() + signal_line.isna().sum() + histogram.isna().sum()
    print(f"  NaN values: {macd_nan}")

    # Summary
    total_nan = fast_nan + slow_nan + rsi_nan + atr_nan + macd_nan
    print(f"\n📋 Summary:")
    print(f"  Total NaN values: {total_nan}")

    if total_nan == 0:
        print("✅ All indicators calculated successfully without NaN values!")
        return True
    else:
        print("❌ Some indicators still have NaN values")
        return False

def test_nan_handling():
    """Test NaN handling in indicators"""
    print("\n🧪 Testing NaN handling")

    # Create data with NaN
    data = pd.Series([100.0, 101.0, np.nan, 103.0, 102.0])

    print("📊 Testing EMA with NaN data...")
    ema_result = TechnicalIndicators.ema(data, 3)
    print(f"  EMA result: {ema_result.tolist()}")

    nan_count = ema_result.isna().sum()
    print(f"  NaN values after processing: {nan_count}")

    if nan_count == 0:
        print("✅ EMA handles NaN correctly!")
        return True
    else:
        print("❌ EMA still produces NaN values")
        return False

if __name__ == "__main__":
    success1 = test_indicators()
    success2 = test_nan_handling()

    if success1 and success2:
        print("\n🎉 All tests passed! Signal generator indicators are working correctly.")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Check the indicator implementations.")
        sys.exit(1)
