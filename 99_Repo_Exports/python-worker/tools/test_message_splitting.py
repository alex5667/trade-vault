#!/usr/bin/env python3
"""
Test script to verify message splitting logic works correctly.
This simulates a long calibration report and tests the splitting function.
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, '/app')

from tools.trailing_autotune_telegram import _split_message

# Create a mock report similar to what we saw in logs
mock_report = """Trailing calibration: CryptoOrderFlow (Standard 5000)
stream=trades:closed, limit=5000, min_trades=10

ETHUSDT
- недостаточно данных для рекомендаций.

BTCUSDT
- недостаточно данных для рекомендаций.

SOLUSDT
- Все win-сделки: n_total=673, n_wins=135, lock_r=0.70R, TP1_OFFSET_ATR=0.70
  MFE_R avg/median=499.26/499.27, giveback_R=498.07, ratio=1.00
  std(MFE_R)=0.43, std(giveback_ratio)=0.00, confidence=0.87

- Только трейлинговые win-сделки: n_total=673, n_wins=135, lock_r=0.70R, TP1_OFFSET_ATR=0.70
  MFE_R avg/median=499.26/499.27, giveback_R=498.07, ratio=1.00
  std(MFE_R)=0.43, std(giveback_ratio)=0.00, confidence=0.87

BNBUSDT
- Все win-сделки: n_total=450, n_wins=90, lock_r=0.65R, TP1_OFFSET_ATR=0.65
  MFE_R avg/median=350.15/350.20, giveback_R=349.00, ratio=0.99
  std(MFE_R)=0.38, std(giveback_ratio)=0.01, confidence=0.82

- Только трейлинговые win-сделки: n_total=450, n_wins=90, lock_r=0.65R, TP1_OFFSET_ATR=0.65
  MFE_R avg/median=350.15/350.20, giveback_R=349.00, ratio=0.99
  std(MFE_R)=0.38, std(giveback_ratio)=0.01, confidence=0.82

XRPUSDT
- Все win-сделки: n_total=520, n_wins=104, lock_r=0.72R, TP1_OFFSET_ATR=0.72
  MFE_R avg/median=425.30/425.35, giveback_R=424.10, ratio=1.00
  std(MFE_R)=0.40, std(giveback_ratio)=0.00, confidence=0.85

- Только трейлинговые win-сделки: n_total=520, n_wins=104, lock_r=0.72R, TP1_OFFSET_ATR=0.72
  MFE_R avg/median=425.30/425.35, giveback_R=424.10, ratio=1.00
  std(MFE_R)=0.40, std(giveback_ratio)=0.00, confidence=0.85

SUIUSDT
- Все win-сделки: n_total=380, n_wins=76, lock_r=0.68R, TP1_OFFSET_ATR=0.68
  MFE_R avg/median=290.45/290.50, giveback_R=289.30, ratio=1.00
  std(MFE_R)=0.35, std(giveback_ratio)=0.00, confidence=0.80

- Только трейлинговые win-сделки: n_total=380, n_wins=76, lock_r=0.68R, TP1_OFFSET_ATR=0.68
  MFE_R avg/median=290.45/290.50, giveback_R=289.30, ratio=1.00
  std(MFE_R)=0.35, std(giveback_ratio)=0.00, confidence=0.80

APTUSDT
- Все win-сделки: n_total=310, n_wins=62, lock_r=0.66R, TP1_OFFSET_ATR=0.66
  MFE_R avg/median=245.20/245.25, giveback_R=244.10, ratio=1.00
  std(MFE_R)=0.33, std(giveback_ratio)=0.00, confidence=0.78

- Только трейлинговые win-сделки: n_total=310, n_wins=62, lock_r=0.66R, TP1_OFFSET_ATR=0.66
  MFE_R avg/median=245.20/245.25, giveback_R=244.10, ratio=1.00
  std(MFE_R)=0.33, std(giveback_ratio)=0.00, confidence=0.78

ARBUSDT
- Все win-сделки: n_total=290, n_wins=58, lock_r=0.64R, TP1_OFFSET_ATR=0.64
  MFE_R avg/median=220.35/220.40, giveback_R=219.25, ratio=1.00
  std(MFE_R)=0.31, std(giveback_ratio)=0.00, confidence=0.76

- Только трейлинговые win-сделки: n_total=290, n_wins=58, lock_r=0.64R, TP1_OFFSET_ATR=0.64
  MFE_R avg/median=220.35/220.40, giveback_R=219.25, ratio=1.00
  std(MFE_R)=0.31, std(giveback_ratio)=0.00, confidence=0.76

DOGEUSDT
- Все win-сделки: n_total=410, n_wins=82, lock_r=0.69R, TP1_OFFSET_ATR=0.69
  MFE_R avg/median=315.50/315.55, giveback_R=314.40, ratio=1.00
  std(MFE_R)=0.37, std(giveback_ratio)=0.00, confidence=0.81

- Только трейлинговые win-сделки: n_total=410, n_wins=82, lock_r=0.69R, TP1_OFFSET_ATR=0.69
  MFE_R avg/median=315.50/315.55, giveback_R=314.40, ratio=1.00
  std(MFE_R)=0.37, std(giveback_ratio)=0.00, confidence=0.81

WIFUSDT
- Все win-сделки: n_total=270, n_wins=54, lock_r=0.63R, TP1_OFFSET_ATR=0.63
  MFE_R avg/median=205.25/205.30, giveback_R=204.15, ratio=1.00
  std(MFE_R)=0.30, std(giveback_ratio)=0.00, confidence=0.75

- Только трейлинговые win-сделки: n_total=270, n_wins=54, lock_r=0.63R, TP1_OFFSET_ATR=0.63
  MFE_R avg/median=205.25/205.30, giveback_R=204.15, ratio=1.00
  std(MFE_R)=0.30, std(giveback_ratio)=0.00, confidence=0.75

1000PEPEUSDT
- Все win-сделки: n_total=340, n_wins=68, lock_r=0.67R, TP1_OFFSET_ATR=0.67
  MFE_R avg/median=260.40/260.45, giveback_R=259.30, ratio=1.00
  std(MFE_R)=0.34, std(giveback_ratio)=0.00, confidence=0.79

- Только трейлинговые win-сделки: n_total=340, n_wins=68, lock_r=0.67R, TP1_OFFSET_ATR=0.67
  MFE_R avg/median=260.40/260.45, giveback_R=259.30, ratio=1.00
  std(MFE_R)=0.34, std(giveback_ratio)=0.00, confidence=0.79

1000SHIBUSDT
- Все win-сделки: n_total=360, n_wins=72, lock_r=0.68R, TP1_OFFSET_ATR=0.68
  MFE_R avg/median=275.55/275.60, giveback_R=274.45, ratio=1.00
  std(MFE_R)=0.36, std(giveback_ratio)=0.00, confidence=0.80

- Только трейлинговые win-сделки: n_total=360, n_wins=72, lock_r=0.68R, TP1_OFFSET_ATR=0.68
  MFE_R avg/median=275.55/275.60, giveback_R=274.45, ratio=1.00
  std(MFE_R)=0.36, std(giveback_ratio)=0.00, confidence=0.80

1000FLOKIUSDT
- Все win-сделки: n_total=320, n_wins=64, lock_r=0.66R, TP1_OFFSET_ATR=0.66
  MFE_R avg/median=250.30/250.35, giveback_R=249.20, ratio=1.00
  std(MFE_R)=0.32, std(giveback_ratio)=0.00, confidence=0.77

- Только трейлинговые win-сделки: n_total=320, n_wins=64, lock_r=0.66R, TP1_OFFSET_ATR=0.66
  MFE_R avg/median=250.30/250.35, giveback_R=249.20, ratio=1.00
  std(MFE_R)=0.32, std(giveback_ratio)=0.00, confidence=0.77

1000BONKUSDT
- Все win-сделки: n_total=300, n_wins=60, lock_r=0.65R, TP1_OFFSET_ATR=0.65
  MFE_R avg/median=230.45/230.50, giveback_R=229.35, ratio=1.00
  std(MFE_R)=0.31, std(giveback_ratio)=0.00, confidence=0.76

- Только трейлинговые win-сделки: n_total=300, n_wins=60, lock_r=0.65R, TP1_OFFSET_ATR=0.65
  MFE_R avg/median=230.45/230.50, giveback_R=229.35, ratio=1.00
  std(MFE_R)=0.31, std(giveback_ratio)=0.00, confidence=0.76
"""

print(f"Original message length: {len(mock_report)} characters")
print(f"Telegram limit: 4096 characters")
print(f"Safe limit (with buffer): 4000 characters")
print()

# Test splitting
chunks = _split_message(mock_report, max_length=4000)

print(f"✅ Message split into {len(chunks)} chunk(s)")
print()

for i, chunk in enumerate(chunks):
    print(f"--- Chunk {i+1}/{len(chunks)} ({len(chunk)} chars) ---")
    # Show first 200 chars and last 100 chars
    if len(chunk) <= 300:
        print(chunk)
    else:
        print(chunk[:200])
        print("...")
        print(chunk[-100:])
    print()

# Verify all chunks are under limit
all_ok = all(len(chunk) <= 4096 for chunk in chunks)
if all_ok:
    print("✅ All chunks are within Telegram's 4096 character limit")
else:
    print("❌ ERROR: Some chunks exceed the limit!")
    for i, chunk in enumerate(chunks):
        if len(chunk) > 4096:
            print(f"  Chunk {i+1}: {len(chunk)} chars (EXCEEDS LIMIT)")

# Verify header is in all chunks
header_present = all("Trailing calibration" in chunk for chunk in chunks)
if header_present:
    print("✅ Header present in all chunks")
else:
    print("⚠️ Warning: Header missing from some chunks")

# Verify part indicators if multiple chunks
if len(chunks) > 1:
    has_indicators = all("Part" in chunk and f"{i+1}/{len(chunks)}" in chunk for i, chunk in enumerate(chunks))
    if has_indicators:
        print("✅ Part indicators present in all chunks")
    else:
        print("⚠️ Warning: Part indicators missing or incorrect")

print()
print("Test complete!")
