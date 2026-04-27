#!/bin/bash

# Apply patches with corrected paths

echo "Applying trade_diag_split (5).patch..."
sed 's|mnt/data/orig/||g; s|mnt/data/new/||g' "trade_diag_split (5).patch" | git apply -

echo "Applying trade_failopen_contracts (3).patch..."
sed 's|python-worker/||g' "trade_failopen_contracts (3).patch" | git apply -

echo "Applying trade_missing_modules (3).patch..."
sed 's|python-worker/||g' "trade_missing_modules (3).patch" | git apply -

echo "Applying fixes (3).patch..."
sed 's|python-worker/||g' "fixes (3).patch" | git apply -

echo "Done applying patches"
