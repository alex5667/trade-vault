#!/bin/bash
cd /app/python-worker
PYTHONPATH=/app/python-worker:/app python runners/trade_monitor_runner.py
