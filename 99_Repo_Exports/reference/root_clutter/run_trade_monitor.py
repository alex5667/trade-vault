#!/usr/bin/env python3
"""
Script to run trade monitor with proper PYTHONPATH setup.
"""
import sys
import os

# Change to python-worker directory
os.chdir('/app/python-worker')

# Add paths
sys.path.insert(0, '/app/python-worker')
sys.path.insert(0, '/app')

# Import and run
from runners.trade_monitor_runner import main
main()
