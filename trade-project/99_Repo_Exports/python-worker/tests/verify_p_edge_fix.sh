#!/bin/bash
# Verify P-Edge Capping Fix

echo "Running reproduction and fix verification test..."
python3 -m pytest tests/test_fix_p_edge_capping.py -v

echo "Running deploy tool verification test..."
python3 -m pytest tests/test_deploy_calibrator.py -v

echo "All verification tests passed."
