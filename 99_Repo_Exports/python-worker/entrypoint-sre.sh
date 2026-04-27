#!/bin/bash
# Entrypoint script for OF Gate SRE monitoring in Docker
# This script starts cron and then runs the main application

# Start cron daemon
cron

# Create log directory
mkdir -p /var/log /var/lib/trade/of_bench /var/lib/trade/of_gate_golden /var/lib/trade/of_replay

# Run main application
exec "$@"
