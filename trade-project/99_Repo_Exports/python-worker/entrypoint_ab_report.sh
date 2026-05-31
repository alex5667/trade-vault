#!/bin/bash
# Entry point for AB Report Service

cd /app/python-worker
exec python3 -m services.entry_policy_ab_report_service
