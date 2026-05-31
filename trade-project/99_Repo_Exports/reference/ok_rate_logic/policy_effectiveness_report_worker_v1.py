#!/usr/bin/env python3
"""Compatibility wrapper for P71 policy effectiveness worker.

Some orchestration paths call this script from ok_rate_logic/.
Implementation is in orderflow_services/policy_effectiveness_report_worker_v1.py.
"""

from __future__ import annotations

from orderflow_services.policy_effectiveness_report_worker_v1 import main

if __name__ == "__main__":
    raise SystemExit(main())
