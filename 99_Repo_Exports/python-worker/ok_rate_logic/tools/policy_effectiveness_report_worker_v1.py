#!/usr/bin/env python3
from __future__ import annotations

"""Tools entrypoint for P71 policy effectiveness report.

Stable command path:
  python3 tools/policy_effectiveness_report_worker_v1.py --once
"""


from orderflow_services.policy_effectiveness_report_worker_v1 import main

if __name__ == "__main__":
    raise SystemExit(main())
