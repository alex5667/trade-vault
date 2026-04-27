"""Thin wrapper for tick_flow_full mirror.

The canonical implementation lives in `orderflow_services/new_features_gauges_smoke_check_v1.py`.
Keeping a wrapper here prevents train/serve directory drift.
"""

from orderflow_services.new_features_gauges_smoke_check_v1 import main


if __name__ == "__main__":
    main()
