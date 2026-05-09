from __future__ import annotations

import sys
from pathlib import Path

# Provide standard mirror pathing.
# We don't prepend tick_flow_full because we are validating mirror.
_PROJ_ROOT = str(Path(__file__).resolve().parents[3]) # python-worker root
if _PROJ_ROOT not in sys.path:
    sys.path.insert(0, _PROJ_ROOT)


def test_sot_metrics_export_liqmap_symbols_and_label_contract() -> None:
    """A2 contract:
    services/orderflow/metrics.py (Mirror) must expose LiqMap metrics.
    """

    from services.orderflow import metrics as sot

    required = [
        "liqmap_snapshot_age_ms_gauge",
        "liqmap_snapshot_parse_errors_total",
        "liqmap_gate_shadow_hit_total",
        "liqmap_gate_veto_total",
    ]

    for k in required:
        assert hasattr(sot, k), f"Mirror metrics missing: {k}"

    # Label contract (keep low-cardinality for alerts).
    sot.liqmap_snapshot_age_ms_gauge.labels(symbol="BTCUSDT", window="1h").set(0.0)
    sot.liqmap_snapshot_parse_errors_total.labels(symbol="BTCUSDT").inc(0)
    sot.liqmap_gate_shadow_hit_total.labels(symbol="BTCUSDT", dir="LONG", window="1h").inc(0)
    sot.liqmap_gate_veto_total.labels(symbol="BTCUSDT", dir="LONG", reason="hot_near").inc(0)
