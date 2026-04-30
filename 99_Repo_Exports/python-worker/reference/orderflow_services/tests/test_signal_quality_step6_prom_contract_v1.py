from __future__ import annotations

import os
from pathlib import Path

def _read(path: str) -> str:
    p = Path(path)
    assert p.exists(), f"Missing file: {path}"
    return p.read_text(encoding="utf-8")

def test_step6_metrics_module_contract_exists():
    txt = _read("services/orderflow/metrics_signal_quality_v1.py")
    # Metric names are the public contract used by dashboards/alerts.
    assert '"dq_level"' in txt
    assert '"dq_veto_total"' in txt
    assert '"tick_gap_n"' in txt
    # Bucket sanitizer must exist to protect cardinality.
    assert "sanitize_dq_bucket" in txt

def test_step6_alerts_yaml_contains_expected_alerts():
    # YAML parsing is optional; we do a light string check to keep this test runnable
    # even in minimal CI containers.
    txt = _read("services/orderflow/prometheus_alerts_signal_quality_v2.yml")
    for name in [
        "TickGapP95HighWarn"
        "TickGapP95HighCrit"
        "TickGapP95Extreme"
        "TickMissingSeqEmaWarn"
        "TickMissingSeqEmaCrit"
        "BookMissingSeqEmaCrit"
        "DqLevel2ShareWarn"
        "DqLevel2ShareCrit"
    ]:
        assert name in txt

    # Ensure we reference the intended low-cardinality metrics.
    for metric in ["tick_gap_p95_ms", "tick_gap_n", "tick_missing_seq_ema", "book_missing_seq_ema", "dq_level"]:
        assert metric in txt
