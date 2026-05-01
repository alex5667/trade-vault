from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module_from_path(mod_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(mod_name, str(path))
    assert spec is not None
    assert spec.loader is not None
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)  # type: ignore[attr-defined]
    return m


def test_liqmap_levels_overlay_metrics_exports_d21():
    root = Path(__file__).resolve().parents[4]  # repo root (scanner_infra)

    # Import only SoT metrics to avoid default-registry collisions from the mirror module.
    sot = _load_module_from_path(
        "tick_flow_full_sot_metrics_d21",
        root / "reference" / "tick_flow_full" / "services" / "orderflow" / "metrics.py",
    )

    expected = [
        "liqmap_levels_overlay_enabled_gauge",
        "liqmap_levels_attempt_total",
        "liqmap_levels_applied_gauge",
        "liqmap_levels_applied_total",
        "liqmap_levels_cap_sl_widen_total",
        "liqmap_tp1_adj_bps_hist",
        "liqmap_sl_adj_bps_hist",
    ]
    for name in expected:
        assert hasattr(sot, name), f"SoT metrics missing {name}"

    # label contracts (low cardinality)
    assert tuple(sot.liqmap_levels_overlay_enabled_gauge._labelnames) == ("symbol", "window")
    assert tuple(sot.liqmap_levels_attempt_total._labelnames) == ("symbol", "window")
    assert tuple(sot.liqmap_levels_applied_gauge._labelnames) == ("symbol", "window")
    assert tuple(sot.liqmap_levels_applied_total._labelnames) == ("symbol", "window", "reason")
    assert tuple(sot.liqmap_levels_cap_sl_widen_total._labelnames) == ("symbol", "window")
    assert tuple(sot.liqmap_tp1_adj_bps_hist._labelnames) == ("symbol", "window")
    assert tuple(sot.liqmap_sl_adj_bps_hist._labelnames) == ("symbol", "window")

    # Mirror: static check (avoid registry collisions on import)
    mir_text = (root / "python-worker" / "services" / "orderflow" / "metrics.py").read_text("utf-8", errors="ignore")
    for name in expected:
        assert name in mir_text, f"Mirror metrics code missing {name}"
