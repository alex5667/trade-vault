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


def test_metrics_modules_import_and_expose_dq_symbols() -> None:
    """Step C contract:

    - Mirror metrics module is importable as a normal python package (services.*).
    - SoT metrics module is loadable from its file path.
    - Both can be loaded in the same process without Prometheus registration
      collisions (helpers must de-dupe by metric name).
    """

    from services.orderflow import metrics as mir

    sot_path = Path(__file__).resolve().parents[1] / "metrics.py"  # .../orderflow/metrics.py
    sot = _load_module_from_path("tick_flow_full_sot_metrics", sot_path)

    required = [
        # tick gap stats
        "tick_gap_p95_ms_gauge",
        "tick_gap_n_gauge",
        # continuity EMAs
        "tick_missing_seq_ema_gauge",
        "book_missing_seq_ema_gauge",
        # trade_id diagnostics
        "tick_id_gap_events_total",
        "tick_id_dup_events_total",
        "tick_id_reorder_events_total",
        # gate surface
        "dq_level_gauge",
        "dq_veto_total",
    ]

    for name in required:
        assert hasattr(sot, name), f"SoT metrics missing: {name}"
        assert hasattr(mir, name), f"Mirror metrics missing: {name}"

    # Minimal sanity: the objects are usable and have expected metric names.
    assert sot.tick_gap_p95_ms_gauge._name == "tick_gap_p95_ms"
    assert sot.tick_missing_seq_ema_gauge._name == "tick_missing_seq_ema"
    assert sot.book_missing_seq_ema_gauge._name == "book_missing_seq_ema"
    assert sot.dq_level_gauge._name == "dq_level"
    assert sot.dq_veto_total._name == "dq_veto"
