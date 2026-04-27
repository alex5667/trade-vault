from __future__ import annotations

import os
import sys

# Ensure project root is on sys.path (core/, services/, tools/)
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
from types import SimpleNamespace

from core.of_confirm_engine import OFConfirmEngine


def test_runtime_snapshot_json_safe_and_book_events_sanitized() -> None:
    eng = OFConfirmEngine()

    # Make runtime with non-JSON objects for book events: export must sanitize to dict
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        ts=1700000000000,
        dynamic_cfg={"foo": 1},
        last_regime="range",
        liq_regime="high",
        book_churn_hi=0,
        pressure_hi=1,
        cont_ctx_ts_ms=0,
        last_obi_event=SimpleNamespace(ts_ms=1700000000000, direction="BUY", obi=0.12, stable_secs=3.0, obi_z=1.1),
        last_iceberg_event=SimpleNamespace(ts_ms=1700000000000, side="BUY", refresh=2, duration=1.5, price=42000.0),
        last_ofi_event=SimpleNamespace(ts_ms=1700000000000, direction="BUY", ofi=123.0, ofi_z=2.0, stable_secs=2.0),
        last_sweep=None,
        last_reclaim=None,
        last_weak_progress=None,
        last_div=None,
        last_wp=None,
        last_fp_edge=SimpleNamespace(ts_ms=1700000000000, bias="BUY", strength=None, p90=0.9, value=1.2, range_expansion=1),
        last_bar=None,
    )
    snap = eng.export_runtime_snapshot(runtime)
    # Must be JSON serializable
    json.dumps(snap, sort_keys=True)

    assert isinstance(snap.get("last_obi_event"), dict)
    assert isinstance(snap.get("last_iceberg_event"), dict)
    assert isinstance(snap.get("last_ofi_event"), dict)


def test_fp_edge_snapshot_fields_cover_evidence_contract() -> None:
    eng = OFConfirmEngine()
    fields = set(getattr(eng, "_SNAP_LAST_FP_EDGE_FIELDS"))
    # fp_edge_evidence can fallback to p90/value if strength missing
    for k in ("ts_ms", "bias", "range_expansion", "p90", "value", "strength"):
        assert k in fields


def test_cfg_snapshot_is_json_safe() -> None:
    eng = OFConfirmEngine()
    cfg = {"a": 1, "b": {"c": 2, "d": [1, 2, 3]}, "x": object()}
    snap = eng.export_cfg_snapshot(cfg)
    json.dumps(snap, sort_keys=True)

