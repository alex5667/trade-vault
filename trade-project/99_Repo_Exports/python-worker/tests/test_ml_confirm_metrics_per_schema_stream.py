"""test_ml_confirm_metrics_per_schema_stream.py

Verifies P1.6 per-schema metrics stream wiring in
`services/ml_confirm/metrics_writer.py`:

  1. Payload carries `feature_schema_ver` derived from `self._model`
     (dict pack or object with attribute), even when the env flag is OFF.
  2. With ML_CONFIRM_METRICS_PER_SCHEMA_STREAM=1 and a non-empty
     feature_schema_ver, the writer dual-XADDs to
     `<base>:<schema_ver>` in addition to the base stream.
  3. Default behavior (flag unset) keeps only base-stream XADD —
     backwards compatible for rollup worker / autopromoter / drift tools.
  4. Empty feature_schema_ver disables the dual-write even with flag=1.
"""
from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any

import pytest


class _FakeRedis:
    """Minimal sync redis-like fake recording xadd calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def xadd(self, stream: str, fields: dict, **kwargs) -> bytes:  # noqa: D401
        self.calls.append((stream, dict(fields)))
        return b"0-1"


def _make_writer(model: Any, *, per_schema: str | None = None):
    """Construct a minimal MetricsWriterMixin-equipped object with the
    invariants `_emit_metrics` reads from `self`. Importing the mixin lazily
    keeps the test runnable even when the wider gate stack has heavy deps."""
    if per_schema is None:
        os.environ.pop("ML_CONFIRM_METRICS_PER_SCHEMA_STREAM", None)
    else:
        os.environ["ML_CONFIRM_METRICS_PER_SCHEMA_STREAM"] = per_schema

    try:
        from services.ml_confirm.metrics_writer import MetricsWriterMixin  # type: ignore
    except Exception as e:
        pytest.skip(f"MetricsWriterMixin unimportable: {e}")

    class _W(MetricsWriterMixin):  # type: ignore[misc, valid-type]
        pass

    w = _W()
    w.r = _FakeRedis()
    w.mode = "SHADOW"
    w._metrics_enable = True
    w._metrics_stream = "metrics:ml_confirm"
    w._metrics_maxlen = 1000
    w._metrics_sample = 1.0  # disable sampling
    w._model = model
    w._cfg = {}
    w._cfg_source = "test"
    return w


def _make_dec():
    return SimpleNamespace(
        kind="edge_stack_v1",
        model_run_id="m1",
        bucket="b1",
        p_edge=0.7,
        p_edge_cal=0.7,
        p_edge_raw=0.6,
        p_min=0.5,
        p_margin=0.1,
        latency_us=1000,
        status="ok",
        allow=True,
        error="",
        abstain=False,
        conf=0.8,
        missing=[],
    )


# ── 1. Payload always carries feature_schema_ver ─────────────────────────────

def test_payload_stamps_schema_ver_from_dict_model():
    model = {"feature_schema_ver": "v15_of", "feature_cols": ["f"] * 531}
    w = _make_writer(model)
    w._emit_metrics(
        _make_dec(),
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="trend",
        rule_score=0.5,
        rule_have=1,
        rule_need=1,
        cancel_spike_veto=0,
        ok_rule=1,
        sid="sid1",
        indicators={},
    )
    assert len(w.r.calls) == 1
    stream, payload = w.r.calls[0]
    assert stream == "metrics:ml_confirm"
    assert payload["feature_schema_ver"] == "v15_of"


def test_payload_stamps_schema_ver_from_object_model():
    model = SimpleNamespace(feature_schema_ver="v14_of")
    w = _make_writer(model)
    w._emit_metrics(
        _make_dec(),
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="trend",
        rule_score=0.5,
        rule_have=1,
        rule_need=1,
        cancel_spike_veto=0,
        ok_rule=1,
        sid="sid2",
        indicators={},
    )
    assert w.r.calls[0][1]["feature_schema_ver"] == "v14_of"


def test_payload_schema_ver_falls_back_to_empty():
    w = _make_writer({"feature_cols": ["f"]})  # no schema_ver key
    w._emit_metrics(
        _make_dec(),
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="trend",
        rule_score=0.5,
        rule_have=1,
        rule_need=1,
        cancel_spike_veto=0,
        ok_rule=1,
        sid="sid3",
        indicators={},
    )
    assert w.r.calls[0][1]["feature_schema_ver"] == ""


# ── 2. Dual-write enabled ─────────────────────────────────────────────────────

def test_per_schema_dual_write_when_enabled():
    model = {"feature_schema_ver": "v15_of", "feature_cols": ["f"] * 531}
    w = _make_writer(model, per_schema="1")
    w._emit_metrics(
        _make_dec(),
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="trend",
        rule_score=0.5,
        rule_have=1,
        rule_need=1,
        cancel_spike_veto=0,
        ok_rule=1,
        sid="sid4",
        indicators={},
    )
    streams = [s for s, _ in w.r.calls]
    assert "metrics:ml_confirm" in streams
    assert "metrics:ml_confirm:v15_of" in streams
    assert len(w.r.calls) == 2


# ── 3. Default OFF — backwards compatible ────────────────────────────────────

def test_no_dual_write_by_default():
    model = {"feature_schema_ver": "v15_of", "feature_cols": ["f"] * 531}
    w = _make_writer(model)  # per_schema flag absent
    w._emit_metrics(
        _make_dec(),
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="trend",
        rule_score=0.5,
        rule_have=1,
        rule_need=1,
        cancel_spike_veto=0,
        ok_rule=1,
        sid="sid5",
        indicators={},
    )
    streams = [s for s, _ in w.r.calls]
    assert streams == ["metrics:ml_confirm"]


# ── 4. Flag set but no schema_ver → only base ────────────────────────────────

def test_dual_write_skipped_when_schema_ver_empty():
    w = _make_writer({}, per_schema="1")
    w._emit_metrics(
        _make_dec(),
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="trend",
        rule_score=0.5,
        rule_have=1,
        rule_need=1,
        cancel_spike_veto=0,
        ok_rule=1,
        sid="sid6",
        indicators={},
    )
    streams = [s for s, _ in w.r.calls]
    assert streams == ["metrics:ml_confirm"]
