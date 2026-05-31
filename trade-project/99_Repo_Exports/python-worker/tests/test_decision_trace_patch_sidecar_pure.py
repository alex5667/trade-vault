from __future__ import annotations

from common.decision_trace import patch_trace_sidecar_obj


def test_patch_sidecar_updates_trace_key() -> None:
    side = {"schema": "decision_trace_sidecar:v2", "trace": {"events": [{"x": 1}]}, "trace_summary": "old"}
    patch = [{"type": "target", "ok": True}]
    out = patch_trace_sidecar_obj(side, patch)
    assert "trace" in out
    assert isinstance(out["trace"], dict)
    assert isinstance(out["trace"].get("events"), list)
    assert out["trace"]["events"][-1]["type"] == "target"
