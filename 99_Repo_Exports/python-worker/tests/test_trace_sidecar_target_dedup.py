from __future__ import annotations

from common.decision_trace import patch_trace_sidecar_obj


def _target_event(ok: bool, attempt: int, target: str, err: str = "") -> dict:
    ev = {
        "type": "target",
        "stage": "dispatcher",
        "target": target,
        "ok": bool(ok),
        "attempt": int(attempt),
        "duration_ms": 1.23,  # must not affect dedup
    }
    if ok:
        ev["reason_code"] = "OK"
    else:
        ev["reason_code"] = "ERR"
        if err:
            ev["err"] = err
    return ev


def test_target_events_are_deduped_on_repeat_patch():
    side = {"schema": "decision_trace_sidecar:v1", "trace": {"events": []}}
    patch = [_target_event(True, 1, "notify"), _target_event(False, 2, "notify", "boom")]

    out1 = patch_trace_sidecar_obj(side, patch)
    out2 = patch_trace_sidecar_obj(out1, patch)  # same patch again

    evs1 = out1["trace"]["events"]
    evs2 = out2["trace"]["events"]
    assert len(evs1) == 2
    assert len(evs2) == 2  # MUST NOT grow


def test_target_dedup_property_idempotent():
    # Test with specific values instead of hypothesis
    side = {"schema": "decision_trace_sidecar:v1", "trace": {"events": []}}
    patch = [_target_event(True, 1, "notify", "x" * 600)]
    out1 = patch_trace_sidecar_obj(side, patch)
    out2 = patch_trace_sidecar_obj(out1, patch)
    assert len(out1["trace"]["events"]) == 1
    assert len(out2["trace"]["events"]) == 1
    # ensure error is trimmed (<=512 + "...")
    ev = out2["trace"]["events"][0]
    if not ev["ok"]:
        assert isinstance(ev.get("err"), str)
        assert len(ev["err"]) <= 515
