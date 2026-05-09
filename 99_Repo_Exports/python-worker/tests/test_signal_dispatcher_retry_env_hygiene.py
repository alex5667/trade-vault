from __future__ import annotations

import os
from typing import Any


def _compact_env_for_retry(env: dict[str, Any]) -> dict[str, Any]:
    """
    Copy of the compaction logic for testing.
    HARD CONTRACT for retry/DLQ payloads:
      - env must be JSON-serializable
      - env["trace"] must be bounded (events <= 64, strings trimmed)
      - targets payloads must NOT contain full trace/events keys (defensive strip)
      - optional hard max bytes (ENV) to avoid oversized Redis entries
    Fail-open: never raises; returns best-effort compact env.
    """
    import copy
    if not isinstance(env, dict):
        return env

    # Work on a deep copy to avoid modifying the original
    env = copy.deepcopy(env)

    try:
        # Defensive strip inside targets (trade payload must not carry full trace)
        tg = env.get("targets")
        if isinstance(tg, dict):
            for _, p in tg.items():
                if isinstance(p, dict):
                    p.pop("trace", None)
                    p.pop("events", None)

        tr = env.get("trace")
        if isinstance(tr, dict):
            ev = tr.get("events")
            if isinstance(ev, list) and len(ev) > 64:
                tr["events"] = ev[-64:]
            # trim common big strings (best-effort)
            for k in ("notes", "error", "diag", "debug"):
                v = tr.get(k)
                if isinstance(v, str) and len(v) > 512:
                    tr[k] = v[:512]
            env["trace"] = tr

        # Optional hard size budget
        max_bytes = 0
        try:
            max_bytes = int(os.getenv("DISPATCHER_RETRY_ENV_MAX_BYTES", "0") or "0")
        except Exception:
            max_bytes = 0
        if max_bytes > 0:
            try:
                import json
                b = json.dumps(env, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                if len(b) > max_bytes:
                    # Keep only minimal trace summary if oversized
                    env.pop("trace", None)
                    # keep trace_id/trace_summary at top-level if present
            except Exception:
                pass
    except Exception:
        pass
    return env


def test_retry_env_compact_strips_trace_from_targets() -> None:
    """Test the env compaction logic directly."""
    env = {
        "targets": {
            "notify": {"text": "x", "trace": {"events": [1, 2, 3]}, "events": [1, 2]},
            "signal_stream_payload": {"data": "clean"},
        },
        "meta": {},
        "attempts": {},
        "trace_id": "T",
        "trace": {"events": [{"i": i} for i in range(200)], "notes": "x" * 600},  # oversized
    }

    compacted = _compact_env_for_retry(env)

    # Check that forbidden keys are stripped from targets
    nt = compacted["targets"]["notify"]
    assert "trace" not in nt
    assert "events" not in nt
    assert nt["text"] == "x"  # other data preserved

    # Clean targets unchanged
    assert compacted["targets"]["signal_stream_payload"]["data"] == "clean"

    # Trace events bounded
    tr = compacted.get("trace")
    assert isinstance(tr, dict)
    assert len(tr["events"]) <= 64
    # Large strings trimmed
    assert len(tr["notes"]) <= 512

    # Original env not modified (defensive copy)
    assert "trace" in env["targets"]["notify"]  # original still has it
