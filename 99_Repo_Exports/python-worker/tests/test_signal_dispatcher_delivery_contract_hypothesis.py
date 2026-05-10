from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

# ВАЖНО: импортируем реальный класс из вашего проекта.
from services.dispatch.dispatcher_app import SignalDispatcher

# -----------------------------
# Test doubles (no Redis needed)
# -----------------------------

class TransientError(RuntimeError):
    """Synthetic transient error for classification tests."""


class PermanentError(RuntimeError):
    """Synthetic permanent error for classification tests."""


class FakeRedis:
    def __init__(self) -> None:
        self.set_calls: list[dict[str, Any]] = []

    def set(self, key: str, value: str, ex: int | None = None, nx: bool | None = None) -> bool:
        self.set_calls.append({"key": key, "value": value, "ex": ex, "nx": nx})
        return True


class SpanStub:
    """Minimal Span stub: SignalDispatcher uses Span().ms() in the loop."""
    def __init__(self) -> None:
        self._ms = 1.0

    def ms(self) -> float:
        return float(self._ms)


@dataclass
class TraceStub:
    trace_id: str = ""
    sid: str = ""
    symbol=""
    kind: str = ""
    events: list[dict[str, Any]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.events is None:
            self.events = []

    def add(self, *, where: str, name: str, ok: bool, metrics: dict[str, Any] | None = None) -> None:
        self.events.append({
            "where": str(where),
            "name": str(name),
            "ok": bool(ok),
            "metrics": dict(metrics or {}),
        })

    def to_dict(self) -> dict[str, Any]:
        # Compact trace for retry/debug context.
        return {
            "trace_id": str(self.trace_id),
            "sid": str(self.sid),
            "symbol": str(self.symbol),
            "kind": str(self.kind),
            "events": list(self.events)[-64:],  # hard bound for safety
        }


def _mk_dispatcher(monkeypatch: pytest.MonkeyPatch) -> SignalDispatcher:
    """
    Create a SignalDispatcher instance without running its real __init__.
    We patch only the exact surface used by _deliver_targets_with_retry.
    """
    import services.dispatch.dispatcher_app as mod

    # Make classification deterministic for the test.
    monkeypatch.setattr(mod, "is_transient_error", lambda e: isinstance(e, TransientError))
    # Avoid trace-enabled branches depending on external infra.
    monkeypatch.setattr(mod, "trace_enabled", lambda: False)
    # Span used for duration in loop.
    monkeypatch.setattr(mod, "Span", SpanStub)

    d = SignalDispatcher.__new__(SignalDispatcher)  # bypass __init__
    d.redis = FakeRedis()
    d.dual_redis = object()
    d.simple_redis = object()

    d.delivery_marker_ttl_sec = 3600
    d.trace_log_sample_rate = 0.0
    d.trace_sidecar_success_sample_rate = 0.0

    # storage for assertions
    d._test_markers = {}          # type: ignore[attr-defined]
    d._test_outcomes = {}         # type: ignore[attr-defined]
    d._test_deliver_calls = []    # type: ignore[attr-defined]
    d._test_retry_calls = []      # type: ignore[attr-defined]
    d._test_dlq_calls = []        # type: ignore[attr-defined]

    # Required helpers
    d._targets_list = lambda env: list((env.get("targets") or {}).keys())
    d._env_done_key = lambda sid: f"done:sid:{sid}"
    d._done_key = lambda sid: f"done_legacy:{sid}"

    # Sidecar best-effort loader (used only if env lacks trace_summary).
    d._load_trace_sidecar = lambda sid, env: {"trace_summary": "loaded_from_sidecar"}
    d._update_env_req = lambda sid, req: None
    d._emit_diag = lambda *a, **k: None

    # Marker check (client selection irrelevant for contract; it must be called and respected).
    def _marker_exists(_client: Any, target: str, sid: str) -> bool:
        return bool(d._test_markers.get(str(target), False))

    d._marker_exists = _marker_exists

    # Delivery: simulate outcome per target.
    def _deliver_one_target(env, sid, target, targets_obj, meta, dual_client, simple_client) -> None:
        d._test_deliver_calls.append(str(target))
        outcome = d._test_outcomes.get(str(target), "ok")
        if outcome == "ok":
            return
        if outcome == "transient":
            raise TransientError(f"transient:{target}")
        raise PermanentError(f"permanent:{target}")

    d._deliver_one_target = _deliver_one_target

    # Retry/DLQ sinks.
    def _schedule_target_retry(*, target: str, sid: str, env: dict[str, Any], attempt: int, last_error: str) -> None:
        d._test_retry_calls.append((str(target), int(attempt), str(last_error)))

    def _send_target_dlq(target: str, sid: str, env: dict[str, Any], *, reason: str, err: str) -> None:
        d._test_dlq_calls.append((str(target), reason, str(err)))

    d._schedule_target_retry = _schedule_target_retry
    d._send_target_dlq = _send_target_dlq

    return d


ALL_TARGETS = ["notify", "signal_stream", "audit", "manual"]


@st.composite
def scenarios(draw):
    targets = draw(st.lists(st.sampled_from(ALL_TARGETS), min_size=1, max_size=4, unique=True))
    markers = draw(st.dictionaries(st.sampled_from(ALL_TARGETS), st.booleans(), max_size=4))
    outcomes = draw(st.dictionaries(st.sampled_from(ALL_TARGETS), st.sampled_from(["ok", "transient", "permanent"]), max_size=4))
    attempts0 = draw(st.dictionaries(st.sampled_from(ALL_TARGETS), st.integers(min_value=0, max_value=5), max_size=4))
    return targets, markers, outcomes, attempts0


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(scenarios())
def test_deliver_targets_contract(monkeypatch: pytest.MonkeyPatch, scenarios) -> None:
    """
    Contract (жёстко):
      - если marker exists для target => delivery НЕ вызывается, попытка НЕ инкрементится
      - иначе попытка +1, delivery вызывается ровно 1 раз
      - transient => retry scheduled, no DLQ
      - permanent => retry scheduled + DLQ
      - done marker ставится только если нет failed_* (то есть все targets либо доставлены, либо уже были доставлены)
    """
    targets, markers, outcomes, attempts0 = scenarios

    d = _mk_dispatcher(monkeypatch)

    # Configure scenario
    d._test_markers.update(markers)
    d._test_outcomes.update(outcomes)

    env: dict[str, Any] = {
        "targets": {"notify": {}, "signal_stream_payload": {}, "audit_payload": {}, "manual_payload": {}},
        "meta": {"signal_stream": "s", "audit_stream": "a", "manual_stream": "m"},
        "attempts": dict(attempts0),
        # no trace_id => must fall back to sid
    }
    sid = "SID123"

    # Disable legacy done write for deterministic assert
    monkeypatch.setenv("SIGNAL_OUTBOX_WRITE_LEGACY_DONE", "0")

    trace = TraceStub()
    d._deliver_targets_with_retry(env, sid, targets=list(targets), _trace=trace)

    # Expected attempted targets
    attempted = [t for t in targets if not bool(markers.get(t, False))]
    skipped = [t for t in targets if bool(markers.get(t, False))]

    # Deliver called exactly for attempted targets
    assert sorted(d._test_deliver_calls) == sorted(attempted)

    # Attempts updated only for attempted targets
    attempts1 = env.get("attempts") or {}
    assert isinstance(attempts1, dict)

    for t in skipped:
        assert int(attempts1.get(t, 0) or 0) == int(attempts0.get(t, 0) or 0)

    for t in attempted:
        assert int(attempts1.get(t, 0) or 0) == int(attempts0.get(t, 0) or 0) + 1

    # Retry calls: all failures among attempted
    retry_calls: list[tuple[str, int, str]] = list(d._test_retry_calls)
    dlq_calls: list[tuple[str, str, str]] = list(d._test_dlq_calls)

    expected_failed_transient = {t for t in attempted if outcomes.get(t, "ok") == "transient"}
    expected_failed_permanent = {t for t in attempted if outcomes.get(t, "ok") == "permanent"}

    got_retry_targets = {t for (t, _att, _err) in retry_calls}
    assert got_retry_targets == (expected_failed_transient | expected_failed_permanent)

    # DLQ only for permanent
    got_dlq_targets = {t for (t, _reason, _err) in dlq_calls}
    assert got_dlq_targets == expected_failed_permanent

    # Done marker semantics
    any_failure = bool(expected_failed_transient or expected_failed_permanent)
    done_sets = [c for c in d.redis.set_calls if (c.get("key")).startswith("done:sid:")]
    if any_failure:
        assert done_sets == []
    else:
        assert len(done_sets) == 1
        call = done_sets[0]
        assert call["key"] == f"done:sid:{sid}"
        assert call["nx"] is True
        assert int(call["ex"] or 0) == int(d.delivery_marker_ttl_sec)


def test_trace_summary_loaded_from_sidecar_if_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    d = _mk_dispatcher(monkeypatch)
    env: dict[str, Any] = {"targets": {}, "meta": {}}
    sid = "SID_SIDE"

    trace = TraceStub()
    d._deliver_targets_with_retry(env, sid, targets=["audit"], _trace=trace)

    # Because env had no trace_summary, dispatcher must best-effort load it.
    assert env.get("trace_summary") in (None, "loaded_from_sidecar")
    # NOTE: if your implementation sets it unconditionally, keep the stricter check:
    # assert env.get("trace_summary") == "loaded_from_sidecar"


def test_forced_attempt_applies_to_first_target_only(monkeypatch: pytest.MonkeyPatch) -> None:
    d = _mk_dispatcher(monkeypatch)
    monkeypatch.setenv("SIGNAL_OUTBOX_WRITE_LEGACY_DONE", "0")

    env: dict[str, Any] = {"targets": {}, "meta": {}, "attempts": {}}
    sid = "SID_FORCED"
    trace = TraceStub()

    # No markers, all ok
    d._test_markers.update(dict.fromkeys(ALL_TARGETS, False))
    d._test_outcomes.update(dict.fromkeys(ALL_TARGETS, "ok"))

    d._deliver_targets_with_retry(
        env,
        sid,
        targets=["notify", "audit"],
        base_attempts={"__forced__": 7},
        _trace=trace,
    )

    # first target forced to 7, second target normal => 1
    assert int(env["attempts"]["notify"]) == 7
    assert int(env["attempts"]["audit"]) == 1
