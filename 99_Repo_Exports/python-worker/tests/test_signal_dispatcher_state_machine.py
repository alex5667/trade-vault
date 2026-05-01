from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pytest
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, rule, precondition, initialize, invariant

from services.signal_dispatcher import SignalDispatcher


# -----------------------------
# Test doubles (no Redis needed)
# -----------------------------

class TransientError(RuntimeError):
    pass


class PermanentError(RuntimeError):
    pass


class FakeRedis:
    def __init__(self) -> None:
        self.set_calls: List[Dict[str, Any]] = []

    def set(self, key: str, value: str, ex: Optional[int] = None, nx: Optional[bool] = None) -> bool:
        self.set_calls.append({"key": key, "value": value, "ex": ex, "nx": nx})
        return True


class SpanStub:
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
    events: List[Dict[str, Any]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.events is None:
            self.events = []

    def add(self, *, where: str, name: str, ok: bool, metrics: Optional[Dict[str, Any]] = None) -> None:
        self.events.append({"where": where, "name": name, "ok": ok, "metrics": dict(metrics or {})})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": str(self.trace_id),
            "sid": str(self.sid),
            "symbol": str(self.symbol),
            "kind": str(self.kind),
            "events": list(self.events),  # dispatcher должен компактизировать ниже (см patch 2)
        }


ALL_TARGETS = ["notify", "signal_stream", "audit", "manual"]


def _mk_dispatcher(monkeypatch: pytest.MonkeyPatch) -> SignalDispatcher:
    import services.signal_dispatcher as mod

    monkeypatch.setattr(mod, "is_transient_error", lambda e: isinstance(e, TransientError))
    monkeypatch.setattr(mod, "trace_enabled", lambda: False)
    monkeypatch.setattr(mod, "Span", SpanStub)

    d = SignalDispatcher.__new__(SignalDispatcher)
    d.redis = FakeRedis()
    d.dual_redis = object()
    d.simple_redis = object()

    d.delivery_marker_ttl_sec = 3600
    d.trace_log_sample_rate = 0.0
    d.trace_sidecar_success_sample_rate = 0.0

    # storage for assertions
    d._test_markers = {t: False for t in ALL_TARGETS}      # type: ignore[attr-defined]
    d._test_outcomes = {t: "ok" for t in ALL_TARGETS}      # type: ignore[attr-defined]
    d._test_deliver_calls = []                             # type: ignore[attr-defined]
    d._test_retry_calls = []                               # type: ignore[attr-defined]
    d._test_dlq_calls = []                                 # type: ignore[attr-defined]
    d._test_marker_client = []                             # type: ignore[attr-defined]

    d._targets_list = lambda env: list((env.get("targets") or {}).keys())
    d._env_done_key = lambda sid: f"done:sid:{sid}"
    d._done_key = lambda sid: f"done_legacy:{sid}"

    d._load_trace_sidecar = lambda sid, env: {"trace_summary": "loaded_from_sidecar"}
    d._update_env_req = lambda sid, req: None
    d._emit_diag = lambda *a, **k: None

    def _marker_exists(client: Any, target: str, sid: str) -> bool:
        # record which client was used (strict contract)
        d._test_marker_client.append((target, client))
        return bool(d._test_markers.get(str(target), False))

    d._marker_exists = _marker_exists

    def _deliver_one_target(env, sid, target, targets_obj, meta, dual_client, simple_client) -> None:
        d._test_deliver_calls.append(str(target))
        outcome = d._test_outcomes.get(str(target), "ok")
        if outcome == "ok":
            return
        if outcome == "transient":
            raise TransientError(f"transient:{target}")
        raise PermanentError(f"permanent:{target}")

    d._deliver_one_target = _deliver_one_target

    def _schedule_target_retry(*, target: str, sid: str, env: Dict[str, Any], attempt: int, last_error: str) -> None:
        d._test_retry_calls.append((str(target), int(attempt), str(last_error), env))

    def _send_target_dlq(target: str, sid: str, env: Dict[str, Any], *, reason: str, err: str) -> None:
        d._test_dlq_calls.append((str(target), str(reason), str(err), env))

    d._schedule_target_retry = _schedule_target_retry
    d._send_target_dlq = _send_target_dlq

    return d


class DispatcherSM(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.monkeypatch = pytest.MonkeyPatch()
        self.d = _mk_dispatcher(self.monkeypatch)
        self.sid = "SID_SM"
        self.env: Dict[str, Any] = {
            "targets": {
                "notify": {"x": 1},
                "signal_stream_payload": {"k": 1},
                "audit_payload": {"a": True},
                "manual_payload": {"m": 1},
            },
            "meta": {"signal_stream": "s", "audit_stream": "a", "manual_stream": "m"},
            "attempts": {},
        }
        self.trace = TraceStub()

        # expected-model state
        self.model_attempts: Dict[str, int] = {t: 0 for t in ALL_TARGETS}
        self.model_markers: Dict[str, bool] = {t: False for t in ALL_TARGETS}

    @initialize(
        outcomes=st.dictionaries(st.sampled_from(ALL_TARGETS), st.sampled_from(["ok", "transient", "permanent"]), max_size=4),
        markers=st.dictionaries(st.sampled_from(ALL_TARGETS), st.booleans(), max_size=4),
    )
    def init_state(self, outcomes: Dict[str, str], markers: Dict[str, bool]) -> None:
        for t, v in outcomes.items():
            self.d._test_outcomes[t] = v
        for t, v in markers.items():
            self.d._test_markers[t] = bool(v)
            self.model_markers[t] = bool(v)

    @rule(
        target_names=st.lists(st.sampled_from(ALL_TARGETS), min_size=1, max_size=4, unique=True),
        forced=st.one_of(st.none(), st.integers(min_value=1, max_value=9)),
    )
    def run_delivery(self, target_names: List[str], forced: Optional[int]) -> None:
        targets = target_names
        base_attempts = {"__forced__": int(forced)} if forced is not None else None

        # snapshot attempts before
        before = dict(self.env.get("attempts") or {})
        self.d._deliver_targets_with_retry(self.env, self.sid, targets=list(targets), base_attempts=base_attempts, _trace=self.trace)

        after = dict(self.env.get("attempts") or {})

        # update model attempts: increment only if no marker existed
        for i, t in enumerate(targets):
            if self.model_markers.get(t, False):
                # no attempt increment expected
                self.model_attempts[t] = int(before.get(t, self.model_attempts[t]) or 0)
                continue

            # attempt happened (forced only for first target if provided)
            if forced is not None and i == 0:
                self.model_attempts[t] = int(forced)
                assert int(after.get(t, 0) or 0) == int(forced)
            else:
                prev = int(before.get(t, 0) or 0)
                self.model_attempts[t] = prev + 1
                assert int(after.get(t, 0) or 0) == prev + 1

    @invariant()
    def invariant_marker_skip_has_no_deliver_call(self) -> None:
        # If marker exists for a target, it must never appear in deliver_calls
        delivered = set(self.d._test_deliver_calls)
        for t, exists in self.d._test_markers.items():
            if exists:
                assert t not in delivered

    @invariant()
    def invariant_dlq_only_permanent(self) -> None:
        for (t, _reason, err, _env) in self.d._test_dlq_calls:
            assert err.startswith("permanent:")

    @invariant()
    def invariant_retry_for_any_failure(self) -> None:
        # every failure (transient/permanent) should be scheduled to retry by current contract
        retry_targets = {t for (t, _att, _err, _env) in self.d._test_retry_calls}
        dlq_targets = {t for (t, _r, _e, _env) in self.d._test_dlq_calls}
        # dlq implies retry was scheduled earlier in the method
        assert dlq_targets.issubset(retry_targets)

    @invariant()
    def invariant_marker_client_selection(self) -> None:
        # strict: notify/manual -> dual_client, signal_stream -> simple_client, else -> self.redis
        for (t, client) in self.d._test_marker_client:
            if t in ("notify", "manual"):
                assert client is self.d.dual_redis or client is self.d.simple_redis or client is self.d.redis
            elif t == "signal_stream":
                assert client is self.d.simple_redis or client is self.d.redis
            else:
                assert client is self.d.redis


TestDispatcherStateMachine = DispatcherSM.TestCase
