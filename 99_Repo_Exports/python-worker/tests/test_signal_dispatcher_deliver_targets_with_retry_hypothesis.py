from __future__ import annotations

import copy
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import services.dispatch.dispatcher_app as sd_mod
from services.dispatch.dispatcher_app import SignalDispatcher
from utils.time_utils import get_ny_time_millis


class FakeRedis:
    def __init__(self) -> None:
        self.kv: dict[str, Any] = {}

    def set(self, key: str, value: Any, ex: int | None = None, nx: bool = False, px: int | None = None) -> bool:
        if nx and key in self.kv:
            return False
        self.kv[str(key)] = value
        return True

    def setex(self, key: str, ttl: int, value: Any) -> bool:
        self.kv[str(key)] = value
        return True

    def get(self, key: str) -> Any:
        return self.kv.get(str(key))

    def ttl(self, key: str) -> int:
        # not needed here; return "unknown"
        return -1

    def zadd(self, name: str, mapping: dict[str, int]) -> int:
        # retries are patched out; keep minimal impl
        return 1

    def xadd(self, stream: str, fields: dict[str, Any], maxlen: int | None = None, approximate: bool = True) -> str:
        # not needed; just accept
        return "0-0"


JSON_SCALAR = st.one_of(
    st.integers(min_value=-10_000, max_value=10_000),
    st.text(min_size=0, max_size=20),
    st.booleans(),
    st.none(),
)


def _payload_strategy():
    # dict payload (json-ish), sometimes already containing sid/trace_id (important)
    base = st.dictionaries(
        keys=st.text(min_size=1, max_size=10),
        values=JSON_SCALAR,
        max_size=20,
    )
    return st.one_of(
        base,
        st.builds(lambda d: {**d, "sid": "already", "trace_id": "already_tid"}, base),
        st.builds(lambda d: {**d, "sid": "already"}, base),
        st.builds(lambda d: {**d, "trace_id": "already_tid"}, base),
    )


@settings(max_examples=80, deadline=None)
@given(
    p_sig=_payload_strategy(),
    p_aud=_payload_strategy(),
    p_man=_payload_strategy(),
    p_snap=_payload_strategy(),
    sid=st.text(min_size=1, max_size=24),
)
def test_deliver_targets_with_retry_never_mutates_targets_payloads(
    monkeypatch: pytest.MonkeyPatch,
    p_sig: dict[str, Any],
    p_aud: dict[str, Any],
    p_man: dict[str, Any],
    p_snap: dict[str, Any],
    sid: str,
) -> None:
    # --- dispatcher instance (bypass __init__) ---
    d = SignalDispatcher.__new__(SignalDispatcher)

    main = FakeRedis()
    dual = FakeRedis()
    simple = FakeRedis()

    d.redis = main
    d.dual_redis = dual
    d.simple_redis = simple

    # attributes referenced by _deliver_one_target/_deliver_targets_with_retry
    d.delivery_marker_ttl_sec = 60
    d.marker_gc_zset = "marker_gc"
    d._sha_main = "sha_main"
    d._sha_dual = "sha_dual"
    d.trace_log_sample_rate = 0.0
    d.trace_sidecar_success_sample_rate = 0.0
    d.trace_diag_enabled = False
    d._ctr = {}

    # stable keys
    monkeypatch.setattr(d, "_delivery_key", lambda target, sid0: f"mk:{target}:{sid0}", raising=True)
    monkeypatch.setattr(d, "_env_done_key", lambda sid0: f"env_done:{sid0}", raising=True)
    monkeypatch.setattr(d, "_done_key", lambda sid0: f"legacy_done:{sid0}", raising=True)

    # marker exists check: look up marker key in the provided client
    def _marker_exists(client: FakeRedis, target: str, sid0: str) -> bool:
        return client.get(f"mk:{target}:{sid0}") not in (None, "", b"")

    monkeypatch.setattr(d, "_marker_exists", _marker_exists, raising=True)

    # choose correct redis per target for marker checks at the end
    def _marker_client_for_target(t: str, dual_client: Any, simple_client: Any) -> Any:
        if t in ("notify", "manual"):
            return dual_client
        if t == "signal_stream":
            return simple_client
        return d.redis

    monkeypatch.setattr(d, "_marker_client_for_target", _marker_client_for_target, raising=True)

    # strict validator/compactor/diag: no-op to isolate mutation contract
    monkeypatch.setattr(d, "_strict_validate_env", lambda env: None, raising=True)
    monkeypatch.setattr(d, "_compact_env_for_retry", lambda env: env, raising=True)
    monkeypatch.setattr(d, "_emit_diag", lambda *a, **k: None, raising=True)
    monkeypatch.setattr(d, "_update_env_req", lambda *a, **k: None, raising=True)
    monkeypatch.setattr(d, "_load_trace_sidecar", lambda *a, **k: {}, raising=True)
    monkeypatch.setattr(d, "_trace_meta_key", lambda sid0, env=None: f"meta:{sid0}", raising=True)
    monkeypatch.setattr(d, "_write_trace_sidecar_best_effort", lambda *a, **k: None, raising=True)

    # retries/dlq: must not raise in success scenario
    monkeypatch.setattr(d, "_schedule_target_retry", lambda *a, **k: None, raising=True)
    monkeypatch.setattr(d, "_send_target_dlq", lambda *a, **k: None, raising=True)

    # NOTE: your code calls a free helper patch_trace_sidecar_best_effort(...) in module scope
    monkeypatch.setattr(sd_mod, "patch_trace_sidecar_best_effort", lambda *a, **k: None, raising=False)

    # fake lua eval: set delivery marker on the passed client (atomic deliver+mark simulation)
    def fake_eval(client: FakeRedis, sha: str, tag: str, script: str, nkeys: int, *argv: Any) -> str:
        marker_key = str(argv[0])
        # store timestamp to emulate marker
        client.set(marker_key, str(get_ny_time_millis()))
        return "OK"

    d._evalsha_or_eval = fake_eval  # type: ignore

    # --- env with targets/meta for the 4 branches shown in your code ---
    env: dict[str, Any] = {
        "sid": sid,
        "symbol": "BTCUSDT",
        "kind": "test",
        "targets": {
            "signal_stream_payload": copy.deepcopy(p_sig),
            "audit_payload": copy.deepcopy(p_aud),
            "manual_payload": copy.deepcopy(p_man),
            "snapshot_payload": copy.deepcopy(p_snap),
        },
        "meta": {
            "signal_stream": "stream:signal",
            "audit_stream": "stream:audit",
            "manual_stream": "stream:manual",
            "snap_key": "snap:key",
            "snap_ttl": 10,
        },
    }

    # original payloads must not be mutated in-place
    orig_targets = copy.deepcopy(env["targets"])

    # run delivery
    d._deliver_targets_with_retry(
        env,
        sid,
        targets=["signal_stream", "audit", "manual", "snapshot"],
        base_attempts=None,
        _trace=None,
    )

    # 1) STRICT: original tradeable payload dicts unchanged (no sid/trace_id injected)
    assert env["targets"] == orig_targets

    # 2) attempts recorded
    attempts = env.get("attempts") or {}
    assert isinstance(attempts, dict)
    assert attempts.get("signal_stream") == 1
    assert attempts.get("audit") == 1
    assert attempts.get("manual") == 1
    assert attempts.get("snapshot") == 1

    # 3) markers exist in correct redis clients
    assert simple.get(f"mk:signal_stream:{sid}") is not None
    assert main.get(f"mk:audit:{sid}") is not None
    assert dual.get(f"mk:manual:{sid}") is not None
    assert main.get(f"mk:snapshot:{sid}") is not None

    # 4) env_done marker set in main redis (since you set it via self.redis.set(self._env_done_key(sid), ...))
    assert main.get(f"env_done:{sid}") is not None
