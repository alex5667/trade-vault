"""F3 regression: meta_schema_version (ML feature-set version) must NOT
contaminate the protocol-level schema_version that SignalDispatcher gates on.

Bug: prior to the fix, passing meta_schema_version=13 (v13_of) into
make_envelope() set envelope.schema_version=13, causing SignalDispatcher
(ACCEPTED_SCHEMA_VERSIONS={1}) to silently DLQ all production signals
as "unsupported_schema_version:13".

Fix: meta_schema_version routes to feature_schema_version; schema_version
stays at SCHEMA_VERSION (protocol).
"""
from __future__ import annotations

from core.outbox_envelope import SCHEMA_VERSION, make_envelope


def _base_kwargs() -> dict:
    return {
        "signal_id": "sid-test-1",
        "ts_ms": 1_700_000_000_000,
        "kind": "test",
        "symbol": "BTCUSDT",
    }


def test_meta_schema_version_does_not_set_schema_version():
    # ML feature-set v14_of must not bump protocol version.
    env = make_envelope(**_base_kwargs(), meta_schema_version=14)
    assert env.schema_version == SCHEMA_VERSION, (
        f"protocol schema_version must remain {SCHEMA_VERSION}, "
        f"got {env.schema_version}"
    )
    assert env.feature_schema_version == 14


def test_meta_schema_version_zero_or_none_is_safe():
    env_zero = make_envelope(**_base_kwargs(), meta_schema_version=0)
    assert env_zero.schema_version == SCHEMA_VERSION
    assert env_zero.feature_schema_version == 0


def test_explicit_schema_version_wins_over_meta():
    # If caller explicitly passes both, schema_version is authoritative.
    env = make_envelope(**_base_kwargs(), schema_version=2, meta_schema_version=14)
    assert env.schema_version == 2
    assert env.feature_schema_version == 14


def test_to_stream_fields_emits_feature_schema_version_when_set():
    env = make_envelope(**_base_kwargs(), meta_schema_version=14)
    fields = env.to_stream_fields()
    assert fields["schema_version"] == str(SCHEMA_VERSION)
    assert fields.get("feature_schema_version") == "14"


def test_to_stream_fields_omits_feature_schema_version_when_zero():
    env = make_envelope(**_base_kwargs())
    fields = env.to_stream_fields()
    assert "feature_schema_version" not in fields


def test_dispatcher_normalization_accepts_protocol_version():
    """End-to-end: dispatcher should accept the envelope under canonical shape."""
    from services.signal_outbox_dispatcher import (
        ACCEPTED_SCHEMA_VERSIONS,
        _normalize_schema_version,
    )

    env = make_envelope(**_base_kwargs(), meta_schema_version=14)
    sv = _normalize_schema_version(env.schema_version)
    assert sv is not None
    assert sv in ACCEPTED_SCHEMA_VERSIONS


def test_schema_version_bumped_to_2():
    """Protocol bump: matches meta.payload_schema='outbox_envelope:v2'."""
    from core.outbox_envelope import LEGACY_SCHEMA_VERSIONS, SCHEMA_VERSION
    assert SCHEMA_VERSION == 2
    assert 1 in LEGACY_SCHEMA_VERSIONS


def test_dispatcher_default_dual_read_accepts_both_v1_and_v2(monkeypatch):
    """Dispatcher default ACCEPTED_SCHEMA_VERSIONS must include current + legacy."""
    monkeypatch.delenv("OUTBOX_ACCEPT_SCHEMA_VERSIONS", raising=False)
    from services.signal_outbox_dispatcher import _parse_accepted_versions
    from core.outbox_envelope import SCHEMA_VERSION
    accepted = _parse_accepted_versions(SCHEMA_VERSION)
    assert 1 in accepted
    assert 2 in accepted


def test_dispatcher_single_read_via_env(monkeypatch):
    """OUTBOX_ACCEPT_SCHEMA_VERSIONS='2' forces single-read once legacy drains."""
    monkeypatch.setenv("OUTBOX_ACCEPT_SCHEMA_VERSIONS", "2")
    from services.signal_outbox_dispatcher import _parse_accepted_versions
    from core.outbox_envelope import SCHEMA_VERSION
    accepted = _parse_accepted_versions(SCHEMA_VERSION)
    assert accepted == frozenset({2})


def test_signal_outbox_publisher_emits_current_schema_version(monkeypatch):
    """SignalOutboxPublisher default-fills schema_version from SCHEMA_VERSION constant."""
    import json as _json
    from core.outbox_envelope import SCHEMA_VERSION
    from core.signal_outbox import SignalOutboxPublisher

    captured = {}

    class _FakeRedis:
        def script_load(self, src):
            return "sha-1"

        def evalsha(self, sha, n, *args):
            # args = [dedup_key, stream, ttl, maxlen, envelope_json]
            captured["envelope_json"] = args[4]
            return [1, b"1234-0"]

    pub = SignalOutboxPublisher(redis_client=_FakeRedis())
    msg_id = pub.publish(
        source="t", strategy="t", symbol="BTCUSDT", side="LONG", kind="of",
        level_key="", ts_ms=1_700_000_000_000, envelope={"sid": "s1", "targets": {}, "meta": {}},
    )
    assert msg_id == "1234-0"
    env = _json.loads(captured["envelope_json"])
    assert env["schema_version"] == SCHEMA_VERSION
