"""Regression guard for the silent-DLQ root cause found 2026-05-19.

Background:
  Both outbox producer paths that bypass SignalOutboxPublisher.publish
  (atomic_xadd_async via orderflow signal_pipeline; atomic_xadd_sync via
  binance iceberg detector) were emitting envelopes WITHOUT a top-level
  `schema_version`. SignalDispatcher gates on that exact field and rejects
  everything missing it as `unsupported_schema_version:unknown` → DLQ.

  All 49 envelopes in the outbox stream at the time of discovery had been
  silently DLQ'd over ~30h. Fix: stamp protocol `schema_version` in:
    1. services/outbox/envelope_builder.build_outbox_envelope
    2. services/outbox/envelope_builder.build_envelope
    3. services/outbox/atomic_outbox._prepare_contract_payload (defense-in-depth
       for raw-payload callers that skip build_outbox_envelope)

  If anyone removes any of these stamps, production silently DLQs 100% of
  signal envelopes again. These tests fail loudly and immediately.
"""
from __future__ import annotations

import pytest


def test_build_outbox_envelope_stamps_schema_version():
    """The canonical builder MUST stamp `schema_version` at the top level.

    Loss mode: SignalDispatcher reads `env["schema_version"]` from the parsed
    envelope, not from XADD fields. Without this stamp, every envelope is
    rejected as `unsupported_schema_version:unknown`.
    """
    from core.outbox_envelope import SCHEMA_VERSION
    from services.outbox.envelope_builder import build_outbox_envelope

    env = build_outbox_envelope(
        sid="t-001",
        symbol="BTCUSDT",
        kind="crypto_orderflow",
        notify_payload={"x": 1},
        audit_payload={"sid": "t-001"},
        signal_stream_payload={"sid": "t-001"},
        audit_stream="audit",
        signal_stream="strategy",
        meta={},
    )
    assert "schema_version" in env, (
        "build_outbox_envelope MUST stamp top-level schema_version. "
        "Without it, SignalDispatcher silently DLQs every produced envelope."
    )
    assert env["schema_version"] == int(SCHEMA_VERSION), (
        f"Expected schema_version={int(SCHEMA_VERSION)}, got {env['schema_version']!r}. "
        "If you intentionally bumped SCHEMA_VERSION, also update the dispatcher's "
        "ACCEPTED_SCHEMA_VERSIONS and roll out per CLAUDE.md §Outbox Data Contracts."
    )


def test_build_envelope_stamps_schema_version():
    """The P3 contract builder MUST also stamp `schema_version`.

    Same loss mode as build_outbox_envelope — both helpers are used by
    different code paths and both feed atomic_xadd_async.
    """
    from core.outbox_envelope import SCHEMA_VERSION
    from services.outbox.envelope_builder import build_envelope

    env = build_envelope(
        sid="t-002",
        payload={"side": "LONG", "qty": 0.001},
        targets_obj={"audit_payload": {"sid": "t-002"}},
        meta={},
    )
    assert "schema_version" in env, (
        "build_envelope MUST stamp top-level schema_version (see comment "
        "in services/outbox/envelope_builder.py twin stamp)."
    )
    assert env["schema_version"] == int(SCHEMA_VERSION)


def test_atomic_outbox_prepare_contract_payload_stamps_schema_version():
    """The defense-in-depth stamp in _prepare_contract_payload protects raw-payload
    callers that XADD directly via atomic_xadd_async without going through
    build_outbox_envelope first.

    Distinct field from `schema_ver` (which is a STRING execution-intent contract
    tag like 'execution_intent:v1'). The proto `schema_version` is an INT and
    is what dispatcher gates on.
    """
    from core.outbox_envelope import SCHEMA_VERSION
    from services.outbox.atomic_outbox import _prepare_contract_payload

    raw_payload = {"foo": "bar"}  # caller forgot schema_version
    prepared = _prepare_contract_payload(
        "t-003",
        "crypto_orderflow",
        "BTCUSDT",
        raw_payload,
        None,
    )
    assert "schema_version" in prepared, (
        "_prepare_contract_payload MUST defense-stamp schema_version for raw-payload "
        "callers. Without it, producers that bypass build_outbox_envelope silently DLQ."
    )
    assert prepared["schema_version"] == int(SCHEMA_VERSION)
    # Sanity: `schema_ver` (string) is distinct and unaffected.
    assert prepared.get("schema_ver") != prepared["schema_version"], (
        "schema_version (int proto) and schema_ver (string contract tag) must NOT "
        "be conflated — they serve different gates."
    )


def test_caller_supplied_schema_version_wins_over_defaults():
    """If a caller explicitly sets schema_version (e.g. tests forcing an
    older protocol version), setdefault must NOT overwrite it."""
    from services.outbox.atomic_outbox import _prepare_contract_payload

    prepared = _prepare_contract_payload(
        "t-004", "kind", "BTCUSDT",
        {"schema_version": 99}, None,
    )
    assert prepared["schema_version"] == 99, (
        "_prepare_contract_payload must use setdefault — explicit caller values win."
    )


@pytest.mark.parametrize("builder_name", ["build_outbox_envelope", "build_envelope"])
def test_envelope_builders_use_canonical_schema_version_constant(builder_name: str):
    """Builders MUST source SCHEMA_VERSION from core.outbox_envelope.

    A hardcoded integer would drift silently when the canonical constant
    is bumped (and pass tests because the assertion would update to match
    the wrong number). Pinning the import path makes drift impossible.
    """
    import importlib
    import inspect

    eb = importlib.import_module("services.outbox.envelope_builder")
    src = inspect.getsource(getattr(eb, builder_name))
    assert "from core.outbox_envelope import SCHEMA_VERSION" in src, (
        f"{builder_name} must import SCHEMA_VERSION from core.outbox_envelope, "
        "not hardcode an int. Found body:\n" + src[:400]
    )
