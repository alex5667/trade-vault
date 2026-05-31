"""Byte-stable wire-level golden tests for stream:signals:outbox.

Fixtures live in tests/fixtures/outbox/*.json. Each fixture pins the exact
XADD fields that producers emit and the gating decision dispatcher must make.

If you intentionally bump the wire shape, regenerate fixtures and update the
expected_* fields. If a test fails without an intentional change — there is a
real contract drift on the outbox path.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "outbox"


def _load_fixture(name: str) -> dict[str, Any]:
    path = FIXTURES_DIR / f"{name}.json"
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def _all_fixtures() -> list[str]:
    return sorted(p.stem for p in FIXTURES_DIR.glob("*.json"))


# ── Helpers: call dispatcher static-like methods without spinning up Redis ──
def _parse_envelope(fields: dict[str, Any]) -> dict[str, Any] | None:
    from services.signal_outbox_dispatcher import SignalDispatcher

    return SignalDispatcher._parse_envelope(MagicMock(), fields)


def _normalize(raw: Any) -> int | None:
    from services.signal_outbox_dispatcher import _normalize_schema_version

    return _normalize_schema_version(raw)


def _accepted_default() -> frozenset[int]:
    from core.outbox_envelope import SCHEMA_VERSION
    from services.signal_outbox_dispatcher import _parse_accepted_versions

    return _parse_accepted_versions(SCHEMA_VERSION)


def _accepted_v2_only(monkeypatch: pytest.MonkeyPatch) -> frozenset[int]:
    monkeypatch.setenv("OUTBOX_ACCEPT_SCHEMA_VERSIONS", "2")
    from core.outbox_envelope import SCHEMA_VERSION
    from services.signal_outbox_dispatcher import _parse_accepted_versions

    return _parse_accepted_versions(SCHEMA_VERSION)


# ── Round-trip / parse ────────────────────────────────────────────────────────


@pytest.mark.parametrize("fixture_name", _all_fixtures())
def test_fixture_wire_fields_are_strings(fixture_name: str) -> None:
    """Redis Stream XADD contract: every field value must be a string/bytes."""
    fx = _load_fixture(fixture_name)
    for k, v in fx["wire_fields"].items():
        assert isinstance(v, str), f"{fixture_name}: field {k!r} is {type(v).__name__}, expected str"


@pytest.mark.parametrize("fixture_name", _all_fixtures())
def test_dispatcher_parses_envelope(fixture_name: str) -> None:
    fx = _load_fixture(fixture_name)
    env = _parse_envelope(fx["wire_fields"])
    assert env is not None, f"{fixture_name}: dispatcher._parse_envelope returned None"
    assert isinstance(env, dict)


@pytest.mark.parametrize("fixture_name", _all_fixtures())
def test_schema_version_matches_fixture(fixture_name: str) -> None:
    fx = _load_fixture(fixture_name)
    env = _parse_envelope(fx["wire_fields"])
    assert env is not None
    sv = _normalize(env.get("schema_version"))
    assert sv == fx["expected_schema_version"], (
        f"{fixture_name}: parsed schema_version={sv!r} != expected={fx['expected_schema_version']!r}"
    )


@pytest.mark.parametrize("fixture_name", _all_fixtures())
def test_sid_presence_matches_fixture(fixture_name: str) -> None:
    fx = _load_fixture(fixture_name)
    env = _parse_envelope(fx["wire_fields"])
    assert env is not None
    has_sid = bool(env.get("sid"))
    assert has_sid is bool(fx["expected_sid_present"]), (
        f"{fixture_name}: sid_present={has_sid} != expected={fx['expected_sid_present']}"
    )


@pytest.mark.parametrize("fixture_name", _all_fixtures())
def test_acceptance_default_dual_read(fixture_name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OUTBOX_ACCEPT_SCHEMA_VERSIONS", raising=False)
    fx = _load_fixture(fixture_name)
    accepted = _accepted_default()
    sv = _normalize(json.loads(fx["wire_fields"]["data"]).get("schema_version"))
    is_accepted = sv is not None and sv in accepted
    assert is_accepted is bool(fx["accepted_default"]), (
        f"{fixture_name}: accepted_default={is_accepted} (accepted set={sorted(accepted)}, sv={sv}) "
        f"!= expected={fx['accepted_default']}"
    )


@pytest.mark.parametrize("fixture_name", _all_fixtures())
def test_acceptance_v2_only(fixture_name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    accepted = _accepted_v2_only(monkeypatch)
    fx = _load_fixture(fixture_name)
    sv = _normalize(json.loads(fx["wire_fields"]["data"]).get("schema_version"))
    is_accepted = sv is not None and sv in accepted
    assert is_accepted is bool(fx["accepted_v2_only"]), (
        f"{fixture_name}: accepted_v2_only={is_accepted} (accepted set={sorted(accepted)}, sv={sv}) "
        f"!= expected={fx['accepted_v2_only']}"
    )


# ── Targeted invariants ───────────────────────────────────────────────────────


def test_v2_canonical_carries_payload_schema_marker() -> None:
    fx = _load_fixture("v2_canonical")
    env = _parse_envelope(fx["wire_fields"])
    assert env is not None
    meta = env.get("meta") or {}
    assert meta.get("payload_schema") == "outbox_envelope:v2"


def test_v2_canonical_trade_back_carries_proto_schema_version() -> None:
    """targets.trade_back must inherit protocol schema_version for HTTP consumer gating."""
    fx = _load_fixture("v2_canonical")
    env = _parse_envelope(fx["wire_fields"])
    assert env is not None
    tb = (env.get("targets") or {}).get("trade_back") or {}
    assert tb.get("schema_version") == 2


def test_v2_flat_legacy_auto_repair_lifts_signal_id_to_sid() -> None:
    """Flat-shape (OutboxWriter legacy) producers use signal_id; dispatcher must lift to sid."""
    fx = _load_fixture("v2_flat_legacy")
    env = _parse_envelope(fx["wire_fields"])
    assert env is not None, "flat envelope must parse"
    assert env.get("sid") == "of:SOLUSDT:1700000000000:LONG", (
        "auto-repair must lift signal_id → sid for flat-shape envelopes"
    )


def test_v2_flat_legacy_auto_repair_lifts_targets() -> None:
    """notify_payload / audit_payload at top-level must be moved into targets dict."""
    fx = _load_fixture("v2_flat_legacy")
    env = _parse_envelope(fx["wire_fields"])
    assert env is not None
    targets = env.get("targets")
    assert isinstance(targets, dict), "targets must be materialized after auto-repair"
    assert "notify" in targets, "notify_payload must be lifted into targets.notify"
    assert "audit_payload" in targets, "audit_payload must be lifted into targets.audit_payload"


def test_v999_unknown_is_rejected_under_default_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OUTBOX_ACCEPT_SCHEMA_VERSIONS", raising=False)
    fx = _load_fixture("v999_unknown")
    accepted = _accepted_default()
    sv = _normalize(json.loads(fx["wire_fields"]["data"]).get("schema_version"))
    assert sv == 999
    assert sv not in accepted, "future-unknown schema_version must NOT slip through dual-read default"


def test_v1_legacy_window_open_under_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Guard: legacy v1 stays accepted in dual-read window until cutoff."""
    monkeypatch.delenv("OUTBOX_ACCEPT_SCHEMA_VERSIONS", raising=False)
    accepted = _accepted_default()
    from core.outbox_envelope import LEGACY_SCHEMA_VERSIONS, SCHEMA_VERSION

    assert SCHEMA_VERSION in accepted
    for legacy in LEGACY_SCHEMA_VERSIONS:
        assert legacy in accepted, f"legacy v{legacy} unexpectedly dropped from accepted set"


def test_v1_legacy_window_closed_under_v2_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """When operator forces single-read, legacy v1 must be rejected."""
    accepted = _accepted_v2_only(monkeypatch)
    assert accepted == frozenset({2})
    assert 1 not in accepted
