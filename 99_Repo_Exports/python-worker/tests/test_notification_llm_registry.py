"""
P2-2: Unit tests for notification_llm_registry routing and payload handling.
Coverage: route_notification (all branches), sanitize_payload, build_deepseek_request,
build_analysis_envelope, render_user_prompt.
"""
import pytest

from utils.notification_llm_registry import (
    MODEL_PROFILE_REGISTRY,
    PAYLOAD_WHITELISTS,
    PROMPT_REGISTRY,
    SOURCE_TO_NOTIFICATION_TYPE,
    SUBTYPE_TO_NOTIFICATION_TYPE,
    TEXT_HEURISTIC_ROUTES,
    build_analysis_envelope,
    build_deepseek_request,
    render_user_prompt,
    route_notification,
    sanitize_payload,
)

# ---------------------------------------------------------------------------
# route_notification — source_service branch
# ---------------------------------------------------------------------------


def test_route_by_source():
    assert route_notification(source_service="services/binance_iceberg_detector.py", payload={}) == "iceberg_detection"


def test_route_by_source_short_name():
    """Short (non-prefixed) filenames should also resolve."""
    assert route_notification(source_service="binance_iceberg_detector.py", payload={}) == "iceberg_detection"


def test_route_all_sources_resolve():
    """Every entry in SOURCE_TO_NOTIFICATION_TYPE must resolve to a known spec."""
    for src, expected_type in SOURCE_TO_NOTIFICATION_TYPE.items():
        result = route_notification(source_service=src, payload={})
        assert result == expected_type, f"source={src!r}: expected {expected_type!r}, got {result!r}"
        assert result in PROMPT_REGISTRY, f"type {result!r} from source {src!r} is not in PROMPT_REGISTRY"


# ---------------------------------------------------------------------------
# route_notification — notification_type passthrough
# ---------------------------------------------------------------------------


def test_route_by_explicit_notification_type():
    assert route_notification(notification_type="freeze_alarm", payload={}) == "freeze_alarm"


def test_route_unknown_notification_type_falls_through():
    """An unknown notification_type must NOT short-circuit — should fall to heuristics."""
    result = route_notification(notification_type="nonexistent_type", payload={})
    assert result in PROMPT_REGISTRY


# ---------------------------------------------------------------------------
# route_notification — subtype branch
# ---------------------------------------------------------------------------


def test_route_by_subtype():
    assert route_notification(subtype="of_gate_sre", payload={}) == "of_gate_sre"


def test_route_all_subtypes_resolve():
    for sub, expected_type in SUBTYPE_TO_NOTIFICATION_TYPE.items():
        result = route_notification(subtype=sub, payload={})
        assert result == expected_type, f"subtype={sub!r}: expected {expected_type!r}, got {result!r}"


# ---------------------------------------------------------------------------
# route_notification — text heuristic branch
# ---------------------------------------------------------------------------


def test_route_by_text_heuristic():
    assert route_notification(source_service="unknown.py", payload={"text": "Rollback Suggestion for BTCUSDT"}) == "rollback_alert"


@pytest.mark.parametrize("token,expected_type", list(TEXT_HEURISTIC_ROUTES.items()))
def test_route_all_text_heuristics(token, expected_type):
    """Every text heuristic keyword should route to its declared type."""
    result = route_notification(source_service="unrelated.py", payload={"text": f"event: {token} fired"})
    assert result == expected_type, f"token={token!r}: expected {expected_type!r}, got {result!r}"
    assert result in PROMPT_REGISTRY


# ---------------------------------------------------------------------------
# route_notification — payload field branch
# ---------------------------------------------------------------------------


def test_route_by_payload_notification_type_field():
    result = route_notification(payload={"notification_type": "calibration_sync"})
    assert result == "calibration_sync"


def test_route_by_payload_kind_field():
    result = route_notification(payload={"kind": "rollback_alert"})
    assert result == "rollback_alert"


# ---------------------------------------------------------------------------
# route_notification — fallback
# ---------------------------------------------------------------------------


def test_route_fallback_to_unknown_notification():
    """Completely unknown payload must fall back to unknown_notification."""
    result = route_notification(source_service="mystery.py", payload={"msg": "nothing recognizable"})
    assert result == "unknown_notification"


def test_route_empty_payload():
    result = route_notification(payload={})
    assert result in PROMPT_REGISTRY


def test_route_none_payload():
    result = route_notification()
    assert result in PROMPT_REGISTRY


# ---------------------------------------------------------------------------
# sanitize_payload
# ---------------------------------------------------------------------------


def test_sanitize_parses_payload_json_and_preserves_identity():
    payload = {
        "symbol": "BTCUSDT",
        "sid": "abc",
        "payload_json": '{"post_n":17,"post_lcb_r":-0.42}',
        "severity": "critical",
        "junk": "drop-me",
    },
    out = sanitize_payload("rollback_alert", payload)
    assert out["symbol"] == "BTCUSDT"
    assert out["sid"] == "abc"
    assert out["payload_json"]["post_n"] == 17
    assert "junk" not in out


def test_sanitize_preserves_critical_identity_keys():
    """Keys like symbol/sid must survive even if not in whitelist."""
    payload = {"symbol": "ETHUSDT", "sid": "s1", "random_key": "drop"}
    out = sanitize_payload("entry_opened", payload)
    assert out["symbol"] == "ETHUSDT"
    assert out["sid"] == "s1"


def test_sanitize_unknown_type_returns_normalized():
    """For a type not in PAYLOAD_WHITELISTS, all normalized keys are preserved."""
    payload = {"foo": "bar", "ts": 1234567890}
    out = sanitize_payload("__unknown_type__", payload)
    assert out["foo"] == "bar"
    assert out["ts_ms"] == 1234567890  # alias normalization


def test_sanitize_alias_normalization():
    """ts → ts_ms, entry → entry_price, sl → sl_price, tp → tp1_price."""
    payload = {"entry": 100.0, "sl": 95.0, "tp": 110.0, "ts": 1000}
    out = sanitize_payload("entry_opened", payload)
    assert out.get("entry_price") == 100.0
    assert out.get("sl_price") == 95.0
    assert out.get("tp1_price") == 110.0
    assert out.get("ts_ms") == 1000


# ---------------------------------------------------------------------------
# build_deepseek_request
# ---------------------------------------------------------------------------


def test_build_request_uses_json_schema():
    from utils.notification_llm_registry import OUTPUT_JSON_SCHEMA
    req = build_deepseek_request(
        source_service="services/active_symbol_guard_incident_notifier.py",
        payload={"fingerprint": "fp1", "classification": "stale_tombstone", "severity": "warning"},
    )
    assert req["response_format"] == OUTPUT_JSON_SCHEMA
    assert req["temperature"] == 0.10


def test_build_request_has_system_and_user_messages():
    req = build_deepseek_request(source_service="binance_iceberg_detector.py", payload={"symbol": "SOLUSDT"})
    messages = req["messages"]
    roles = [m["role"] for m in messages]
    assert "system" in roles
    assert "user" in roles


def test_build_request_all_registered_types():
    """build_deepseek_request must succeed for every type in PROMPT_REGISTRY."""
    for ntype in PROMPT_REGISTRY:
        req = build_deepseek_request(notification_type=ntype, source_service="test.py", payload={})
        assert "messages" in req


# ---------------------------------------------------------------------------
# build_analysis_envelope
# ---------------------------------------------------------------------------


def test_build_envelope_key_contains_identity():
    env = build_analysis_envelope(
        source_service="services/binance_dust_cleanup_admin_notifier.py",
        payload={"symbol": "ETHUSDT", "kind": "old_denylist", "ts_ms": 1234567890},
    )
    assert env["notification_type"] == "dust_cleanup"
    assert "ETHUSDT" in env["analysis_key"]


def test_build_envelope_has_required_keys():
    env = build_analysis_envelope(notification_type="freeze_alarm", source_service="test.py", payload={})
    assert "analysis_key" in env
    assert "notification_type" in env
    assert "sanitized_payload" in env
    assert "llm_request" in env


# ---------------------------------------------------------------------------
# render_user_prompt
# ---------------------------------------------------------------------------


def test_render_user_prompt_contains_notification_type():
    prompt = render_user_prompt("rollback_alert", "entry_policy_rollback_guard_v2.py", {"symbol": "BTCUSDT"})
    assert "rollback_alert" in prompt
    assert "BTCUSDT" in prompt


# ---------------------------------------------------------------------------
# New features tests
# ---------------------------------------------------------------------------


def test_all_prompt_specs_have_profile_and_whitelist():
    for nt, spec in PROMPT_REGISTRY.items():
        assert spec.profile in MODEL_PROFILE_REGISTRY
        if nt != "unknown_notification":
            assert nt in PAYLOAD_WHITELISTS


def test_unknown_notification_not_entry_opened():
    routed = route_notification(
        source_service="unknown.py",
        payload={"text": "unrecognized ops payload"},
    )
    assert routed == "unknown_notification"


def test_source_service_basename_routes():
    routed = route_notification(
        source_service="/app/services/binance_iceberg_detector.py",
        payload={},
    )
    assert routed == "iceberg_detection"


def test_nested_payload_redacts_secret():
    sanitized = sanitize_payload(
        "freeze_alarm",
        {
            "text": "freeze triggered",
            "payload": {
                "api_key": "SECRET",
                "symbol": "BTCUSDT",
            },
        },
    )
    assert sanitized["payload"]["api_key"] == "[redacted]"


def test_iceberg_fields_are_preserved():
    sanitized = sanitize_payload(
        "iceberg_detection",
        {
            "symbol": "BTCUSDT",
            "level_kind": "bid_wall",
            "level_price": 64000,
            "refresh_count": 12,
            "visible_qty": 4.2,
            "duration_sec": 18,
            "atr_used": 120,
        },
    )
    assert sanitized["level_kind"] == "bid_wall"
    assert sanitized["refresh_count"] == 12


def test_meta_freeze_not_auto_apply_without_skipped_text():
    routed = route_notification(
        source_service="nightly_meta_enforce_ramp_or_freeze_bundle.py",
        payload={"text": "CFG_SUGGESTIONS meta_freeze scopes=['ALL']"},
    )
    assert routed == "meta_freeze_status"
