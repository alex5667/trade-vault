from __future__ import annotations

from common.payload_fingerprint import fingerprint_tradeable_payload
from services.outbox.envelope_builder import build_outbox_envelope


def test_envelope_builder_always_writes_req_targets_and_fingerprint(monkeypatch):
    monkeypatch.setenv("OUTBOX_META_PREFIX", "signal:meta:")

    env = build_outbox_envelope(
        sid="S1",
        meta={
            "signal_stream": "stream:signals",
            "audit_stream": "stream:audit",
            "manual_stream": "stream:manual",
            "snap_key": "snap:S1",
        },
        targets={
            "notify": {"type": "signal"},
            "signal_stream_payload": {"x": 1},
            "audit_payload": {"y": 2},
            "manual_payload": {"z": 3},
            "snapshot_payload": {"k": "v"},
        },
        symbol="BTCUSDT",
        kind="crypto_orderflow",
    )

    assert env["meta"]["trace_meta_key"] == "signal:meta:S1"
    assert env["meta"]["req_targets"] == ["notify", "signal_stream", "audit", "manual", "snapshot"]
    assert isinstance(env["meta"].get("payload_sha1"), str)
    assert isinstance(env["meta"].get("payload_bytes"), int)

    sha1, nbytes = fingerprint_tradeable_payload(env)
    assert env["meta"]["payload_sha1"] == sha1
    assert env["meta"]["payload_bytes"] == nbytes


def test_fingerprint_excludes_its_own_meta_fields():
    env = {
        "sid": "S1",
        "ts_ms": 1,
        "targets": {"notify": {"a": 1}},
        "meta": {"payload_sha1": "X", "payload_bytes": 999},
    }
    sha1a, nb_a = fingerprint_tradeable_payload(env)
    env["meta"]["payload_sha1"] = "Y"
    env["meta"]["payload_bytes"] = 111
    sha1b, nb_b = fingerprint_tradeable_payload(env)
    assert sha1a == sha1b
    assert nb_a == nb_b
