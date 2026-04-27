from services.outbox.envelope_builder import build_envelope


def test_build_envelope_writes_required_meta():
    env = build_envelope(
        sid="S1",
        payload={"symbol": "BTCUSDT", "kind": "signal", "x": 1},
        targets_obj={
            "notify": True,
            "signal_stream_payload": {"a": 1},
            "audit_payload": {"b": 2},
            "manual_payload": {"c": 3},
            "snapshot_payload": {"d": 4},
        },
        meta={"signal_stream": "s", "audit_stream": "a", "manual_stream": "m"},
    )

    assert env["sid"] == "S1"
    assert isinstance(env.get("targets"), dict)
    assert isinstance(env.get("meta"), dict)

    meta = env["meta"]
    assert meta.get("trace_meta_key")
    assert meta.get("payload_fp_v") == 1
    assert isinstance(meta.get("payload_sha1"), str)
    assert isinstance(meta.get("payload_bytes"), int)

    # required targets recorded
    assert sorted(meta.get("req_targets") or []) == sorted(["notify", "signal_stream", "audit", "manual", "snapshot"])

    # no full trace object inside tradeable env
    assert "trace" not in env
    assert "decision_trace" not in env
