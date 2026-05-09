
def test_build_outbox_envelope_trace_summary_only(monkeypatch):
    monkeypatch.setenv("OUTBOX_META_PREFIX", "signal:meta:")

    import services.outbox.envelope_builder as eb
    from common.decision_trace import DecisionTrace

    # принудительно включаем trace-путь (без зависимости от ENV)
    monkeypatch.setattr(eb, "trace_enabled", lambda: True)

    trace = DecisionTrace(trace_id="T-1", sid="S-1")
    # Simulate set_summary_fields by monkeypatching it
    def fake_set_summary(env_dict, tr):
        env_dict["trace_id"] = "T-1"
        env_dict["trace_summary"] = "gates:conf=OK(0.1ms)"

    monkeypatch.setattr(eb, "set_summary_fields", fake_set_summary)

    env = eb.build_outbox_envelope(
        sid="S-1",
        trace=trace,
        kind="breakout",
        symbol="BTCUSDT",
        notify_payload={"text": "hi", "trace": {"events": [1, 2, 3]}},
        meta={"x": 1, "events": [9], "trace": {"events": [8]}},
    )

    assert env["sid"] == "S-1"
    assert env["meta"]["trace_meta_key"] == "signal:meta:S-1"

    # envelope содержит только summary/id, но НЕ полный trace/events
    assert env.get("trace_id") == "T-1"
    assert env.get("trace_summary") == "gates:conf=OK(0.1ms)"
    assert "trace" not in env
    assert "events" not in env

    # targets/meta тоже не должны содержать trace/events
    notify = env.get("targets", {}).get("notify", {})
    assert isinstance(notify, dict)
    assert "trace" not in notify
    assert "events" not in notify

    meta = env.get("meta", {})
    assert isinstance(meta, dict)
    assert "trace" not in meta
    assert "events" not in meta


def test_build_outbox_envelope_meta_merge_is_json_safe(monkeypatch):
    import services.outbox.envelope_builder as eb
    monkeypatch.setattr(eb, "trace_enabled", lambda: False)

    env = eb.build_outbox_envelope(
        sid="S-2",
        meta={"a": 1, "b": {"c": 2}},
        notify_payload={"ok": True},
    )
    assert env["sid"] == "S-2"
    assert env["targets"]["notify"]["ok"] is True
    assert env["meta"]["a"] == 1
    assert env["meta"]["b"]["c"] == 2
