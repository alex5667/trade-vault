import json

def test_env_trace_is_always_bounded(dispatcher, r):
    sid = "sid_trace_bound"
    env = {"sid": sid, "targets": {"notify": {"sid": sid}}, "meta": {}}
    dispatcher._deliver_targets_with_retry(env, sid, targets=["notify"])
    raw = json.dumps(env.get("trace") or {}, ensure_ascii=False, separators=(",", ":")).encode("utf-8", "ignore")
    assert len(raw) <= 16_000
