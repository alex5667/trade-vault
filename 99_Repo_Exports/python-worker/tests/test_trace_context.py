from common.trace_context import ensure_trace_id, get_trace_id_from_env


class Ctx:
    pass


def test_ensure_trace_id_propagates_to_meta_and_ctx():
    ctx = Ctx()
    meta = {}
    tid = ensure_trace_id(ctx=ctx, meta=meta)
    assert tid
    assert ctx.trace_id == tid
    assert meta["trace_id"] == tid


def test_get_trace_id_from_env_meta():
    env = {"meta": {"trace_id": "abc"}}
    assert get_trace_id_from_env(env) == "abc"

