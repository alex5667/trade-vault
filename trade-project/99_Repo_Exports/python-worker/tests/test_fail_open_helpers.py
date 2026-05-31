from types import SimpleNamespace


def test_safe_call_fail_open_appends_dq_flag_on_error():
    from handlers.base_orderflow_handler import _append_flag, _safe_call_fail_open

    class L:
        def __init__(self):
            self.seen = []
        def debug(self, msg, *args):
            self.seen.append((msg, args))

    ctx = SimpleNamespace()
    logger = L()

    def boom():
        raise RuntimeError("x")

    ok = _safe_call_fail_open(
        logger,
        key="k",
        fn=boom,
        ctx=ctx,
        dq_flag="hm_error",
        append_flag=_append_flag,
    )
    assert ok is False
    assert "hm_error" in getattr(ctx, "data_quality_flags", [])


def test_safe_call_fail_open_returns_true_on_success():
    from handlers.base_orderflow_handler import _safe_call_fail_open

    class L:
        def debug(self, msg, *args):
            pass

    ok = _safe_call_fail_open(L(), key="k2", fn=lambda: 1)
    assert ok is True
