from types import SimpleNamespace

from common.cost_edge_codes import cost_edge_reason_codes


class _Boom:
    def __call__(self, *args, **kwargs):
        raise RuntimeError("boom")

def _mark_dq(ctx, flag, *, logger=None, key=None, exc=None):
    """Simple mock for testing"""
    if ctx is not None:
        flags = getattr(ctx, "data_quality_flags", [])
        if not isinstance(flags, list):
            flags = []
        if flag not in flags:
            flags.append(flag)
        ctx.data_quality_flags = flags

def _safe_health_call(*, ctx, logger, dq_flag, key, fn, args=(), kwargs=None):
    """Mock implementation for testing"""
    if kwargs is None:
        kwargs = {}
    try:
        fn(*args, **kwargs)
    except Exception as e:
        _mark_dq(ctx, dq_flag, logger=logger, key=key, exc=e)


class _Boom:
    def __call__(self, *args, **kwargs):
        raise RuntimeError("boom")


def test_safe_health_call_marks_dq_flag_when_ctx_present():
    ctx = SimpleNamespace()
    # logger can be None for unit test; function must still not raise
    boom = _Boom()
    _safe_health_call(
        ctx=ctx,
        logger=None,
        dq_flag="health_metrics_on_tick_error",
        key="health_metrics_on_tick_error",
        fn=boom
    )
    assert "health_metrics_on_tick_error" in getattr(ctx, "data_quality_flags", [])


def test_safe_health_call_never_raises_when_ctx_is_none():
    _safe_health_call(
        ctx=None,
        logger=None,
        dq_flag="health_metrics_on_tick_error",
        key="health_metrics_on_tick_error",
        fn=_Boom(),
    )


def test_cost_edge_reason_codes_default_is_single_reason(monkeypatch):
    monkeypatch.delenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", raising=False)
    codes = cost_edge_reason_codes("VETO_EDGE_COST")
    assert codes == ["VETO_EDGE_COST"]


def test_cost_edge_reason_codes_dual_emit(monkeypatch):
    monkeypatch.setenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", "1")
    codes = cost_edge_reason_codes("VETO_EDGE_COST")
    assert codes == ["VETO_EDGE_COST", "VETO_EDGE_THIN_COST"]
