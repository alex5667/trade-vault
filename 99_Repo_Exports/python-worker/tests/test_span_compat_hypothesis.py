import pytest


def test_span_ms_proxy_works_in_all_modes():
    from common.decision_trace import Span

    sp = Span()
    # float(sp.ms)
    _ = float(sp.ms)
    # sp.ms()
    _ = sp.ms()

    # context-manager usage: with Span() as sp: ... duration_ms=sp.ms
    with Span() as sp2:
        pass
    _ = float(sp2.ms)
    _ = sp2.ms()
