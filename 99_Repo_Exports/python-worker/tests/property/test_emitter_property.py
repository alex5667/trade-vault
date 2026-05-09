from __future__ import annotations

import pytest

try:
    hyp = pytest.importorskip("hypothesis")
    from hypothesis import given
    from hypothesis import strategies as st
    HAS_HYPOTHESIS = True
except pytest.skip.Exception:
    # hypothesis not available, skip these tests
    hyp = None
    given = lambda *args, **kwargs: lambda f: f  # noop decorator
    class MockSt:
        def one_of(self, *args, **kwargs): return None
        def dictionaries(self, *args, **kwargs): return None
        def text(self, *args, **kwargs): return None
        def integers(self, *args, **kwargs): return None
        def floats(self, *args, **kwargs): return None
        def lists(self, *args, **kwargs): return None
        def none(self): return None
    st = MockSt()
    HAS_HYPOTHESIS = False


def test_import_emitter():
    # Если у вас другой путь — поправьте import здесь.
    from handlers.emitter.unified_signal_emitter import UnifiedSignalEmitter  # noqa: F401


class DummyOutbox:
    def __init__(self):
        self.items = []

    def publish(self, payload):
        self.items.append(payload)


class DummyLogger:
    def exception(self, msg):
        pass


@given(
    labels=st.one_of(
        st.none(),
        st.dictionaries(
            keys=st.text(min_size=0, max_size=10),
            values=st.one_of(
                st.integers(),
                st.text(min_size=0, max_size=20),
                st.floats(allow_nan=True, allow_infinity=True),
                st.none(),
            ),
            max_size=10,
        ),
        st.lists(st.integers(), max_size=10),  # намеренно "плохой" тип
        st.integers(),  # намеренно "плохой" тип
    )
)
def test_emitter_emit_is_total_and_returns_bool(labels):
    """
    Инвариант:
      - emit(...) не падает на "грязных" labels
      - возвращает bool
      - payload["labels"] после emit — dict (fail-open)
    """
    from handlers.emitter.unified_signal_emitter import UnifiedSignalEmitter

    outbox = DummyOutbox()
    logger = DummyLogger()
    em = UnifiedSignalEmitter(outbox=outbox, logger=logger)

    payload = {"kind": "breakout", "symbol": "BTCUSDT", "ts": 123, "signal_id": "sid-x", "level_price": 1.0}
    ok = em.emit(payload, labels=labels, dedup=False)
    assert isinstance(ok, bool)
    assert "labels" not in payload or isinstance(payload["labels"], dict)
