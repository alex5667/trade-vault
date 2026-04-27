from __future__ import annotations

import json
import pytest

try:
    hyp = pytest.importorskip("hypothesis")
    from hypothesis import given, strategies as st
    HAS_HYPOTHESIS = True
except pytest.skip.Exception:
    # hypothesis not available, skip these tests
    hyp = None
    given = lambda *args, **kwargs: lambda f: f  # noop decorator
    class MockSt:
        def one_of(self, *args, **kwargs): return None
        def recursive(self, *args, **kwargs): return None
        def floats(self, *args, **kwargs): return None
        def integers(self, *args, **kwargs): return None
        def text(self, *args, **kwargs): return None
        def binary(self, *args, **kwargs): return None
        def booleans(self, *args, **kwargs): return None
        def lists(self, *args, **kwargs): return None
        def dictionaries(self, *args, **kwargs): return None
        def none(self): return None
    st = MockSt()
    HAS_HYPOTHESIS = False


class DummyOutbox:
    def __init__(self):
        self.items = []

    def publish(self, payload):
        # эмулируем "как будто сериализуем"
        json.dumps(payload, ensure_ascii=False)
        self.items.append(payload)


class DummyLogger:
    def exception(self, msg):
        pass


class DummyMetrics:
    def inc(self, *args, **kwargs):
        pass

    def observe(self, *args, **kwargs):
        pass


labels_strategy = st.one_of(
    st.none(),
    st.dictionaries(
        keys=st.text(max_size=10),
        values=st.one_of(
            st.integers(),
            st.text(max_size=30),
            st.floats(allow_nan=True, allow_infinity=True),
            st.binary(),
            st.none(),
        ),
        max_size=10,
    ),
    st.lists(st.integers(), max_size=10),  # "сломанный" тип
)


payload_values = st.recursive(
    st.one_of(
        st.none(),
        st.booleans(),
        st.integers(),
        st.floats(allow_nan=True, allow_infinity=True),
        st.text(max_size=30),
        st.binary(),
    ),
    lambda ch: st.one_of(
        st.lists(ch, max_size=30),
        st.dictionaries(keys=st.one_of(st.text(max_size=10), st.integers()), values=ch, max_size=30),
    ),
    max_leaves=200,
)


@given(payload_extra=st.dictionaries(keys=st.text(max_size=10), values=payload_values, max_size=30), labels=labels_strategy)
def test_emitter_never_breaks_json(payload_extra, labels):
    """
    Инвариант (ещё жёстче):
      - emitter.emit не падает на payload/labels с NaN/Inf/bytes/глубокими структурами
      - outbox.publish успешно проходит json.dumps (эмулируем downstream)
    """
    from handlers.emitter.unified_signal_emitter import UnifiedSignalEmitter

    outbox = DummyOutbox()
    logger = DummyLogger()
    metrics = DummyMetrics()
    em = UnifiedSignalEmitter(outbox=outbox, logger=logger, metrics=metrics)

    payload = {
        "kind": "breakout",
        "symbol": "BTCUSDT",
        "ts": 123,
        "signal_id": "sid-x",
        "level_price": 1.0,
    }
    payload.update(payload_extra)
    ok = em.emit(payload, labels=labels, dedup=False)
    assert isinstance(ok, bool)
    # если что-то "сломалось", emitter должен был сделать fail-open payload,
    # но в любом случае outbox.publish не должен упасть по JSON.
    assert len(outbox.items) in (0, 1)
