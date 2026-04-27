from __future__ import annotations

from types import SimpleNamespace
import pytest


class _FixedCalibrator:
    def __init__(self, value: float) -> None:
        self._v = float(value)

    def calibrate(self, *, symbol: str, kind: str, final_score: float) -> float:
        return float(self._v)


def test_pipeline_sets_confidence_and_overrides_publisher():
    # Import pipeline
    from signals.unified_pipeline import UnifiedSignalPipeline

    published = []

    class Publisher:
        def build_payload(self, *, ctx, result):
            # Malicious/buggy publisher tries to set confidence -> pipeline must delete + override.
            return {
                "kind": "breakout",
                "side": 1,
                "symbol": getattr(ctx, "symbol", None),
                "ts": getattr(ctx, "ts", None),
                "price": getattr(ctx, "price", None),
                "raw_score": 3.0,
                "final_score": 1.5,
                "confidence": 1.0,  # WRONG: should never be here
                "signal_id": "sid-1",
                "parts": {},
                "reasons": [],
            }

        def publish(self, payload):
            published.append(payload)
            return True

    class ScoringEngine:
        def score(self, ctx):
            # Minimal ScoringResult contract used by pipeline:
            return SimpleNamespace(
                should_emit=True,
                kind="breakout",
                side=1,
                raw_score=3.0,
                final_score=1.5,
                signal_id="sid-1",
                reasons=[],
                parts={},
            )

    # Unused deps in this test; pipeline must not hard-require their methods.
    regime_service = object()
    golden_logic = object()
    exec_filters = object()

    pipe = UnifiedSignalPipeline(
        ScoringEngine(),
        regime_service,
        golden_logic,
        exec_filters,
        Publisher(),
        calibrator=_FixedCalibrator(42.0),
    )

    ctx = SimpleNamespace(symbol="BTCUSDT", ts=123_000, price=100.0)
    pipe.process(ctx)

    assert len(published) == 1
    payload = published[0]

    # Pipeline is the only writer of confidence and must override publisher.
    assert payload.get("confidence") == 42.0
    assert "final_score" in payload and payload["final_score"] == 1.5


def test_pipeline_fallback_confidence_without_calibrator():
    from signals.unified_pipeline import UnifiedSignalPipeline

    published = []

    class Publisher:
        def build_payload(self, *, ctx, result):
            return {
                "kind": "extreme",
                "side": -1,
                "symbol": getattr(ctx, "symbol", None),
                "ts": getattr(ctx, "ts", None),
                "price": getattr(ctx, "price", None),
                "raw_score": 10.0,
                "final_score": 77.7,
                "signal_id": "sid-2",
                "parts": {},
                "reasons": [],
            }

        def publish(self, payload):
            published.append(payload)
            return True

    class ScoringEngine:
        def score(self, ctx):
            return SimpleNamespace(
                should_emit=True,
                kind="extreme",
                side=-1,
                raw_score=10.0,
                final_score=77.7,
                signal_id="sid-2",
                reasons=[],
                parts={},
            )

    pipe = UnifiedSignalPipeline(
        ScoringEngine(),
        object(),
        object(),
        object(),
        Publisher(),
        calibrator=None,
    )

    ctx = SimpleNamespace(symbol="ETHUSDT", ts=999, price=200.0)
    pipe.process(ctx)

    assert len(published) == 1
    payload = published[0]
    # Fallback maps |final_score| to <= 95
    assert 0.0 <= float(payload.get("confidence", -1.0)) <= 95.0
    assert payload["confidence"] == 77.7


def test_pipeline_strict_single_writer_raises(monkeypatch):
    """
    STRICT_CONFIDENCE_SINGLE_WRITER=1:
      Any attempt by publisher to set payload['confidence'] must hard-fail.
    This is the "1/1024" guardrail that prevents silent drift between pipeline/publisher.
    """
    monkeypatch.setenv("STRICT_CONFIDENCE_SINGLE_WRITER", "1")
    from signals.unified_pipeline import UnifiedSignalPipeline

    class Publisher:
        def build_payload(self, *, ctx, result):
            return {
                "kind": "breakout",
                "side": 1,
                "symbol": getattr(ctx, "symbol", None),
                "ts": getattr(ctx, "ts", None),
                "price": getattr(ctx, "price", None),
                "raw_score": 3.0,
                "final_score": 1.5,
                "confidence": 13.37,  # forbidden in strict mode
                "signal_id": "sid-3",
                "parts": {},
                "reasons": [],
            }

        def publish(self, payload):
            return True

    class ScoringEngine:
        def score(self, ctx):
            return SimpleNamespace(
                should_emit=True,
                kind="breakout",
                side=1,
                raw_score=3.0,
                final_score=1.5,
                signal_id="sid-3",
                reasons=[],
                parts={},
            )

    pipe = UnifiedSignalPipeline(
        ScoringEngine(),
        object(),
        object(),
        object(),
        Publisher(),
        calibrator=_FixedCalibrator(42.0),
    )

    ctx = SimpleNamespace(symbol="BTCUSDT", ts=123_000, price=100.0)
    with pytest.raises(ValueError, match="pipeline-only field"):
        pipe.process(ctx)
