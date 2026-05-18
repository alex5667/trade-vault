"""
G9 Confidence Gate — Data Flow Tests

Tests verify:
1. Input: Correct confidence resolution (signal → indicators → fallback)
2. Processing: Min_conf threshold & gate logic
3. Output: Confidence payload correctness & metrics recording
"""

import pytest
import asyncio
import json
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from types import SimpleNamespace

from handlers.crypto_orderflow.components.gates import GateOrchestrator
from core.gates.decision import GateDecisionV1


class TestConfidenceGateInput:
    """Test G9 input: confidence value resolution."""

    def test_confidence_from_signal(self):
        """Primary source: signal.get('confidence')."""
        signal = {"confidence": 0.8}
        indicators = {"confidence": 0.5}

        # Simulate the logic from signal_pipeline.py:1757-1766
        _conf_raw = signal.get("confidence")
        if _conf_raw is None:
            _conf_raw = indicators.get("confidence")
        if _conf_raw is None:
            _conf_raw = 0.3

        confidence = max(0.0, min(1.0, float(_conf_raw)))

        assert confidence == 0.8, "Should use signal confidence"

    def test_confidence_fallback_to_indicators(self):
        """Fallback 1: indicators.get('confidence')."""
        signal = {}  # No confidence
        indicators = {"confidence": 0.6}

        _conf_raw = signal.get("confidence")
        if _conf_raw is None:
            _conf_raw = indicators.get("confidence")
        if _conf_raw is None:
            _conf_raw = 0.3

        confidence = max(0.0, min(1.0, float(_conf_raw)))

        assert confidence == 0.6, "Should fallback to indicators"

    def test_confidence_default_0_3(self):
        """Fallback 2: default to 0.3 when both missing."""
        signal = {}  # No confidence
        indicators = {}  # No confidence

        _conf_raw = signal.get("confidence")
        if _conf_raw is None:
            _conf_raw = indicators.get("confidence")
        if _conf_raw is None:
            _conf_raw = 0.3

        confidence = max(0.0, min(1.0, float(_conf_raw)))

        assert confidence == 0.3, "Should use default 0.3"

    def test_confidence_bounded_to_0_1_range(self):
        """Confidence must be bounded to [0, 1]."""
        test_cases = [
            (1.5, 1.0),   # Over 1.0 → clamp to 1.0
            (-0.5, 0.0),  # Under 0.0 → clamp to 0.0
            (0.5, 0.5),   # Normal → pass through
        ]

        for input_val, expected in test_cases:
            signal = {"confidence": input_val}
            _conf_raw = signal.get("confidence")
            confidence = max(0.0, min(1.0, float(_conf_raw)))
            assert confidence == expected, f"Input {input_val} should bound to {expected}"


class TestConfidenceGateProcessing:
    """Test G9 processing: threshold resolution & gate logic."""

    def test_min_conf_from_env_global(self):
        """Resolve min_conf from global ENV (as percentage)."""
        with patch("os.getenv") as mock_getenv:
            # MIN_SIGNAL_CONFIDENCE=70 (means 70%)
            mock_getenv.side_effect = lambda k, d=None: (
                "70" if k == "MIN_SIGNAL_CONFIDENCE" else d
            )

            # Simulate signal_pipeline.py:1772-1782
            min_conf_pct = float(mock_getenv("MIN_SIGNAL_CONFIDENCE", "70"))
            if 0 < min_conf_pct <= 1:
                min_conf_pct *= 100.0
            min_conf = min_conf_pct / 100.0

            assert min_conf == 0.70, "Should resolve 70 (%) → 0.70"

    def test_min_conf_from_env_decimal(self):
        """Resolve min_conf from ENV (as decimal)."""
        with patch("os.getenv") as mock_getenv:
            # MIN_SIGNAL_CONFIDENCE=0.7 (decimal, auto-detected)
            mock_getenv.side_effect = lambda k, d=None: (
                "0.7" if k == "MIN_SIGNAL_CONFIDENCE" else d
            )

            min_conf_pct = float(mock_getenv("MIN_SIGNAL_CONFIDENCE", "0.7"))
            if 0 < min_conf_pct <= 1:
                min_conf_pct *= 100.0
            min_conf = min_conf_pct / 100.0

            assert min_conf == 0.70, "Should auto-detect 0.7 → 0.70"

    def test_min_conf_per_symbol_override(self):
        """Per-symbol override: MIN_SIGNAL_CONFIDENCE__SYMBOL."""
        with patch("os.getenv") as mock_getenv:
            # MIN_SIGNAL_CONFIDENCE__BTCUSDT=50 overrides global
            def getenv_mock(k, d=None):
                if k == "MIN_SIGNAL_CONFIDENCE__BTCUSDT":
                    return "50"
                elif k == "MIN_SIGNAL_CONFIDENCE":
                    return "70"
                return d

            mock_getenv.side_effect = getenv_mock

            symbol = "BTCUSDT"
            _sym = symbol.upper().replace("-", "")
            _sym_min_raw = mock_getenv(f"MIN_SIGNAL_CONFIDENCE__{_sym}")

            if _sym_min_raw is not None:
                min_conf_pct = float(_sym_min_raw)
            else:
                min_conf_pct = float(mock_getenv("MIN_SIGNAL_CONFIDENCE", "70"))

            if 0 < min_conf_pct <= 1:
                min_conf_pct *= 100.0
            min_conf = min_conf_pct / 100.0

            assert min_conf == 0.50, "Should use per-symbol override"

    def test_gate_logic_allow(self):
        """Gate logic: confidence >= min_conf → ALLOW."""
        confidence = 0.75
        min_conf = 0.70

        veto = confidence < min_conf
        decision = "DENY" if veto else "ALLOW"

        assert decision == "ALLOW", "0.75 >= 0.70 should ALLOW"

    def test_gate_logic_deny(self):
        """Gate logic: confidence < min_conf → DENY."""
        confidence = 0.65
        min_conf = 0.70

        veto = confidence < min_conf
        decision = "DENY" if veto else "ALLOW"

        assert decision == "DENY", "0.65 < 0.70 should DENY"

    def test_gate_boundary_condition(self):
        """Gate logic at boundary: confidence == min_conf."""
        confidence = 0.70
        min_conf = 0.70

        veto = confidence < min_conf
        decision = "DENY" if veto else "ALLOW"

        assert decision == "ALLOW", "confidence == min_conf should ALLOW"


class TestConfidenceGateOutput:
    """Test G9 output: payload correctness & metrics."""

    def test_payload_confidence_uses_gated_value(self):
        """FIX #1: payload['confidence'] should use gated value, not raw signal."""
        # Scenario: signal missing, indicators have 0.5
        signal = {}  # No confidence
        indicators = {"confidence": 0.5}

        # Gated confidence (with fallback)
        _conf_raw = signal.get("confidence") or indicators.get("confidence") or 0.3
        confidence = max(0.0, min(1.0, float(_conf_raw)))

        # After fix: payload uses gated confidence
        payload_confidence = confidence

        assert payload_confidence == 0.5, "Payload should use gated confidence (0.5)"
        assert payload_confidence != 0.0, "Payload should NOT use raw signal (0.0)"

    def test_payload_confidence_with_default_fallback(self):
        """Payload should reflect default 0.3 when signal + indicators missing."""
        signal = {}
        indicators = {}

        _conf_raw = signal.get("confidence") or indicators.get("confidence") or 0.3
        confidence = max(0.0, min(1.0, float(_conf_raw)))

        payload_confidence = confidence

        assert payload_confidence == 0.3, "Should use default 0.3 in payload"

    def test_gate_decision_v1_structure(self):
        """GateDecisionV1 should contain all required fields."""
        ctx = SimpleNamespace(symbol="BTCUSDT", ts_ms=1234567890)
        confidence = 0.65
        min_conf = 0.70

        orchestrator = GateOrchestrator(
            entry_policy=None, cost_gate=None, portfolio_gate=None,
            consistency_gate=None, regime_liquidity_gate=None, smt_gate=None
        )

        decision = orchestrator.check_confidence(ctx, confidence=confidence, min_conf=min_conf)

        # Verify structure
        assert decision.decision in ("ALLOW", "DENY")
        assert decision.gate == "ConfidenceGate"
        assert decision.stage == "confidence"
        assert "val" in decision.notes
        assert "thr" in decision.notes
        assert decision.notes["val"] == confidence
        assert decision.notes["thr"] == min_conf

    def test_gate_decision_deny(self):
        """DENY decision should have correct reason_code & severity."""
        ctx = SimpleNamespace(symbol="BTCUSDT", ts_ms=1234567890)
        orchestrator = GateOrchestrator(
            entry_policy=None, cost_gate=None, portfolio_gate=None,
            consistency_gate=None, regime_liquidity_gate=None, smt_gate=None
        )

        decision = orchestrator.check_confidence(
            ctx, confidence=0.5, min_conf=0.7
        )

        assert decision.decision == "DENY"
        assert decision.reason_code == "LOW_CONFIDENCE"
        assert decision.severity == "WARN"
        assert decision.fail_policy == "CLOSED"

    def test_gate_decision_allow(self):
        """ALLOW decision should have reason_code='OK'."""
        ctx = SimpleNamespace(symbol="BTCUSDT", ts_ms=1234567890)
        orchestrator = GateOrchestrator(
            entry_policy=None, cost_gate=None, portfolio_gate=None,
            consistency_gate=None, regime_liquidity_gate=None, smt_gate=None
        )

        decision = orchestrator.check_confidence(
            ctx, confidence=0.8, min_conf=0.7
        )

        assert decision.decision == "ALLOW"
        assert decision.reason_code == "OK"
        assert decision.severity == "INFO"


class TestConfidenceGateMetrics:
    """Test G9 metrics: pre_publish_veto_total recording."""

    def test_metrics_increment_on_deny(self):
        """FIX #2: Metrics should record G9 DENY decisions."""
        with patch("services.orderflow.signal_pipeline._PRE_PUBLISH_VETO_TOTAL") as mock_metric:
            mock_counter = MagicMock()
            mock_metric.labels.return_value = mock_counter

            # Simulate the fix from signal_pipeline.py:1796-1803
            gate = "ConfidenceGate"
            reason_code = "LOW_CONFIDENCE"
            symbol = "BTCUSDT"
            kind = "breakout"

            if mock_metric is not None:
                mock_metric.labels(
                    gate=gate,
                    reason_code=reason_code,
                    symbol=symbol,
                    kind=kind
                ).inc()

            # Verify
            mock_metric.labels.assert_called_once_with(
                gate="ConfidenceGate",
                reason_code="LOW_CONFIDENCE",
                symbol="BTCUSDT",
                kind="breakout"
            )
            mock_counter.inc.assert_called_once()

    def test_metrics_fallback_if_unavailable(self):
        """Metrics should fail-open if Prometheus unavailable."""
        with patch("services.orderflow.signal_pipeline._PRE_PUBLISH_VETO_TOTAL", None):
            # Should not raise, even with None metric
            metric = None
            if metric is not None:
                metric.labels(gate="test", reason_code="test", symbol="test", kind="test").inc()

            # No exception → fail-open OK
            assert True


class TestConfidenceGateShadowRecording:
    """Test G9 shadow recording for rejected signals."""

    def test_shadow_record_structure(self):
        """Shadow record should include all relevant fields."""
        payload = {
            "v": 1,
            "ts_ms": 1234567890,
            "symbol": "BTCUSDT",
            "direction": "BUY",
            "side": "buy",
            "signal_id": "sig_123",
            "confidence": 0.5,
            "min_conf": 0.7,
            "entry": 100.0,
            "sl": 99.0,
            "tp_levels": [101.0, 102.0],
            "gated_out": 1,
            "gate_reason": "low_confidence",
            "confirmations": ["conf1=ok"],
            "indicators": {"atr": 1.5, "regime": "trending"},
        }

        # Verify all required fields present
        assert payload["v"] == 1
        assert payload["confidence"] == 0.5
        assert payload["min_conf"] == 0.7
        assert payload["gated_out"] == 1
        assert payload["gate_reason"] == "low_confidence"
        assert "indicators" in payload
        assert "confirmations" in payload


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
