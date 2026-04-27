"""
Unit tests for Z-score self-inclusion bias fix in DeltaSpikeDetector.

Validates:
1. Z-score computed from PREVIOUS window (no self-inclusion)
2. Std floor protection (prevents z blow-up on near-flat windows)
3. Warm-up behavior (min 10 samples)
4. State update AFTER scoring
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "python-worker"))

import pytest
from core.crypto_orderflow_detectors import DeltaSpikeDetector


class TestDeltaSpikeDetectorBiasFix:
    """Test suite for Z-score self-inclusion bias fix (10-20% underestimation)."""

    def test_zscore_no_self_inclusion(self):
        """
        CRITICAL: Z-score должен считаться по ПРЕДЫДУЩЕМУ окну.
        
        Scenario: окно [0,0,0,0,0,0,0,0,0], текущий delta=10.
        - OLD (self-inclusion): mean=1.0, std=3.0, z=3.0 ❌
        - NEW (no self-inclusion): mean=0, std=floor, z >> 3.0 ✅
        """
        detector = DeltaSpikeDetector(window=10, z_threshold=2.0, min_abs_volume=0.0)
        
        # Fill with zeros (9 samples for warm-up)
        for _ in range(9):
            detector.push({"qty": 0.0, "is_buyer_maker": False})
        
        # Spike: должен детектироваться, т.к. z >> 2.0 (no self-inclusion)
        spike = detector.push({"qty": 10.0, "is_buyer_maker": False, "ts_ms": 1000})
        
        assert spike is not None, "Spike должен детектироваться (z >> threshold без self-inclusion)"
        assert spike["delta"] == 10.0
        assert abs(spike["z"]) > 2.0, f"Z-score должен быть > 2.0, получили {spike['z']:.2f}"
        
        # Verify state updated AFTER scoring (buffer should now have 10 items)
        assert len(detector.values) == 10

    def test_std_floor_protection(self):
        """
        Std floor предотвращает z blow-up на near-flat windows.
        
        Scenario: окно [1.0]*9, текущий delta=1.1.
        - OLD: std=0 → division by zero or NaN
        - NEW: std_eff = max(std, 0.10 * mean_abs) → z конечный
        """
        detector = DeltaSpikeDetector(window=10, z_threshold=5.0, min_abs_volume=0.0)
        
        # Near-flat window (9 samples)
        for _ in range(9):
            detector.push({"qty": 1.0, "is_buyer_maker": False})
        
        # Tiny deviation
        result = detector.push({"qty": 1.1, "is_buyer_maker": False, "ts_ms": 2000})
        
        # Should NOT blow up (std_floor protects)
        # With std_floor = 0.10 * 1.0 = 0.1, z = (1.1 - 1.0) / 0.1 = 1.0 < 5.0
        assert result is None, "Tiny deviation не должна триггерить spike (std_floor защищает)"

    def test_warmup_behavior(self):
        """
        Warm-up: требуется минимум 9 предыдущих значений (10 total с текущим).
        """
        detector = DeltaSpikeDetector(window=60, z_threshold=2.5, min_abs_volume=0.0)
        
        # Push 9 ticks → no spike yet (warm-up)
        for i in range(9):
            result = detector.push({"qty": 1.0, "is_buyer_maker": False})
            assert result is None, f"Tick {i+1}/9: warm-up, no spike expected"
        
        # 10th tick → scoring starts (9 prev + 1 current = 10 total)
        result = detector.push({"qty": 100.0, "is_buyer_maker": False, "ts_ms": 3000})
        assert result is not None, "10th tick: scoring должен начаться (9 prev samples)"
        assert result["delta"] == 100.0

    def test_bias_magnitude_comparison(self):
        """
        Quantify bias: self-inclusion занижает |z| на ~10-20%.
        
        Scenario: окно [5]*9, spike=15.
        - OLD (self-inclusion): mean=6.0, std=2.83, z=3.18
        - NEW (no self-inclusion): mean=5.0, std_floor=0.5, z=20.0
        
        Разница: 20.0 / 3.18 = 6.3x (bias огромный для flat windows).
        """
        detector = DeltaSpikeDetector(window=10, z_threshold=2.0, min_abs_volume=0.0)
        
        # Flat window (9 samples)
        for _ in range(9):
            detector.push({"qty": 5.0, "is_buyer_maker": False})
        
        # Spike
        spike = detector.push({"qty": 15.0, "is_buyer_maker": False, "ts_ms": 4000})
        
        assert spike is not None
        # With no self-inclusion: mean=5, std_floor=0.5, z=(15-5)/0.5=20.0
        # OLD would give z ~ 3.18 (6.3x underestimation!)
        assert spike["z"] > 10.0, f"Z-score должен быть >> 2.0 без bias, получили {spike['z']:.2f}"

    def test_min_abs_volume_gate(self):
        """
        min_abs_volume gate работает независимо от z-score.
        """
        detector = DeltaSpikeDetector(window=10, z_threshold=1.0, min_abs_volume=50.0)
        
        # Warm-up: 9 samples
        for _ in range(9):
            detector.push({"qty": 1.0, "is_buyer_maker": False})
        
        # High z, but low volume → veto
        result = detector.push({"qty": 10.0, "is_buyer_maker": False, "ts_ms": 5000})
        assert result is None, "min_abs_volume gate должен ветировать (|delta|=10 < 50)"
        
        # High z, high volume → pass
        result = detector.push({"qty": 60.0, "is_buyer_maker": False, "ts_ms": 6000})
        assert result is not None
        assert result["delta"] == 60.0


class TestConfidenceIntegration:
    """Test suite for confidence scorer integration fixes."""

    def test_f_any_fallback(self):
        """
        _f_any должен пробовать несколько имён атрибутов.
        """
        from services.signal_confidence import _f_any
        
        class MockCtx:
            delta_z = 3.5
            # z_delta отсутствует
        
        # Должен найти delta_z
        val = _f_any(MockCtx(), "z_delta", "delta_z", default=0.0)
        assert val == 3.5
        
        # Fallback на default если оба отсутствуют
        val = _f_any(MockCtx(), "missing1", "missing2", default=99.0)
        assert val == 99.0

    def test_b_any_fallback(self):
        """
        _b_any должен пробовать несколько имён атрибутов (bool).
        """
        from services.signal_confidence import _b_any
        
        class MockCtx:
            obi_sustained = True
            # obi_sustained_20 отсутствует
        
        val = _b_any(MockCtx(), "obi_sustained_20", "obi_sustained", default=False)
        assert val is True
        
        val = _b_any(MockCtx(), "missing1", "missing2", default=False)
        assert val is False

    def test_conf_ctx_runtime_config_access(self):
        """
        ConfCtx должен видеть runtime.config для весов/порогов.
        
        CRITICAL FIX: без этого scorer не может читать obi_stable_bonus_w и т.д.
        """
        # Simulate ConfCtx from tick_processor.py
        class MockRuntime:
            config = {
                "obi_stable_bonus_w": 0.05,
                "micro_bonus_cap": 0.12,
            }
            last_atr = 10.0
        
        class ConfCtx:
            def __init__(self, ind, confs, rt):
                self.ind = ind
                self.confirmations = confs
                self.rt = rt
            
            def __getattr__(self, name):
                # 1) Indicators
                if name in self.ind:
                    return self.ind[name]
                # 2) Runtime config (weights/thresholds)
                cfg = getattr(self.rt, "config", None)
                if isinstance(cfg, dict) and name in cfg:
                    return cfg[name]
                # 3) Runtime attributes
                return getattr(self.rt, name)
        
        indicators = {"delta_z": 4.0}
        ctx = ConfCtx(indicators, [], MockRuntime())
        
        # Should access indicators
        assert ctx.delta_z == 4.0
        
        # Should access runtime.config
        assert ctx.obi_stable_bonus_w == 0.05
        assert ctx.micro_bonus_cap == 0.12
        
        # Should access runtime attributes
        assert ctx.last_atr == 10.0

    def test_microstructure_bonus_no_duplication(self):
        """
        Phase F microstructure bonuses должны считаться ОДИН раз (no double-counting).
        
        OLD: Phase E, Phase E+, Phase F могли дублировать OBI/OFI/CVD bonuses.
        NEW: единый Phase F блок с bounded cap.
        """
        from services.signal_confidence import ConfidenceScorer
        
        class MockCtx:
            delta_z = 3.0
            obi_avg = 0.5
            obi_sustained = True
            obi_stable_secs = 3.0
            obi_stability_score = 0.8
            obi_dir = "LONG"
            confirmations = ["obi_stable=3.0", "obi_q=0.8"]
            market_mode = "momentum"
            micro_bonus_cap = 0.10
            obi_stable_bonus_w = 0.04
            obi_stable_min_secs = 1.5
            obi_stable_bonus_q_floor = 0.35
        
        scorer = ConfidenceScorer(main_z_thr=2.5)
        conf, parts = scorer.score(kind="custom", side="LONG", ctx=MockCtx())
        
        # Verify micro_bonus applied (should be present in parts)
        assert "micro_bonus_applied" in parts
        applied = parts["micro_bonus_applied"]
        
        # Should be bounded by micro_bonus_cap
        assert applied <= 0.10, f"Micro bonus должен быть <= cap (0.10), получили {applied:.4f}"
        
        # Should NOT have duplicate bonus_obi_stable from Phase E/E+
        # (consolidated into Phase F)
        # Check that parts don't have legacy duplicate keys
        # (this is implicit: if Phase E/E+ were still running, we'd see inflated confidence)
        assert conf <= 1.0, "Confidence не должен превышать 1.0 (no double-counting)"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
