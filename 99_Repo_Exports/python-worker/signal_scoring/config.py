from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class PatternScoringConfig:
    weight: float = 1.0          # мультипликатор к combined_q
    min_confidence: int | None = None  # override CRYPTO_SIGNAL_MIN_CONF(...)


@dataclass
class ScoringConfig:
    min_confidence_default: float = 80.0
    
    # NEW: fields for compatibility with InitializationManager
    confidence_threshold: float = 0.5
    max_score_age_ms: int = 300000

    golden_pattern_min_confidence: float = 90.0

    # веса метрик в комбинированном q
    metric_weights: Dict[str, float] = field(
        default_factory=lambda: {
            "delta_spike_z": 1.0,
            "obi": 0.7,
            "weak_progress": 0.5,
            "atr_quantile": 0.3,
        }
    )

    pattern_config: Dict[str, PatternScoringConfig] = field(default_factory=dict)

    # NEW: вклад ликвидности в скоринг
    liquidity_weight: float = 0.2      # насколько сильно liquidity влияет на суммарный score
    liquidity_hard_floor: float = 0.1  # ниже этого — можно жёстко обрезать/сильно штрафовать
    liquidity_enabled: bool = True
    liquidity_break_floor: float = 0.3      # порог для break паттерна
    liquidity_absorption_kill_floor: float = 0.6  # порог для absorption kill-switch

    @classmethod
    def from_env(cls) -> "ScoringConfig":
        cfg = cls()

        # глобальный минимум
        if "CRYPTO_SIGNAL_MIN_CONF" in os.environ:
            cfg.min_confidence_default = float(os.environ["CRYPTO_SIGNAL_MIN_CONF"])

        # golden threshold
        if "GOLDEN_PATTERN_MIN_CONFIDENCE" in os.environ:
            cfg.golden_pattern_min_confidence = float(
                os.environ["GOLDEN_PATTERN_MIN_CONFIDENCE"]
            )

        # веса метрик: SIGNAL_METRIC_WEIGHT__DELTA_SPIKE_Z=1.0 и т.п.
        for key, value in os.environ.items():
            if key.startswith("SIGNAL_METRIC_WEIGHT__"):
                metric = key.split("__", 1)[1].lower()
                cfg.metric_weights[metric] = float(value)

        # per-pattern config:
        patterns: Dict[str, PatternScoringConfig] = {}

        for key, value in os.environ.items():
            if key.startswith("SIGNAL_PATTERN_WEIGHT__"):
                pattern = key.split("__", 1)[1].lower()
                patterns.setdefault(pattern, PatternScoringConfig()).weight = float(
                    value
                )
            if key.startswith("SIGNAL_PATTERN_MIN_CONF__"):
                pattern = key.split("__", 1)[1].lower()
                patterns.setdefault(pattern, PatternScoringConfig()).min_confidence = (
                    int(value)
                )

        cfg.pattern_config = patterns

        # liquidity parameters
        if "SCORING_LIQUIDITY_WEIGHT" in os.environ:
            cfg.liquidity_weight = float(os.environ["SCORING_LIQUIDITY_WEIGHT"])
        if "SCORING_LIQUIDITY_HARD_FLOOR" in os.environ:
            cfg.liquidity_hard_floor = float(os.environ["SCORING_LIQUIDITY_HARD_FLOOR"])
        if "SCORING_LIQUIDITY_ENABLED" in os.environ:
            cfg.liquidity_enabled = os.environ["SCORING_LIQUIDITY_ENABLED"].lower() in ("1", "true", "yes")
        if "SCORING_LIQUIDITY_BREAK_FLOOR" in os.environ:
            cfg.liquidity_break_floor = float(os.environ["SCORING_LIQUIDITY_BREAK_FLOOR"])
        if "SCORING_LIQUIDITY_ABSORPTION_KILL_FLOOR" in os.environ:
            cfg.liquidity_absorption_kill_floor = float(os.environ["SCORING_LIQUIDITY_ABSORPTION_KILL_FLOOR"])

        return cfg

    def get_min_confidence(self, symbol: str, pattern: str | None) -> int:
        base = self.min_confidence_default

        # 2) pattern-специфичный override, если есть
        if pattern:
            p_key = pattern.lower()
            p_cfg = self.pattern_config.get(p_key)
            if p_cfg and p_cfg.min_confidence is not None:
                return p_cfg.min_confidence

        return base

    def get_pattern_weight(self, pattern: str | None) -> float:
        if not pattern:
            return 1.0
        p_cfg = self.pattern_config.get(pattern.lower())
        if not p_cfg:
            return 1.0
        return p_cfg.weight