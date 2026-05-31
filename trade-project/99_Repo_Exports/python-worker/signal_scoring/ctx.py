from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class SignalContext:
    ts: datetime
    symbol: str
    side: str          # 'buy' / 'sell'
    session: str       # 'asia' / 'europe' / 'us'
    regime: str        # 'trend' / 'range' / 'mixed'
    pattern_name: str | None = None

    # raw метрики
    delta_spike_z: float | None = None
    obi: float | None = None
    weak_progress: float | None = None   # |range| / ATR metric
    atr_quantile: float | None = None

    # локальные квантильные Z (0..1) после калибровки
    delta_spike_z_local_q: float | None = None
    obi_local_q: float | None = None
    weak_progress_local_q: float | None = None  # уже "инвертированный" q (low лучше)
    atr_local_q: float | None = None

    # итоговый скор и порог
    confidence: int | None = None              # 0..100
    min_confidence_used: int | None = None

    # golden-паттерн
    is_golden_pattern: bool = False
    golden_pattern_label: str | None = None

    # качество сигнала (из исторических данных)
    quality_offline: float | None = None
    quality_online: float | None = None
    quality_combined: float | None = None
    quality_status: str | None = None

    # финальный скор (ConfScore + QualityScore)
    final_score: float | None = None
    is_disabled_by_quality: bool = False

    # скоринг / quality
    score_raw: float = 0.0              # базовый score до golden / pattern_weight
    quality_label: str | None = None  # SignalQualityLabel enum value
    quality_reasons: list[str] = field(default_factory=list)

    # weak progress fields
    progress_score_component: int | None = None  # contribution to confidence from weak progress
    pattern_family: str | None = None            # 'continuation' / 'fade' / 'other'

    # Additional fields for fade patterns
    reverse_delta_spike_z: float | None = None  # confirming reverse impulse
    volume_z: float | None = None               # volume-based impulse strength
