from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class SignalContext:
    ts: datetime
    symbol: str
    side: str          # 'buy' / 'sell'
    session: str       # 'asia' / 'europe' / 'us'
    regime: str        # 'trend' / 'range' / 'mixed'
    pattern_name: Optional[str] = None

    # raw метрики
    delta_spike_z: Optional[float] = None
    obi: Optional[float] = None
    weak_progress: Optional[float] = None   # |range| / ATR metric
    atr_quantile: Optional[float] = None

    # локальные квантильные Z (0..1) после калибровки
    delta_spike_z_local_q: Optional[float] = None
    obi_local_q: Optional[float] = None
    weak_progress_local_q: Optional[float] = None  # уже "инвертированный" q (low лучше)
    atr_local_q: Optional[float] = None

    # итоговый скор и порог
    confidence: Optional[int] = None              # 0..100
    min_confidence_used: Optional[int] = None

    # golden-паттерн
    is_golden_pattern: bool = False
    golden_pattern_label: Optional[str] = None

    # качество сигнала (из исторических данных)
    quality_offline: Optional[float] = None
    quality_online: Optional[float] = None
    quality_combined: Optional[float] = None
    quality_status: Optional[str] = None

    # финальный скор (ConfScore + QualityScore)
    final_score: Optional[float] = None
    is_disabled_by_quality: bool = False

    # скоринг / quality
    score_raw: float = 0.0              # базовый score до golden / pattern_weight
    quality_label: Optional[str] = None  # SignalQualityLabel enum value
    quality_reasons: List[str] = field(default_factory=list)

    # weak progress fields
    progress_score_component: Optional[int] = None  # contribution to confidence from weak progress
    pattern_family: Optional[str] = None            # 'continuation' / 'fade' / 'other'

    # Additional fields for fade patterns
    reverse_delta_spike_z: Optional[float] = None  # confirming reverse impulse
    volume_z: Optional[float] = None               # volume-based impulse strength