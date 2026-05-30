"""manip_calibrator.py

Core logic for MANIP Gate Autocalibrator.
Calculates dynamic thresholds based on percentile distributions of
manipulation features (layering_score, quote_stuffing_score).
"""
import time
from collections import deque
from dataclasses import dataclass, field
import numpy as np

@dataclass
class _ManipSample:
    ts_ms: int
    layering_score: float
    quote_stuffing_score: float

@dataclass
class SymbolManipStats:
    buf: deque[_ManipSample] = field(default_factory=lambda: deque(maxlen=10000))

    @property
    def n(self) -> int:
        return len(self.buf)

    def evict_old(self, cutoff_ms: int) -> None:
        while self.buf and self.buf[0].ts_ms < cutoff_ms:
            self.buf.popleft()

class ManipCalibrator:
    def __init__(self, window_ms: int = 43_200_000): # 12 hours
        self.window_ms = window_ms
        self.bins: dict[str, SymbolManipStats] = {}

    def observe(self, symbol: str, layering_score: float, quote_stuffing_score: float, ts_ms: int | None = None) -> None:
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)
            
        if symbol not in self.bins:
            self.bins[symbol] = SymbolManipStats()
            
        self.bins[symbol].buf.append(_ManipSample(
            ts_ms=ts_ms,
            layering_score=layering_score,
            quote_stuffing_score=quote_stuffing_score
        ))

    def evict_all(self, now_ms: int | None = None) -> None:
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        cutoff = now_ms - self.window_ms
        for stats in self.bins.values():
            stats.evict_old(cutoff)

    def compute_thresholds(self, symbol: str, min_samples: int = 100) -> dict[str, float] | None:
        if symbol not in self.bins:
            return None
            
        stats = self.bins[symbol]
        if stats.n < min_samples:
            return None

        layering_scores = [s.layering_score for s in stats.buf]
        qs_scores = [s.quote_stuffing_score for s in stats.buf]

        p95_layering = float(np.percentile(layering_scores, 95))
        p99_layering = float(np.percentile(layering_scores, 99))
        
        p95_qs = float(np.percentile(qs_scores, 95))
        p99_qs = float(np.percentile(qs_scores, 99))

        # Dynamic mapping for tighten_bps based on p95 layering severity
        # Baseline: normal layering p95 is ~0.1 - 0.2.
        # If p95 is high (> 0.4), market is toxic -> higher tighten cap.
        # Max cap is 15 bps, min cap is 3.0 bps.
        base_bps = 4.0
        # 1.0 layering score -> +10 bps
        dynamic_tighten = base_bps + (p95_layering * 10.0)
        tighten_bps = max(3.0, min(15.0, dynamic_tighten))

        # Dynamic threshold for gating
        # Normally static 0.6. Here we can set it to p99 + margin
        layering_max = min(0.95, max(0.4, p99_layering * 1.5))
        qs_max = min(0.95, max(0.4, p99_qs * 1.5))

        return {
            "p95_layering": p95_layering,
            "p99_layering": p99_layering,
            "p95_qs": p95_qs,
            "p99_qs": p99_qs,
            "layering_score_max": layering_max,
            "qs_score_max": qs_max,
            "tighten_bps": tighten_bps,
            "n_samples": stats.n
        }

    def dump_state(self, min_samples: int = 100) -> dict[str, dict]:
        res = {}
        for sym in self.bins:
            thr = self.compute_thresholds(sym, min_samples)
            if thr:
                res[sym] = thr
        return res
