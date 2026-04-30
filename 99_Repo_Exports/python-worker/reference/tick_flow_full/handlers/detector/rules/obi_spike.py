from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import os

from handlers.pipeline.candidate import Candidate
from handlers.detector.rules._util import f, nz, infer_side


@dataclass
class ObiSpikeRule:
    obi_spike_min: float = float(os.getenv("DET_OBI_SPIKE_MIN", "1.8"))
    min_raw: float = float(os.getenv("DET_MIN_RAW_SCORE", "0.10"))

    def detect(self, ctx: Any) -> list[Candidate]:
        price = f(getattr(ctx, "price", None), 0.0)
        if price <= 0:
            return []
        obi = f(getattr(ctx, "obi", None), 0.0)
        sust = nz(getattr(ctx, "obi_sustained", False))
        if not (abs(obi) >= self.obi_spike_min or (sust and abs(obi) >= self.obi_spike_min * 0.8)):
            return []
        side = infer_side(ctx, fallback=0)
        raw = max(self.min_raw, abs(obi) / max(self.obi_spike_min, 1e-9))
        return [
            Candidate(
                kind="obi_spike"
                side=side
                raw_score=float(raw)
                level_price=None
                level_key=None
                reasons=["obi_spike"]
                meta={"obi": float(obi), "obi_sustained": bool(sust)}
            )
        ]
