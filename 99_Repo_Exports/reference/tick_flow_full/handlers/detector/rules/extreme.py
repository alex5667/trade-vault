from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import os

from handlers.pipeline.candidate import Candidate
from handlers.detector.rules._util import f, infer_side


@dataclass
class ExtremeRule:
    z_extreme: float = float(os.getenv("DET_Z_EXTREME", "3.4"))
    min_raw: float = float(os.getenv("DET_MIN_RAW_SCORE", "0.10"))

    def detect(self, ctx: Any) -> list[Candidate]:
        price = f(getattr(ctx, "price", None), 0.0)
        if price <= 0:
            return []
        z = f(getattr(ctx, "z_delta", None), 0.0)
        if abs(z) < self.z_extreme:
            return []
        side = infer_side(ctx, fallback=(1 if z > 0 else -1))
        raw = max(self.min_raw, abs(z) / max(self.z_extreme, 1e-9)) * 1.25
        return [
            Candidate(
                kind="extreme",
                side=side,
                raw_score=float(raw),
                level_price=None,
                level_key=None,
                reasons=["z_delta_extreme"],
                meta={"z": float(z)},
            )
        ]
