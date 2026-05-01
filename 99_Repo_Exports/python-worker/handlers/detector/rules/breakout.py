from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import os

from handlers.pipeline.candidate import Candidate
from handlers.detector.rules._util import f, infer_side


@dataclass
class BreakoutRule:
    z_breakout: float = float(os.getenv("DET_Z_BREAKOUT", "2.2"))
    min_raw: float = float(os.getenv("DET_MIN_RAW_SCORE", "0.10"))

    def detect(self, ctx: Any) -> list[Candidate]:
        price = f(getattr(ctx, "price", None), 0.0)
        if price <= 0:
            return []
        z = f(getattr(ctx, "z_delta", None), 0.0)
        if abs(z) < self.z_breakout:
            return []
        side = infer_side(ctx, fallback=(1 if z > 0 else -1))
        level_price = (
            getattr(ctx, "level_price", None)
            or getattr(ctx, "breakout_level", None)
            or getattr(ctx, "pdh", None)
            or getattr(ctx, "pdl", None)
        )
        raw = max(self.min_raw, abs(z) / max(self.z_breakout, 1e-9))
        return [
            Candidate(
                kind="breakout",
                side=side,
                raw_score=float(raw),
                level_price=float(level_price) if level_price is not None else None,
                level_key=getattr(ctx, "level_key", None),
                reasons=["z_delta_breakout"],
                meta={"z": float(z)},
            )
        ]
