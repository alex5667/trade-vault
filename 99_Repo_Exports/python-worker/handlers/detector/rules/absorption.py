from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import os

from handlers.pipeline.candidate import Candidate
from handlers.detector.rules._util import f, nz, infer_side


@dataclass
class AbsorptionRule:
    weak_progress_max: float = float(os.getenv("DET_WEAK_PROGRESS_MAX", "0.35"))
    min_raw: float = float(os.getenv("DET_MIN_RAW_SCORE", "0.10"))

    def detect(self, ctx: Any) -> list[Candidate]:
        price = f(getattr(ctx, "price", None), 0.0)
        if price <= 0:
            return []

        wp = f(getattr(ctx, "weak_progress", None), f(getattr(ctx, "weak_progress_ratio", None), 0.0))
        if not (wp > 0 and wp <= self.weak_progress_max):
            return []

        # событие: "есть хоть что-то" (без veto и без требований 2-of-2 — это уйдёт в валидатор)
        if not any(nz(getattr(ctx, k, False)) for k in ("wall_here", "refill", "mp_contra", "micro_proxy")):
            return []

        side = infer_side(ctx, fallback=0)
        level_price = getattr(ctx, "absorption_level", None) or getattr(ctx, "level_price", None)
        raw = max(self.min_raw, (self.weak_progress_max - wp) / max(self.weak_progress_max, 1e-9))
        return [
            Candidate(
                kind="absorption",
                side=side,
                raw_score=float(raw),
                level_price=float(level_price) if level_price is not None else None,
                level_key=getattr(ctx, "level_key", None),
                reasons=["weak_progress_absorption"],
                meta={"weak_progress": float(wp)},
            )
        ]
