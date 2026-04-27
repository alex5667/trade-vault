from __future__ import annotations

import os

"""
Strict-mode для "жёстких" инвариантов в dev/CI.

Идея:
  - в проде: fail-open (не падаем, ставим safe defaults + data_quality_flags)
  - в CI/dev: можно включить жесткий контракт и падать, если в ctx/payload протекли NaN/Inf

ENV:
  PIPELINE_STRICT_CONTRACTS=1  -> включить
"""


def strict_contracts_enabled() -> bool:
    v = str(os.getenv("PIPELINE_STRICT_CONTRACTS", "0")).strip().lower()
    return v in {"1", "true", "yes", "on"}
