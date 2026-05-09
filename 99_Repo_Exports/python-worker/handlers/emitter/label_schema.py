from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from common.qf_codes import QF as QualityFlag


def qf_labels(flags: Iterable[str | QualityFlag]) -> dict[str, int]:
    """
    Strict labels schema:
      - quality flags: "qf/<code>" => 1
      - system labels are separate ("sys/<name>") and never mixed with qf-codes.
    """
    out: dict[str, int] = {}
    for f in flags or []:
        code = f.value if isinstance(f, QualityFlag) else str(f)
        if not code:
            continue
        out[f"qf/{code}"] = 1
    return out


def sys_labels(**kv: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in kv.items():
        if not k:
            continue
        out[f"sys/{k}"] = v
    return out
