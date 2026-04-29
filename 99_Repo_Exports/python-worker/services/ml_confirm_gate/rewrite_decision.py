import os

with open("python-worker/services/ml_confirm_gate/decision_policy.py", "r") as f:
    content = f.read()

header = """import math
import logging
from typing import Any, Dict, List, Optional
import numpy as np

from .dto import MLConfirmDecision
from .feature_builder import build_feature_row

logger = logging.getLogger("ml_confirm_gate.decision")

class DecisionPolicy:
    def __init__(self, gate):
        # We hold a reference to the main facade or its fields
        self.gate = gate

    def _build_feature_row(self, *args, **kwargs):
        # Delegate to pure function
        return build_feature_row(*args, **kwargs, forbid_scenario_v4_onehot=getattr(self.gate, "_forbid_scenario_v4_onehot", False))

"""

# Write it out
with open("python-worker/services/ml_confirm_gate/decision_policy.py", "w") as f:
    f.write(header + content)

import sys
with open("python-worker/services/ml_confirm_gate/decision_policy.py", "a") as f:
    f.write("""
    @staticmethod
    def _conf_from_margin(p_margin: float) -> float:
        try:
            return float(1.0 - math.exp(-abs(float(p_margin))))
        except Exception:
            return 0.0

    def _apply_selective(self, dec: MLConfirmDecision, *, ok_rule: int) -> None:
        if self.gate.mode != "ENFORCE" or int(ok_rule) != 1:
            if self.gate.mode == "SHADOW":
                dec.status = dec.status or "SHADOW"
            return
        if dec.error:
            dec.status = dec.status or "ERR"
            return
        if dec.missing:
            return
        band = float(self.gate._abstain_band or 0.0)
        p_min = float(self.gate._cfg.get("p_min", 0.5)) if getattr(self.gate, "_cfg", None) else 0.5
        if band > 0.0 and abs(float(dec.p_margin)) <= band:
            dec.abstain = True
            dec.allow = True
            dec.status = "ABSTAIN_BAND"
            dec.reason = f"ml_abstain_band(margin={dec.p_margin:.6f},band={band:.6f})"
            return
        cmin = float(self.gate._conf_min or 0.0)
        if cmin > 0.0 and float(dec.conf) < cmin:
            dec.abstain = True
            dec.allow = True
            dec.status = "ABSTAIN_LOWCONF"
            dec.reason = f"ml_abstain_lowconf(conf={dec.conf:.6f},min={cmin:.6f})"

""")
