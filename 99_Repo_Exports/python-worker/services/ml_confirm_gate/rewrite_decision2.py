import re

with open("python-worker/services/ml_confirm_gate/decision_policy.py") as f:
    text = f.read()

replacements = {
    r'self\._cfg': 'self.gate._cfg',
    r'self\._models': 'self.gate._models',
    r'self\.mode': 'self.gate.mode',
    r'self\._calib_type': 'self.gate._calib_type',
    r'self\._calibrator': 'self.gate._calibrator',
    r'self\._p_min_hard_floor': 'self.gate._p_min_hard_floor',
    r'self\._enforce_share_by_symbol': 'self.gate._enforce_share_by_symbol',
    r'self\._enforce_share_by_sym_by_kind': 'self.gate._enforce_share_by_sym_by_kind',
    # But leave self._build_feature_row, self._conf_from_margin, self._apply_selective alone
}

for pattern, repl in replacements.items():
    text = re.sub(pattern, repl, text)

# Ensure P_MIN >= 0.5 HARD FLOOR for ENFORCE is injected correctly
# I can do it dynamically or just rely on the __init__.py facade to enforce it.
# Wait, let's inject it into _decide_edge_stack_v1 before returning if ENFORCE.
text = re.sub(r'(dec\.p_min = p_min)', r'\1\n        if self.gate.mode == "ENFORCE" and dec.p_min < 0.5:\n            logger.error(f"ML gate: CRITICAL: p_min < 0.5 ({dec.p_min}) in ENFORCE mode. Forcing to 0.5 to prevent silent open.")\n            dec.p_min = 0.5\n', text)

with open("python-worker/services/ml_confirm_gate/decision_policy.py", "w") as f:
    f.write(text)

