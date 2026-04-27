from services.ml_confirm_gate import MLConfirmGate
import os

os.environ["ML_CONFIRM_AB_VARIANT"] = "v99"
gate = MLConfirmGate.from_env()
print(gate.ab_variant)
