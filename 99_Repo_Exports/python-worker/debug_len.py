import sys
import os
sys.path.insert(0, os.getcwd())
from services.ml_scoring_gate import _NUMERIC_FEATURE_ATTRS
print(f"DEBUG: NUMERIC_FEATURES_LEN={len(_NUMERIC_FEATURE_ATTRS)}")
for i, (name, attrs) in enumerate(_NUMERIC_FEATURE_ATTRS):
    print(f"  {i+1}: {name}")
