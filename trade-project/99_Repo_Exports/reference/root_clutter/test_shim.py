import os
os.makedirs("test_pkg/tick_flow_full/core", exist_ok=True)
with open("test_pkg/tick_flow_full/core/__init__.py", "w") as f:
    f.write("print('loaded tick_flow_full.core')\n")
with open("test_pkg/tick_flow_full/core/foo.py", "w") as f:
    f.write("def x(): print('x')\n")
os.makedirs("test_pkg/core", exist_ok=True)
with open("test_pkg/core/__init__.py", "w") as f:
    f.write("""
import sys
from pathlib import Path
_target = Path(__file__).parent.parent / 'tick_flow_full'
if str(_target) not in sys.path: sys.path.insert(0, str(_target))
import tick_flow_full.core
sys.modules[__name__] = tick_flow_full.core
print('core shim initialized')
""")
