import os
os.makedirs("test_pkg4/tick_flow_full/core", exist_ok=True)
with open("test_pkg4/tick_flow_full/core/__init__.py", "w") as f:
    f.write("print('loaded tick_flow_full.core')\n")
with open("test_pkg4/tick_flow_full/core/foo.py", "w") as f:
    f.write("def x(): print('x')\n")
os.makedirs("test_pkg4/core", exist_ok=True)
with open("test_pkg4/core/__init__.py", "w") as f:
    f.write("""
import os
import sys
# The absolute path to the real 'core' directory
_real_core = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tick_flow_full', 'core'))
__path__ = [_real_core]
import tick_flow_full.core
for k, v in vars(tick_flow_full.core).items():
    if not k.startswith('__'): globals()[k] = v
""")
