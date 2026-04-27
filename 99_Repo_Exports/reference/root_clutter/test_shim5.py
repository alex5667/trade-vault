import os
os.makedirs("test_pkg5/tick_flow_full/core", exist_ok=True)
with open("test_pkg5/tick_flow_full/core/__init__.py", "w") as f:
    f.write("print('loaded tick_flow_full.core')\n")
with open("test_pkg5/tick_flow_full/core/foo.py", "w") as f:
    f.write("def x(): print('x')\n")
os.makedirs("test_pkg5/core", exist_ok=True)
with open("test_pkg5/core/__init__.py", "w") as f:
    f.write("""
import sys
import os
_target = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tick_flow_full'))
if _target not in sys.path:
    sys.path.insert(0, _target)
import core
__path__ = core.__path__
""")
