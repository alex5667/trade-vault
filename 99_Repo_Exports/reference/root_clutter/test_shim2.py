import os
os.makedirs("test_pkg2/tick_flow_full/core", exist_ok=True)
with open("test_pkg2/tick_flow_full/core/__init__.py", "w") as f:
    f.write("print('loaded tick_flow_full.core')\n")
with open("test_pkg2/tick_flow_full/core/foo.py", "w") as f:
    f.write("def x(): print('x')\n")
os.makedirs("test_pkg2/core", exist_ok=True)
with open("test_pkg2/core/__init__.py", "w") as f:
    f.write("""
import os
__path__ = [os.path.join(os.path.dirname(os.path.dirname(__file__)), 'tick_flow_full', 'core')]
from tick_flow_full.core import *
""")
