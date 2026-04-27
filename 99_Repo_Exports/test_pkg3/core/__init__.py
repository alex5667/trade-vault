
import sys
import os
_target = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'tick_flow_full')
if _target not in sys.path:
    sys.path.insert(0, _target)

del sys.modules[__name__]
import core
