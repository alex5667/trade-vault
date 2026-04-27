
import sys
import os
_target = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tick_flow_full'))
if _target not in sys.path:
    sys.path.insert(0, _target)
import core
__path__ = core.__path__
