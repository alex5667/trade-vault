
import sys
from pathlib import Path
_target = Path(__file__).parent.parent / 'tick_flow_full'
if str(_target) not in sys.path: sys.path.insert(0, str(_target))
import tick_flow_full.core
sys.modules[__name__] = tick_flow_full.core
print('core shim initialized')
