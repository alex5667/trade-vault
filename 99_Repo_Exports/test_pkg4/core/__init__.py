
import os
import sys
# The absolute path to the real 'core' directory
_real_core = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tick_flow_full', 'core'))
__path__ = [_real_core]
import tick_flow_full.core
for k, v in vars(tick_flow_full.core).items():
    if not k.startswith('__'): globals()[k] = v
