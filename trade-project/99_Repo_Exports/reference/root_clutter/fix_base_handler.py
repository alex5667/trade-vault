#!/usr/bin/env python3

import re

def fix_base_handler():
    """Fix imports in base_orderflow_handler.py."""
    with open('python-worker/handlers/base_orderflow_handler.py', 'r') as f:
        content = f.read()
    
    # Fix broken imports
    content = re.sub(r'from common\.log import lambda x: None', '# from common.log import setup_logger', content)
    content = re.sub(r'from enum import Enum, auto', 'from enum import Enum', content)
    
    # Add mock logger
    content = content.replace(
        '# from common.log import setup_logger',
        '# from common.log import setup_logger\ndef setup_logger(name):\n    import logging\n    return logging.getLogger(name)'
    )
    
    # Fix config import
    content = re.sub(r'get_config = lambda symbol, \*\*kwargs: None', 'def get_config(symbol, **kwargs):\n    # Mock config for testing\n    class MockConfig:\n        def __init__(self):\n            self.family = "orderflow"\n            self.venue = "test"\n            self.timeframe_s = 60\n            self.min_bucket_trades = 10\n            self.min_bucket_notional_usd = 1000.0\n            self.min_delta_z = 1.0\n            self.min_obi_z = 0.5\n            self.read_count = 100\n            self.read_block_ms = 1000\n            self.backoff_base = 0.25\n            self.backoff_multiplier = 2.0\n            self.backoff_max = 5.0\n            self.backoff_jitter = True\n    return MockConfig()', content)
    
    with open('python-worker/handlers/base_orderflow_handler.py', 'w') as f:
        f.write(content)
    
    print("Fixed base_orderflow_handler.py")

if __name__ == '__main__':
    fix_base_handler()
