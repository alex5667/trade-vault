#!/usr/bin/env python3

mock_code = '''
# Mock classes for testing
class OrderFlowConfig:
    def __init__(self):
        self.family = "orderflow"
        self.venue = "test"
        self.timeframe_s = 60
        self.min_bucket_trades = 10
        self.min_bucket_notional_usd = 1000.0
        self.min_delta_z = 1.0
        self.min_obi_z = 0.5
        self.read_count = 100
        self.read_block_ms = 1000
        self.backoff_base = 0.25
        self.backoff_multiplier = 2.0
        self.backoff_max = 5.0
        self.backoff_jitter = True

class SymbolSpecs:
    def __init__(self):
        self.price_precision = 2
        self.size_precision = 4

class SignalOutboxPublisher:
    def __init__(self, *args, **kwargs):
        pass
    def publish(self, envelope):
        print(f"Mock publish: {envelope}")
        return type('Result', (), {'sent': True, 'dedup': False})()

class OutboxSettings:
    pass

class HTFLevelsProvider:
    def get_levels(self, symbol):
        return {}

class HTFLevels:
    def __init__(self):
        pass

class CoreSignalContext:
    def __init__(self):
        self.symbol = "TEST"
        self.session = "mixed"
        self.regime_label = "mixed"
        self.metrics = {}
        self.calibrated = {}

class LocalCalibrationManager:
    def get_metric_cfg(self, *args):
        return None

class Signal:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

class UnifiedSignalFormatter:
    @staticmethod
    def create_signal(**kwargs):
        return Signal(**kwargs)

def create_signal(**kwargs):
    return Signal(**kwargs)

def get_session_from_ts(ts):
    return "mixed"

def classify_regime(*args):
    from handlers.signal_types import MarketRegime
    return MarketRegime.UNKNOWN
'''

with open('python-worker/handlers/base_orderflow_handler.py', 'r') as f:
    content = f.read()

# Replace all core imports with mocks
content = content.replace(
    'from core.instrument_config import OrderFlowConfig, SymbolSpecs, lambda symbol, **kwargs: None',
    'from core.instrument_config import OrderFlowConfig, SymbolSpecs, get_config'
)

content = content.replace(
    'from core.signal_outbox import SignalOutboxPublisher, OutboxSettings',
    '# from core.signal_outbox import SignalOutboxPublisher, OutboxSettings'
)

content = content.replace(
    '# from core.redis_stream_consumer import ...',
    '# from core.redis_stream_consumer import ...'
)

content = content.replace(
    'from core.htf_levels import HTFLevelsProvider, HTFLevels',
    '# from core.htf_levels import HTFLevelsProvider, HTFLevels'
)

content = content.replace(
    'from core.signal_context import SignalContext as CoreSignalContext',
    '# from core.signal_context import SignalContext as CoreSignalContext'
)

content = content.replace(
    'from core.local_calibration import LocalCalibrationManager',
    '# from core.local_calibration import LocalCalibrationManager'
)

content = content.replace(
    'from core.unified_signal_formatter import Signal, UnifiedSignalFormatter, create_signal',
    '# from core.unified_signal_formatter import Signal, UnifiedSignalFormatter, create_signal'
)

content = content.replace(
    'from core.sessions import get_session_from_ts',
    '# from core.sessions import get_session_from_ts'
)

content = content.replace(
    'from core.regime import classify_regime',
    '# from core.regime import classify_regime'
)

# Add mocks at the end
content += '\n\n' + mock_code

with open('python-worker/handlers/base_orderflow_handler.py', 'w') as f:
    f.write(content)

print("Added mocks to base_orderflow_handler.py")
