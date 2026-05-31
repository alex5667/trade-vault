# mocks.py - Mock classes for testing and development
# NOT intended for production use



# Mock classes from base_orderflow_handler.py
class L2BookTracker:
    pass


class L3LiteTracker:
    def __init__(self, symbol="", l3_queue_events_proxy=None):
        pass


class L3QueueEventsProxy:
    def __init__(self, bucket_ms=1000):
        pass


class QueueETAEvaluator:
    def __init__(self, eps=1e-8):
        self.eps = eps


class BurstinessTracker:
    def __init__(self, bucket_ms=1000, half_life_short_ms=250, half_life_long_ms=2000,
                 fano_window_buckets=60, dt_alpha=0.05):
        pass


class BurstStats:
    pass


class TouchLevelTracker:
    pass


class TouchSnapshot:
    pass


# Additional infrastructure mocks
class HTFLevelsProvider:
    def get_levels(self, symbol):
        return {}


class HTFLevels:
    def __init__(self):
        pass


class LCStoreV2:
    def __init__(self, redis_client=None, symbol=""):
        pass

    def get_metric_cfg(self, *args):
        return None


def eval_local_quantile(*args):
    return None


# Pipeline mocks
class UnifiedSignalPipeline:
    def __init__(self, *args, **kwargs):
        pass


class GoldenPatternService:
    pass


class CalibrationService:
    pass


class ExecFiltersGroup:
    pass


class SignalPublisher:
    pass


class SignalScoringEngine:
    pass


class ScoringConfig:
    pass


# Signal mocks
class SignalQualityEstimator:
    pass
