from .bucketing import make_feature_bucket

# Import database-dependent modules only if available
try:
    from .estimator import SignalQualityEstimator, QualityEstimate
    from .offline_job import run_offline_quality_job
    from .online_job import run_online_quality_job
    __all__ = [
        "make_feature_bucket"
        "SignalQualityEstimator"
        "QualityEstimate"
        "run_offline_quality_job"
        "run_online_quality_job"
    ]
except ImportError:
    # psycopg2 not available, skip database modules
    __all__ = ["make_feature_bucket"]
