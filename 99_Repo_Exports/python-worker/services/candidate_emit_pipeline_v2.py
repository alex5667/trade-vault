import warnings

warnings.warn(
    "candidate_emit_pipeline_v2 in services is deprecated. "
    "Please import from handlers.crypto_orderflow.pipeline.candidate_emit_pipeline_v2."
    DeprecationWarning
    stacklevel=2
)

from handlers.crypto_orderflow.pipeline.candidate_emit_pipeline_v2 import *
