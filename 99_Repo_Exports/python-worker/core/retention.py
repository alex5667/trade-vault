"""
Centralized Redis Stream Retention Policy for Python Worker
Mirrors go-worker/internal/streams/retention.go

Single source of truth for MAXLEN of named streams is STREAM_RETENTION dict
in core/redis_keys.py:

    from core.redis_keys import RS, STREAM_RETENTION
    maxlen = STREAM_RETENTION.get(RS.SIGNAL_DLQ, 2_000)
"""

# ~4h at 15 signals/sec at ~1KB/msg = 60MB
MAXLEN_GLOBAL = 200_000
MAXLEN_CANDLES = 50_000
MAXLEN_PER_SYMBOL = 10_000
MAXLEN_OUTBOX = 50_000
