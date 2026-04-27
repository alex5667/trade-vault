"""
Centralized Redis Stream Retention Policy for Python Worker
Mirrors go-worker/internal/streams/retention.go

DEPRECATION NOTICE (P0-Fix #1, 2026-04-18):
  MAXLEN_DLQ здесь (50_000) расходился с STREAM_RETENTION[SIGNAL_DLQ]=2_000
  в redis_keys.py в 25x. Единственный source-of-truth для MAXLEN всех
  named streams — STREAM_RETENTION dict в core/redis_keys.py.

  НЕ используйте MAXLEN_DLQ из этого файла для SIGNAL_DLQ и других
  named streams — используйте:
      from core.redis_keys import RS, STREAM_RETENTION
      maxlen = STREAM_RETENTION.get(RS.SIGNAL_DLQ, 2_000)
"""

# ~4h at 15 signals/sec at ~1KB/msg = 60MB
MAXLEN_GLOBAL = 200_000
MAXLEN_CANDLES = 50_000
# DEPRECATED: для DLQ streams используйте STREAM_RETENTION из redis_keys.py (значение 2_000).
MAXLEN_DLQ = 50_000
MAXLEN_PER_SYMBOL = 10_000
MAXLEN_OUTBOX = 50_000
