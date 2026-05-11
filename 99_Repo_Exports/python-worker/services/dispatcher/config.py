"""
Configuration for SignalDispatcher.

Extracted from SignalDispatcher.__init__ to reduce initialization complexity.
All configuration values are loaded from environment variables with sensible defaults.
"""

import os

from core.redis_keys import RedisKeyPrefixes as RK
from core.redis_keys import RedisStreams as RS


class SignalDispatcherConfig:
    """
    Configuration for SignalDispatcher loaded from environment variables.
    
    This reduces the __init__ method complexity by centralizing all ENV-based configuration.
    All values are loaded at instantiation time from environment variables.
    """

    def __init__(self):
        """Load all configuration from environment variables."""
        # Streams and groups
        self.outbox_stream = os.getenv("SIGNAL_OUTBOX_STREAM", RS.SIGNAL_OUTBOX)
        self.dlq_stream = os.getenv("SIGNAL_DLQ_STREAM", RS.SIGNAL_DLQ)
        self.dlq_notify = os.getenv("SIGNAL_DLQ_NOTIFY_STREAM", RS.DLQ_SIGNAL_NOTIFY)
        self.dlq_signal_stream = os.getenv("SIGNAL_DLQ_SIGNAL_STREAM", RS.DLQ_SIGNAL_STREAM)
        self.dlq_audit = os.getenv("SIGNAL_DLQ_AUDIT_STREAM", RS.DLQ_SIGNAL_AUDIT)
        self.dlq_manual = os.getenv("SIGNAL_DLQ_MANUAL_STREAM", RS.DLQ_SIGNAL_MANUAL)
        self.dlq_snapshot = os.getenv("SIGNAL_DLQ_SNAPSHOT_STREAM", RS.DLQ_SIGNAL_SNAPSHOT)
        self.snapshot_stream = os.getenv("SIGNAL_SNAPSHOT_STREAM", RS.DECISION_SNAPSHOT)

        self.group = os.getenv("SIGNAL_OUTBOX_GROUP", "signals-outbox-group")
        self.consumer = os.getenv("SIGNAL_OUTBOX_CONSUMER", f"dispatcher-{os.getpid()}")
        self.read_count = int(os.getenv("SIGNAL_OUTBOX_READ_COUNT", "200"))
        self.read_block_ms = int(os.getenv("SIGNAL_OUTBOX_READ_BLOCK_MS", "1000"))

        # DONE markers
        self.msg_done_prefix = os.getenv("SIGNAL_OUTBOX_MSG_DONE_PREFIX", "signal:outbox:done:v2")
        self.done_ttl_sec = int(os.getenv("SIGNAL_OUTBOX_DONE_TTL_SEC", "86400"))

        # Diagnostics
        self.diag_stream = os.getenv("SIGNAL_DIAG_STREAM", RS.SIGNAL_DIAG)
        self.diag_maxlen = int(os.getenv("SIGNAL_DIAG_MAXLEN", "20000"))
        self.diag_sample = float(os.getenv("SIGNAL_DIAG_SAMPLE", "0.05"))
        self.diag_every_sec = int(os.getenv("SIGNAL_DIAG_EVERY_SEC", "10"))

        # Trace settings
        self.trace_store_enabled = os.getenv("DECISION_TRACE_STORE_ENABLED", "1").lower() not in {"0", "false", "no"}
        self.trace_diag_enabled = os.getenv("DECISION_TRACE_DIAG_ENABLED", "1").lower() not in {"0", "false", "no"}
        self.outbox_meta_prefix = os.getenv("OUTBOX_META_PREFIX", "signal:meta:")
        self.outbox_meta_ttl_sec = int(os.getenv("SIGNAL_OUTBOX_META_TTL_SEC", "86400"))
        self.trace_sidecar_update_on_success = bool(int(os.getenv("DECISION_TRACE_SIDECAR_UPDATE_ON_SUCCESS", "0") or "0"))
        self.trace_sidecar_success_sample_rate = float(os.getenv("DECISION_TRACE_SIDECAR_SUCCESS_SAMPLE_RATE", "0.02") or "0.02")
        self.trace_log_sample_rate = float(os.getenv("DECISION_TRACE_LOG_SAMPLE_RATE", "0.02") or "0.02")
        self.trace_env_max_events = int(os.getenv("DECISION_TRACE_ENV_MAX_EVENTS", "64"))
        self.trace_env_max_bytes = int(os.getenv("DECISION_TRACE_ENV_MAX_BYTES", "16000"))

        # Marker namespaces
        self.marker_prefix = os.getenv("SIGNAL_DELIVERY_MARKER_PREFIX", "signal:deliver:v2")
        self.env_done_prefix = os.getenv("SIGNAL_ENV_DONE_PREFIX", "signal:env_done:v2")
        self.marker_gc_zset = os.getenv("SIGNAL_DELIVERY_GC_ZSET", "zset:signal:deliver:gc")
        self.done_gc_zset = os.getenv("SIGNAL_DONE_GC_ZSET", "zset:signal:done:gc")

        # Lease settings
        self.sid_lease_prefix = os.getenv("SIGNAL_OUTBOX_SID_LEASE_PREFIX", "outbox:sid_lease:v1")
        self.sid_lease_ttl_ms = int(os.getenv("SIGNAL_OUTBOX_SID_LEASE_TTL_MS", "120000"))
        self.sid_lease_extend_every_ms = int(os.getenv("SIGNAL_OUTBOX_SID_LEASE_EXTEND_EVERY_MS", "30000"))
        self.msg_lease_prefix = os.getenv("SIGNAL_OUTBOX_MSG_LEASE_PREFIX", "outbox:msg_lease:v1")
        self.msg_lease_ttl_ms = int(os.getenv("SIGNAL_OUTBOX_MSG_LEASE_TTL_MS", "60000"))
        self.msg_lease_extend_every_ms = int(os.getenv("SIGNAL_OUTBOX_MSG_LEASE_EXTEND_EVERY_MS", "15000"))
        self.lock_ttl_ms = int(os.getenv("SIGNAL_OUTBOX_LOCK_TTL_MS", "10000"))

        # Retry settings
        self.retry_stream = os.getenv("SIGNAL_RETRY_STREAM", "stream:signals:retry")
        self.retry_group = os.getenv("SIGNAL_RETRY_GROUP", "signals-retry-group")
        self.retry_consumer = os.getenv("SIGNAL_RETRY_CONSUMER", f"retry-{os.getpid()}")
        self.retry_read_count = int(os.getenv("SIGNAL_RETRY_READ_COUNT", "10"))
        self.retry_read_block_ms = int(os.getenv("SIGNAL_RETRY_READ_BLOCK_MS", "200"))
        self.retry_dedup_prefix = os.getenv("SIGNAL_RETRY_DEDUP_PREFIX", "retry:scheduled")
        self.retry_dedup_ttl_sec = int(os.getenv("SIGNAL_RETRY_DEDUP_TTL_SEC", "1800"))
        self.max_attempts = int(os.getenv("SIGNAL_MAX_ATTEMPTS", "3"))
        self.retry_base_ms = int(os.getenv("SIGNAL_RETRY_BASE_MS", "250"))
        self.retry_max_ms = int(os.getenv("SIGNAL_RETRY_MAX_MS", "15000"))
        self.retry_jitter_ms = int(os.getenv("SIGNAL_RETRY_JITTER_MS", "250"))

        # Pending and reconciliation
        self.pending_interval_sec = int(os.getenv("SIGNAL_PENDING_INTERVAL_SEC", "20"))
        self.pending_min_idle_ms = int(os.getenv("SIGNAL_PENDING_MIN_IDLE_MS", "30000"))
        self.pending_claim_count = int(os.getenv("SIGNAL_PENDING_CLAIM_COUNT", "10"))

        # Maintenance
        self.orphan_repair_every_sec = int(os.getenv("SIGNAL_ORPHAN_REPAIR_EVERY_SEC", "600"))
        self.marker_ttl_repair_every_sec = int(os.getenv("SIGNAL_MARKER_TTL_REPAIR_EVERY_SEC", "300"))

        # ACK retry
        self.ack_retry_attempts = int(os.getenv("SIGNAL_ACK_RETRY_ATTEMPTS", "3"))
        self.ack_retry_delay_ms = int(os.getenv("SIGNAL_ACK_RETRY_DELAY_MS", "1000"))

        # Circuit breaker
        self.circuit_breaker_threshold = int(os.getenv("SIGNAL_CIRCUIT_BREAKER_THRESHOLD", "10"))
        self.circuit_breaker_window_sec = int(os.getenv("SIGNAL_CIRCUIT_BREAKER_WINDOW_SEC", "60"))
        self.circuit_breaker_cooldown_sec = int(os.getenv("SIGNAL_CIRCUIT_BREAKER_COOLDOWN_SEC", "120"))

        # Circuit Breaker V3 Integration (P100)
        self.cb_enabled = os.getenv("SIGNAL_CB_ENABLED", "0") == "1"
        self.cb_timeout_ms = int(os.getenv("SIGNAL_CB_TIMEOUT_MS", "500"))
        self.cb_refresh_every_ms = int(os.getenv("SIGNAL_CB_REFRESH_EVERY_MS", "10000"))
        self.cb_window_ms = int(os.getenv("SIGNAL_CB_WINDOW_MS", str(self.circuit_breaker_window_sec * 1000)))
        self.cb_max_downgrades = int(os.getenv("SIGNAL_CB_MAX_DOWNGRADES", str(self.circuit_breaker_threshold)))
        self.cb_disable_ms = int(os.getenv("SIGNAL_CB_DISABLE_MS", str(self.circuit_breaker_cooldown_sec * 1000)))
        self.cb_block_auto_apply = os.getenv("SIGNAL_CB_BLOCK_AUTO_APPLY", "1") == "1"
        self.cb_auto_apply_reason = os.getenv("SIGNAL_CB_AUTO_APPLY_REASON", "of_inputs_v3")


        # --- NEWLY ADDED for Phase 6 ---
        self.retry_zset = os.getenv("SIGNAL_RETRY_ZSET", "zset:signals:retry")
        self.retry_pop_limit = int(os.getenv("SIGNAL_RETRY_POP_LIMIT", "50"))
        self.retry_drain_every_ms = int(os.getenv("SIGNAL_RETRY_DRAIN_EVERY_MS", "200"))

        self.env_store_prefix = os.getenv("SIGNAL_ENV_STORE_PREFIX", "env:store")
        self.env_store_ttl_sec = int(os.getenv("SIGNAL_ENV_STORE_TTL_SEC", "3600"))

        self.maybe_done_zset = os.getenv("SIGNAL_MAYBE_DONE_ZSET", "zset:signal:maybe_done")
        self.maybe_done_limit = int(os.getenv("SIGNAL_MAYBE_DONE_LIMIT", "100"))

        self.maintenance_every_ms = int(os.getenv("SIGNAL_MAINTENANCE_EVERY_MS", "60000"))
        self.maintenance_scan_count = int(os.getenv("SIGNAL_MAINTENANCE_SCAN_COUNT", "400"))

        self.diag_every_ms = int(os.getenv("SIGNAL_DIAG_EVERY_MS", "30000"))

        # Outbox claim
        self.claim_min_idle_ms = int(os.getenv("SIGNAL_OUTBOX_CLAIM_MIN_IDLE_MS", "60000"))
        self.claim_count = int(os.getenv("SIGNAL_OUTBOX_CLAIM_COUNT", str(self.read_count)))
        self.claim_every_ms = int(os.getenv("SIGNAL_OUTBOX_CLAIM_EVERY_MS", "5000"))
        self.claim_budget_per_tick = int(os.getenv("SIGNAL_OUTBOX_CLAIM_BUDGET_PER_TICK", "400"))

        # ACK retry
        self.ack_retry_ttl_s = float(os.getenv("SIGNAL_OUTBOX_ACK_RETRY_TTL_S", "600"))
        self.ack_retry_max = int(os.getenv("SIGNAL_OUTBOX_ACK_RETRY_MAX", "20000"))

        # Circuit Breaker per target
        self.cb_fail_threshold = int(os.getenv("SIGNAL_TARGET_CB_FAIL_THRESHOLD", "5"))
        self.cb_open_sec = float(os.getenv("SIGNAL_TARGET_CB_OPEN_SEC", "15"))

        # Retry throttling
        self.retry_sleep_sec = float(os.getenv("SIGNAL_OUTBOX_RETRY_SLEEP_SEC", "0.25"))
        self.retry_sleep_max_sec = float(os.getenv("SIGNAL_OUTBOX_RETRY_SLEEP_MAX_SEC", "2.0"))

        # Dead consumer cleanup
        self.dead_consumer_idle_ms = int(os.getenv("SIGNAL_OUTBOX_DEAD_CONSUMER_IDLE_MS", "600000"))
        self.cleanup_dead_consumers = os.getenv("SIGNAL_OUTBOX_CLEANUP_DEAD_CONSUMERS", "0") == "1"

        # Attempts
        self.attempt_prefix = os.getenv("SIGNAL_OUTBOX_ATTEMPT_PREFIX", "outbox:attempt:v2")
        self.attempt_ttl_sec = int(os.getenv("SIGNAL_OUTBOX_ATTEMPT_TTL_SEC", "86400"))

        # Metrics/Diag/Janitor
        self.metrics_every_sec = int(os.getenv("SIGNAL_OUTBOX_METRICS_EVERY_SEC", "10"))
        self.outbox_diag_every_sec = float(os.getenv("SIGNAL_OUTBOX_DIAG_EVERY_SEC", "10"))
        self.janitor_enabled = os.getenv("SIGNAL_DISPATCHER_JANITOR", "0") == "1"
        self.janitor_every_sec = float(os.getenv("SIGNAL_DISPATCHER_JANITOR_EVERY_SEC", "60"))
        self.janitor_scan_count = int(os.getenv("SIGNAL_DISPATCHER_JANITOR_SCAN_COUNT", "200"))

        # Notify
        self.notify_stream = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
        self.notify_signal_counter_key = os.getenv("NOTIFY_SIGNAL_COUNTER_KEY", RK.NOTIFY_SIGNAL_COUNTER)
        try:
            # Use CRYPTO_NOTIFY_SIGNAL_EVERY_N as primary source, default 100
            every_n_str = os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", "100")
            self.notify_signal_every_n = max(1, int(every_n_str))
        except ValueError:
            self.notify_signal_every_n = 100

        # Marker repair
        self.marker_repair_every_sec = int(os.getenv("SIGNAL_MARKER_REPAIR_EVERY_SEC", "300"))
        self.marker_repair_batch = int(os.getenv("SIGNAL_MARKER_REPAIR_BATCH", "200"))
        self.marker_repair_scan_count = int(os.getenv("SIGNAL_MARKER_REPAIR_SCAN_COUNT", "1000"))

        # Delivery TTL
        self.delivery_marker_ttl_sec = int(os.getenv("SIGNAL_DELIVERY_TTL_SEC", "86400"))

        # Target Streams
        self.signal_stream = os.getenv("SIGNAL_STREAM", "stream:signals")
        self.signal_maxlen = int(os.getenv("SIGNAL_MAXLEN", "20000"))

        self.audit_stream = os.getenv("SIGNAL_AUDIT_STREAM", "stream:signals:audit")
        self.audit_maxlen = int(os.getenv("SIGNAL_AUDIT_MAXLEN", "20000"))

        self.manual_stream = os.getenv("SIGNAL_MANUAL_STREAM", RS.SIGNAL_MANUAL)
        self.manual_maxlen = int(os.getenv("SIGNAL_MANUAL_MAXLEN", "5000"))

        self.mt5_plans_stream = os.getenv("SIGNAL_MT5_PLANS_STREAM", RS.SIGNAL_PLANS)
        self.mt5_plans_maxlen = int(os.getenv("SIGNAL_MT5_PLANS_MAXLEN", "20000"))

        self.snapshot_prefix = os.getenv("SIGNAL_SNAPSHOT_PREFIX", "signal:snapshot")
        self.snapshot_ttl_sec = int(os.getenv("SIGNAL_SNAPSHOT_TTL_SEC", "86400"))
        # self.snapshot_stream added in __init__

        # Prefixes
        self.metrics_prefix = os.getenv("SIGNAL_METRICS_PREFIX", "signal_dispatcher")
        self.done_prefix = os.getenv("SIGNAL_DONE_PREFIX", "signal:done:v2")
        self.outbox_maxlen = int(os.getenv("SIGNAL_OUTBOX_MAXLEN", "100000"))
        self.dlq_maxlen = int(os.getenv("SIGNAL_DLQ_MAXLEN", "50000"))
        self.env_state_ttl_sec = int(os.getenv("SIGNAL_ENV_STATE_TTL_SEC", "3600"))
        self.lock_ttl_ms = int(os.getenv("SIGNAL_LOCK_TTL_MS", "30000"))

        # Redis connection
        self.redis_url = os.getenv("SIGNAL_REDIS_URL", os.getenv("REDIS_URL", "redis://localhost:6379/0"))
        self.redis_max_connections = int(os.getenv("REDIS_MAX_CONNECTIONS", "20"))
        self.redis_socket_timeout = float(os.getenv("REDIS_SOCKET_TIMEOUT", "1.0"))
        self.redis_socket_connect_timeout = float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "1.0"))

    @classmethod
    def from_env(cls) -> "SignalDispatcherConfig":
        """
        Create configuration from environment variables.
        
        Returns:
            SignalDispatcherConfig instance with all values loaded from ENV
        """
        return cls()
