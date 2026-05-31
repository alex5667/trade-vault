from __future__ import annotations

import os
from core.redis_keys import RedisStreams as RS


def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

# Streams
NEWS_RAW_STREAM = env("NEWS_RAW_STREAM", RS.NEWS_RAW)
NEWS_ANALYSIS_STREAM = env("NEWS_ANALYSIS_STREAM", RS.NEWS_ANALYSIS)
CALENDAR_EVENTS_STREAM = env("CALENDAR_EVENTS_STREAM", RS.CALENDAR_EVENTS)

# DLQ Streams
NEWS_RAW_DLQ = env("NEWS_RAW_DLQ", "news:raw:dlq")
NEWS_ANALYSIS_DLQ = env("NEWS_ANALYSIS_DLQ", "news:analysis:dlq")
CALENDAR_EVENTS_DLQ = env("CALENDAR_EVENTS_DLQ", "calendar:events:dlq")

# Consumer Groups
NEWS_ANALYZER_GROUP = env("NEWS_ANALYZER_GROUP", "news-analyzer")
NEWS_FEATURE_GROUP = env("NEWS_FEATURE_GROUP", "news-feature-store")
CALENDAR_FEATURE_GROUP = env("CALENDAR_FEATURE_GROUP", "calendar-feature-store")

# Dedupe/TTL
NEWS_DEDUP_TTL_SEC = int(env("NEWS_DEDUPE_TTL_SEC", "172800"))
NEWS_TS_BUCKET_SEC = int(env("NEWS_TS_BUCKET_SEC", "60"))

# Heavy JSON TTL (days)
NEWS_ANALYSIS_KEY_TTL_SEC = int(env("NEWS_ANALYSIS_TTL_SEC", "259200"))

# Online aggregates TTL (minutes/hours)
NEWS_AGG_TTL_SEC = int(env("NEWS_AGG_TTL_SEC", "3600"))
CALENDAR_EVENT_TTL_SEC = int(env("CALENDAR_EVENT_TTL_SEC", "604800"))
CALENDAR_AGG_TTL_SEC = int(env("CALENDAR_AGG_TTL_SEC", "3600"))

# Processing settings
NEWS_EWMA_HALFLIFE_SEC = float(env("NEWS_RISK_HALF_LIFE_SEC", "1800"))
NEWS_MAXLEN = int(env("NEWS_STREAM_MAXLEN", "200000"))

# Debug/Sampling
NEWS_DEBUG_SAMPLE_RATE = float(env("NEWS_DEBUG_SAMPLE_RATE", "0.01"))

# LLM
GEMINI_API_KEY = env("GEMINI_API_KEY", "")
GEMINI_MODEL = env("GEMINI_MODEL", "gemini-1.5-pro")

NVIDIA_API_KEY = env("NVIDIA_API_KEY", "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")
NVIDIA_MODEL = env("NVIDIA_MODEL", "deepseek-ai/deepseek-r1")
NVIDIA_MODEL_KIMI = env("NVIDIA_MODEL_KIMI", "moonshotai/kimi-k2.5")
LLM_FALLBACK_ENABLED = env("LLM_FALLBACK_ENABLED", "1") == "1"

# Leader lock
NEWS_INGESTOR_LEADER_KEY = env("NEWS_INGESTOR_LEADER_KEY", "news:ingestor:leader")
NEWS_INGESTOR_LEADER_TTL_SEC = int(env("NEWS_INGESTOR_LEADER_TTL_SEC", "8"))

# ------------------------------------------------------------------
# Feature-store anti-flap & retry knobs (env-configurable)
# ------------------------------------------------------------------

# Grade anti-flap: upgrades are stricter than downgrades
NEWS_GRADE_COOLDOWN_UP_SEC = int(env("NEWS_GRADE_COOLDOWN_UP_SEC", "900"))     # 15m
NEWS_GRADE_COOLDOWN_DOWN_SEC = int(env("NEWS_GRADE_COOLDOWN_DOWN_SEC", "300")) # 5m

# Redis write retry (only on transient errors)
NEWS_REDIS_RETRY_ATTEMPTS = int(env("NEWS_REDIS_RETRY_ATTEMPTS", "2"))
NEWS_REDIS_RETRY_SLEEP_MS = int(env("NEWS_REDIS_RETRY_SLEEP_MS", "25"))

# DLQ stream for feature-store failures
NEWS_FEATURE_DLQ_STREAM = env("NEWS_FEATURE_DLQ_STREAM", "news:analysis:dlq")
NEWS_FEATURE_DLQ_MAXLEN = int(env("NEWS_FEATURE_DLQ_MAXLEN", "200000"))

# ------------------------------------------------------------------
# Feature store: time quality gates
# ------------------------------------------------------------------
# Maximum age for processing (stale detection)
NEWS_FEATURE_MAX_AGE_MS = int(env("NEWS_FEATURE_MAX_AGE_MS", str(15 * 60 * 1000)))  # 15m
# Future time tolerance before sanitization
NEWS_FEATURE_FUTURE_TOLERANCE_MS = int(env("NEWS_FEATURE_FUTURE_TOLERANCE_MS", str(30 * 1000)))  # 30s
# Action for stale messages: skip|force_zero
NEWS_FEATURE_STALE_ACTION = env("NEWS_FEATURE_STALE_ACTION", "skip").strip().lower()
# DLQ rate limit per minute per process
NEWS_FEATURE_DLQ_MAX_PER_MIN = int(env("NEWS_FEATURE_DLQ_MAX_PER_MIN", "120"))
# DLQ field size limit
NEWS_FEATURE_DLQ_FIELDS_LIMIT = int(env("NEWS_FEATURE_DLQ_FIELDS_LIMIT", "4096"))
