from __future__ import annotations
"""
news_pipeline/p6_dto.py — P6 contract DTOs and idempotency helpers.

Provides:
  - stable_event_uuid()   — deterministic UUIDv5 for (doc_id, prompt_ver, model_id)
  - NewsEventContractDTO  — strict public contract for stream:news_events (schema v1)

Design:
  - stable_event_uuid prevents cross-worker duplicate emissions on replay.
  - NewsEventContractDTO has extra="forbid" to catch schema drift early.
  - Only import from here; do not inline these in worker modules.
"""

import uuid
from typing import List, Optional

from dataclasses import dataclass, field

# Fixed namespace for news event UUIDs.
# Changing this UUID would break idempotency for existing events.
# Do NOT change unless you want to regenerate all historical event IDs.
_NEWS_EVENT_NAMESPACE = uuid.UUID("2b8fd6d1-25ae-4a04-8ed1-2e3b1d1b0b8a")


def stable_event_uuid(doc_id: str, prompt_ver: str, model_id: str) -> str:
    """Deterministic UUIDv5 for a news extraction event.

    Key design:
    - Deterministic by (doc_id, prompt_ver, model_id) → no duplicates on replay.
    - Provider is intentionally excluded: provider may change on fallback/repair
      but semantically it is the same extraction pass.
    - Returns a valid UUID string (Postgres UUID type compatible).
    """
    name = f"{doc_id}|{prompt_ver}|{model_id}"
    return str(uuid.uuid5(_NEWS_EVENT_NAMESPACE, name))


@dataclass(slots=True)
class NewsEventContractDTO:
    """Public contract for stream:news_events — strict, minimal.

    This is the payload that downstream systems (trade core, UI) should depend on.
    Intentionally excludes:
      - created_ts_ms  (ingestion plumbing, not part of the signal)
      - rationale      (free-text, non-contract)

    Contract reference: P5.1.
    Strict: extra fields are rejected to catch schema drift at publish time.
    """

    schema_ver: str = "v1"
    prompt_ver: str = ""
    provider: str = ""
    model_id: str = ""
    doc_id: str = ""

    event_type: str = ""
    symbols: List[str] = field(default_factory=list)
    impact: float = 0.0

    # bias: simplified as dict for portability (avoids sub-schema import cycle)
    bias: dict = field(default_factory=dict)

    confidence: float = 0.0
    credibility_hint: float = 0.5

    # citations: list of dicts {text, source, url} — optional
    citations: List[dict] = field(default_factory=list)

    event_ts_ms: int = 0


@dataclass(slots=True)
class NewsPriorDTO:
    """Prior signal published to Redis key news:prior:<SYMBOL>.

    TTL is calculated from expires_ms: px = expires_ms - now_ms.
    This ensures the key expires exactly when the prior becomes stale,
    rather than using a fixed TTL that could be miscalibrated.
    """
    schema_ver: str = "v1"
    event_id: str = ""
    event_type: str = ""
    symbols: List[str] = field(default_factory=list)
    impact: float = 0.0
    bias: dict = field(default_factory=dict)
    confidence: float = 0.0
    expires_ms: int = 0
