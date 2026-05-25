"""news_pipeline.llm_job

DTOs, validator, and LLMStatus for the async Playwright enrichment pipeline.

Schema contract:
  Job:    news_llm_job_v1
  Result: news_llm_analysis_v2

When NEWS_LLM_PLAYWRIGHT_ENABLE=0 this module is never imported.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass, field
from typing import Any

# ── Constants ─────────────────────────────────────────────────────────────────

SCHEMA_VER_JOB    = "news_llm_job_v1"
SCHEMA_VER_RESULT = "news_llm_analysis_v2"
PROMPT_VERSION    = "v2.1.0"

ALLOWED_EVENT_TYPES = frozenset({
    "macro_cpi", "macro_ppi", "macro_fomc", "macro_fed_speech", "macro_nfp",
    "macro_rates", "macro_inflation", "crypto_regulation", "crypto_etf",
    "exchange_outage", "exchange_listing", "exchange_delisting", "security_hack",
    "geopolitics", "liquidation", "earnings", "market_commentary", "noise", "unknown",
})
ALLOWED_EVENT_CLASSES = frozenset({
    "macro", "crypto", "exchange", "security", "geopolitics", "earnings",
    "market", "noise", "unknown",
})
ALLOWED_SENTIMENTS = frozenset({"risk_on", "risk_off", "neutral", "mixed", "unknown"})
ALLOWED_ACTIONS    = frozenset({"allow", "tighten", "block", "protective_only"})


# ── LLMStatus ─────────────────────────────────────────────────────────────────

class LLMStatus:
    OK               = "ok"
    INVALID_JSON     = "invalid_json"
    SCHEMA_ERROR     = "schema_error"
    TIMEOUT          = "timeout"
    LOGIN_ERROR      = "login_error"
    PROVIDER_ERROR   = "provider_error"
    CACHE_HIT        = "cache_hit"
    CIRCUIT_OPEN     = "circuit_open"
    DEADLINE_EXPIRED = "deadline_expired"
    SKIPPED          = "skipped_low_grade"

USABLE_STATUSES = {LLMStatus.OK, LLMStatus.CACHE_HIT}


# ── Job DTO ───────────────────────────────────────────────────────────────────

@dataclass
class LLMJob:
    schema_ver:       str = SCHEMA_VER_JOB
    job_id:           str = ""
    news_uid:         str = ""
    source:           str = ""
    title:            str = ""
    url:              str = ""
    summary:          str = ""
    published_ts_ms:  int = 0
    ingested_ts_ms:   int = 0
    priority:         str = "normal"   # high | normal | low
    deadline_ts_ms:   int = 0
    attempt:          int = 1
    rule_grade_id:    int = 0

    def __post_init__(self) -> None:
        if not self.job_id:
            self.job_id = make_job_id(self.news_uid)
        if not self.deadline_ts_ms:
            self.deadline_ts_ms = int(time.time() * 1000) + 120_000  # 2-min default

    def is_expired(self) -> bool:
        return int(time.time() * 1000) > self.deadline_ts_ms

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_stream_fields(self) -> dict[str, str]:
        return {k: str(v) for k, v in self.to_dict().items()}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> LLMJob:
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in d.items() if k in known})

    @classmethod
    def from_stream_fields(cls, fields: dict[str, Any]) -> LLMJob:
        d: dict[str, Any] = {}
        for k, v in fields.items():
            if k in ("published_ts_ms", "ingested_ts_ms", "deadline_ts_ms",
                     "attempt", "rule_grade_id"):
                d[k] = int(v or 0)
            else:
                d[k] = str(v or "")
        return cls.from_dict(d)


# ── Result DTO ────────────────────────────────────────────────────────────────

@dataclass
class LLMResult:
    schema_ver:        str  = SCHEMA_VER_RESULT
    job_id:            str  = ""
    news_uid:          str  = ""
    provider:          str  = ""
    model:             str  = ""
    prompt_version:    str  = PROMPT_VERSION
    status:            str  = LLMStatus.OK

    event_type:        str  = "unknown"
    event_class:       str  = "unknown"
    grade_id:          int  = 0

    risk_score:        float = 0.0
    surprise_score:    float = 0.0
    confidence:        float = 0.0
    sentiment:         str   = "unknown"

    affected_symbols:  list  = field(default_factory=list)
    directional_bias:  dict  = field(default_factory=dict)

    recommended_action: str  = "allow"
    risk_factor_bps:    int  = 10000
    reason_code:        str  = ""
    time_window_sec:    int  = 0

    evidence:           list = field(default_factory=list)
    dq_flags:           list = field(default_factory=list)
    summary:            str  = ""

    latency_ms:         int  = 0
    created_ts_ms:      int  = field(default_factory=lambda: int(time.time() * 1000))

    @property
    def usable(self) -> bool:
        return self.status in USABLE_STATUSES

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_stream_fields(self) -> dict[str, str]:
        d = self.to_dict()
        return {
            k: json.dumps(v) if isinstance(v, (list, dict)) else str(v)
            for k, v in d.items()
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> LLMResult:
        known = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in d.items() if k in known})

    @classmethod
    def error(cls, *, job_id: str, news_uid: str, provider: str,
              status: str, dq_flag: str, latency_ms: int = 0) -> LLMResult:
        return cls(
            job_id=job_id, news_uid=news_uid, provider=provider,
            status=status, dq_flags=[dq_flag], latency_ms=latency_ms,
            recommended_action="allow",  # fail-open: error ≠ safe, but also ≠ block
        )


# ── Validator ─────────────────────────────────────────────────────────────────

def validate_llm_result(raw: dict[str, Any]) -> tuple[LLMResult, list[str]]:
    """
    Parse and validate raw LLM JSON (v2 schema).
    Returns (LLMResult, errors).
    errors=[] → status=ok; errors → status=schema_error.
    Fail-open: invalid result gets recommended_action="allow" (not block).
    """
    errors: list[str] = []

    event_type = str(raw.get("event_type") or "unknown")
    if event_type not in ALLOWED_EVENT_TYPES:
        errors.append(f"invalid_event_type:{event_type}")
        event_type = "unknown"

    event_class = str(raw.get("event_class") or "unknown")
    if event_class not in ALLOWED_EVENT_CLASSES:
        errors.append(f"invalid_event_class:{event_class}")
        event_class = "unknown"

    sentiment = str(raw.get("sentiment") or "unknown")
    if sentiment not in ALLOWED_SENTIMENTS:
        errors.append(f"invalid_sentiment:{sentiment}")
        sentiment = "unknown"

    action = str(raw.get("recommended_action") or "allow")
    if action not in ALLOWED_ACTIONS:
        errors.append(f"invalid_action:{action}")
        action = "allow"

    if not raw.get("reason_code"):
        errors.append("missing_reason_code")

    if not raw.get("time_window_sec"):
        errors.append("missing_time_window_sec")

    try:
        grade_id = int(raw.get("grade_id") or 0)
    except (ValueError, TypeError):
        grade_id = 0
        errors.append("invalid_grade_id")

    def _f(k: str, lo: float, hi: float) -> float:
        try:
            return max(lo, min(hi, float(raw.get(k) or 0.0)))
        except (ValueError, TypeError):
            errors.append(f"invalid_{k}")
            return 0.0

    affected = raw.get("affected_symbols") or []
    if not isinstance(affected, list):
        affected = []

    bias = raw.get("directional_bias") or {}
    if not isinstance(bias, dict):
        bias = {}

    evidence = raw.get("evidence") or []
    if not isinstance(evidence, list):
        evidence = []

    result = LLMResult(
        status=LLMStatus.SCHEMA_ERROR if errors else LLMStatus.OK,
        event_type=event_type,
        event_class=event_class,
        grade_id=grade_id,
        risk_score=_f("risk_score", 0.0, 1.0),
        surprise_score=_f("surprise_score", -1.0, 1.0),
        confidence=_f("confidence", 0.0, 1.0),
        sentiment=sentiment,
        affected_symbols=affected,
        directional_bias=bias,
        recommended_action=action,
        risk_factor_bps=int(raw.get("risk_factor_bps") or 10000),
        reason_code=str(raw.get("reason_code") or ""),
        time_window_sec=int(raw.get("time_window_sec") or 0),
        evidence=evidence,
        dq_flags=errors,
        summary=str(raw.get("summary") or "")[:240],
    )
    return result, errors


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_job_id(news_uid: str, prompt_ver: str = PROMPT_VERSION) -> str:
    return hashlib.sha1(f"{news_uid}:{prompt_ver}".encode()).hexdigest()[:16]


def resolve_action(
    *,
    rule_action: str,
    rule_grade_id: int,
    llm_result: LLMResult | None,
    hard_block_allow: bool,
) -> tuple[str, int, str]:
    """
    Merge rules action and LLM recommendation.
    Returns (action, risk_factor_bps, reason_code).

    Policy:
    - Rules calendar grade>=5 confirmed → block always allowed
    - LLM-only block → downgrade to tighten unless hard_block_allow=True AND confidence>=0.80
    - Invalid LLM → fallback to rules
    """
    # Rules always win for critical grade
    if rule_grade_id >= 5 and rule_action == "block":
        bps = 0
        return "block", bps, "rules_grade5_block"

    # No usable LLM → rules-only
    if llm_result is None or not llm_result.usable:
        bps = _action_to_bps(rule_action)
        return rule_action, bps, f"rules_only:{rule_action}"

    llm_action = llm_result.recommended_action
    llm_bps    = llm_result.risk_factor_bps
    llm_reason = llm_result.reason_code or f"llm_{llm_action}"

    if llm_action == "block":
        if not hard_block_allow:
            return "tighten", 5000, f"llm_block_downgraded:{llm_reason}"
        if llm_result.confidence < 0.80:
            return "tighten", 5000, f"llm_block_low_conf:{llm_result.confidence:.2f}"
        return "block", 0, llm_reason

    return llm_action, llm_bps, llm_reason


def _action_to_bps(action: str) -> int:
    return {"block": 0, "tighten": 5000, "protective_only": 7500, "allow": 10000}.get(action, 10000)
