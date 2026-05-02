from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server


STREAM_FEEDBACK_SUMMARY = os.getenv("ML_OPERATOR_RCA_FEEDBACK_SUMMARY_STREAM", "stream:ml:operator_rca_feedback_summary")
STREAM_GOV_DECISIONS = os.getenv("ML_OPERATOR_RCA_GOVERNOR_DECISIONS_STREAM", "stream:ml:operator_rca_governor_decisions")
STREAM_GOV_AUDIT = os.getenv("ML_OPERATOR_RCA_GOVERNOR_AUDIT_STREAM", "stream:ml:operator_rca_governor_audit")
STREAM_GOV_DLQ = os.getenv("ML_OPERATOR_RCA_GOVERNOR_DLQ_STREAM", "stream:ml:operator_rca_governor_dlq")
GROUP = os.getenv("ML_OPERATOR_RCA_GOVERNOR_GROUP", "cg:ml_operator_rca_governor_v2_2")
CONSUMER = os.getenv("HOSTNAME", "ml-operator-rca-governor-v2-2")
POLICY_KEY_PREFIX = os.getenv("ML_OPERATOR_RCA_POLICY_KEY_PREFIX", "cfg:ml:operator_rca_governor")

PROM_PORT = int(os.getenv("ML_OPERATOR_RCA_GOVERNOR_METRICS_PORT", "9873"))
READ_COUNT = int(os.getenv("ML_OPERATOR_RCA_GOVERNOR_READ_COUNT", "200"))
WINDOW_MIN = int(os.getenv("ML_OPERATOR_RCA_GOVERNOR_WINDOW_MIN", "1440"))
MIN_SAMPLE = int(os.getenv("ML_OPERATOR_RCA_GOVERNOR_MIN_SAMPLE", "12"))
SUPPRESS_SCORE_MAX = float(os.getenv("ML_OPERATOR_RCA_GOVERNOR_SUPPRESS_SCORE_MAX", "0.45"))
PROMOTE_SCORE_MIN = float(os.getenv("ML_OPERATOR_RCA_GOVERNOR_PROMOTE_SCORE_MIN", "0.72"))
SUPPRESS_ACTION_RATE_MIN = float(os.getenv("ML_OPERATOR_RCA_GOVERNOR_SUPPRESS_ACTION_RATE_MIN", "0.55"))
PROMOTE_ACTION_RATE_MIN = float(os.getenv("ML_OPERATOR_RCA_GOVERNOR_PROMOTE_ACTION_RATE_MIN", "0.55"))
MAX_PROVIDER_WEIGHT = float(os.getenv("ML_OPERATOR_RCA_GOVERNOR_MAX_PROVIDER_WEIGHT", "0.30"))
ADVISORY_ONLY = int(os.getenv("ML_OPERATOR_RCA_GOVERNOR_ADVISORY_ONLY", "1")) == 1

RUNS = Counter("ml_operator_rca_governor_runs_total", "Governor runs", ["status"])
DECISIONS = Counter("ml_operator_rca_governor_decisions_total", "Governor decisions", ["decision_type"])
ROWS = Counter("ml_operator_rca_governor_rows_total", "Rows processed", ["source"])
QUEUE_LAG_MS = Gauge("ml_operator_rca_governor_queue_lag_ms", "Queue lag ms")
LAST_RUN_TS = Gauge("ml_operator_rca_governor_last_run_ts_seconds", "Last run ts")
SUPPRESS_PATTERNS = Gauge("ml_operator_rca_governor_suppress_patterns", "Suppressed patterns count")
PROMOTE_PATTERNS = Gauge("ml_operator_rca_governor_promote_patterns", "Promoted patterns count")
LOOP_LAT = Histogram("ml_operator_rca_governor_loop_seconds", "Loop latency")


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _b2s_map(fields: Dict[Any, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in fields.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        vv = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        out[kk] = vv
    return out


def _json_loads_or_empty(s: Any) -> Dict[str, Any]:
    try:
        if s is None:
            return {}
        if isinstance(s, (dict, list)):
            return s  # type: ignore[return-value]
        return json.loads(s)
    except Exception:
        return {}


@dataclass
class GovernorRow:
    recommendation_id: str
    analysis_run_id: str
    provider: str
    model_name: str
    prompt_version: str
    policy_version: str
    action_type: str
    quality_score: float
    usefulness_score: float
    feedback_n: int
    ts_ms: int


def _weighted_score(quality_score: float, usefulness_score: float, feedback_n: int) -> float:
    # usefulness carries more weight, but low-sample feedback is shrunk.
    n = max(0, int(feedback_n))
    shrink = min(1.0, n / 6.0)
    usefulness_adj = (0.50 * (1.0 - shrink)) + (usefulness_score * shrink)
    score = (0.40 * quality_score) + (0.60 * usefulness_adj)
    return max(0.0, min(1.0, score))


def _decision_from_stats(avg_score: float, useful_rate: float, sample_n: int) -> str:
    if sample_n < MIN_SAMPLE:
        return "HOLD"
    if avg_score <= SUPPRESS_SCORE_MAX and useful_rate <= (1.0 - SUPPRESS_ACTION_RATE_MIN):
        return "SUPPRESS"
    if avg_score >= PROMOTE_SCORE_MIN and useful_rate >= PROMOTE_ACTION_RATE_MIN:
        return "PROMOTE"
    return "HOLD"


def _reason_codes(avg_score: float, useful_rate: float, sample_n: int, decision: str) -> List[str]:
    reasons: List[str] = []
    if sample_n < MIN_SAMPLE:
        reasons.append("LOW_SAMPLE")
    if avg_score <= SUPPRESS_SCORE_MAX:
        reasons.append("LOW_COMBINED_SCORE")
    if avg_score >= PROMOTE_SCORE_MIN:
        reasons.append("HIGH_COMBINED_SCORE")
    if useful_rate < 0.50:
        reasons.append("LOW_USEFUL_RATE")
    if useful_rate >= PROMOTE_ACTION_RATE_MIN:
        reasons.append("HIGH_USEFUL_RATE")
    reasons.append(f"DECISION_{decision}")
    return reasons


def build_action_pattern_decisions(rows: Iterable[GovernorRow]) -> List[Dict[str, Any]]:
    agg: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for r in rows:
        key = (r.action_type, r.prompt_version, r.policy_version)
        state = agg.setdefault(
            key,
            {
                "sample_n": 0,
                "score_sum": 0.0,
                "useful_n": 0,
                "quality_sum": 0.0,
                "providers": {},
            }
        )
        state["sample_n"] += 1
        score = _weighted_score(r.quality_score, r.usefulness_score, r.feedback_n)
        state["score_sum"] += score
        state["quality_sum"] += max(0.0, min(1.0, r.quality_score))
        if r.usefulness_score >= 0.75:
            state["useful_n"] += 1
        providers = state["providers"]
        providers[r.provider] = providers.get(r.provider, 0) + 1

    decisions: List[Dict[str, Any]] = []
    for (action_type, prompt_version, policy_version), state in agg.items():
        sample_n = int(state["sample_n"])
        avg_score = (state["score_sum"] / sample_n) if sample_n else 0.0
        useful_rate = (state["useful_n"] / sample_n) if sample_n else 0.0
        quality_avg = (state["quality_sum"] / sample_n) if sample_n else 0.0
        decision = _decision_from_stats(avg_score, useful_rate, sample_n)
        reasons = _reason_codes(avg_score, useful_rate, sample_n, decision)
        decisions.append(
            {
                "scope": "action_pattern",
                "action_type": action_type,
                "prompt_version": prompt_version,
                "policy_version": policy_version,
                "sample_n": sample_n,
                "combined_score": round(avg_score, 6),
                "quality_avg": round(quality_avg, 6),
                "useful_rate": round(useful_rate, 6),
                "decision": decision,
                "reason_codes_json": json.dumps(reasons, ensure_ascii=False),
            }
        )
    return decisions


def build_provider_version_decisions(rows: Iterable[GovernorRow]) -> List[Dict[str, Any]]:
    agg: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for r in rows:
        key = (r.provider, r.model_name, r.prompt_version)
        state = agg.setdefault(key, {"sample_n": 0, "score_sum": 0.0, "useful_n": 0})
        state["sample_n"] += 1
        score = _weighted_score(r.quality_score, r.usefulness_score, r.feedback_n)
        state["score_sum"] += score
        if r.usefulness_score >= 0.75:
            state["useful_n"] += 1

    decisions: List[Dict[str, Any]] = []
    for (provider, model_name, prompt_version), state in agg.items():
        sample_n = int(state["sample_n"])
        avg_score = (state["score_sum"] / sample_n) if sample_n else 0.0
        useful_rate = (state["useful_n"] / sample_n) if sample_n else 0.0
        base_decision = _decision_from_stats(avg_score, useful_rate, sample_n)
        # Provider-level actions are intentionally damped.
        decision = base_decision
        if decision == "PROMOTE" and avg_score < min(1.0, PROMOTE_SCORE_MIN + MAX_PROVIDER_WEIGHT):
            decision = "HOLD"
        reasons = _reason_codes(avg_score, useful_rate, sample_n, decision)
        decisions.append(
            {
                "scope": "provider_prompt",
                "provider": provider,
                "model_name": model_name,
                "prompt_version": prompt_version,
                "sample_n": sample_n,
                "combined_score": round(avg_score, 6),
                "useful_rate": round(useful_rate, 6),
                "decision": decision,
                "reason_codes_json": json.dumps(reasons, ensure_ascii=False),
            }
        )
    return decisions


class GovernorRepo:
    def __init__(self, redis_url: str, database_url: str = "") -> None:
        if redis is None:  # pragma: no cover
            raise RuntimeError("redis.asyncio is required")
        self.redis = redis.from_url(redis_url, decode_responses=False)
        self.database_url = database_url

    async def ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(STREAM_FEEDBACK_SUMMARY, GROUP, id="0", mkstream=True)
        except Exception:
            return

    async def read_feedback(self) -> List[Tuple[str, Dict[str, str]]]:
        rows = await self.redis.xreadgroup(GROUP, CONSUMER, {STREAM_FEEDBACK_SUMMARY: ">"}, count=READ_COUNT, block=2000)
        out: List[Tuple[str, Dict[str, str]]] = []
        if not rows:
            return out
        for _stream, messages in rows:
            for msg_id, fields in messages:
                out.append((msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id), _b2s_map(fields)))
        return out

    async def ack(self, msg_ids: List[str]) -> None:
        if msg_ids:
            await self.redis.xack(STREAM_FEEDBACK_SUMMARY, GROUP, *msg_ids)

    async def fetch_recent_rows(self, lookback_ms: int) -> List[GovernorRow]:
        # Reference dataset lives in SQL in production; Redis fallback keeps worker useful even before full SQL hydration.
        raw = await self.redis.xrevrange(STREAM_FEEDBACK_SUMMARY, "+", "-", count=5000)
        out: List[GovernorRow] = []
        now_ms = _now_ms()
        for _msg_id, fields in raw:
            d = _b2s_map(fields)
            ts_ms = _safe_int(d.get("ts_ms", now_ms), now_ms)
            if now_ms - ts_ms > lookback_ms:
                continue
            out.append(
                GovernorRow(
                    recommendation_id=d.get("recommendation_id", ""),
                    analysis_run_id=d.get("analysis_run_id", ""),
                    provider=d.get("provider", "unknown"),
                    model_name=d.get("model_name", "unknown"),
                    prompt_version=d.get("prompt_version", "unknown"),
                    policy_version=d.get("policy_version", "unknown"),
                    action_type=d.get("action_type", "unknown"),
                    quality_score=_safe_float(d.get("quality_score", 0.0), 0.0),
                    usefulness_score=_safe_float(d.get("usefulness_score", 0.5), 0.5),
                    feedback_n=_safe_int(d.get("feedback_n", 0), 0),
                    ts_ms=ts_ms,
                )
            )
        return out

    async def write_decision(self, decision: Dict[str, Any]) -> None:
        payload = {k: (json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)) for k, v in decision.items()}
        payload.setdefault("ts_ms", str(_now_ms()))
        await self.redis.xadd(STREAM_GOV_DECISIONS, payload, maxlen=200_000, approximate=True)
        await self.redis.hset("metrics:ml:operator_rca_governor:last", mapping=payload)
        key = _policy_key_for(decision)
        if ADVISORY_ONLY:
            await self.redis.hset(key, mapping={**payload, "apply_mode": "ADVISORY_ONLY"})
        else:
            await self.redis.hset(key, mapping={**payload, "apply_mode": "LIVE_POLICY"})
        await self.redis.xadd(STREAM_GOV_AUDIT, {**payload, "event": "GOVERNOR_DECISION"}, maxlen=200_000, approximate=True)

    async def write_dlq(self, reason: str, row: Dict[str, Any]) -> None:
        payload = {"ts_ms": str(_now_ms()), "reason": reason, "row_json": json.dumps(row, ensure_ascii=False)}
        await self.redis.xadd(STREAM_GOV_DLQ, payload, maxlen=20_000, approximate=True)


def _policy_key_for(decision: Dict[str, Any]) -> str:
    scope = decision.get("scope", "unknown")
    if scope == "action_pattern":
        return f"{POLICY_KEY_PREFIX}:action:{decision.get('action_type','unknown')}:{decision.get('prompt_version','unknown')}:{decision.get('policy_version','unknown')}"
    return f"{POLICY_KEY_PREFIX}:provider:{decision.get('provider','unknown')}:{decision.get('model_name','unknown')}:{decision.get('prompt_version','unknown')}"


class OperatorRCAUsefulnessGovernor:
    def __init__(self, repo: GovernorRepo) -> None:
        self.repo = repo

    async def run_once(self) -> Dict[str, int]:
        t0 = time.perf_counter()
        rows = await self.repo.fetch_recent_rows(lookback_ms=WINDOW_MIN * 60_000)
        ROWS.labels(source="feedback_summary").inc(len(rows))
        action_decisions = build_action_pattern_decisions(rows)
        provider_decisions = build_provider_version_decisions(rows)
        decisions = action_decisions + provider_decisions

        suppress_n = 0
        promote_n = 0
        for d in decisions:
            await self.repo.write_decision(d)
            DECISIONS.labels(decision_type=d["decision"]).inc()
            if d["decision"] == "SUPPRESS":
                suppress_n += 1
            elif d["decision"] == "PROMOTE":
                promote_n += 1

        SUPPRESS_PATTERNS.set(suppress_n)
        PROMOTE_PATTERNS.set(promote_n)
        LAST_RUN_TS.set(time.time())
        RUNS.labels(status="ok").inc()
        LOOP_LAT.observe(time.perf_counter() - t0)
        return {"rows": len(rows), "decisions": len(decisions), "suppress": suppress_n, "promote": promote_n}


async def _main_async() -> None:
    start_http_server(PROM_PORT)
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    repo = GovernorRepo(redis_url=redis_url, database_url=os.getenv("DATABASE_URL", ""))
    await repo.ensure_group()
    gov = OperatorRCAUsefulnessGovernor(repo)

    while True:
        t0 = _now_ms()
        try:
            await gov.run_once()
        except Exception as exc:  # pragma: no cover
            RUNS.labels(status="err").inc()
            await repo.write_dlq("RUN_EXCEPTION", {"error": repr(exc)})
        QUEUE_LAG_MS.set(max(0, _now_ms() - t0))
        await time_async_sleep(float(os.getenv("ML_OPERATOR_RCA_GOVERNOR_LOOP_SEC", "300")))


async def time_async_sleep(seconds: float) -> None:
    import asyncio
    await asyncio.sleep(seconds)


def main() -> None:
    import asyncio
    asyncio.run(_main_async())


if __name__ == "__main__":  # pragma: no cover
    main()
