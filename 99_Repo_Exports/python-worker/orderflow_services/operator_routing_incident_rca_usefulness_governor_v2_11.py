from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

try:  # pragma: no cover
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

try:  # pragma: no cover
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None


APP_NAME = "operator_routing_incident_rca_usefulness_governor_v2_11"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_GOVERNOR_PORT", "9889"))
ADVISORY_ONLY = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_GOVERNOR_ADVISORY_ONLY", "1"))
WINDOW_MIN = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_GOVERNOR_WINDOW_MIN", "1440"))
MIN_SAMPLE = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_GOVERNOR_MIN_SAMPLE", "8"))
SUPPRESS_SCORE_MAX = float(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_GOVERNOR_SUPPRESS_SCORE_MAX", "0.45"))
PROMOTE_SCORE_MIN = float(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_GOVERNOR_PROMOTE_SCORE_MIN", "0.72"))

DECISIONS_STREAM = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_GOVERNOR_DECISIONS_STREAM",
    "stream:ml:operator_routing_incident_rca_governor_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_GOVERNOR_AUDIT_STREAM",
    "stream:ml:operator_routing_incident_rca_governor_audit",
)
REDIS_POLICY_PREFIX = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_GOVERNOR_REDIS_PREFIX",
    "cfg:ml:operator_routing_incident_rca_governor",
)
POLL_INTERVAL = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_GOVERNOR_POLL_INTERVAL", "60"))


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_operator_routing_incident_rca_governor_runs_total",
    "Routing incident RCA governor runs",
    ("status",),
)
LAT = _hist(
    "ml_operator_routing_incident_rca_governor_latency_seconds",
    "Routing incident RCA governor latency seconds",
)
LAST_RUN_TS = _gauge(
    "ml_operator_routing_incident_rca_governor_last_run_ts_seconds",
    "Routing incident RCA governor last run timestamp",
)
EVAL_ACTIONS = _gauge(
    "ml_operator_routing_incident_rca_governor_eval_actions_count",
    "Routing incident RCA governor evaluated actions",
)
EVAL_PROVIDERS = _gauge(
    "ml_operator_routing_incident_rca_governor_eval_providers_count",
    "Routing incident RCA governor evaluated providers",
)


def now_ms() -> int:
    return get_ny_time_millis()


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class GovernorRepo:
    def __init__(self, redis_url: str, database_url: str) -> None:
        if psycopg is None:
            raise RuntimeError("psycopg is required")
        if redis is None:
            raise RuntimeError("redis.asyncio is required")
        self.redis_url = redis_url
        self.database_url = database_url
        self.r = redis.from_url(redis_url)

    def get_aggregated_stats(self, window_min: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        cutoff_ms = now_ms() - (window_min * 60 * 1000)
        actions: List[Dict[str, Any]] = []
        providers: List[Dict[str, Any]] = []
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        task_type,
                        prompt_version,
                        policy_version,
                        COUNT(output_hash) as sample_n,
                        AVG(COALESCE(quality_score, 0.0)) as avg_quality,
                        AVG(COALESCE(usefulness_score, 0.0)) as avg_usefulness
                    FROM llm_operator_routing_incident_rca_results
                    WHERE ts_ms >= %(cutoff_ms)s
                    GROUP BY task_type, prompt_version, policy_version
                    """,
                    {"cutoff_ms": cutoff_ms},
                )
                for row in cur.fetchall():
                    actions.append(
                        {
                            "task_type": row[0],
                            "prompt_version": row[1],
                            "policy_version": row[2],
                            "sample_n": row[3],
                            "avg_quality": float(row[4]),
                            "avg_usefulness": float(row[5]),
                        }
                    )
                cur.execute(
                    """
                    SELECT
                        provider,
                        model_name,
                        prompt_version,
                        COUNT(output_hash) as sample_n,
                        AVG(COALESCE(quality_score, 0.0)) as avg_quality,
                        AVG(COALESCE(usefulness_score, 0.0)) as avg_usefulness
                    FROM llm_operator_routing_incident_rca_results
                    WHERE ts_ms >= %(cutoff_ms)s
                    GROUP BY provider, model_name, prompt_version
                    """,
                    {"cutoff_ms": cutoff_ms},
                )
                for row in cur.fetchall():
                    providers.append(
                        {
                            "provider": row[0],
                            "model_name": row[1],
                            "prompt_version": row[2],
                            "sample_n": row[3],
                            "avg_quality": float(row[4]),
                            "avg_usefulness": float(row[5]),
                        }
                    )
        return actions, providers

    async def publish_decision(self, decision: Dict[str, Any]) -> None:
        decision["ts_ms"] = now_ms()
        decision["advisory_only"] = ADVISORY_ONLY
        await self.r.xadd(DECISIONS_STREAM, decision, maxlen=20000, approximate=True)
        await self.r.xadd(AUDIT_STREAM, decision, maxlen=20000, approximate=True)

        if decision.get("scope_type") == "action":
            redis_key = f"{REDIS_POLICY_PREFIX}:action:{decision['task_type']}:{decision['prompt_version']}:{decision['policy_version']}"
        else:
            redis_key = f"{REDIS_POLICY_PREFIX}:provider:{decision['provider']}:{decision['model_name']}:{decision['prompt_version']}"
        
        await self.r.hset(
            redis_key,
            mapping={
                "action": decision["action"],
                "score": str(decision["score"]),
                "sample_n": str(decision["sample_n"]),
                "ts_ms": str(decision["ts_ms"]),
            },
        )

    def persist_decision_sql(self, decisions: List[Dict[str, Any]]) -> None:
        if not decisions:
            return
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                for d in decisions:
                    cur.execute(
                        """
                        INSERT INTO llm_operator_routing_incident_rca_governor_decisions (
                            scope_type,
                            scope_key,
                            action,
                            score,
                            sample_n,
                            advisory_only,
                            ts_ms
                        ) VALUES (
                            %(scope_type)s,
                            %(scope_key)s,
                            %(action)s,
                            %(score)s,
                            %(sample_n)s,
                            %(advisory_only)s,
                            %(ts_ms)s
                        )
                        """,
                        {
                            "scope_type": d["scope_type"],
                            "scope_key": d["scope_key"],
                            "action": d["action"],
                            "score": d["score"],
                            "sample_n": d["sample_n"],
                            "advisory_only": ADVISORY_ONLY == 1,
                            "ts_ms": d["ts_ms"],
                        },
                    )
                conn.commit()


def evaluate_score(sample_n: int, avg_quality: float, avg_usefulness: float) -> Tuple[str, float]:
    if sample_n < MIN_SAMPLE:
        return "HOLD", 0.0
    score = (0.4 * avg_quality) + (0.6 * avg_usefulness)
    if score <= SUPPRESS_SCORE_MAX:
        return "SUPPRESS", score
    if score >= PROMOTE_SCORE_MIN:
        return "PROMOTE", score
    return "HOLD", score


async def governance_loop(repo: GovernorRepo) -> None:
    started = time.perf_counter()
    status = "ok"
    decisions_made: List[Dict[str, Any]] = []
    try:
        actions, providers = repo.get_aggregated_stats(WINDOW_MIN)
        if EVAL_ACTIONS:
            EVAL_ACTIONS.set(len(actions))
        if EVAL_PROVIDERS:
            EVAL_PROVIDERS.set(len(providers))

        for a in actions:
            action_obj, score = evaluate_score(a["sample_n"], a["avg_quality"], a["avg_usefulness"])
            decision = {
                "scope_type": "action",
                "task_type": a["task_type"],
                "prompt_version": a["prompt_version"],
                "policy_version": a["policy_version"],
                "scope_key": f"{a['task_type']}:{a['prompt_version']}:{a['policy_version']}",
                "action": action_obj,
                "score": score,
                "sample_n": a["sample_n"],
                "avg_quality": a["avg_quality"],
                "avg_usefulness": a["avg_usefulness"],
            }
            await repo.publish_decision(decision)
            decisions_made.append(decision)

        for p in providers:
            action_obj, score = evaluate_score(p["sample_n"], p["avg_quality"], p["avg_usefulness"])
            decision = {
                "scope_type": "provider",
                "provider": p["provider"],
                "model_name": p["model_name"],
                "prompt_version": p["prompt_version"],
                "scope_key": f"{p['provider']}:{p['model_name']}:{p['prompt_version']}",
                "action": action_obj,
                "score": score,
                "sample_n": p["sample_n"],
                "avg_quality": p["avg_quality"],
                "avg_usefulness": p["avg_usefulness"],
            }
            await repo.publish_decision(decision)
            decisions_made.append(decision)

        repo.persist_decision_sql(decisions_made)

        if LAST_RUN_TS:
            LAST_RUN_TS.set(time.time())
    except Exception:
        status = "error"
    finally:
        if RUNS:
            RUNS.labels(status=status).inc()
        if LAT:
            LAT.observe(max(time.perf_counter() - started, 0.0))


async def main() -> None:  # pragma: no cover
    start_http_server(PORT)
    repo = GovernorRepo(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        database_url=os.getenv("DATABASE_URL", ""),
    )
    while True:
        await governance_loop(repo)
        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
