from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from utils.time_utils import get_ny_time_millis

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


APP_NAME = "operator_routing_incident_rca_experiment_winner_selector_v2_13"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_WINNER_PORT", "9892"))

EXPERIMENT_ID = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_ID", "routing_incident_rca_ab_v1")
WINDOW_MINUTES = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_WINDOW_MIN", "1440"))
MIN_SAMPLE = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_MIN_SAMPLE", "8"))

DECISIONS_STREAM = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_WINNER_DECISIONS_STREAM",
    "stream:ml:operator_routing_incident_rca_experiment_winner_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_AUDIT_STREAM",
    "stream:ml:operator_routing_incident_rca_experiment_audit",
)
WINNER_KEY_PREFIX = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_WINNER_REDIS_PREFIX",
    "cfg:ml:operator_routing_incident_rca_experiment:winner",
)

POLL_INTERVAL = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_WINNER_POLL_INTERVAL", "60"))
MAXLEN = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_MAXLEN", "10000"))


def _counter(name: str, doc: str, labels: tuple = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: tuple = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: tuple = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_operator_routing_incident_rca_experiment_winner_runs_total",
    "Routing incident RCA experiment winner runs",
    ("status",),
)
LAT = _hist(
    "ml_operator_routing_incident_rca_experiment_winner_latency_seconds",
    "Routing incident RCA experiment winner latency seconds",
)
LAST_RUN_TS = _gauge(
    "ml_operator_routing_incident_rca_experiment_winner_last_run_ts_seconds",
    "Routing incident RCA experiment winner last run ts",
)
WINNERS_SELECTED = _counter(
    "ml_operator_routing_incident_rca_experiment_winners_selected_total",
    "Routing incident RCA experiment winners selected",
    ("experiment_id", "winner_bucket"),
)


def now_ms() -> int:
    return get_ny_time_millis()


class ExperimentWinnerRepo:
    def __init__(self, db_url: str) -> None:
        self.db_url = db_url

    def fetch_bucket_stats(self) -> list[dict[str, Any]]:
        if psycopg is None:
            return []
        cutoff_ms = now_ms() - (WINDOW_MINUTES * 60 * 1000)
        with psycopg.connect(self.db_url) as conn, conn.cursor() as cur:
            cur.execute(
                """,
                    SELECT
                        bucket,
                        COUNT(*) as sample_n,
                        AVG(quality_score) as avg_quality,
                        AVG(usefulness_score) as avg_usefulness
                    FROM llm_operator_routing_incident_rca_exposures e
                    JOIN llm_operator_routing_incident_rca_results r
                      ON e.route_change_id = r.route_change_id
                    WHERE e.experiment_id = %s
                      AND e.ts_ms > %s
                      AND r.quality_score IS NOT NULL
                      AND r.usefulness_score IS NOT NULL
                    GROUP BY bucket,
                    """
                (EXPERIMENT_ID, cutoff_ms),
            )
            rows = cur.fetchall()
        return [
            {
                "bucket": row[0],
                "sample_n": row[1],
                "avg_quality": float(row[2]) if row[2] else 0.0,
                "avg_usefulness": float(row[3]) if row[3] else 0.0,
            }
            for row in rows
        ]


def calculate_combined_score(avg_quality: float, avg_usefulness: float) -> float:
    # identical to usefulness governor
    # use USEFULNESS 60%, QUALITY 40%
    return (avg_quality * 0.4) + (avg_usefulness * 0.6)


def select_winner(stats: list[dict[str, Any]]) -> tuple[str, float]:
    best_bucket = "none"
    best_score = -1.0

    for s in stats:
        if s["sample_n"] < MIN_SAMPLE:
            continue
        score = calculate_combined_score(s["avg_quality"], s["avg_usefulness"])
        if score > best_score:
            best_score = score
            best_bucket = s["bucket"]

    return best_bucket, best_score


async def winner_loop(r: Any, repo: ExperimentWinnerRepo) -> None:
    started = time.perf_counter()
    status = "ok"
    try:
        stats = repo.fetch_bucket_stats()
        winner_bucket, winner_score = select_winner(stats)

        if winner_bucket != "none":
            # formulate decision
            decision = {
                "experiment_id": EXPERIMENT_ID,
                "winner_bucket": winner_bucket,
                "winner_score": winner_score,
                "ts_ms": now_ms(),
                "advisory_only": "1",
            }

            # append stats
            for s in stats:
                b = s["bucket"]
                decision[f"bucket_{b}_sample_n"] = s["sample_n"]
                decision[f"bucket_{b}_avg_quality"] = s["avg_quality"]
                decision[f"bucket_{b}_avg_usefulness"] = s["avg_usefulness"]

            # Publish
            await r.xadd(DECISIONS_STREAM, decision, maxlen=MAXLEN, approximate=True)
            await r.xadd(AUDIT_STREAM, decision, maxlen=MAXLEN, approximate=True)

            # Update advisory state hash
            key = f"{WINNER_KEY_PREFIX}:{EXPERIMENT_ID}"
            await r.hset(key, mapping={
                "winner_bucket": winner_bucket,
                "winner_score": str(winner_score),
                "ts_ms": str(decision["ts_ms"])
            })

            if WINNERS_SELECTED:
                WINNERS_SELECTED.labels(experiment_id=EXPERIMENT_ID, winner_bucket=winner_bucket).inc()

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
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    repo = ExperimentWinnerRepo(db_url=os.getenv("DATABASE_URL", ""))
    while True:
        await winner_loop(r, repo)
        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
