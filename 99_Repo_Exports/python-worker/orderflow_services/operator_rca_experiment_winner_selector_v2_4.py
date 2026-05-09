from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from statistics import mean
from typing import Any

from utils.time_utils import get_ny_time_millis

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, start_http_server

EXPOSURES_STREAM = os.getenv("ML_OPERATOR_RCA_EXPOSURES_STREAM", "stream:ml:operator_rca_exposures")
FEEDBACK_STREAM = os.getenv("ML_OPERATOR_RCA_FEEDBACK_STREAM", "stream:ml:operator_rca_feedback")
QUALITY_STREAM = os.getenv("ML_OPERATOR_RCA_QUALITY_STREAM", "stream:ml:operator_rca_quality_results")
DECISIONS_STREAM = os.getenv("ML_OPERATOR_RCA_EXPERIMENT_DECISIONS_STREAM", "stream:ml:operator_rca_experiment_winner_decisions")
AUDIT_STREAM = os.getenv("ML_OPERATOR_RCA_EXPERIMENT_AUDIT_STREAM", "stream:ml:operator_rca_experiment_audit")

WINNERS_TOTAL = Counter(
    "ml_operator_rca_experiment_winner_decisions_total",
    "Experiment winner decisions",
    ["experiment", "decision", "winning_arm"],
)
WINNER_SCORE = Gauge(
    "ml_operator_rca_experiment_winner_score",
    "Winning arm composite score",
    ["experiment", "arm"],
)
WINNER_LAST_RUN_TS = Gauge(
    "ml_operator_rca_experiment_winner_last_run_ts_seconds",
    "Last run timestamp",
)
WINNER_UP = Gauge(
    "ml_operator_rca_experiment_winner_up",
    "Winner selector heartbeat",
)


def usefulness_to_score(decision: str) -> float:
    return {
        "VERY_USEFUL": 1.0,
        "USEFUL": 0.75,
        "MIXED": 0.50,
        "NOT_USEFUL": 0.0,
    }.get(str(decision).upper(), 0.50)


@dataclass
class ArmStats:
    experiment_id: str
    arm: str
    provider: str
    model_name: str
    prompt_version: str
    exposures: int = 0
    quality: list[float] = None  # type: ignore
    usefulness: list[float] = None  # type: ignore

    def __post_init__(self) -> None:
        self.quality = self.quality or []
        self.usefulness = self.usefulness or []

    @property
    def quality_avg(self) -> float:
        return float(mean(self.quality)) if self.quality else 0.0

    @property
    def usefulness_avg(self) -> float:
        return float(mean(self.usefulness)) if self.usefulness else 0.0

    @property
    def composite(self) -> float:
        # usefulness matters more than shape quality
        return 0.35 * self.quality_avg + 0.65 * self.usefulness_avg


def _within_window(ts_ms: int, now_ms: int, window_min: int) -> bool:
    return ts_ms > 0 and now_ms - ts_ms <= window_min * 60_000


def aggregate_arm_stats(
    exposures: Iterable[dict[str, Any]],
    quality_rows: Iterable[dict[str, Any]],
    feedback_rows: Iterable[dict[str, Any]],
    now_ms: int,
    window_min: int,
) -> dict[tuple[str, str], ArmStats]:
    by_req: dict[str, tuple[str, str]] = {}
    stats: dict[tuple[str, str], ArmStats] = {}
    for row in exposures:
        ts_ms = int(row.get("ts_ms", 0) or 0)
        if not _within_window(ts_ms, now_ms, window_min):
            continue
        exp = (row.get("experiment_id", ""))
        arm = (row.get("arm", ""))
        if not exp or not arm:
            continue
        key = (exp, arm)
        if key not in stats:
            stats[key] = ArmStats(exp, arm, (row.get("provider", "")), (row.get("model_name", "")), (row.get("prompt_version", "")))
        stats[key].exposures += 1
        req_id = (row.get("request_id", ""))
        if req_id:
            by_req[req_id] = key

    for row in quality_rows:
        ts_ms = int(row.get("ts_ms", 0) or 0)
        if not _within_window(ts_ms, now_ms, window_min):
            continue
        req_id = (row.get("request_id", ""))
        key = by_req.get(req_id)
        if not key or key not in stats:
            continue
        try:
            stats[key].quality.append(float(row.get("quality_score", 0.0) or 0.0))
        except Exception:
            continue

    for row in feedback_rows:
        ts_ms = int(row.get("ts_ms", 0) or 0)
        if not _within_window(ts_ms, now_ms, window_min):
            continue
        req_id = (row.get("request_id", "")) or (row.get("recommendation_id", ""))
        key = by_req.get(req_id)
        if not key or key not in stats:
            continue
        stats[key].usefulness.append(usefulness_to_score((row.get("decision", "MIXED"))))
    return stats


def choose_winner(stats: dict[tuple[str, str], ArmStats], min_sample: int) -> list[dict[str, Any]]:
    grouped: defaultdict[str, list[ArmStats]] = defaultdict(list)
    for (exp, _arm), st in stats.items():
        grouped[exp].append(st)

    out: list[dict[str, Any]] = []
    for experiment_id, arms in grouped.items():
        eligible = [a for a in arms if a.exposures >= min_sample]
        if len(eligible) < 2:
            out.append({
                "experiment_id": experiment_id,
                "decision": "HOLD",
                "winning_arm": "",
                "reason": "INSUFFICIENT_SAMPLE",
            })
            continue
        eligible = sorted(eligible, key=lambda x: (x.composite, x.usefulness_avg, x.quality_avg), reverse=True)
        winner = eligible[0]
        runner_up = eligible[1]
        delta = winner.composite - runner_up.composite
        decision = "PROMOTE" if delta >= 0.08 else "HOLD"
        out.append({
            "experiment_id": experiment_id,
            "decision": decision,
            "winning_arm": winner.arm,
            "winning_provider": winner.provider,
            "winning_model_name": winner.model_name,
            "winning_prompt_version": winner.prompt_version,
            "winning_score": round(winner.composite, 6),
            "runner_up_arm": runner_up.arm,
            "runner_up_score": round(runner_up.composite, 6),
            "delta_score": round(delta, 6),
            "reason": "BEST_COMPOSITE_USEFULNESS",
        })
    return out


async def _xrevrange_all(r: Any, stream: str, count: int) -> list[dict[str, Any]]:
    try:
        rows = await r.xrevrange(stream, count=count)
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for _msg_id, payload in rows:
        out.append(dict(payload))
    return out


async def run() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(int(os.getenv("ML_OPERATOR_RCA_EXPERIMENT_WINNER_METRICS_PORT", "9876")))
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    WINNER_UP.set(1)
    window_min = int(os.getenv("ML_OPERATOR_RCA_EXPERIMENT_WINDOW_MIN", "1440") or 1440)
    min_sample = int(os.getenv("ML_OPERATOR_RCA_EXPERIMENT_MIN_SAMPLE", "8") or 8)
    scan_count = int(os.getenv("ML_OPERATOR_RCA_EXPERIMENT_SCAN_COUNT", "2000") or 2000)
    loop_sec = float(os.getenv("ML_OPERATOR_RCA_EXPERIMENT_WINNER_LOOP_SEC", "300") or 300)
    while True:
        now_ms = get_ny_time_millis()
        exposures = await _xrevrange_all(r, EXPOSURES_STREAM, scan_count)
        quality_rows = await _xrevrange_all(r, QUALITY_STREAM, scan_count)
        feedback_rows = await _xrevrange_all(r, FEEDBACK_STREAM, scan_count)
        stats = aggregate_arm_stats(exposures, quality_rows, feedback_rows, now_ms, window_min)
        decisions = choose_winner(stats, min_sample)
        for row in decisions:
            exp = (row.get("experiment_id", ""))
            arm = (row.get("winning_arm", ""))
            decision = (row.get("decision", "HOLD"))
            await r.xadd(DECISIONS_STREAM, {"ts_ms": now_ms, **row}, maxlen=100000, approximate=True)
            await r.xadd(AUDIT_STREAM, {"ts_ms": now_ms, "event": "WINNER_DECISION", **row}, maxlen=50000, approximate=True)
            await r.hset(f"cfg:ml:operator_rca_experiment:winner:{exp}", mapping={k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in row.items()})
            WINNERS_TOTAL.labels(exp, decision, arm).inc()
            if arm:
                WINNER_SCORE.labels(exp, arm).set(float(row.get("winning_score", 0.0) or 0.0))
        WINNER_LAST_RUN_TS.set(time.time())
        await r.hset("metrics:ml:operator_rca_experiment:last", mapping={"ts_ms": now_ms, "decisions_n": len(decisions)})
        await r.xadd(AUDIT_STREAM, {"ts_ms": now_ms, "event": "WINNER_LOOP_COMPLETE", "decisions_n": len(decisions)}, maxlen=50000, approximate=True)
        import asyncio
        await asyncio.sleep(loop_sec)


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(run())
