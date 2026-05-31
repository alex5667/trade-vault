"""Single report cycle: read streams → normalize → group → stats → decision → emit."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import asdict
from typing import Any

from redis.asyncio import Redis

from core.redis_keys import RedisStreams as RS
from services.gate_value_reporter import prometheus_metrics as pm
from services.gate_value_reporter.bootstrap import bootstrap_avg_r_lift
from services.gate_value_reporter.contracts import (
    GateDecisionResult,
    GateOutcomeRecord,
)
from services.gate_value_reporter.decision import decide_gate_action
from services.gate_value_reporter.normalize import (
    build_ml_confirm_by_sid,
    group_key,
    normalize_gated_out_outcome,
    normalize_passed_label,
)
from services.gate_value_reporter.redis_reader import xrange_recent
from services.gate_value_reporter.stats import compute_cohort_stats, compute_gate_lift

log = logging.getLogger("gate_value_reporter.reporter")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return default


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v else default


def _streams_cfg() -> dict[str, str]:
    return {
        "passed_labels": _env_str("GATE_VALUE_PASSED_LABELS_STREAM", RS.TB_LABELS),
        "ml_confirm": _env_str("GATE_VALUE_ML_CONFIRM_STREAM", RS.ML_CONFIRM_METRICS),
        "gated_out_outcomes": _env_str(
            "GATE_VALUE_GATED_OUT_OUTCOMES_STREAM", RS.SIGNAL_GATED_OUT_OUTCOMES
        ),
    }


def _thresholds_cfg() -> dict[str, Any]:
    return {
        "min_n_passed": _env_int("GATE_VALUE_MIN_N_PASSED", 500),
        "min_n_gated_out": _env_int("GATE_VALUE_MIN_N_GATED_OUT", 500),
        "min_avg_r_lift": _env_float("GATE_VALUE_MIN_AVG_R_LIFT", 0.05),
        "max_false_negative_rate": _env_float(
            "GATE_VALUE_MAX_FALSE_NEGATIVE_RATE", 0.25
        ),
        "min_passed_expectancy_r": _env_float(
            "GATE_VALUE_MIN_PASSED_EXPECTANCY_R", 0.0
        ),
        "bootstrap_n": _env_int("GATE_VALUE_BOOTSTRAP_N", 1000),
        "bootstrap_seed": _env_int("GATE_VALUE_BOOTSTRAP_SEED", 42),
    }


async def load_records(
    r: Redis,
    *,
    lookback_ms: int,
    streams: dict[str, str],
) -> tuple[list[GateOutcomeRecord], list[GateOutcomeRecord]]:
    """Read 3 streams concurrently, return (passed_rows, gated_out_rows)."""
    labels_entries, ml_entries, gated_entries = await asyncio.gather(
        xrange_recent(r, streams["passed_labels"], lookback_ms),
        xrange_recent(r, streams["ml_confirm"], lookback_ms),
        xrange_recent(r, streams["gated_out_outcomes"], lookback_ms),
    )

    ml_by_sid = build_ml_confirm_by_sid(ml_entries)

    passed: list[GateOutcomeRecord] = []
    for _id, fields in labels_entries:
        rec = normalize_passed_label(
            fields, ml_by_sid, source_stream=streams["passed_labels"]
        )
        if rec is not None:
            passed.append(rec)

    gated_out: list[GateOutcomeRecord] = []
    for _id, fields in gated_entries:
        rec = normalize_gated_out_outcome(
            fields, source_stream=streams["gated_out_outcomes"]
        )
        if rec is not None:
            gated_out.append(rec)

    return passed, gated_out


def _group_label_values(gkey: tuple[str, str, int, int, int]) -> dict[str, str]:
    symbol, kind, horizon_ms, _tp, _sl = gkey
    return {
        "symbol": symbol,
        "kind": kind,
        "horizon": str(horizon_ms),
    }


def _build_group_report(
    gkey: tuple[str, str, int, int, int],
    passed_rows: list[GateOutcomeRecord],
    gated_rows: list[GateOutcomeRecord],
    *,
    thresholds: dict[str, Any],
) -> dict[str, Any]:
    p_stats = compute_cohort_stats(passed_rows)
    g_stats = compute_cohort_stats(gated_rows)
    lift = compute_gate_lift(p_stats, g_stats)
    ci = bootstrap_avg_r_lift(
        [r.r_mult for r in passed_rows],
        [r.r_mult for r in gated_rows],
        n_boot=int(thresholds["bootstrap_n"]),
        seed=int(thresholds["bootstrap_seed"]),
    )
    decision = decide_gate_action(
        passed=p_stats,
        gated_out=g_stats,
        lift=lift,
        avg_r_ci=ci,
        min_n_passed=int(thresholds["min_n_passed"]),
        min_n_gated_out=int(thresholds["min_n_gated_out"]),
        min_avg_r_lift=float(thresholds["min_avg_r_lift"]),
        max_false_negative_rate=float(thresholds["max_false_negative_rate"]),
    )

    symbol, kind, horizon_ms, tp_bucket, sl_bucket = gkey

    return {
        "group": {
            "symbol": symbol,
            "kind": kind,
            "horizon_ms": horizon_ms,
            "tp_bps_bucket": tp_bucket,
            "sl_bps_bucket": sl_bucket,
        },
        "passed": asdict(p_stats),
        "gated_out": asdict(g_stats),
        "lift": asdict(lift),
        "ci": {
            "avg_r_lift_p05": ci.lo,
            "avg_r_lift_p50": ci.mid,
            "avg_r_lift_p95": ci.hi,
        },
        "decision": {
            "action": decision.decision,
            "severity": decision.severity,
            "confidence": decision.confidence,
            "reason_codes": list(decision.reason_codes),
        },
    }


_DECISION_VALUES = (
    "KEEP_GATE",
    "TIGHTEN_GATE",
    "RELAX_GATE",
    "DISABLE_GATE",
    "INSUFFICIENT_DATA",
    "INCONCLUSIVE",
)


def _emit_group_prometheus(
    gkey: tuple[str, str, int, int, int],
    grp: dict[str, Any],
) -> None:
    labels = _group_label_values(gkey)
    p = grp["passed"]
    g = grp["gated_out"]
    lift = grp["lift"]
    ci = grp["ci"]
    decision = grp["decision"]["action"]

    pm.gate_value_passed_n.labels(**labels).set(p["n"])
    pm.gate_value_gated_out_n.labels(**labels).set(g["n"])
    pm.gate_value_passed_avg_r.labels(**labels).set(p["avg_r"])
    pm.gate_value_gated_out_avg_r.labels(**labels).set(g["avg_r"])
    pm.gate_value_passed_win_rate.labels(**labels).set(p["win_rate"])
    pm.gate_value_gated_out_win_rate.labels(**labels).set(g["win_rate"])
    pm.gate_value_passed_profit_factor.labels(**labels).set(p["profit_factor"])
    pm.gate_value_gated_out_profit_factor.labels(**labels).set(g["profit_factor"])
    pm.gate_value_avg_r_lift.labels(**labels).set(lift["avg_r_lift"])
    pm.gate_value_win_rate_lift.labels(**labels).set(lift["win_rate_lift"])
    pm.gate_value_profit_factor_lift.labels(**labels).set(lift["profit_factor_lift"])
    pm.gate_value_false_negative_rate.labels(**labels).set(lift["false_negative_rate"])
    pm.gate_value_avg_r_lift_ci_low.labels(**labels).set(ci["avg_r_lift_p05"])
    pm.gate_value_avg_r_lift_ci_high.labels(**labels).set(ci["avg_r_lift_p95"])

    for d in _DECISION_VALUES:
        pm.gate_value_decision.labels(**labels, decision=d).set(
            1.0 if d == decision else 0.0
        )


def build_report(
    passed: list[GateOutcomeRecord],
    gated_out: list[GateOutcomeRecord],
    *,
    lookback_hours: int,
    thresholds: dict[str, Any],
) -> dict[str, Any]:
    """Group + compute per-group; also emit an ALL-symbols/ALL-kinds rollup."""
    groups: dict[tuple[str, str, int, int, int], dict[str, list[GateOutcomeRecord]]] = {}
    for rec in passed:
        groups.setdefault(group_key(rec), {"passed": [], "gated_out": []})["passed"].append(rec)
    for rec in gated_out:
        groups.setdefault(group_key(rec), {"passed": [], "gated_out": []})["gated_out"].append(rec)

    per_group: list[dict[str, Any]] = []
    for gkey, bucket in sorted(groups.items()):
        grp = _build_group_report(
            gkey, bucket["passed"], bucket["gated_out"], thresholds=thresholds
        )
        per_group.append(grp)
        try:
            _emit_group_prometheus(gkey, grp)
        except Exception as e:
            log.warning("emit prometheus failed for %s: %s", gkey, e)

    overall_key = ("ALL", "ALL", 0, 0, 0)
    overall = _build_group_report(
        overall_key, passed, gated_out, thresholds=thresholds
    )
    try:
        _emit_group_prometheus(overall_key, overall)
    except Exception as e:
        log.warning("emit prometheus failed for overall: %s", e)

    return {
        "ts_ms": int(time.time() * 1000),
        "lookback_hours": lookback_hours,
        "n_groups": len(per_group),
        "overall": overall,
        "groups": per_group,
    }


async def write_report(r: Redis, report: dict[str, Any]) -> None:
    payload = json.dumps(report, separators=(",", ":"), default=str)
    latest_key = _env_str("GATE_VALUE_REPORT_REDIS_KEY", "report:gate_value:latest")
    history_stream = _env_str(
        "GATE_VALUE_REPORT_HISTORY_STREAM", "stream:reports:gate_value"
    )
    history_maxlen = _env_int("GATE_VALUE_REPORT_HISTORY_MAXLEN", 2000)

    try:
        await r.set(latest_key, payload)
    except Exception as e:
        log.warning("write latest report failed: %s", e)

    try:
        await r.xadd(
            history_stream,
            {"payload": payload},
            maxlen=history_maxlen,
            approximate=True,
        )
    except Exception as e:
        log.warning("write history report failed: %s", e)


async def run_once(r: Redis, *, lookback_hours: int | None = None) -> dict[str, Any]:
    t0 = time.time()
    lookback_h = lookback_hours if lookback_hours is not None else _env_int(
        "GATE_VALUE_LOOKBACK_HOURS", 24
    )
    lookback_ms = lookback_h * 3600 * 1000

    passed, gated_out = await load_records(
        r, lookback_ms=lookback_ms, streams=_streams_cfg()
    )
    report = build_report(
        passed,
        gated_out,
        lookback_hours=lookback_h,
        thresholds=_thresholds_cfg(),
    )
    await write_report(r, report)

    pm.gate_value_reporter_up.set(1.0)
    pm.gate_value_report_age_seconds.set(0.0)
    pm.gate_value_cycle_duration_seconds.set(time.time() - t0)

    log.info(
        "gate_value report: groups=%d passed=%d gated_out=%d elapsed=%.2fs",
        report["n_groups"],
        len(passed),
        len(gated_out),
        time.time() - t0,
    )
    return report


async def run_loop(redis_url: str) -> None:
    """Periodic loop. Interval from GATE_VALUE_INTERVAL_SEC (default 300s)."""
    from redis.asyncio import Redis as AsyncRedis

    interval = _env_int("GATE_VALUE_INTERVAL_SEC", 300)
    log.info("gate_value_reporter starting; interval=%ds", interval)

    r = AsyncRedis.from_url(redis_url, decode_responses=True)
    last_ok_ts = 0.0

    try:
        while True:
            cycle_start = time.time()
            try:
                report: GateDecisionResult | dict[str, Any] = await run_once(r)
                last_ok_ts = time.time()
                _ = report
            except Exception as e:
                log.exception("report cycle failed: %s", e)
                pm.gate_value_reporter_up.set(0.0)

            if last_ok_ts > 0:
                pm.gate_value_report_age_seconds.set(time.time() - last_ok_ts)

            elapsed = time.time() - cycle_start
            sleep_for = max(1.0, interval - elapsed)
            await asyncio.sleep(sleep_for)
    finally:
        try:
            await r.aclose()
        except Exception:
            pass
