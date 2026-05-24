"""
Precision / ECE / AUC / expectancy for ml_confirm decisions.

Joins entries from `metrics:ml_confirm` (predictions, kind, p_edge_cal) with
`labels:tb` (outcomes, y_edge, r_mult), per `kind` label (e.g. edge_stack_v1,
ml_scorer_v4_enriched), over a rolling time window.

Without this, ml_confirm models are black-boxes — we know they emit ALLOW/DENY
but not whether they discriminate. This closes the loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from typing import Any

from prometheus_client import Gauge
from redis.asyncio import Redis

from core.redis_keys import RedisStreams as RS

log = logging.getLogger("ml_confirm_sre_poller.outcome_metrics")

# Prometheus gauges, labeled by `kind` so multiple model kinds can coexist.
ml_outcome_n = Gauge("ml_outcome_n", "Paired (prediction, outcome) sample size", ["kind"])
ml_outcome_auc = Gauge("ml_outcome_auc", "ROC AUC for ml_confirm kind", ["kind"])
ml_outcome_precision_top5pct = Gauge(
    "ml_outcome_precision_top5pct",
    "Precision (win rate) on top 5% of p_edge",
    ["kind"],
)
ml_outcome_expectancy_r_top5pct = Gauge(
    "ml_outcome_expectancy_r_top5pct",
    "Mean realized R on top 5% of p_edge",
    ["kind"],
)
ml_outcome_ece = Gauge("ml_outcome_ece", "Expected Calibration Error (10 bins)", ["kind"])
ml_outcome_brier = Gauge("ml_outcome_brier", "Brier score (mean squared error)", ["kind"])
ml_outcome_window_age_seconds = Gauge(
    "ml_outcome_window_age_seconds",
    "Age (seconds) of the data window used in this evaluation",
    ["kind"],
)


def _f(x: Any, default: float = float("nan")) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


async def _xrange_recent(r: Redis, stream: str, lookback_ms: int, batch: int = 5000) -> list[tuple[str, dict[str, Any]]]:
    """Read recent entries from a stream within lookback window. Returns (id, fields)."""
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - lookback_ms
    out: list[tuple[str, dict[str, Any]]] = []
    cursor = f"{start_ms}-0"
    try:
        while True:
            chunk = await r.xrange(stream, min=cursor, max="+", count=batch)
            if not chunk:
                break
            for entry_id, fields in chunk:
                out.append((entry_id, fields))
            last_id = chunk[-1][0]
            if last_id == cursor:
                break
            # bump cursor by 1 sequence
            base, _, seq = last_id.partition("-")
            cursor = f"{base}-{int(seq) + 1}"
            if len(out) >= batch * 20:
                # cap memory; downstream metrics will be on the sample.
                log.warning("xrange %s capped at %d entries", stream, len(out))
                break
    except Exception as e:
        log.warning("xrange failed for %s: %s", stream, e)
    return out


def _normalize_sid(raw_sid: Any) -> str:
    """Canonicalize sid to `crypto-of:SYMBOL:ts_ms` for cross-stream join.

    `metrics:ml_confirm` writes `crypto-of:SYMBOL:ts_ms`; `labels:tb` writes
    `<kind>:SYMBOL:ts_ms[:DIR]` where kind ∈ {of, iceberg, delta_spike, …}.
    Direct string compare misses all of them; collapse to a shared shape that
    keys only on (symbol, signal_ts) — direction stays on the label side
    via `r_mult`/`y_edge`.
    """
    s = str(raw_sid or "").strip()
    if not s:
        return ""
    parts = s.split(":")
    if len(parts) < 3:
        return s
    sym = (parts[1] or "").upper()
    try:
        t = int(parts[2])
    except (TypeError, ValueError):
        return s
    return f"crypto-of:{sym}:{t}"


def _parse_metrics_entry(fields: dict[str, Any]) -> dict[str, Any] | None:
    """metrics:ml_confirm fields are flat strings (XADD pairs). Return needed subset."""
    try:
        sid = str(fields.get("sid") or "")
        if not sid:
            return None
        kind = str(fields.get("kind") or "") or "unknown"
        p = _f(fields.get("p_edge_cal"), _f(fields.get("p_edge"), float("nan")))
        if not math.isfinite(p):
            return None
        return {"sid": _normalize_sid(sid), "kind": kind, "p_edge": p}
    except Exception:
        return None


def _parse_label_entry(fields: dict[str, Any]) -> dict[str, Any] | None:
    """labels:tb fields contain a JSON `payload`. Return needed subset for primary horizon."""
    try:
        raw = fields.get("payload")
        if not raw:
            return None
        d = json.loads(raw)
        if int(d.get("primary", 0) or 0) != 1:
            return None
        sid = str(d.get("sid") or "")
        if not sid:
            return None
        y = int(d.get("y_edge", 0) or 0)
        r = _f(d.get("r_mult"), float("nan"))
        if not math.isfinite(r):
            return None
        return {"sid": _normalize_sid(sid), "y": 1 if y > 0 else 0, "r_mult": r}
    except Exception:
        return None


def _auc(y_true: list[int], y_score: list[float]) -> float:
    pos = [s for s, yy in zip(y_score, y_true) if yy == 1]
    neg = [s for s, yy in zip(y_score, y_true) if yy == 0]
    if not pos or not neg:
        return 0.5
    wins = 0.0
    for p in pos:
        for n in neg:
            if p > n:
                wins += 1.0
            elif p == n:
                wins += 0.5
    return wins / (len(pos) * len(neg))


def _ece(y_true: list[int], y_prob: list[float], bins: int = 10) -> float:
    n = len(y_true)
    if n == 0:
        return 1.0
    bsum = [0.0] * bins
    bpos = [0] * bins
    bn = [0] * bins
    for yp, yt in zip(y_prob, y_true):
        idx = min(int(yp * bins), bins - 1)
        bsum[idx] += yp
        bpos[idx] += yt
        bn[idx] += 1
    ece = 0.0
    for i in range(bins):
        if bn[i] == 0:
            continue
        avg_p = bsum[i] / bn[i]
        avg_y = bpos[i] / bn[i]
        ece += (bn[i] / n) * abs(avg_p - avg_y)
    return ece


def _brier(y_true: list[int], y_prob: list[float]) -> float:
    if not y_true:
        return 1.0
    return sum((y - p) ** 2 for y, p in zip(y_true, y_prob)) / len(y_true)


def evaluate_kind(pairs: list[dict[str, Any]]) -> dict[str, float]:
    """pairs: list of {p_edge, y, r_mult}. Returns metrics dict."""
    n = len(pairs)
    if n == 0:
        return {"n": 0, "auc": 0.5, "precision_top5pct": 0.0, "expectancy_r_top5pct": 0.0, "ece": 1.0, "brier": 1.0}
    y_true = [p["y"] for p in pairs]
    y_prob = [p["p_edge"] for p in pairs]
    r_mult = [p["r_mult"] for p in pairs]
    top_n = max(1, int(n * 0.05))
    order = sorted(range(n), key=lambda i: y_prob[i], reverse=True)
    top = order[:top_n]
    precision = sum(y_true[i] for i in top) / top_n
    expectancy = sum(r_mult[i] for i in top) / top_n
    return {
        "n": float(n),
        "auc": _auc(y_true, y_prob),
        "precision_top5pct": precision,
        "expectancy_r_top5pct": expectancy,
        "ece": _ece(y_true, y_prob),
        "brier": _brier(y_true, y_prob),
    }


async def evaluate_outcomes(
    r: Redis,
    *,
    metrics_stream: str = RS.ML_CONFIRM_METRICS,
    labels_stream: str = RS.TB_LABELS,
    lookback_ms: int = 24 * 3600 * 1000,
) -> dict[str, dict[str, float]]:
    """One pass: read both streams, join by sid, compute metrics per kind. Updates Prometheus gauges."""
    t0 = time.time()
    metrics_entries, labels_entries = await asyncio.gather(
        _xrange_recent(r, metrics_stream, lookback_ms),
        _xrange_recent(r, labels_stream, lookback_ms),
    )

    # Build sid → outcome map (labels:tb is smaller, latest wins).
    labels_by_sid: dict[str, dict[str, Any]] = {}
    for _id, fields in labels_entries:
        rec = _parse_label_entry(fields)
        if rec:
            labels_by_sid[rec["sid"]] = rec

    # Group predictions by kind, joined.
    pairs_by_kind: dict[str, list[dict[str, Any]]] = {}
    for _id, fields in metrics_entries:
        m = _parse_metrics_entry(fields)
        if not m:
            continue
        outcome = labels_by_sid.get(m["sid"])
        if not outcome:
            continue
        pairs_by_kind.setdefault(m["kind"], []).append({
            "p_edge": m["p_edge"],
            "y": outcome["y"],
            "r_mult": outcome["r_mult"],
        })

    result: dict[str, dict[str, float]] = {}
    for kind, pairs in pairs_by_kind.items():
        m = evaluate_kind(pairs)
        result[kind] = m
        with contextlib_suppress():
            ml_outcome_n.labels(kind=kind).set(m["n"])
            ml_outcome_auc.labels(kind=kind).set(m["auc"])
            ml_outcome_precision_top5pct.labels(kind=kind).set(m["precision_top5pct"])
            ml_outcome_expectancy_r_top5pct.labels(kind=kind).set(m["expectancy_r_top5pct"])
            ml_outcome_ece.labels(kind=kind).set(m["ece"])
            ml_outcome_brier.labels(kind=kind).set(m["brier"])
            ml_outcome_window_age_seconds.labels(kind=kind).set(int(time.time() - t0))

    log.info("evaluate_outcomes: kinds=%d, pairs=%s, elapsed=%.2fs",
             len(pairs_by_kind),
             {k: int(v["n"]) for k, v in result.items()},
             time.time() - t0)
    return result


class contextlib_suppress:
    """Lightweight contextlib.suppress(Exception) without importing contextlib here."""
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return exc is not None  # swallow any exception


def get_eval_interval_sec() -> int:
    """Interval between outcome-metric evaluations (default 5 min)."""
    try:
        return int(os.getenv("ML_OUTCOME_METRICS_INTERVAL_SEC", "300") or 300)
    except (TypeError, ValueError):
        return 300


def get_lookback_ms() -> int:
    """Lookback window for joining predictions ↔ outcomes (default 24h)."""
    try:
        return int(os.getenv("ML_OUTCOME_METRICS_LOOKBACK_HOURS", "24") or 24) * 3600 * 1000
    except (TypeError, ValueError):
        return 24 * 3600 * 1000
