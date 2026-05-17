#!/usr/bin/env python3
"""ml_outcome_calibration_tracker_v1.py

Consumer service that joins ML scoring predictions to realized trade outcomes
and emits per-bucket ECE / Brier / precision histograms for the ML scorer.

Reads `trades:closed` Redis stream (produced by services/label_joiner.py),
which already carries the decision-time `ml_prob` (= conf01, the calibrated
edge probability) alongside the realized `result` (WIN/LOSS/BE) and `r_multiple`.

Output Prometheus metrics (Section 6 of ADR-0004):
  ml_p_edge_outcome_bucket{result, model_ver}  — histogram of p_edge values
                                                  segmented by realized outcome
  ml_ece_per_bucket{bucket, model_ver}          — Expected Calibration Error gauge
  ml_brier_per_bucket{bucket, model_ver}        — Brier score gauge per bucket
  ml_precision_top5_pct{model_ver}              — fraction of WINs in top-5% conf01
  ml_outcome_count{result, bucket, model_ver}   — counter by outcome class

Pipeline:
  trades:closed → consumer group → per-bucket EMA aggregator → /metrics

ENV
  ML_OUTCOME_TRACKER_GROUP            (default "ml-outcome-tracker")
  ML_OUTCOME_TRACKER_CONSUMER         (default ml-outcome-tracker-1)
  ML_OUTCOME_TRACKER_PORT             (default 9143)
  ML_OUTCOME_TRACKER_BATCH            (default 200, XREADGROUP COUNT)
  ML_OUTCOME_TRACKER_EMA_HL_TRADES    (default 500, half-life in trade count)
  ML_OUTCOME_TRACKER_BUCKETS          (default "0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0")
"""
from __future__ import annotations

import logging
import math
import os
import signal
import time
from typing import Any

from prometheus_client import REGISTRY, Counter, Gauge, Histogram, start_http_server  # type: ignore

from core.redis_client import get_redis
from core.redis_keys import RedisStreams as RS
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("ml_outcome_tracker")


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "")
    try:
        return float(raw) if raw else default
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    try:
        return int(raw) if raw else default
    except Exception:
        return default


def _parse_buckets(env_value: str) -> list[float]:
    try:
        edges = [float(x.strip()) for x in env_value.split(",") if x.strip()]
        edges = sorted(set(edges))
        if edges[0] > 0.0:
            edges = [0.0, *edges]
        if edges[-1] < 1.0:
            edges = [*edges, 1.0]
        return edges
    except Exception:
        return [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]


def _bucket_for(p: float, edges: list[float]) -> str:
    """Return human-readable bucket label e.g. '0.5-0.6' for p=0.55."""
    p = max(0.0, min(1.0, p))
    for i in range(len(edges) - 1):
        lo, hi = edges[i], edges[i + 1]
        if lo <= p < hi or (i == len(edges) - 2 and p == hi):
            return f"{lo:.1f}-{hi:.1f}"
    return f"{edges[0]:.1f}-{edges[-1]:.1f}"


def _safe_float(val: Any, default: float = float("nan")) -> float:
    try:
        if val is None:
            return default
        if isinstance(val, (bytes, bytearray)):
            val = val.decode("utf-8", "ignore")
        f = float(val)
        return f if math.isfinite(f) else default
    except Exception:
        return default


def _get_or_create_counter(name: str, doc: str, labels: list[str]) -> Counter:
    try:
        return Counter(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


def _get_or_create_gauge(name: str, doc: str, labels: list[str]) -> Gauge:
    try:
        return Gauge(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


def _get_or_create_histogram(name: str, doc: str, labels: list[str], buckets: tuple) -> Histogram:
    try:
        return Histogram(name, doc, labels, buckets=buckets)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


class OutcomeTracker:
    """Per-bucket EMA aggregator for win-rate, ECE, Brier."""

    def __init__(self, edges: list[float], ema_half_life: float) -> None:
        self.edges = edges
        # EMA alpha derived from half-life in trade-count units:
        # alpha = 1 - 0.5 ** (1/half_life)
        self.alpha = 1.0 - 0.5 ** (1.0 / max(1.0, ema_half_life))

        # Per (model_ver, bucket): EMA of win rate + EMA of p_edge midpoint
        self._win_rate: dict[tuple[str, str], float] = {}
        self._mean_p: dict[tuple[str, str], float] = {}
        self._brier_sum: dict[tuple[str, str], float] = {}
        self._sample_count: dict[tuple[str, str], int] = {}

        # Top-5% tracker (ring buffer of recent (p_edge, win) tuples)
        self._top5_buffer: dict[str, list[tuple[float, int]]] = {}
        self._top5_max_len = 1000

    def update(self, model_ver: str, p_edge: float, win: int) -> None:
        bucket = _bucket_for(p_edge, self.edges)
        key = (model_ver, bucket)

        # EMA update for win rate
        prev_wr = self._win_rate.get(key, p_edge)  # initialize at p_edge (= calibrated belief)
        self._win_rate[key] = (1 - self.alpha) * prev_wr + self.alpha * float(win)

        prev_mp = self._mean_p.get(key, p_edge)
        self._mean_p[key] = (1 - self.alpha) * prev_mp + self.alpha * p_edge

        # Brier per-sample contribution: (p - outcome)^2
        sample_brier = (p_edge - float(win)) ** 2
        prev_brier = self._brier_sum.get(key, sample_brier)
        self._brier_sum[key] = (1 - self.alpha) * prev_brier + self.alpha * sample_brier

        self._sample_count[key] = self._sample_count.get(key, 0) + 1

        # Top-5% buffer
        buf = self._top5_buffer.setdefault(model_ver, [])
        buf.append((p_edge, win))
        if len(buf) > self._top5_max_len:
            del buf[: len(buf) - self._top5_max_len]

    def get_ece_per_bucket(self, model_ver: str) -> dict[str, float]:
        """ECE_bucket = |empirical_win_rate - mean_p|, weighted by sample share."""
        out: dict[str, float] = {}
        for (mv, bucket), wr in self._win_rate.items():
            if mv != model_ver:
                continue
            mp = self._mean_p.get((mv, bucket), 0.0)
            out[bucket] = abs(wr - mp)
        return out

    def get_brier_per_bucket(self, model_ver: str) -> dict[str, float]:
        return {b: v for (mv, b), v in self._brier_sum.items() if mv == model_ver}

    def get_precision_top5(self, model_ver: str) -> float:
        """Fraction of WINs in the top-5% highest p_edge in the running buffer."""
        buf = self._top5_buffer.get(model_ver, [])
        if len(buf) < 20:  # need minimum sample
            return float("nan")
        sorted_buf = sorted(buf, key=lambda x: -x[0])
        top_n = max(1, len(sorted_buf) // 20)
        wins = sum(w for _, w in sorted_buf[:top_n])
        return wins / float(top_n)

    def get_bucket_sample_count(self, model_ver: str) -> dict[str, int]:
        return {b: v for (mv, b), v in self._sample_count.items() if mv == model_ver}


def _decode(val: Any) -> str:
    if isinstance(val, (bytes, bytearray)):
        return val.decode("utf-8", "ignore")
    return str(val) if val is not None else ""


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    port = _env_int("ML_OUTCOME_TRACKER_PORT", 9143)
    group = os.getenv("ML_OUTCOME_TRACKER_GROUP", "ml-outcome-tracker")
    consumer = os.getenv("ML_OUTCOME_TRACKER_CONSUMER", "ml-outcome-tracker-1")
    batch = _env_int("ML_OUTCOME_TRACKER_BATCH", 200)
    ema_hl = _env_float("ML_OUTCOME_TRACKER_EMA_HL_TRADES", 500.0)
    buckets_env = os.getenv("ML_OUTCOME_TRACKER_BUCKETS", "0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0")
    edges = _parse_buckets(buckets_env)

    logger.info(
        "Starting outcome tracker: port=%d group=%s consumer=%s ema_hl=%.0f buckets=%s",
        port, group, consumer, ema_hl, edges,
    )

    # Prometheus metrics
    metric_p_edge_outcome = _get_or_create_histogram(
        "ml_p_edge_outcome",
        "Distribution of decision-time p_edge segmented by realized outcome",
        ["result", "model_ver"],
        buckets=(0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95),
    )
    metric_ece = _get_or_create_gauge(
        "ml_ece_per_bucket",
        "Expected Calibration Error per p_edge bucket (|win_rate_ema - mean_p_ema|)",
        ["bucket", "model_ver"],
    )
    metric_brier = _get_or_create_gauge(
        "ml_brier_per_bucket",
        "Brier score EMA per p_edge bucket",
        ["bucket", "model_ver"],
    )
    metric_precision_top5 = _get_or_create_gauge(
        "ml_precision_top5_pct",
        "Win-rate fraction in top-5% highest p_edge over running buffer",
        ["model_ver"],
    )
    metric_outcome_count = _get_or_create_counter(
        "ml_outcome_count",
        "Total outcomes processed by outcome tracker, by class and bucket",
        ["result", "bucket", "model_ver"],
    )
    metric_lag_ms = _get_or_create_gauge(
        "ml_outcome_tracker_lag_ms",
        "Lag (ts_close - ts_decision) ms between decision and outcome",
        ["model_ver"],
    )
    metric_skipped = _get_or_create_counter(
        "ml_outcome_tracker_skipped",
        "Trades skipped due to missing/invalid ml_prob or result",
        ["reason"],
    )

    tracker = OutcomeTracker(edges, ema_hl)
    redis_client = get_redis()
    stream_key = RS.TRADES_CLOSED

    # Create consumer group (idempotent)
    try:
        redis_client.xgroup_create(stream_key, group, id="$", mkstream=True)
        logger.info("Created consumer group %s on %s", group, stream_key)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            logger.info("Consumer group %s already exists on %s", group, stream_key)
        else:
            logger.error("xgroup_create failed: %s", e)

    start_http_server(port)
    logger.info("HTTP /metrics on :%d", port)

    stop = {"flag": False}

    def _sig(_signo: int, _frame: Any) -> None:
        stop["flag"] = True
        logger.info("Shutdown signal received")

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    last_publish_ms = 0
    while not stop["flag"]:
        try:
            resp = redis_client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream_key: ">"},
                count=batch,
                block=2000,
            )
        except Exception as e:
            logger.error("XREADGROUP failed: %s", e)
            time.sleep(1.0)
            continue

        if not resp:
            # Periodic gauge re-publish even when stream is idle
            now_ms = get_ny_time_millis()
            if now_ms - last_publish_ms >= 30_000:
                _publish_all(tracker, metric_ece, metric_brier, metric_precision_top5)
                last_publish_ms = now_ms
            continue

        # resp is list of (stream_key, [(msg_id, fields_dict), ...]) — sync client.
        ack_ids: list[Any] = []
        for _sk, messages in resp:  # type: ignore[union-attr]
            for msg_id, fields in messages:
                try:
                    ack_ids.append(msg_id)
                    fields = {_decode(k): _decode(v) for k, v in fields.items()}
                    p_edge = _safe_float(fields.get("ml_prob"))
                    if not math.isfinite(p_edge):
                        metric_skipped.labels(reason="ml_prob_missing").inc()
                        continue
                    result = fields.get("result", "").upper()
                    if result not in ("WIN", "LOSS", "BE"):
                        metric_skipped.labels(reason="result_invalid").inc()
                        continue
                    # BE excluded from calibration math but still counted
                    win = 1 if result == "WIN" else 0
                    model_ver = fields.get("ml_version") or fields.get("model_ver") or "unknown"

                    bucket = _bucket_for(p_edge, edges)
                    metric_outcome_count.labels(result=result, bucket=bucket, model_ver=model_ver).inc()
                    metric_p_edge_outcome.labels(result=result, model_ver=model_ver).observe(p_edge)

                    # EV-R distribution histogram (Section 6)
                    r_mult = _safe_float(fields.get("r_multiple"))
                    if math.isfinite(r_mult) and result in ("WIN", "LOSS"):
                        _sym = fields.get("symbol", "")
                        _sfam = (
                            "BTC" if "BTC" in _sym else
                            "ETH" if "ETH" in _sym else
                            "SOL" if "SOL" in _sym else
                            "other"
                        )
                        try:
                            from services.orderflow.metrics import ml_ev_r_bucket
                            ml_ev_r_bucket.labels(bucket=bucket, symbol_family=_sfam).observe(r_mult)
                        except Exception:
                            pass

                    # ECE/Brier only on WIN/LOSS (BE muddles binary calibration)
                    if result in ("WIN", "LOSS"):
                        tracker.update(model_ver, p_edge, win)

                    ts_dec = _safe_float(fields.get("ts_decision"))
                    ts_close = _safe_float(fields.get("ts_close"))
                    if math.isfinite(ts_dec) and math.isfinite(ts_close) and ts_close >= ts_dec:
                        metric_lag_ms.labels(model_ver=model_ver).set(ts_close - ts_dec)

                except Exception as e:
                    logger.warning("Failed to process trade close %s: %s", msg_id, e)
                    metric_skipped.labels(reason="exception").inc()

        if ack_ids:
            try:
                redis_client.xack(stream_key, group, *ack_ids)
            except Exception as e:
                logger.error("XACK failed: %s", e)

        # Periodic publish after batch
        now_ms = get_ny_time_millis()
        if now_ms - last_publish_ms >= 5_000:
            _publish_all(tracker, metric_ece, metric_brier, metric_precision_top5)
            last_publish_ms = now_ms

    logger.info("Outcome tracker stopped")


def _publish_all(
    tracker: OutcomeTracker,
    metric_ece: Gauge,
    metric_brier: Gauge,
    metric_precision_top5: Gauge,
) -> None:
    seen_versions: set[str] = set()
    for (mv, _b) in tracker._win_rate:
        seen_versions.add(mv)
    for mv in seen_versions:
        for bucket, ece in tracker.get_ece_per_bucket(mv).items():
            metric_ece.labels(bucket=bucket, model_ver=mv).set(ece)
        for bucket, brier in tracker.get_brier_per_bucket(mv).items():
            metric_brier.labels(bucket=bucket, model_ver=mv).set(brier)
        prec = tracker.get_precision_top5(mv)
        if math.isfinite(prec):
            metric_precision_top5.labels(model_ver=mv).set(prec)


if __name__ == "__main__":
    main()
