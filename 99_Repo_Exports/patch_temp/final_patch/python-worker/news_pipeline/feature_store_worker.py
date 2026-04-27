from __future__ import annotations

import logging
import math
import os
import time
from typing import Any, Dict, Optional

import redis

from news_pipeline.stream_worker import StreamWorker
from news_pipeline.grade import compute_news_grade_id, compute_horizon_sec

# Optional Postgres persistence (raw & aggregates). We keep it optional so
# the worker can run in Redis-only mode.
try:
    from news_pipeline.postgres_writer import NewsPostgresWriter
except Exception:  # pragma: no cover
    NewsPostgresWriter = None  # type: ignore


log = logging.getLogger("news_feature_store")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Stream produced by analyzer_worker.py (per-symbol rows).
NEWS_ANALYSIS_STREAM = os.getenv("NEWS_ANALYSIS_STREAM", "news:analysis")

# Consumer group for this worker.
GROUP = os.getenv("NEWS_FEATURE_GROUP", "news-feature-store")
CONSUMER = os.getenv("NEWS_FEATURE_CONSUMER", os.getenv("HOSTNAME", "news-feature-1"))

# Dead-letter stream for messages that permanently fail.
DLQ = os.getenv("NEWS_ANALYSIS_DLQ", "news:analysis:dlq")

# TTL for news:agg:<symbol> hashes.
AGG_TTL_SEC = int(os.getenv("NEWS_AGG_TTL_SEC", str(2 * 3600)))

# EMA/decay half-life (seconds). Interpreted as a time constant for decay factor.
HALF_LIFE_SEC = int(os.getenv("NEWS_RISK_HALF_LIFE_SEC", "1800"))  # 30m

# Postgres optional
POSTGRES_DSN = os.getenv("NEWS_POSTGRES_DSN", os.getenv("POSTGRES_DSN", ""))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def decay_factor(dt_sec: float, half_life_sec: float) -> float:
    """Compute multiplicative decay factor for elapsed time.

    d = exp(-ln2 * dt/half_life)

    Interpretation:
      prev' = prev*d + new*(1-d)

    Properties:
      - dt=0  => d=1 (keep prev)
      - dt=hl => d≈0.5
      - dt->∞ => d->0 (forget prev)
    """
    if half_life_sec <= 1:
        return 0.0
    if dt_sec <= 0:
        return 1.0
    return math.exp(-math.log(2.0) * (dt_sec / half_life_sec))


class NewsFeatureStoreWorker(StreamWorker):
    """Consume news:analysis stream and maintain online per-symbol aggregates.

    Redis write model (fast path for tick-loop):
      key: news:agg:<SYMBOL> (HASH)
      fields:
        - ref            : "news:analysis:<uid>" pointer to heavy JSON
        - risk_ema       : float (0..1)
        - surprise_ema   : float (signed, [-1..1] typical)
        - news_grade_id  : int (0..4)
        - tags_mask      : uint64 as int
        - primary_tag_id : int
        - horizon_sec    : int (seconds)
        - confidence     : float (0..1)
        - asof_ts_ms     : int64 ms epoch

    Persistence (optional, for backtests):
      - news_analysis        (uid, symbol) PK
      - news_features_symbol (symbol, ts_ms) PK

    Important: this worker is NOT in the tick loop, so it can afford heavier
    operations than NewsEnricherSync. Still we keep it efficient and fail-open.
    """

    def __init__(self, *, redis: redis.Redis, pg: Optional["NewsPostgresWriter"] = None):
        super().__init__(
            redis=redis,
            stream=NEWS_ANALYSIS_STREAM,
            group=GROUP,
            consumer=CONSUMER,
            dlq_stream=DLQ,
            block_ms=2000,
            count=200,
            claim_idle_ms=60_000,
        )
        self.pg = pg
        self._flush_deadline_ms = 0

    def handle_message(self, msg_id: str, fields: Dict[str, Any]) -> None:
        uid = str(fields.get("uid") or "")
        symbol = (str(fields.get("symbol") or "") or "GLOBAL").upper()
        if not uid:
            return

        # --- input from analyzer (all fields are stringable) ---
        risk_new = _safe_float(fields.get("risk"), 0.0)
        surprise_new = _safe_float(fields.get("surprise"), 0.0)
        tags_mask_new = _safe_int(fields.get("tags_mask"), 0) & ((1 << 64) - 1)
        primary_tag_new = _safe_int(fields.get("primary_tag_id"), 0)
        if primary_tag_new == 0:
            primary_tag_new = _safe_int(fields.get("primary_tag"), 0)  # backward compat

        # analyzer may omit confidence; default is a safe mid value.
        conf_new = _safe_float(fields.get("confidence"), float(os.getenv("NEWS_DEFAULT_CONFIDENCE", "0.5")))

        # published ts is supplied by analyzer, but may be missing
        published_ts_ms = _safe_int(fields.get("ts_ms"), 0)

        # as-of timestamp for aggregates
        asof_ts_ms = int(time.time() * 1000)

        key = f"news:agg:{symbol}"
        prev = self.r.hgetall(key) or {}

        prev_risk = _safe_float(prev.get("risk_ema", 0.0), 0.0)
        prev_ts = _safe_int(prev.get("asof_ts_ms", 0), 0)

        dt_sec = max(0.0, (asof_ts_ms - prev_ts) / 1000.0) if prev_ts > 0 else 0.0
        d = decay_factor(dt_sec, HALF_LIFE_SEC)

        # --- EMA/decay logic ---
        # risk: we keep the max of decayed previous vs new, so a large-impact news
        # stays elevated until it naturally decays away.
        risk_ema = max(prev_risk * d, risk_new)

        # surprise: keep sign, but compare by absolute value so we preserve direction.
        prev_surprise = _safe_float(prev.get("surprise_ema", 0.0), 0.0)
        surprise_candidate = abs(prev_surprise) * d
        if abs(surprise_new) >= surprise_candidate:
            surprise_ema = surprise_new
        else:
            # decay old surprise, preserving sign
            surprise_ema = prev_surprise * d

        # tags: union (OR) to avoid losing categories while a big event is active.
        prev_mask = _safe_int(prev.get("tags_mask", 0), 0) & ((1 << 64) - 1)
        tags_mask = int((prev_mask | tags_mask_new) & ((1 << 64) - 1))

        # primary tag selection:
        # if new risk dominates current aggregate, replace primary tag and confidence.
        prev_primary = _safe_int(prev.get("primary_tag_id", 0), 0)
        if risk_new >= (prev_risk * d):
            primary_tag_id = primary_tag_new
            ref = f"news:analysis:{uid}"
            confidence = conf_new
        else:
            primary_tag_id = prev_primary
            ref = str(prev.get("ref", "") or f"news:analysis:{uid}")
            confidence = _safe_float(prev.get("confidence", conf_new), conf_new)

        # grade + horizon: policy lives in news_pipeline/grade.py
        news_grade_id = compute_news_grade_id(news_risk=risk_ema, confidence=confidence, primary_tag_id=primary_tag_id)
        horizon_sec = compute_horizon_sec(primary_tag_id=primary_tag_id)

        # --- Redis update (single pipeline) ---
        pipe = self.r.pipeline(transaction=False)
        pipe.hset(
            key,
            mapping={
                "ref": ref,
                "risk_ema": float(risk_ema),
                "surprise_ema": float(surprise_ema),
                "news_grade_id": int(news_grade_id),
                "tags_mask": int(tags_mask),
                "primary_tag_id": int(primary_tag_id),
                "horizon_sec": int(horizon_sec),
                "confidence": float(confidence),
                "asof_ts_ms": int(asof_ts_ms),
            },
        )
        pipe.expire(key, AGG_TTL_SEC)
        pipe.execute()

        # --- Optional Postgres persistence (batch, fail-open) ---
        if self.pg is not None:
            try:
                # 1) persist the per-message analysis row (raw).
                self.pg.enqueue_analysis(
                    uid=uid,
                    symbol=symbol,
                    ts_ms=published_ts_ms,
                    source=str(fields.get("source") or ""),
                    risk=risk_new,
                    surprise=surprise_new,
                    tags_mask=tags_mask_new,
                    primary_tag=primary_tag_new,
                    payload_json=dict(fields),
                )
                # 2) persist the aggregate snapshot (features).
                self.pg.enqueue_feature(
                    symbol=symbol,
                    ts_ms=asof_ts_ms,
                    risk=risk_ema,
                    surprise=surprise_ema,
                    tags_mask=tags_mask,
                    primary_tag=primary_tag_id,
                    ref=ref,
                )
                # Flush in batches to reduce DB overhead.
                now_ms = asof_ts_ms
                if now_ms >= self._flush_deadline_ms:
                    self.pg.flush_all()
                    self._flush_deadline_ms = now_ms + 500  # at most twice per second
            except Exception:
                # Do not break the Redis pipeline if Postgres is down.
                pass


def main() -> None:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True, health_check_interval=30)

    pg = None
    if POSTGRES_DSN and NewsPostgresWriter is not None:
        try:
            pg = NewsPostgresWriter(dsn=POSTGRES_DSN)
            pg.ensure_schema()
        except Exception as e:
            log.warning("Postgres disabled (init failed): %s", e)
            pg = None

    NewsFeatureStoreWorker(redis=r, pg=pg).run_forever()


if __name__ == "__main__":
    main()
