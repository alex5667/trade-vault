from __future__ import annotations

import logging
import math
import os
from typing import Any

import redis

from news_pipeline.grade import compute_horizon_sec, compute_news_grade_id
from news_pipeline.postgres_writer import NewsPostgresWriter
from news_pipeline.stream_worker import StreamWorker
from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

log = logging.getLogger("news_feature_store")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
NEWS_ANALYSIS_STREAM = os.getenv("NEWS_ANALYSIS_STREAM", RS.NEWS_ANALYSIS)
GROUP = os.getenv("NEWS_FEATURE_GROUP", "news-feature-store")
CONSUMER = os.getenv("NEWS_FEATURE_CONSUMER", os.getenv("HOSTNAME", "news-feature-1"))
DLQ = os.getenv("NEWS_ANALYSIS_DLQ", "news:analysis:dlq")
AGG_TTL_SEC = int(os.getenv("NEWS_AGG_TTL_SEC", str(2 * 3600)))
HALF_LIFE_SEC = int(os.getenv("NEWS_RISK_HALF_LIFE_SEC", "1800"))

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
    if half_life_sec <= 1:
        return 0.0
    if dt_sec <= 0:
        return 1.0
    return math.exp(-math.log(2.0) * (dt_sec / half_life_sec))

class NewsFeatureStoreWorker(StreamWorker):
    """
    Consume news:analysis stream and maintain online per-symbol aggregates.
    Redis key: news:agg:<SYMBOL> (HASH)
    Postgres: news_features_symbol, news_analysis
    """

    def __init__(self, *, redis: redis.Redis, pg: NewsPostgresWriter | None = None):
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

    def handle_message(self, msg_id: str, fields: dict[str, Any]) -> None:
        uid = (fields.get("uid") or "")
        symbol = ((fields.get("symbol") or "") or "GLOBAL").upper()
        if not uid:
            return

        risk_new = _safe_float(fields.get("risk"), 0.0)
        surprise_new = _safe_float(fields.get("surprise"), 0.0)
        tags_mask_new = _safe_int(fields.get("tags_mask"), 0) & ((1 << 64) - 1)
        primary_tag_new = _safe_int(fields.get("primary_tag_id"), 0)
        if primary_tag_new == 0:
            primary_tag_new = _safe_int(fields.get("primary_tag"), 0)

        conf_new = _safe_float(fields.get("confidence"), float(os.getenv("NEWS_DEFAULT_CONFIDENCE", "0.5")))
        published_ts_ms = _safe_int(fields.get("ts_ms"), 0)
        asof_ts_ms = get_ny_time_millis()

        key = f"news:agg:{symbol}"
        prev = self.r.hgetall(key) or {}

        prev_risk = _safe_float(prev.get("risk_ema", 0.0), 0.0)
        prev_ts = _safe_int(prev.get("asof_ts_ms", 0), 0)

        dt_sec = max(0.0, (asof_ts_ms - prev_ts) / 1000.0) if prev_ts > 0 else 0.0
        d = decay_factor(dt_sec, HALF_LIFE_SEC)

        risk_ema = max(prev_risk * d, risk_new)

        prev_surprise = _safe_float(prev.get("surprise_ema", 0.0), 0.0)
        if abs(surprise_new) >= abs(prev_surprise) * d:
            surprise_ema = surprise_new
        else:
            surprise_ema = prev_surprise * d

        prev_mask = _safe_int(prev.get("tags_mask", 0), 0) & ((1 << 64) - 1)
        tags_mask = int((prev_mask | tags_mask_new) & ((1 << 64) - 1))

        if risk_new >= (prev_risk * d):
            primary_tag_id = primary_tag_new
            ref = f"news:analysis:{uid}"
            confidence = conf_new
        else:
            primary_tag_id = _safe_int(prev.get("primary_tag_id", 0), 0)
            ref = str(prev.get("ref", "") or f"news:analysis:{uid}")
            confidence = _safe_float(prev.get("confidence", conf_new), conf_new)

        news_grade_id = compute_news_grade_id(news_risk=risk_ema, confidence=confidence, primary_tag_id=primary_tag_id)
        horizon_sec = compute_horizon_sec(primary_tag_id=primary_tag_id)

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

        if self.pg is not None:
            try:
                # Insert analysis
                self.pg.insert_news_analysis(
                    uid=uid,
                    symbol=symbol,
                    ts_ms=published_ts_ms or asof_ts_ms,
                    source=(fields.get("source") or "unknown"),
                    risk=risk_new,
                    surprise=surprise_new,
                    tags_mask=tags_mask_new,
                    primary_tag=primary_tag_new,
                    payload_json=dict(fields)
                )
                # Insert features
                self.pg.insert_news_features_symbol(
                    symbol=symbol,
                    ts_ms=asof_ts_ms,
                    risk=risk_ema,
                    surprise=surprise_ema,
                    tags_mask=tags_mask,
                    primary_tag=primary_tag_id,
                    ref=ref
                )
            except Exception:
                pass

def main() -> None:
    try:
        r = redis.Redis.from_url(REDIS_URL, decode_responses=True, health_check_interval=30)
        pg = None
        if os.getenv("PG_ENABLED", "1").lower() not in {"0", "false", "no"}:
            pg = NewsPostgresWriter.from_env()
            pg.ensure_schema()
        NewsFeatureStoreWorker(redis=r, pg=pg).run_forever()
    except BaseException as e:
        log.error(f"FATAL Exception in main: {e}", exc_info=True)
        import sys
        sys.exit(1)
    finally:
        log.info("news_feature_store main() completely exited.")
        import sys
        sys.stdout.flush()
        sys.stderr.flush()

if __name__ == "__main__":
    main()
