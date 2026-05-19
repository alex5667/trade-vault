# news_pipeline/analyzer_worker.py
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import redis

from news_pipeline.llm_client import FallbackLLMClient, GeminiHTTPClient, NvidiaKimiClient, NvidiaQwenClient
from news_pipeline.stream_worker import StreamWorker
from news_pipeline.tags import pick_primary_tag, tags_to_mask
from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS

log = logging.getLogger("news_analyzer")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

NEWS_RAW_STREAM = os.getenv("NEWS_RAW_STREAM", RS.NEWS_RAW)
NEWS_ANALYSIS_STREAM = os.getenv("NEWS_ANALYSIS_STREAM", RS.NEWS_ANALYSIS)

GROUP = os.getenv("NEWS_ANALYZER_GROUP", "news-analyzer")
CONSUMER = os.getenv("NEWS_ANALYZER_CONSUMER", os.getenv("HOSTNAME", "news-analyzer-1"))
DLQ = os.getenv("NEWS_ANALYZER_DLQ", "news:raw:dlq")

ANALYSIS_TTL_SEC = int(float(os.getenv("NEWS_ANALYSIS_TTL_SEC", "259200")))  # 3d
ANALYSIS_DONE_TTL_SEC = int(float(os.getenv("NEWS_ANALYSIS_DONE_TTL_SEC", "604800")))  # 7d


def _safe_s(v: Any) -> str:
    return (v or "").strip()


def _parse_symbols_json(s: str) -> list[str]:
    s = (s or "").strip()
    if not s:
        return []
    try:
        j = json.loads(s)
        if isinstance(j, list):
            out = []
            for x in j:
                if isinstance(x, str) and x.strip():
                    out.append(x.strip().upper())
            return out
    except Exception:
        pass
    return []


class NewsAnalyzerWorker(StreamWorker):
    """
    Pipeline:
    news:raw (from Go ingestor) ->
      - dedupe: SETNX news:analysis:done:<uid> EX 7d
      - LLM classify -> risk/surprise/tags/confidence/summary
      - heavy store: SET news:analysis:<uid> JSON EX 3d
      - emit one record per symbol to news:analysis stream
    """

    def __init__(self, *, redis: redis.Redis):
        super().__init__(
            redis=redis,
            stream=NEWS_RAW_STREAM,
            group=GROUP,
            consumer=CONSUMER,
            dlq_stream=DLQ,
            block_ms=2000,
            count=100,
            claim_idle_ms=60_000,
        )
        primary_llm = GeminiHTTPClient()
        if os.getenv("LLM_FALLBACK_ENABLED", "1") == "1":
            fallback_llm_qwen = NvidiaQwenClient()
            fallback_llm_kimi = NvidiaKimiClient()
            self.llm = FallbackLLMClient([primary_llm, fallback_llm_qwen, fallback_llm_kimi])
        else:
            self.llm = primary_llm

    def handle_message(self, msg_id: str, fields: dict[str, Any]) -> None:
        uid = _safe_s(fields.get("uid"))
        if not uid:
            return

        # Idempotency:
        # - done_key is set ONLY after successful heavy-store + stream emit
        # - lease_key prevents parallel duplicate work and auto-expires on crashes
        done_key = f"news:analysis:done:{uid}"
        if self.r.get(done_key):
            return
        lease_key = f"news:analysis:lease:{uid}"
        lease_sec = int(float(os.getenv("NEWS_ANALYSIS_LEASE_SEC", "300")))
        if not self.r.set(lease_key, CONSUMER, nx=True, ex=lease_sec):
            return

        try:
            title = _safe_s(fields.get("title"))
            url = _safe_s(fields.get("url"))
            source = _safe_s(fields.get("source"))
            summary = _safe_s(fields.get("summary"))

            published_ts_ms = int(float(fields.get("published_ts_ms") or 0) or 0)
            if published_ts_ms <= 0:
                published_ts_ms = get_ny_time_millis()

            syms = _parse_symbols_json(_safe_s(fields.get("symbols")))
            if not syms:
                syms = ["GLOBAL"]

            # LLM analysis
            a = self.llm.analyze(title=title, url=url, source=source, summary=summary)

            # heavy JSON store
            heavy = {
                "uid": uid,
                "source": source,
                "url": url,
                "title": title,
                "ts_ms": published_ts_ms,
                "symbols": syms,
                "analysis": a,
                "raw": {k: _safe_s(v) for k, v in fields.items()},
            }
            heavy_key = f"news:analysis:{uid}"
            self.r.setex(heavy_key, ANALYSIS_TTL_SEC, json.dumps(heavy, ensure_ascii=False))

            # emit to stream (per symbol)
            # важно: все поля stringable
            now_ms = get_ny_time_millis()

            # Calculate tags metrics
            tags = a.get("tags") or []
            mask = tags_to_mask(tags)
            primary_tag_id = pick_primary_tag(tags)

            for sym in syms:
                out = {
                    "uid": uid,
                    "symbol": sym,
                    "risk": (a.get("risk", 0.0)),
                    "surprise": (a.get("surprise", 0.0)),
                    "confidence": (a.get("confidence", 0.0)),
                    "tags_mask": str(mask),
                    "primary_tag_id": str(primary_tag_id),
                    "summary": (a.get("summary", "")),
                    "ts_ms": str(published_ts_ms),
                    "ingested_ts_ms": str(now_ms),
                }
                self.r.xadd(NEWS_ANALYSIS_STREAM, out, maxlen=int(os.getenv("NEWS_ANALYSIS_MAXLEN", "200000")))

            # Mark processed only after successful writes
            self.r.set(done_key, "1", ex=ANALYSIS_DONE_TTL_SEC)
            # Важно: если хотите "ат-лиз-ван" семантику, ack должен быть только после xadd/setex.
            # StreamWorker должен делать ACK после handle_message (как у вас уже сделано).

        except Exception:
            log.exception("handle_message failed for uid=%s", uid)
        finally:
            with contextlib.suppress(Exception):
                self.r.delete(lease_key)
def main() -> None:
    try:
        # Отключаем CLIENT SETINFO для совместимости со старыми версиями Redis
        import redis.connection
        redis.connection.Connection.lib_name = None
        redis.connection.Connection.lib_version = None

        # Ждем готовности Redis с retry для BusyLoadingError
        r = _wait_for_redis_ready(REDIS_URL)
        NewsAnalyzerWorker(redis=r).run_forever()
    except BaseException as e:
        log.error(f"FATAL Exception in main: {e}", exc_info=True)
        import sys
        sys.exit(1)
    finally:
        log.info("analyzer_worker main() completely exited.")
        import sys
        sys.stdout.flush()
        sys.stderr.flush()


def _wait_for_redis_ready(redis_url: str) -> redis.Redis:
    """Wait for Redis to be ready, handling BusyLoadingError"""
    import redis

    max_retries = 60  # 10 минут при 10сек задержке
    retry_count = 0

    while retry_count < max_retries:
        try:
            r = redis.Redis.from_url(
                redis_url,
                decode_responses=True,
                health_check_interval=30,
            )
            # Test connection
            r.ping()
            log.info("Redis connection established successfully")
            return r
        except redis.BusyLoadingError:
            retry_count += 1
            log.warning(f"Redis is loading dataset, waiting... ({retry_count}/{max_retries})")
            time.sleep(10)
        except Exception as e:
            retry_count += 1
            log.warning(f"Redis connection failed (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(10)

    raise Exception(f"Failed to connect to Redis after {max_retries} retries")


if __name__ == "__main__":
    main()
