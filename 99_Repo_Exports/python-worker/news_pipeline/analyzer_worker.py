# news_pipeline/analyzer_worker.py
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import redis

from news_pipeline.classifier import classify
from news_pipeline.llm_client import FallbackLLMClient, GeminiHTTPClient
from news_pipeline.reco_builder import RecoMapWriter
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

# ── Playwright async enrichment (opt-in) ─────────────────────────────────────
# When NEWS_LLM_PLAYWRIGHT_ENABLE=0 (default) — works exactly as before.
# When =1 — also enqueues qualifying news to news:llm:jobs for the separate
#            playwright_enrichment_worker to process asynchronously.
_PLAYWRIGHT_ENABLE  = os.getenv("NEWS_LLM_PLAYWRIGHT_ENABLE", "0") == "1"
_LLM_JOBS_STREAM    = os.getenv("NEWS_LLM_JOBS_STREAM", "news:llm:jobs")
_LLM_MIN_GRADE      = int(os.getenv("NEWS_LLM_MIN_RULE_GRADE", "3"))
_LLM_JOBS_MAXLEN    = int(os.getenv("NEWS_LLM_JOBS_MAXLEN", "10000"))

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
    Pipeline (deterministic-first design):

    news:raw ->
      1. Rule classifier (sync, <1 ms) → event_type / grade_id / reason_code
      2. Write trade:cache:news_reco_map immediately if grade_id > 0
      3. LLM enrichment (optional, async-shadow) → updates heavy key only
      4. Emit news:analysis stream entry for feature store

    LLM is NEVER a gate blocker. It only adds confidence/summary to audit store.
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
        self._reco_writer = RecoMapWriter(redis_client=redis)
        self._llm_enabled = os.getenv("NEWS_LLM_ENRICH_ENABLED", "1") == "1"
        self.llm: Any = None
        if self._llm_enabled and not _PLAYWRIGHT_ENABLE:
            # Classic mode: inline LLM enrichment in this worker.
            # When NEWS_LLM_PLAYWRIGHT_ENABLE=1, enrichment is done by the
            # separate playwright_enrichment_worker — no inline LLM here.
            if os.getenv("LLM_FALLBACK_ENABLED", "1") == "1":
                self.llm = FallbackLLMClient.build_default()
            else:
                self.llm = GeminiHTTPClient()
        elif _PLAYWRIGHT_ENABLE:
            log.info("playwright enrichment mode: inline LLM disabled, jobs → %s", _LLM_JOBS_STREAM)

    def handle_message(self, msg_id: str, fields: dict[str, Any]) -> None:
        uid = _safe_s(fields.get("uid"))
        if not uid:
            return

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
            now_ms = get_ny_time_millis()

            published_ts_ms = int(float(fields.get("published_ts_ms") or 0) or 0)
            if published_ts_ms <= 0:
                published_ts_ms = now_ms

            syms = _parse_symbols_json(_safe_s(fields.get("symbols")))
            if not syms:
                syms = ["GLOBAL"]

            # ── Step 1: deterministic rule classifier (always, no LLM) ──────
            result = classify(title, summary=summary, source=source)

            # ── Step 2: write reco map immediately (grade > 0) ───────────────
            if result.grade_id > 0:
                self._reco_writer.apply(
                    result=result,
                    now_ts_ms=now_ms,
                    source_event_id=uid,
                    confidence=1.0,  # deterministic rules: full confidence
                )
                log.info(
                    "news classified uid=%s event_type=%s grade=%d action=%s title=%.80s",
                    uid, result.event_type, result.grade_id, result.default_action, title,
                )

            # ── Step 3: LLM enrichment ────────────────────────────────────────
            a: dict[str, Any] = {}
            if _PLAYWRIGHT_ENABLE:
                # Async path: enqueue to playwright_enrichment_worker if grade qualifies
                self._maybe_enqueue_llm_job(
                    uid=uid, title=title, url=url, source=source,
                    summary=summary, published_ts_ms=published_ts_ms,
                    grade_id=result.grade_id,
                )
            elif self._llm_enabled and self.llm is not None:
                # Classic path: inline LLM call (blocking, same as before playwright)
                try:
                    a = self.llm.analyze(title=title, url=url, source=source, summary=summary)
                except Exception as llm_exc:
                    log.warning("llm_enrich failed uid=%s err=%r", uid, llm_exc)
                    a = {}

            # ── Step 4: heavy store + stream emit ────────────────────────────
            tags = a.get("tags") or []
            mask = tags_to_mask(tags)
            primary_tag_id = pick_primary_tag(tags)

            heavy = {
                "uid": uid,
                "source": source,
                "url": url,
                "title": title,
                "ts_ms": published_ts_ms,
                "symbols": syms,
                "rule_classification": {
                    "event_type": result.event_type,
                    "grade_id": result.grade_id,
                    "reason_code": result.reason_code,
                    "action": result.default_action,
                    "sentiment": result.sentiment,
                    "matched": result.matched,
                },
                "llm_enrichment": a,
                "raw": {k: _safe_s(v) for k, v in fields.items()},
            }
            heavy_key = f"news:analysis:{uid}"
            self.r.setex(heavy_key, ANALYSIS_TTL_SEC, json.dumps(heavy, ensure_ascii=False))

            risk = float(a.get("risk") or 0.0)
            surprise = float(a.get("surprise") or 0.0)
            confidence = float(a.get("confidence") or (1.0 if result.matched else 0.0))

            for sym in syms:
                out = {
                    "uid": uid,
                    "symbol": sym,
                    "event_type": result.event_type,
                    "grade_id": str(result.grade_id),
                    "reason_code": result.reason_code,
                    "action": result.default_action,
                    "risk": str(round(risk, 6)),
                    "surprise": str(round(surprise, 6)),
                    "confidence": str(round(confidence, 6)),
                    "tags_mask": str(mask),
                    "primary_tag_id": str(primary_tag_id),
                    "summary": str(a.get("summary", ""))[:512],
                    "ts_ms": str(published_ts_ms),
                    "ingested_ts_ms": str(now_ms),
                }
                self.r.xadd(NEWS_ANALYSIS_STREAM, out, maxlen=int(os.getenv("NEWS_ANALYSIS_MAXLEN", "200000")))  # type: ignore[arg-type]

            self.r.set(done_key, "1", ex=ANALYSIS_DONE_TTL_SEC)

        except Exception:
            log.exception("handle_message failed for uid=%s", uid)
        finally:
            with contextlib.suppress(Exception):
                self.r.delete(lease_key)

    def _maybe_enqueue_llm_job(
        self,
        *,
        uid: str,
        title: str,
        url: str,
        source: str,
        summary: str,
        published_ts_ms: int,
        grade_id: int,
    ) -> None:
        """Enqueue to news:llm:jobs if this news item is worth LLM enrichment."""
        # Only send high-enough grade items to Playwright (skip noise/low-grade)
        if grade_id < _LLM_MIN_GRADE:
            return

        # Skip market commentary and noise keywords
        if os.getenv("NEWS_LLM_SKIP_NOISE", "1") == "1":
            low = title.lower()
            if any(w in low for w in ("analysis:", "opinion:", "sponsored", "advertisement")):
                return

        try:
            from news_pipeline.llm_job import LLMJob, make_job_id
            now_ms = int(time.time() * 1000)
            job = LLMJob(
                news_uid=uid,
                source=source,
                title=title,
                url=url,
                summary=summary,
                published_ts_ms=published_ts_ms,
                ingested_ts_ms=now_ms,
                priority="high" if grade_id >= 5 else "normal",
                deadline_ts_ms=now_ms + int(os.getenv("NEWS_LLM_JOB_DEADLINE_MS", "120000")),
                rule_grade_id=grade_id,
            )
            # Idempotency: skip if already queued for this job_id
            dedup_key = f"news:llm:queued:{job.job_id}"
            if self.r.set(dedup_key, "1", nx=True, ex=300):
                self.r.xadd(_LLM_JOBS_STREAM, job.to_stream_fields(),  # type: ignore[arg-type]
                            maxlen=_LLM_JOBS_MAXLEN)
                log.info("llm_job enqueued job_id=%s grade=%d uid=%s", job.job_id, grade_id, uid)
        except Exception as exc:
            log.warning("_maybe_enqueue_llm_job failed uid=%s: %r", uid, exc)


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
