"""services/playwright_enrichment_worker/main.py

Async Playwright LLM enrichment worker.
Reads jobs from news:llm:jobs → calls Playwright LLMs → writes to news:llm:results.

Architecture:
  - Isolated from trading hot-path (separate container)
  - Round-robin across enabled Playwright clients
  - Circuit breaker per provider
  - Redis LLM result cache (24h TTL)
  - All errors go to news:llm:dlq with reason_code
  - Prometheus metrics on :9832

Enabled only when NEWS_LLM_PLAYWRIGHT_ENABLE=1.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any

import redis as redis_lib

from news_pipeline.circuit_breaker import is_open, record_outcome, reset as cb_reset
from news_pipeline.llm_cache import get as cache_get, put as cache_put
from news_pipeline.llm_job import (
    LLMJob, LLMResult, LLMStatus, make_job_id, validate_llm_result,
)
from news_pipeline.prompt_v2 import PROMPT_VERSION
from news_pipeline.stream_worker import StreamWorker

log = logging.getLogger("playwright_enrichment_worker")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(name)s  %(message)s",
    stream=sys.stdout,
)

REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379/0")
JOBS_STREAM    = os.getenv("NEWS_LLM_JOBS_STREAM", "news:llm:jobs")
RESULTS_STREAM = os.getenv("NEWS_LLM_RESULTS_STREAM", "news:llm:results")
DLQ_STREAM     = os.getenv("NEWS_LLM_DLQ_STREAM", "news:llm:dlq")
GROUP          = os.getenv("NEWS_LLM_WORKER_GROUP", "playwright-enrichment")
CONSUMER       = os.getenv("NEWS_LLM_WORKER_CONSUMER", os.getenv("HOSTNAME", "pw-worker-1"))

HARD_BLOCK_ALLOW = os.getenv("NEWS_LLM_PLAYWRIGHT_HARD_BLOCK_ALLOW", "0") == "1"
LLM_MODE         = os.getenv("NEWS_LLM_MODE", "SHADOW")   # SHADOW | ENFORCE

# Reco writer (reuses existing builder to update trade:cache:news_reco_map)
_RECO_ENABLED = os.getenv("NEWS_LLM_RECO_UPDATE_ENABLED", "1") == "1"

# Stream maxlens
RESULTS_MAXLEN = int(os.getenv("NEWS_LLM_RESULTS_MAXLEN", "50000"))
DLQ_MAXLEN     = int(os.getenv("NEWS_LLM_DLQ_MAXLEN", "10000"))


# ── Prometheus metrics ────────────────────────────────────────────────────────

def _init_metrics() -> Any:
    try:
        from prometheus_client import Counter, Histogram, Gauge, start_http_server
        port = int(os.getenv("PLAYWRIGHT_ENRICHMENT_METRICS_PORT", "9832"))
        start_http_server(port)
        log.info("prometheus metrics on :%d", port)
        c = lambda n, h, labels=(): Counter(n, h, labels)
        h = lambda n, h, labels=(): Histogram(n, h, labels)
        g = lambda n, h, labels=(): Gauge(n, h, labels)
        return {
            "jobs_total":        c("news_llm_jobs_total", "LLM jobs received", ["priority"]),
            "cache_hits":        c("news_llm_cache_hits_total", "Cache hits", ["prompt_version"]),
            "provider_requests": c("news_llm_provider_requests_total", "Provider calls", ["provider", "status"]),
            "provider_latency":  h("news_llm_provider_latency_ms", "Latency ms", ["provider"]),
            "invalid_json":      c("news_llm_invalid_json_total", "Invalid JSON", ["provider"]),
            "schema_error":      c("news_llm_schema_error_total", "Schema errors", ["provider"]),
            "timeout":           c("news_llm_timeout_total", "Timeouts", ["provider"]),
            "login_error":       c("news_llm_login_error_total", "Login errors", ["provider"]),
            "circuit_open":      g("news_llm_circuit_open", "Circuit open", ["provider"]),
            "action":            c("news_llm_action_total", "Actions", ["provider", "action"]),
            "block_downgraded":  c("news_llm_block_downgraded_total", "Block→tighten", ["reason"]),
            "deadline_expired":  c("news_llm_deadline_expired_total", "Deadline expired", []),
        }
    except Exception as exc:
        log.warning("prometheus not available: %r", exc)
        return {}


_M: dict[str, Any] = {}


def _inc(name: str, labels: dict | None = None) -> None:
    try:
        m = _M.get(name)
        if m is None:
            return
        if labels:
            m.labels(**labels).inc()
        else:
            m.inc()
    except Exception:
        pass


def _observe(name: str, value: float, labels: dict | None = None) -> None:
    try:
        m = _M.get(name)
        if m is None:
            return
        if labels:
            m.labels(**labels).observe(value)
        else:
            m.observe(value)
    except Exception:
        pass


# ── Client pool ───────────────────────────────────────────────────────────────

class _ClientPool:
    """Round-robin over Playwright clients, skipping CB-open providers."""

    def __init__(self) -> None:
        from news_pipeline.playwright_llm_client import build_playwright_clients
        self._clients = build_playwright_clients()
        self._idx = 0
        if not self._clients:
            raise RuntimeError("No Playwright clients built — check USE_PLAYWRIGHT_* env vars")
        log.info("playwright clients: %s", [c.CLIENT_NAME for c in self._clients])

    def call_v2(
        self,
        *,
        job: LLMJob,
        redis: Any,
    ) -> tuple[dict[str, Any], str, int]:
        """
        Try clients in round-robin order, skipping CB-open ones.
        Returns (raw_dict, provider_name, latency_ms).
        raw_dict has _status key injected by analyze_v2().
        """
        import threading
        with threading.Lock():
            start = self._idx % len(self._clients)
            self._idx += 1

        ordered = self._clients[start:] + self._clients[:start]

        for client in ordered:
            provider = client.CLIENT_NAME
            if is_open(provider, redis):
                log.debug("circuit open, skipping provider=%s", provider)
                continue

            t0 = time.monotonic()
            try:
                raw = client.analyze_v2(
                    title=job.title,
                    url=job.url,
                    source=job.source,
                    summary=job.summary,
                    published_ts_ms=job.published_ts_ms,
                    ingested_ts_ms=job.ingested_ts_ms,
                )
            except Exception as exc:
                raw = {"_status": "provider_error", "_provider": provider, "_err": str(exc)[:120]}

            latency_ms = int((time.monotonic() - t0) * 1000)
            status = raw.get("_status", "ok")

            record_outcome(provider, redis, status=status, latency_ms=latency_ms)
            _inc("provider_requests", {"provider": provider, "status": status})
            _observe("provider_latency", latency_ms, {"provider": provider})

            if status == "ok":
                return raw, provider, latency_ms

            log.warning("provider=%s status=%s latency=%dms, trying next", provider, status, latency_ms)

        return {"_status": "all_providers_failed"}, "none", 0


# ── Worker ────────────────────────────────────────────────────────────────────

class PlaywrightEnrichmentWorker(StreamWorker):

    def __init__(self, *, redis: redis_lib.Redis) -> None:
        super().__init__(
            redis=redis,
            stream=JOBS_STREAM,
            group=GROUP,
            consumer=CONSUMER,
            dlq_stream=DLQ_STREAM,
            block_ms=5000,
            count=1,          # process one job at a time (browser is slow)
            claim_idle_ms=300_000,  # 5 min — jobs take time
        )
        self._pool = _ClientPool()
        self._reco_writer: Any = None
        if _RECO_ENABLED:
            try:
                from news_pipeline.reco_builder import RecoMapWriter
                self._reco_writer = RecoMapWriter(redis_client=redis)
            except Exception as exc:
                log.warning("reco_writer init failed: %r", exc)

    def handle_message(self, msg_id: str, fields: dict[str, Any]) -> None:
        job = LLMJob.from_stream_fields(fields)
        _inc("jobs_total", {"priority": job.priority})

        # ── Deadline check ───────────────────────────────────────────────────
        if job.is_expired():
            log.warning("job expired job_id=%s news_uid=%s", job.job_id, job.news_uid)
            _inc("deadline_expired")
            self._write_dlq(job, LLMStatus.DEADLINE_EXPIRED, "job_deadline_exceeded")
            return

        # ── Cache check ──────────────────────────────────────────────────────
        cached = cache_get(job.job_id, self.r)
        if cached:
            log.info("cache hit job_id=%s", job.job_id)
            _inc("cache_hits", {"prompt_version": PROMPT_VERSION})
            self._write_result(cached, job)
            return

        # ── Call Playwright ──────────────────────────────────────────────────
        raw, provider, latency_ms = self._pool.call_v2(job=job, redis=self.r)
        status = raw.get("_status", "ok")

        if status != "ok":
            err_status = {
                "timeout": LLMStatus.TIMEOUT,
                "invalid_json": LLMStatus.INVALID_JSON,
                "login_error": LLMStatus.LOGIN_ERROR,
                "all_providers_failed": LLMStatus.PROVIDER_ERROR,
            }.get(status, LLMStatus.PROVIDER_ERROR)
            error_result = LLMResult.error(
                job_id=job.job_id, news_uid=job.news_uid,
                provider=provider, status=err_status,
                dq_flag=f"llm_{status}", latency_ms=latency_ms,
            )
            self._write_dlq(job, err_status, status)
            self._write_result(error_result, job)
            return

        # ── Validate v2 schema ───────────────────────────────────────────────
        result, errors = validate_llm_result(raw)
        result.job_id     = job.job_id
        result.news_uid   = job.news_uid
        result.provider   = provider
        result.latency_ms = latency_ms

        if errors:
            _inc("schema_error", {"provider": provider})
            log.warning("schema_error provider=%s errors=%s", provider, errors)

        # ── Hard-block policy ────────────────────────────────────────────────
        if result.recommended_action == "block" and not HARD_BLOCK_ALLOW:
            original = result.recommended_action
            result.recommended_action = "tighten"
            result.risk_factor_bps    = 5000
            result.dq_flags.append(f"block_downgraded_to_tighten")
            _inc("block_downgraded", {"reason": "hard_block_disabled"})
            log.info("block downgraded → tighten (hard_block_allow=0) provider=%s", provider)

        # ── Shadow mode: log but don't update reco_map ───────────────────────
        if LLM_MODE == "SHADOW":
            result.dq_flags.append("shadow_mode_no_reco_update")
            log.info(
                "SHADOW job_id=%s provider=%s action=%s grade=%d conf=%.2f",
                job.job_id, provider, result.recommended_action, result.grade_id, result.confidence,
            )
        elif self._reco_writer is not None and result.usable:
            self._apply_reco(result, job)

        # ── Cache + stream ───────────────────────────────────────────────────
        cache_put(job.job_id, result, self.r)
        self._write_result(result, job)
        _inc("action", {"provider": provider, "action": result.recommended_action})

        log.info(
            "enriched job_id=%s provider=%s event=%s grade=%d action=%s latency=%dms",
            job.job_id, provider, result.event_type,
            result.grade_id, result.recommended_action, latency_ms,
        )

    def _apply_reco(self, result: LLMResult, job: LLMJob) -> None:
        """Update trade:cache:news_reco_map from LLM result."""
        try:
            from news_pipeline.classifier import ClassifyResult
            from news_pipeline.llm_job import resolve_action

            # Build a synthetic ClassifyResult so we can reuse RecoMapWriter.apply()
            fake_rule = ClassifyResult(
                event_type=result.event_type,
                grade_id=result.grade_id,
                reason_code=result.reason_code,
                default_action=result.recommended_action,
                sentiment=result.sentiment,
                matched=True,
                symbols=tuple(result.affected_symbols) if result.affected_symbols else ("GLOBAL",),
                asset_classes=(),
                pre_sec=0,
                post_sec=result.time_window_sec,
            )
            self._reco_writer.apply(
                result=fake_rule,
                now_ts_ms=result.created_ts_ms,
                source_event_id=job.news_uid,
                confidence=result.confidence,
            )
        except Exception as exc:
            log.warning("_apply_reco failed: %r", exc)

    def _write_result(self, result: LLMResult, job: LLMJob) -> None:
        try:
            fields = result.to_stream_fields()
            self.r.xadd(RESULTS_STREAM, fields, maxlen=RESULTS_MAXLEN)  # type: ignore[arg-type]
        except Exception as exc:
            log.warning("write_result failed job_id=%s: %r", job.job_id, exc)

    def _write_dlq(self, job: LLMJob, status: str, reason: str) -> None:
        try:
            self.r.xadd(DLQ_STREAM, {  # type: ignore[arg-type]
                "job_id":   job.job_id,
                "news_uid": job.news_uid,
                "status":   status,
                "reason":   reason,
                "ts_ms":    str(int(time.time() * 1000)),
            }, maxlen=DLQ_MAXLEN)
        except Exception as exc:
            log.warning("write_dlq failed job_id=%s: %r", job.job_id, exc)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    global _M
    _M = _init_metrics()

    r = _wait_redis(REDIS_URL)
    log.info("playwright_enrichment_worker starting stream=%s group=%s consumer=%s",
             JOBS_STREAM, GROUP, CONSUMER)
    log.info("mode=%s hard_block_allow=%s", LLM_MODE, HARD_BLOCK_ALLOW)
    PlaywrightEnrichmentWorker(redis=r).run_forever()


def _wait_redis(url: str) -> redis_lib.Redis:
    for attempt in range(60):
        try:
            r = redis_lib.Redis.from_url(url, decode_responses=True, health_check_interval=30)
            r.ping()
            log.info("redis connected")
            return r
        except Exception as exc:
            log.warning("redis not ready (attempt %d/60): %r", attempt + 1, exc)
            time.sleep(10)
    raise RuntimeError("Could not connect to Redis")


if __name__ == "__main__":
    main()
