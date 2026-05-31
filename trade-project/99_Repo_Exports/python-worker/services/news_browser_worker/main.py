"""
services/news_browser_worker/main.py
────────────────────────────────────
Async news-page fetcher using HumanBrowser.

Reads source URLs from ENV NEWS_BROWSER_SOURCES (csv),
fetches each with Playwright stealth browser, deduplicates,
and pushes raw items to Redis stream news:raw (same format as Go ingestor).

Runs on minik (headless Chromium).

ENV:
    REDIS_URL                redis://…
    NEWS_RAW_STREAM          news:raw
    NEWS_RAW_DLQ             news:raw:dlq
    NEWS_BROWSER_SOURCES     https://coindesk.com,https://cointelegraph.com,…
    NEWS_BROWSER_POLL_SEC    120          (per-source interval)
    NEWS_BROWSER_DEDUPE_TTL  1800         (seconds)
    NEWS_STREAM_MAXLEN       100000
    PLAYWRIGHT_HEADLESS      1
    PLAYWRIGHT_BROWSER_SESSIONS  /data/playwright_sessions
    PLAYWRIGHT_PROXY_LIST    (optional, csv)
    LOG_LEVEL                INFO
    TZ                       UTC
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from typing import Any

import redis.asyncio as aioredis

from news_pipeline.playwright_browser import HumanBrowser, PageFetchResult

log = logging.getLogger("news_browser_worker")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

# ── Config ────────────────────────────────────────────────────────────────────

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
NEWS_RAW_STREAM = os.getenv("NEWS_RAW_STREAM", "news:raw")
NEWS_RAW_DLQ = os.getenv("NEWS_RAW_DLQ", "news:raw:dlq")
STREAM_MAXLEN = int(os.getenv("NEWS_STREAM_MAXLEN", "100000"))

_raw_sources = os.getenv("NEWS_BROWSER_SOURCES", "")
SOURCES: list[str] = [u.strip() for u in _raw_sources.split(",") if u.strip()]

POLL_SEC = float(os.getenv("NEWS_BROWSER_POLL_SEC", "120"))
DEDUPE_TTL = int(os.getenv("NEWS_BROWSER_DEDUPE_TTL", "1800"))

# Per-domain min gap (sec) to avoid aggressive crawling
DOMAIN_GAP_SEC = float(os.getenv("NEWS_BROWSER_DOMAIN_GAP_SEC", "12"))

# Prometheus (optional, fail-open)
METRICS_PORT = int(os.getenv("NEWS_BROWSER_METRICS_PORT", "9831"))


# ── Dedup ─────────────────────────────────────────────────────────────────────

def _uid(url: str, title: str) -> str:
    h = hashlib.sha1(f"{url}|{title}".encode()).hexdigest()
    return h[:16]


async def _dedup_pass(r: Any, uid: str) -> bool:
    key = f"news:browser:dedup:{uid}"
    return bool(await r.set(key, "1", nx=True, ex=DEDUPE_TTL))


# ── Text → news items extractor ───────────────────────────────────────────────

def _extract_items_from_page(result: PageFetchResult) -> list[dict[str, Any]]:
    """
    Heuristic: split page text into paragraphs and treat non-trivial
    first paragraphs as news items. Real production use should have
    per-site extractors or use trafilatura/readability.
    """
    items = []
    lines = [l.strip() for l in result.text.splitlines() if len(l.strip()) > 40]

    if not lines:
        return []

    # Treat the page as a single news item (article page)
    title = result.title or (lines[0][:200] if lines else "")
    body = "\n".join(lines[:30])  # first 30 lines as summary

    if title:
        items.append({
            "uid": _uid(result.url, title),
            "source": f"browser:{result.url}",
            "title": title,
            "url": result.url,
            "ts_ms": result.timestamp_ms,
            "summary": body[:512],
            "symbols": "",
            "asset_class": "crypto",
        })

    return items


# ── Metrics (optional) ────────────────────────────────────────────────────────

def _start_metrics_server() -> None:
    try:
        from prometheus_client import Counter, Gauge, Histogram, start_http_server
        global _fetch_ok, _fetch_err, _fetch_quarantine, _items_published, _fetch_duration

        _fetch_ok = Counter(
            "news_browser_fetch_ok_total", "Successful page fetches",
            ["domain"],
        )
        _fetch_err = Counter(
            "news_browser_fetch_error_total", "Failed page fetches",
            ["domain"],
        )
        _fetch_quarantine = Counter(
            "news_browser_fetch_quarantine_total", "Quarantined pages",
            ["domain", "reason"],
        )
        _items_published = Counter(
            "news_browser_items_published_total", "Items pushed to news:raw",
        )
        _fetch_duration = Histogram(
            "news_browser_fetch_duration_seconds",
            "Page fetch duration",
            buckets=[1, 3, 6, 10, 18, 30, 60],
        )
        start_http_server(METRICS_PORT)
        log.info("metrics server on :%d", METRICS_PORT)
    except Exception as exc:
        log.debug("metrics unavailable: %r", exc)


def _observe_fetch(result: PageFetchResult, domain: str, elapsed: float) -> None:
    try:
        if result.status == "success":
            _fetch_ok.labels(domain=domain).inc()
        elif result.status == "quarantine":
            _fetch_quarantine.labels(domain=domain, reason=result.reason_code).inc()
        else:
            _fetch_err.labels(domain=domain).inc()
        _fetch_duration.observe(elapsed)
    except Exception:
        pass


def _observe_published(n: int) -> None:
    try:
        _items_published.inc(n)
    except Exception:
        pass


# ── Worker loop ───────────────────────────────────────────────────────────────

class NewsBrowserWorker:
    def __init__(self, r: Any, browser: HumanBrowser) -> None:
        self._r = r
        self._browser = browser
        self._domain_last_fetch: dict[str, float] = {}

    async def _publish(self, item: dict[str, Any]) -> bool:
        try:
            if not await _dedup_pass(self._r, item["uid"]):
                return False
            fields = {k: str(v) for k, v in item.items()}
            await self._r.xadd(NEWS_RAW_STREAM, fields, maxlen=STREAM_MAXLEN, approximate=True)
            return True
        except Exception as exc:
            log.warning("publish failed uid=%s err=%r", item.get("uid"), exc)
            try:
                await self._r.xadd(
                    NEWS_RAW_DLQ,
                    {"uid": item.get("uid", ""), "err": str(exc)[:200], "url": item.get("url", "")},
                    maxlen=5000,
                )
            except Exception:
                pass
            return False

    async def _fetch_source(self, url: str) -> None:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc

        # Per-domain rate limit
        last = self._domain_last_fetch.get(domain, 0.0)
        wait = DOMAIN_GAP_SEC - (time.monotonic() - last)
        if wait > 0:
            await asyncio.sleep(wait)

        t0 = time.monotonic()
        result = await self._browser.fetch(url)
        elapsed = time.monotonic() - t0
        self._domain_last_fetch[domain] = time.monotonic()

        _observe_fetch(result, domain, elapsed)

        if not result.ok:
            log.info("fetch %s → %s [%s] %.1fs", url, result.status, result.reason_code, elapsed)
            return

        items = _extract_items_from_page(result)
        published = 0
        for item in items:
            if await self._publish(item):
                published += 1

        _observe_published(published)
        log.info("fetched url=%s items=%d published=%d %.1fs", url, len(items), published, elapsed)

    async def run_once(self) -> None:
        tasks = [self._fetch_source(url) for url in SOURCES]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def run_forever(self) -> None:
        log.info("news_browser_worker started, sources=%d poll_sec=%.0f", len(SOURCES), POLL_SEC)
        while True:
            try:
                await self.run_once()
            except Exception:
                log.exception("run_once error")

            # Cleanup stale contexts periodically
            try:
                await self._browser.cleanup_stale_contexts()
            except Exception:
                pass

            await asyncio.sleep(POLL_SEC)


# ── Entrypoint ────────────────────────────────────────────────────────────────

async def _main() -> None:
    if not SOURCES:
        log.warning("NEWS_BROWSER_SOURCES is empty — nothing to fetch, exiting")
        return

    _start_metrics_server()

    # Redis async client
    r = aioredis.from_url(REDIS_URL, decode_responses=True)

    browser = HumanBrowser()
    await browser.start()

    worker = NewsBrowserWorker(r=r, browser=browser)
    try:
        await worker.run_forever()
    finally:
        await browser.stop()
        await r.aclose()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
