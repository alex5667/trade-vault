# news_pipeline/playwright_browser.py
"""
Async HumanBrowser — stealth Playwright browser for news page fetching.

Usage:
    browser = HumanBrowser()
    await browser.start()
    result = await browser.fetch("https://coindesk.com/some-article")
    await browser.stop()

ENV:
    PLAYWRIGHT_HEADLESS          "1" (default) | "0"
    PLAYWRIGHT_BROWSER_SESSIONS  "/data/playwright_sessions"
    PLAYWRIGHT_PROXY_LIST        "http://p1:port,http://p2:port" (csv)
    PLAYWRIGHT_FETCH_TIMEOUT_MS  45000
    PLAYWRIGHT_CONTEXT_TTL_MIN   30  (cleanup stale contexts)
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger("news.playwright_browser")

# ── Fingerprint pools ────────────────────────────────────────────────────────

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
]

_VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1280, "height": 800},
    {"width": 2560, "height": 1440},
    {"width": 1366, "height": 768},
]

_LOCALES = ["en-US", "en-GB", "en-US"]
_TIMEZONES = ["America/New_York", "America/Chicago", "Europe/London", "America/Los_Angeles"]

# Stealth init script — applied to every new context
_STEALTH_SCRIPT = """
(() => {
  // Remove webdriver flag
  Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  // Fake plugins
  Object.defineProperty(navigator, 'plugins', { get: () => [
    { name: 'Chrome PDF Plugin' }, { name: 'Chrome PDF Viewer' }, { name: 'Native Client' }
  ]});
  // Languages
  Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
  // Platform
  Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
  // Hardware concurrency
  Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
  // Chrome runtime
  if (!window.chrome) window.chrome = {};
  if (!window.chrome.runtime) window.chrome.runtime = {};
  // Notification permissions (don't expose bot)
  const originalQuery = window.navigator.permissions.query;
  window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications'
      ? Promise.resolve({ state: Notification.permission })
      : originalQuery(parameters)
  );
})();
"""


# ── Result DTO ───────────────────────────────────────────────────────────────

@dataclass
class PageFetchResult:
    url: str
    title: str
    text: str
    html: str
    timestamp_ms: int
    status: str = "success"   # success | quarantine | error
    reason_code: str = "OK"

    @property
    def ok(self) -> bool:
        return self.status == "success"


# ── HumanBrowser ─────────────────────────────────────────────────────────────

class HumanBrowser:
    """
    Persistent async browser with:
    - Domain-scoped contexts (session reuse)
    - Human-like mouse + scroll patterns
    - Stealth patches (no webdriver, fake plugins)
    - Proxy rotation
    - Session persistence to disk
    - Stale context cleanup
    """

    def __init__(
        self,
        session_dir: str | None = None,
        proxy_list: list[str] | None = None,
        headless: bool | None = None,
        fetch_timeout_ms: int | None = None,
    ) -> None:
        self._session_dir = session_dir or os.getenv(
            "PLAYWRIGHT_BROWSER_SESSIONS", "/data/playwright_sessions"
        )
        raw_proxies = os.getenv("PLAYWRIGHT_PROXY_LIST", "")
        self._proxy_list = proxy_list or [p.strip() for p in raw_proxies.split(",") if p.strip()]
        self._headless = headless if headless is not None else os.getenv("PLAYWRIGHT_HEADLESS", "1") != "0"
        self._fetch_timeout_ms = fetch_timeout_ms or int(
            os.getenv("PLAYWRIGHT_FETCH_TIMEOUT_MS", "45000")
        )
        self._context_ttl_sec = int(os.getenv("PLAYWRIGHT_CONTEXT_TTL_MIN", "30")) * 60

        self._pw: Any = None
        self._browser: Any = None
        self._contexts: dict[str, Any] = {}   # domain → context
        self._context_born: dict[str, float] = {}  # domain → time.monotonic()

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise RuntimeError("playwright not installed — run: pip install playwright && playwright install chromium --with-deps")

        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(
            headless=self._headless,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
                "--lang=en-US,en",
                "--window-size=1920,1080",
            ],
        )
        logger.info("playwright browser started headless=%s", self._headless)

    async def stop(self) -> None:
        for ctx in list(self._contexts.values()):
            try:
                await ctx.close()
            except Exception:
                pass
        self._contexts.clear()
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
        if self._pw:
            try:
                await self._pw.stop()
            except Exception:
                pass
        logger.info("playwright browser stopped")

    # ── Context management ─────────────────────────────────────────────────

    @staticmethod
    def _domain(url: str) -> str:
        try:
            return urlparse(url).netloc
        except Exception:
            return url[:64]

    def _session_path(self, domain: str) -> str:
        safe = domain.replace(".", "_").replace(":", "_").replace("/", "_")
        return os.path.join(self._session_dir, f"{safe}.json")

    async def _get_context(self, url: str) -> Any:
        domain = self._domain(url)

        # Evict stale context
        born = self._context_born.get(domain, 0)
        if domain in self._contexts and (time.monotonic() - born) > self._context_ttl_sec:
            try:
                await self._contexts[domain].close()
            except Exception:
                pass
            del self._contexts[domain]
            del self._context_born[domain]

        if domain in self._contexts:
            return self._contexts[domain]

        ua = random.choice(_USER_AGENTS)
        vp = random.choice(_VIEWPORTS)
        locale = random.choice(_LOCALES)
        tz = random.choice(_TIMEZONES)

        kwargs: dict[str, Any] = {
            "viewport": vp,
            "user_agent": ua,
            "locale": locale,
            "timezone_id": tz,
            "has_touch": False,
            "is_mobile": False,
            "java_script_enabled": True,
            "extra_http_headers": {
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "DNT": "1",
                "Sec-Ch-Ua": '"Chromium";v="136", "Not;A=Brand";v="8"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
            },
        }

        if self._proxy_list:
            kwargs["proxy"] = {"server": random.choice(self._proxy_list)}

        session_path = self._session_path(domain)
        if os.path.exists(session_path):
            kwargs["storage_state"] = session_path
            logger.debug("loaded session for domain=%s", domain)

        ctx = await self._browser.new_context(**kwargs)
        await ctx.add_init_script(_STEALTH_SCRIPT)

        self._contexts[domain] = ctx
        self._context_born[domain] = time.monotonic()
        return ctx

    async def _save_session(self, domain: str) -> None:
        ctx = self._contexts.get(domain)
        if not ctx:
            return
        try:
            os.makedirs(self._session_dir, exist_ok=True)
            await ctx.storage_state(path=self._session_path(domain))
        except Exception as exc:
            logger.debug("save_session failed domain=%s err=%r", domain, exc)

    # ── Human behavior ─────────────────────────────────────────────────────

    @staticmethod
    async def human_scroll(page: Any, steps: int | None = None) -> None:
        n = steps or random.randint(2, 5)
        for _ in range(n):
            dy = random.randint(200, 700)
            await page.evaluate(f"window.scrollBy(0, {dy})")
            await asyncio.sleep(random.uniform(0.5, 1.8))
        await page.evaluate("window.scrollBy(0, -150)")
        await asyncio.sleep(random.uniform(0.2, 0.7))

    @staticmethod
    async def human_mouse(page: Any, moves: int | None = None) -> None:
        n = moves or random.randint(2, 6)
        for _ in range(n):
            x = random.randint(80, 1840)
            y = random.randint(80, 960)
            await page.mouse.move(x, y, steps=random.randint(12, 35))
            await asyncio.sleep(random.uniform(0.05, 0.35))

    @staticmethod
    async def human_type(page: Any, locator: Any, text: str) -> None:
        for ch in text:
            await locator.type(ch, delay=random.randint(45, 160))

    # ── Fetch ──────────────────────────────────────────────────────────────

    async def fetch(self, url: str, timeout_ms: int | None = None) -> PageFetchResult:
        ts_ms = int(time.time() * 1000)
        tms = timeout_ms or self._fetch_timeout_ms
        domain = self._domain(url)

        try:
            ctx = await self._get_context(url)
            page = await ctx.new_page()
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=tms)
                http_status = resp.status if resp else 0

                if http_status in (403, 429, 503):
                    return PageFetchResult(
                        url=url, title="", html="", text="",
                        timestamp_ms=ts_ms, status="quarantine",
                        reason_code=f"http_{http_status}",
                    )

                # Human-like dwell time
                await asyncio.sleep(random.uniform(1.0, 2.5))
                await self.human_mouse(page)
                await self.human_scroll(page)

                # Wait for network to calm
                try:
                    await page.wait_for_load_state("networkidle", timeout=7_000)
                except Exception:
                    pass

                title = await page.title()
                html = await page.content()
                text = await page.evaluate(
                    "document.body ? document.body.innerText.trim() : ''"
                )

                # Quarantine: suspiciously empty
                if len(text) < 80:
                    return PageFetchResult(
                        url=url, title=title, html=html, text=text,
                        timestamp_ms=ts_ms, status="quarantine",
                        reason_code="content_too_small",
                    )

                await self._save_session(domain)
                return PageFetchResult(
                    url=url, title=title, html=html, text=text,
                    timestamp_ms=ts_ms, status="success", reason_code="OK",
                )
            finally:
                await page.close()

        except Exception as exc:
            logger.warning("fetch error url=%s err=%r", url, exc)
            return PageFetchResult(
                url=url, title="", html="", text="",
                timestamp_ms=ts_ms, status="error",
                reason_code=str(exc)[:150],
            )

    # ── Maintenance ────────────────────────────────────────────────────────

    async def cleanup_stale_contexts(self) -> None:
        """Evict contexts older than TTL. Call periodically (every 30 min)."""
        now = time.monotonic()
        stale = [d for d, t in self._context_born.items() if now - t > self._context_ttl_sec]
        for domain in stale:
            try:
                await self._contexts[domain].close()
            except Exception:
                pass
            self._contexts.pop(domain, None)
            self._context_born.pop(domain, None)
        if stale:
            logger.info("evicted stale contexts: %s", stale)
