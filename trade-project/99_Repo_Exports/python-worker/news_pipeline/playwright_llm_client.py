# news_pipeline/playwright_llm_client.py
"""
Playwright-based LLM clients that interact with web chat UIs to avoid API rate limits.
Provides sync interface compatible with FallbackLLMClient.

Chain order (highest priority first):
  PlaywrightQwenClient      → chat.qwenlm.ai     (Google OAuth login, Qwen3-235B)
  PlaywrightDeepSeekClient  → chat.deepseek.com  (login required)
  PlaywrightChatGPTClient   → chatgpt.com        (session-based)
  PlaywrightGeminiClient    → gemini.google.com  (Google login required)

ENV (shared):
    PLAYWRIGHT_HEADLESS           "1" (default) | "0" for visible window
    PLAYWRIGHT_BROWSER_SESSIONS   "/data/playwright_sessions"
    PLAYWRIGHT_LLM_TIMEOUT_SEC    90
    USE_PLAYWRIGHT_LLM            "1" to enable (checked by caller)

ENV (per client):
    QWEN_EMAIL / QWEN_PASSWORD
    QWEN_MODEL                    qwen3-235b-a22b (default, latest flagship)
    DEEPSEEK_EMAIL / DEEPSEEK_PASSWORD
    CHATGPT_EMAIL  / CHATGPT_PASSWORD
    GEMINI_GOOGLE_EMAIL / GEMINI_GOOGLE_PASSWORD
    PLAYWRIGHT_QWEN_RATE_SEC      5.0
    PLAYWRIGHT_DEEPSEEK_RATE_SEC  5.0
    PLAYWRIGHT_CHATGPT_RATE_SEC   6.0
    PLAYWRIGHT_GEMINI_RATE_SEC    7.0
"""
from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import random
import re
import threading
import time
from typing import Any

logger = logging.getLogger("news.playwright_llm")

# ── Helpers (duplicated from llm_client to avoid circular imports) ────────────

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _clamp01(x: float) -> float:
    return _clamp(x, 0.0, 1.0)


def _extract_json_obj(text: str) -> dict | None:
    text = (text or "").strip()
    if not text:
        return None
    # strip <think> blocks
    if "<think>" in text:
        parts = text.split("</think>")
        text = parts[-1].strip()
    # strip markdown fences
    text = re.sub(r"```(?:json)?\s*", "", text).strip()
    try:
        v = json.loads(text)
        return v if isinstance(v, dict) else None
    except Exception:
        pass
    i, j = text.find("{"), text.rfind("}")
    if i >= 0 and j > i:
        try:
            v = json.loads(text[i : j + 1])
            return v if isinstance(v, dict) else None
        except Exception:
            pass
    return None


_ALLOWED_TAGS = sorted([
    "cpi", "ppi", "fomc", "fed_speech", "nfp", "rates", "inflation",
    "risk_off", "risk_on", "earnings", "geopolitics", "crypto_reg",
    "exchange", "hack", "etf", "liquidation", "macro",
])

_PROMPT_TEMPLATE = (
    "Analyze this news item for crypto/financial trading impact.\n"
    "Return ONLY a compact JSON object — no prose, no markdown fences:\n"
    "{{\n"
    '  "risk": <0..1 float>,\n'
    '  "surprise": <-1..1 float>,\n'
    '  "confidence": <0..1 float>,\n'
    '  "tags": <array from allowed_tags>,\n'
    '  "primary_tag": <string from allowed_tags>,\n'
    '  "summary": <string max 160 chars>\n'
    "}}\n\n"
    "allowed_tags: {allowed_tags}\n"
    "source: {source}\n"
    "title: {title}\n"
    "url: {url}\n"
    "{summary_line}"
)

_STEALTH_SCRIPT = """
(function() {
  // ── Navigator overrides ──────────────────────────────────────────────────
  Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
  Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
  Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
  Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
  Object.defineProperty(navigator, 'maxTouchPoints', {get: () => 0});
  Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
  Object.defineProperty(navigator, 'plugins', {get: () => {
    const ps = [
      {name:'Chrome PDF Plugin', filename:'internal-pdf-viewer', description:'Portable Document Format'},
      {name:'Chrome PDF Viewer', filename:'mhjfbmdgcfjbbpaeojofohoefgiehjai', description:''},
      {name:'Native Client',     filename:'internal-nacl-plugin', description:''},
    ];
    ps.refresh = function(){};
    ps.item = function(i){ return this[i]; };
    ps.namedItem = function(n){ return Array.prototype.find.call(this, p => p.name===n); };
    return ps;
  }});

  // ── Chrome runtime (full shape) ──────────────────────────────────────────
  window.chrome = {
    runtime: {},
    app: {
      isInstalled: false,
      InstallState: {DISABLED:'disabled', INSTALLED:'installed', NOT_INSTALLED:'not_installed'},
      RunningState: {CANNOT_RUN:'cannot_run', READY_TO_RUN:'ready_to_run', RUNNING:'running'},
    },
    csi: function() {
      return {startE: Date.now(), onloadT: Date.now()+180, pageT: Date.now()+260, tran: 15};
    },
    loadTimes: function() {
      var t = Date.now() / 1000;
      return {commitLoadTime:t-1, connectionInfo:'h2', finishDocumentLoadTime:t,
              finishLoadTime:t, firstPaintAfterLoadTime:0, firstPaintTime:t-0.5,
              navigationType:'Other', npnNegotiatedProtocol:'h2', requestTime:t-1.5,
              startLoadTime:t-1.2, wasAlternateProtocolAvailable:false,
              wasFetchedViaSpdy:true, wasNpnNegotiated:true};
    },
  };

  // ── Permissions (avoid 'denied' for notifications check) ────────────────
  try {
    var origQuery = window.navigator.permissions.query.bind(window.navigator.permissions);
    window.navigator.permissions.query = function(p) {
      if (p && p.name === 'notifications')
        return Promise.resolve({state: (typeof Notification !== 'undefined' ? Notification.permission : 'default')});
      return origQuery(p);
    };
  } catch(e) {}

  // ── Canvas fingerprint noise (±1 bit per channel, imperceptible) ─────────
  try {
    var origGC = HTMLCanvasElement.prototype.getContext;
    HTMLCanvasElement.prototype.getContext = function(type) {
      var ctx = origGC.apply(this, arguments);
      if (type === '2d' && ctx) {
        var origGID = ctx.getImageData.bind(ctx);
        ctx.getImageData = function(x, y, w, h) {
          var d = origGID(x, y, w, h);
          for (var i = 0; i < d.data.length; i += 4) {
            d.data[i]   ^= (Math.random() < 0.3) ? 1 : 0;
            d.data[i+1] ^= (Math.random() < 0.3) ? 1 : 0;
            d.data[i+2] ^= (Math.random() < 0.3) ? 1 : 0;
          }
          return d;
        };
      }
      return ctx;
    };
  } catch(e) {}

  // ── WebGL vendor / renderer spoof ────────────────────────────────────────
  try {
    var _getParam = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(p) {
      if (p === 37445) return 'Intel Inc.';
      if (p === 37446) return 'Intel(R) Iris(TM) Plus Graphics';
      return _getParam.call(this, p);
    };
  } catch(e) {}
  try {
    var _getParam2 = WebGL2RenderingContext.prototype.getParameter;
    WebGL2RenderingContext.prototype.getParameter = function(p) {
      if (p === 37445) return 'Intel Inc.';
      if (p === 37446) return 'Intel(R) Iris(TM) Plus Graphics';
      return _getParam2.call(this, p);
    };
  } catch(e) {}

  // ── performance.now jitter (subtle, < 0.1 ms) ────────────────────────────
  try {
    var _origNow = performance.now.bind(performance);
    performance.now = function() { return _origNow() + Math.random() * 0.08; };
  } catch(e) {}

  // ── AudioContext fingerprint noise ───────────────────────────────────────
  try {
    var origCA = AudioContext.prototype.createAnalyser;
    AudioContext.prototype.createAnalyser = function() {
      var a = origCA.apply(this, arguments);
      var origGFD = a.getFloatFrequencyData.bind(a);
      a.getFloatFrequencyData = function(arr) {
        origGFD(arr);
        for (var i = 0; i < arr.length; i++) arr[i] += Math.random() * 0.0001;
      };
      return a;
    };
  } catch(e) {}
})();
"""

_USER_AGENTS = [
    # Chrome 136 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    # Chrome 135 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    # Chrome 134 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    # Chrome 136 macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    # Chrome 135 macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    # Edge 136 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
    # Edge 135 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
    # Chrome 136 Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    # Chrome 135 Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    # Chrome 136 Win11
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    # Chrome 134 macOS Ventura
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_7_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    # Brave (Chrome 135 fingerprint)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
]

# Behaviour profiles — different timing for chat UIs vs news pages
_BEHAVIOR_PROFILES: dict[str, dict] = {
    "chat": {
        "scroll_speed":   (0.5, 1.5),
        "mouse_jitter":   True,
        "read_pause":     (1.5, 3.5),
        "interaction_p":  0.20,
    },
    "news_article": {
        "scroll_speed":   (1.2, 3.5),
        "mouse_jitter":   False,
        "read_pause":     (3.0, 8.0),
        "interaction_p":  0.35,
    },
    "crypto_fast": {
        "scroll_speed":   (0.5, 1.5),
        "mouse_jitter":   True,
        "read_pause":     (1.0, 2.5),
        "interaction_p":  0.15,
    },
}


# ── Base class ────────────────────────────────────────────────────────────────

class PlaywrightLLMBase:
    """
    Sync LLM client backed by a Playwright browser.
    Browser is started once (lazy) and kept alive across calls.
    Thread-safe: single lock serialises all browser interactions.
    """

    CLIENT_NAME = "playwright_base"
    SESSION_NAME = "base"
    _ERROR_PREFIX = "playwright_base_error:"
    _CRED_PREFIX = ""
    _BEHAVIOR_PROFILE = "chat"  # subclasses may override

    def __init__(self) -> None:
        self._session_dir = os.getenv("PLAYWRIGHT_BROWSER_SESSIONS", "/data/playwright_sessions")
        self._headless = os.getenv("PLAYWRIGHT_HEADLESS", "1") != "0"
        self._timeout_sec = float(os.getenv("PLAYWRIGHT_LLM_TIMEOUT_SEC", "90"))
        self._rate_sec = 5.0
        self._last_call: float = 0.0
        self._pw: Any = None
        self._browser: Any = None
        self._context: Any = None
        self._page: Any = None
        self._profile = _BEHAVIOR_PROFILES.get(self._BEHAVIOR_PROFILE, _BEHAVIOR_PROFILES["chat"])
        # Dedicated thread per client — sync_playwright binds its event loop
        # to the calling thread, so each client must own exactly one thread.
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    # ── Browser lifecycle ──────────────────────────────────────────────────

    def _session_path(self) -> str:
        os.makedirs(self._session_dir, exist_ok=True)
        return os.path.join(self._session_dir, f"{self.SESSION_NAME}.json")

    def _ensure_browser(self) -> None:
        if self._pw is not None:
            return
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise RuntimeError("playwright not installed")

        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=self._headless,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--window-size=1920,1080",
                "--lang=en-US,en",
            ],
        )
        ctx_kwargs: dict[str, Any] = {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": random.choice(_USER_AGENTS),
            "locale": "en-US",
            "timezone_id": "America/New_York",
        }
        sp = self._session_path()
        if os.path.exists(sp):
            ctx_kwargs["storage_state"] = sp
            logger.info("%s: loaded session from %s", self.CLIENT_NAME, sp)

        self._context = self._browser.new_context(**ctx_kwargs)
        self._context.add_init_script(_STEALTH_SCRIPT)
        self._page = self._context.new_page()
        logger.info("%s: browser started headless=%s profile=%s",
                    self.CLIENT_NAME, self._headless, self._BEHAVIOR_PROFILE)

    def _save_session(self) -> None:
        try:
            self._context.storage_state(path=self._session_path())
        except Exception as exc:
            logger.debug("%s: save_session failed: %r", self.CLIENT_NAME, exc)

    def _reset_browser(self) -> None:
        """Close and clear browser state so next call re-creates it."""
        try:
            if self._context:
                self._context.close()
        except Exception:
            pass
        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._pw:
                self._pw.stop()
        except Exception:
            pass
        self._pw = self._browser = self._context = self._page = None

    # ── Human behavior helpers ─────────────────────────────────────────────

    def _bezier_mouse_move(self, x2: int, y2: int) -> None:
        """Move mouse along a quadratic Bezier curve from current position."""
        try:
            # Use a random control point to create a natural arc
            cx = random.randint(min(100, x2) - 60, max(100, x2) + 60)
            cy = random.randint(min(300, y2) - 60, max(300, y2) + 60)
            steps = random.randint(22, 42)
            for i in range(steps + 1):
                t = i / steps
                bx = int((1 - t) ** 2 * 960 + 2 * (1 - t) * t * cx + t ** 2 * x2)
                by = int((1 - t) ** 2 * 540 + 2 * (1 - t) * t * cy + t ** 2 * y2)
                self._page.mouse.move(bx, by)
                time.sleep(random.uniform(0.004, 0.022))
            # Micro-jitter at destination
            if self._profile.get("mouse_jitter"):
                self._page.mouse.move(
                    x2 + random.randint(-8, 8),
                    y2 + random.randint(-6, 6),
                )
                time.sleep(random.uniform(0.03, 0.10))
                self._page.mouse.move(x2, y2)
        except Exception:
            pass

    def _human_scroll(self) -> None:
        speed = self._profile.get("scroll_speed", (0.5, 1.5))
        for _ in range(random.randint(2, 5)):
            dy = random.randint(120, 480)
            self._page.evaluate(f"window.scrollBy(0, {dy})")
            time.sleep(random.uniform(*speed))
            # Occasional small upward scroll (realistic reading)
            if random.random() < 0.25:
                self._page.evaluate(f"window.scrollBy(0, -{random.randint(40, 120)})")
                time.sleep(random.uniform(0.3, 0.7))

    def _human_type(self, locator: Any, text: str) -> None:
        for ch in text:
            # Occasional longer pause (thinking before typing next char)
            delay = random.randint(35, 130)
            if random.random() < 0.06:
                delay += random.randint(200, 600)
            locator.type(ch, delay=delay)

    def _human_keyboard_activity(self) -> None:
        """Random arrow/scroll key presses — adds keyboard events to fingerprint."""
        keys = ["ArrowDown", "ArrowDown", "PageDown", "ArrowUp", "End", "Home"]
        for _ in range(random.randint(2, 5)):
            self._page.keyboard.press(random.choice(keys))
            time.sleep(random.uniform(0.10, 0.55))

    def _human_reading_pause(self) -> None:
        lo, hi = self._profile.get("read_pause", (1.5, 3.5))
        time.sleep(random.uniform(lo, hi))

    def _random_interaction(self) -> None:
        """Probabilistic micro-interactions to mimic an idle reader."""
        p = self._profile.get("interaction_p", 0.20)
        if random.random() < p:
            x = random.randint(80, 1840)
            y = random.randint(80, 900)
            self._bezier_mouse_move(x, y)
        if random.random() < p * 0.7:
            self._human_keyboard_activity()
        if random.random() < p * 0.5:
            # Click on an empty area (no element hit)
            self._page.mouse.click(
                random.randint(100, 400),
                random.randint(700, 900),
            )
            time.sleep(random.uniform(0.1, 0.3))

    def _paste_text(self, selector: str, text: str) -> None:
        """Paste text via JS value setter + input event (fast, human-like)."""
        escaped = json.dumps(text)
        self._page.evaluate(f"""
            (() => {{
                const el = document.querySelector({json.dumps(selector)});
                if (!el) return;
                const setter = Object.getOwnPropertyDescriptor(
                    window.HTMLTextAreaElement.prototype, 'value'
                ) || Object.getOwnPropertyDescriptor(
                    window.HTMLInputElement.prototype, 'value'
                );
                if (setter && setter.set) {{
                    setter.set.call(el, {escaped});
                }} else {{
                    el.value = {escaped};
                }}
                el.dispatchEvent(new Event('input', {{bubbles: true}}));
                el.dispatchEvent(new Event('change', {{bubbles: true}}));
            }})()
        """)

    def _wait_for_stable_text(
        self, locator_expr: str, stable_polls: int = 4, poll_sec: float = 1.5
    ) -> str:
        """
        Poll for text in locator_expr until stable (no change for stable_polls cycles).
        Returns final text.
        """
        last = ""
        streak = 0
        deadline = time.monotonic() + self._timeout_sec
        while time.monotonic() < deadline:
            try:
                els = self._page.locator(locator_expr).all()
                if els:
                    current = els[-1].inner_text().strip()
                    if current and current == last:
                        streak += 1
                        if streak >= stable_polls:
                            return current
                    else:
                        streak = 0
                        last = current
            except Exception:
                pass
            time.sleep(poll_sec)
        return last

    # ── Abstract interface ─────────────────────────────────────────────────

    def _is_logged_in(self) -> bool:
        raise NotImplementedError

    def _do_login(self, email: str, password: str) -> None:
        raise NotImplementedError

    def _send_prompt(self, prompt: str) -> str:
        raise NotImplementedError

    # ── Public analyze() ──────────────────────────────────────────────────

    def _build_prompt(self, *, title: str, url: str, source: str, summary: str) -> str:
        summary_line = f"summary: {summary}\n" if summary else ""
        return _PROMPT_TEMPLATE.format(
            allowed_tags=_ALLOWED_TAGS,
            source=source or "unknown",
            title=title,
            url=url,
            summary_line=summary_line,
        )

    def _parse_response(self, raw: str) -> dict[str, Any]:
        obj = _extract_json_obj(raw)
        if not obj:
            return {
                "risk": 0.0, "surprise": 0.0, "confidence": 0.0,
                "tags": [], "primary_tag": "",
                "summary": f"{self._ERROR_PREFIX}no_json:{raw[:80]}",
            }
        tags_raw = obj.get("tags") or []
        if not isinstance(tags_raw, list):
            tags_raw = []
        tags = [t for t in tags_raw if isinstance(t, str) and t in set(_ALLOWED_TAGS)]
        pt = obj.get("primary_tag") or ""
        if pt not in set(_ALLOWED_TAGS):
            pt = tags[0] if tags else ""
        return {
            "risk": _clamp01(float(obj.get("risk") or 0.0)),
            "surprise": _clamp(float(obj.get("surprise") or 0.0), -1.0, 1.0),
            "confidence": _clamp01(float(obj.get("confidence") or 0.0)),
            "tags": tags,
            "primary_tag": pt,
            "summary": str(obj.get("summary") or "")[:200],
        }

    def _analyze_in_thread(self, *, title: str, url: str, source: str, summary: str) -> dict[str, Any]:
        """Runs inside self._executor's dedicated thread (owns its own event loop)."""
        # Per-client rate limit (no lock needed — single worker thread)
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._rate_sec:
            time.sleep(self._rate_sec - elapsed)
        self._last_call = time.monotonic()

        try:
            self._ensure_browser()

            if not self._is_logged_in():
                self._do_login(
                    os.getenv(f"{self._CRED_PREFIX}_EMAIL", ""),
                    os.getenv(f"{self._CRED_PREFIX}_PASSWORD", ""),
                )

            prompt = self._build_prompt(title=title, url=url, source=source, summary=summary)
            raw = self._send_prompt(prompt)
            result = self._parse_response(raw)
            logger.info(
                "%s ok title=%.60s risk=%.2f tags=%s",
                self.CLIENT_NAME, title, result["risk"], result["tags"],
            )
            return result

        except Exception as exc:
            logger.warning("%s failed title=%.60s err=%r", self.CLIENT_NAME, title, exc)
            self._reset_browser()
            return {
                "risk": 0.0, "surprise": 0.0, "confidence": 0.0,
                "tags": [], "primary_tag": "",
                "summary": f"{self._ERROR_PREFIX}{str(exc)[:100]}",
            }

    def analyze(self, *, title: str, url: str, source: str, summary: str = "") -> dict[str, Any]:
        """v1 interface — submit to dedicated executor thread."""
        future = self._executor.submit(
            self._analyze_in_thread,
            title=title, url=url, source=source, summary=summary,
        )
        try:
            return future.result(timeout=self._timeout_sec + 30)
        except concurrent.futures.TimeoutError:
            return {
                "risk": 0.0, "surprise": 0.0, "confidence": 0.0,
                "tags": [], "primary_tag": "",
                "summary": f"{self._ERROR_PREFIX}executor_timeout",
            }

    def analyze_v2(
        self,
        *,
        title: str,
        url: str,
        source: str,
        summary: str = "",
        published_ts_ms: int = 0,
        ingested_ts_ms: int = 0,
    ) -> dict[str, Any]:
        """
        v2 interface — uses prompt_v2 and returns news_llm_analysis_v2 dict.
        Called by playwright_enrichment_worker. Returns raw dict (not LLMResult)
        so the worker can attach job_id / provider / latency before validation.
        """
        from news_pipeline.prompt_v2 import build_prompt_v2
        prompt = build_prompt_v2(
            title=title, url=url, source=source, summary=summary,
            published_ts_ms=published_ts_ms, ingested_ts_ms=ingested_ts_ms,
        )
        future = self._executor.submit(self._send_prompt_raw, prompt)
        try:
            raw_text = future.result(timeout=self._timeout_sec + 30)
        except concurrent.futures.TimeoutError:
            return {"_status": "timeout", "_provider": self.CLIENT_NAME}
        obj = _extract_json_obj(raw_text or "")
        if not obj:
            return {"_status": "invalid_json", "_provider": self.CLIENT_NAME,
                    "_raw": (raw_text or "")[:200]}
        obj["_status"] = "ok"
        obj["_provider"] = self.CLIENT_NAME
        return obj

    def _send_prompt_raw(self, prompt: str) -> str:
        """Run _send_prompt inside the executor thread (includes rate-limit + login)."""
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._rate_sec:
            time.sleep(self._rate_sec - elapsed)
        self._last_call = time.monotonic()
        self._ensure_browser()
        if not self._is_logged_in():
            self._do_login(
                os.getenv(f"{self._CRED_PREFIX}_EMAIL", ""),
                os.getenv(f"{self._CRED_PREFIX}_PASSWORD", ""),
            )
        return self._send_prompt(prompt)


# ── DeepSeek web UI ───────────────────────────────────────────────────────────

class PlaywrightDeepSeekClient(PlaywrightLLMBase):
    """
    Interacts with chat.deepseek.com web chat.
    Requires DEEPSEEK_EMAIL + DEEPSEEK_PASSWORD.
    """

    CLIENT_NAME = "playwright_deepseek"
    SESSION_NAME = "deepseek"
    _ERROR_PREFIX = "playwright_deepseek_error:"
    _CRED_PREFIX = "DEEPSEEK"

    # Robust locators (role + text fallbacks)
    _CHAT_URL = "https://chat.deepseek.com"
    _LOGIN_URL = "https://chat.deepseek.com/sign_in"

    def __init__(self) -> None:
        super().__init__()
        self._rate_sec = float(os.getenv("PLAYWRIGHT_DEEPSEEK_RATE_SEC", "5.0"))

    def _is_logged_in(self) -> bool:
        try:
            cur = self._page.url or ""
            if "chat.deepseek.com" not in cur:
                self._page.goto(self._CHAT_URL, wait_until="domcontentloaded", timeout=20_000)
                time.sleep(2.0)
            # Textarea visible → logged in
            return self._page.locator("textarea").count() > 0
        except Exception:
            return False

    def _do_login(self, email: str, password: str) -> None:
        if not email or not password:
            logger.warning("deepseek: no credentials, skipping login")
            return
        page = self._page
        try:
            page.goto(self._LOGIN_URL, wait_until="domcontentloaded", timeout=20_000)
            time.sleep(random.uniform(1.5, 2.5))

            # Email field
            email_loc = page.locator('input[type="text"], input[name="email"], input[autocomplete="email"]').first
            email_loc.wait_for(state="visible", timeout=10_000)
            email_loc.click()
            time.sleep(random.uniform(0.3, 0.7))
            self._human_type(email_loc, email)
            time.sleep(random.uniform(0.4, 0.9))

            # Password field
            pwd_loc = page.locator('input[type="password"]').first
            pwd_loc.click()
            time.sleep(random.uniform(0.3, 0.6))
            self._human_type(pwd_loc, password)
            time.sleep(random.uniform(0.4, 0.8))

            page.keyboard.press("Enter")
            time.sleep(3.5)

            self._save_session()
            logger.info("deepseek: login ok")
        except Exception as exc:
            logger.warning("deepseek: login failed: %r", exc)

    def _send_prompt(self, prompt: str) -> str:
        page = self._page
        if "chat.deepseek.com" not in (page.url or ""):
            page.goto(self._CHAT_URL, wait_until="domcontentloaded", timeout=20_000)
            time.sleep(random.uniform(1.5, 2.5))

        # Click "New chat" to avoid context pollution from previous conversation
        try:
            new_chat = page.locator('[aria-label*="new chat"], button:has-text("New chat")').first
            if new_chat.is_visible(timeout=2_000):
                new_chat.click()
                time.sleep(1.0)
        except Exception:
            pass

        ta = page.locator("textarea").first
        ta.wait_for(state="visible", timeout=15_000)
        ta.click()
        time.sleep(random.uniform(0.3, 0.7))
        self._paste_text("textarea", prompt)
        time.sleep(0.5)

        page.mouse.move(
            random.randint(300, 600), random.randint(400, 600),
            steps=random.randint(10, 25),
        )
        page.keyboard.press("Enter")
        time.sleep(1.5)

        # JSON-polling loop: wait until response contains parsed dict with "risk" key.
        # This avoids grabbing partial streaming text or the user's own prompt.
        _RESP_SELECTORS = [
            '[class*="ds-markdown"]', '[class*="markdown"]',
            '[class*="prose"]', '[class*="message-content"]',
            '[role="presentation"] p', 'article',
        ]
        STABLE_NEEDED = 3
        POLL_SEC = 2.0
        deadline = time.monotonic() + self._timeout_sec
        last_json_text = ""
        stable_streak = 0
        response_text = ""

        while time.monotonic() < deadline:
            json_candidate = ""
            for sel in _RESP_SELECTORS:
                try:
                    els = page.locator(sel).all()
                    if els:
                        t = els[-1].inner_text().strip()
                        if t and "{" in t:
                            parsed = _extract_json_obj(t)
                            if parsed and ("risk" in parsed or "risk_score" in parsed or "event_type" in parsed):
                                json_candidate = t
                                break
                except Exception:
                    pass
            if json_candidate:
                if json_candidate == last_json_text:
                    stable_streak += 1
                    if stable_streak >= STABLE_NEEDED:
                        response_text = json_candidate
                        break
                else:
                    stable_streak = 0
                    last_json_text = json_candidate
            else:
                stable_streak = 0
            time.sleep(POLL_SEC)

        if not response_text and last_json_text:
            response_text = last_json_text

        if not response_text or "{" not in response_text:
            try:
                page_text = page.evaluate("document.body ? document.body.innerText : ''")
                dump_path = "/tmp/deepseek_page_debug.txt"
                with open(dump_path, "w") as f:
                    f.write(page_text[:8000])
                logger.warning("deepseek: no JSON in response — page dump → %s", dump_path)
                if "{" in page_text:
                    response_text = page_text
            except Exception:
                pass

        self._save_session()
        return response_text


# ── ChatGPT web UI ────────────────────────────────────────────────────────────

class PlaywrightChatGPTClient(PlaywrightLLMBase):
    """
    Interacts with chatgpt.com.
    Works with or without login (free tier uses GPT-4o).
    Login enables GPT-5 (requires Plus subscription).

    ENV:
        CHATGPT_EMAIL / CHATGPT_PASSWORD  (optional, for Plus)
    """

    CLIENT_NAME = "playwright_chatgpt"
    SESSION_NAME = "chatgpt"
    _ERROR_PREFIX = "playwright_chatgpt_error:"
    _CRED_PREFIX = "CHATGPT"

    _CHAT_URL = "https://chatgpt.com"
    _LOGIN_URL = "https://chatgpt.com/auth/login"

    # GPT-4o is free; switch model if Plus available
    _MODEL = os.getenv("CHATGPT_MODEL", "")  # e.g. "gpt-4o" or "" (use default)

    def __init__(self) -> None:
        super().__init__()
        self._timeout_sec = float(os.getenv("PLAYWRIGHT_CHATGPT_TIMEOUT_SEC", "300"))
        self._rate_sec = float(os.getenv("PLAYWRIGHT_CHATGPT_RATE_SEC", "6.0"))

    def _is_logged_in(self) -> bool:
        try:
            cur = self._page.url or ""
            if "chatgpt.com" not in cur:
                self._page.goto(self._CHAT_URL, wait_until="domcontentloaded", timeout=25_000)
                time.sleep(2.5)
            # Textarea or contenteditable input visible
            has_input = (
                self._page.locator("#prompt-textarea").count() > 0
                or self._page.locator('[contenteditable="true"]').count() > 0
            )
            return has_input
        except Exception:
            return False

    def _do_login(self, email: str, password: str) -> None:
        page = self._page
        try:
            page.goto(self._LOGIN_URL, wait_until="domcontentloaded", timeout=25_000)
            time.sleep(random.uniform(2.0, 3.0))

            # Click "Log in" button if on landing
            try:
                login_btn = page.locator('button:has-text("Log in"), a:has-text("Log in")').first
                if login_btn.is_visible(timeout=3_000):
                    login_btn.click()
                    time.sleep(1.5)
            except Exception:
                pass

            if email:
                email_loc = page.locator('input[name="email"], input[type="email"]').first
                email_loc.wait_for(state="visible", timeout=10_000)
                email_loc.click()
                self._human_type(email_loc, email)
                time.sleep(0.5)

                cont_btn = page.locator('button[type="submit"]').first
                cont_btn.click()
                time.sleep(1.5)

            if password:
                pwd_loc = page.locator('input[type="password"]').first
                pwd_loc.wait_for(state="visible", timeout=8_000)
                pwd_loc.click()
                self._human_type(pwd_loc, password)
                time.sleep(0.5)
                page.keyboard.press("Enter")
                time.sleep(3.0)

            self._save_session()
            logger.info("chatgpt: login ok")
        except Exception as exc:
            logger.warning("chatgpt: login failed: %r — continuing as guest", exc)

    def _send_prompt(self, prompt: str) -> str:
        page = self._page

        if "chatgpt.com" not in (page.url or ""):
            page.goto(self._CHAT_URL, wait_until="domcontentloaded", timeout=25_000)
            time.sleep(random.uniform(2.0, 3.0))

        # Dismiss any modal/overlay
        try:
            overlay = page.locator('[role="dialog"] button:has-text("OK"), [role="dialog"] button:has-text("Got it")').first
            if overlay.is_visible(timeout=2_000):
                overlay.click()
                time.sleep(0.8)
        except Exception:
            pass

        # Find prompt textarea (contenteditable or textarea)
        input_loc = page.locator("#prompt-textarea").first
        if input_loc.count() == 0:
            input_loc = page.locator('[contenteditable="true"]').first

        input_loc.wait_for(state="visible", timeout=15_000)
        input_loc.click()
        time.sleep(random.uniform(0.3, 0.7))

        # Paste via clipboard API or key combination
        page.evaluate(f"""
            (() => {{
                const el = document.querySelector('#prompt-textarea') ||
                           document.querySelector('[contenteditable="true"]');
                if (!el) return;
                if (el.tagName === 'TEXTAREA') {{
                    const nv = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value');
                    if (nv && nv.set) nv.set.call(el, {json.dumps(prompt)});
                    el.dispatchEvent(new InputEvent('input', {{bubbles: true, data: 'x'}}));
                }} else {{
                    el.focus();
                    document.execCommand('selectAll', false, null);
                    document.execCommand('insertText', false, {json.dumps(prompt)});
                }}
            }})()
        """)
        time.sleep(0.5)

        # Click send button
        try:
            send_btn = page.locator('[data-testid="send-button"], button[aria-label*="Send"]').first
            send_btn.wait_for(state="visible", timeout=5_000)
            send_btn.click()
        except Exception:
            page.keyboard.press("Enter")

        time.sleep(1.5)

        # Wait for assistant response to stabilise
        response_text = self._wait_for_stable_text(
            '[data-message-author-role="assistant"] .markdown, '
            '[data-message-author-role="assistant"] p, '
            '.message-content'
        )

        # Fallback: grab last assistant bubble
        if not response_text:
            response_text = self._wait_for_stable_text(
                '[class*="assistant"] [class*="markdown"]'
            )

        self._save_session()
        return response_text


# ── Gemini web UI ─────────────────────────────────────────────────────────────

class PlaywrightGeminiClient(PlaywrightLLMBase):
    """
    Interacts with gemini.google.com.
    Requires Google account (GEMINI_GOOGLE_EMAIL / GEMINI_GOOGLE_PASSWORD).
    Google login is complex; use session persistence to avoid re-auth.
    """

    CLIENT_NAME = "playwright_gemini"
    SESSION_NAME = "gemini"
    _ERROR_PREFIX = "playwright_gemini_error:"
    _CRED_PREFIX = "GEMINI_GOOGLE"

    _CHAT_URL = "https://gemini.google.com"

    def __init__(self) -> None:
        super().__init__()
        self._rate_sec = float(os.getenv("PLAYWRIGHT_GEMINI_RATE_SEC", "7.0"))

    def _is_logged_in(self) -> bool:
        try:
            cur = self._page.url or ""
            if "gemini.google.com" not in cur:
                self._page.goto(self._CHAT_URL, wait_until="domcontentloaded", timeout=25_000)
                time.sleep(2.5)
            # If redirected to accounts.google.com → not logged in
            if "accounts.google.com" in (self._page.url or ""):
                return False
            # Presence of input area = logged in
            return (
                self._page.locator('[contenteditable="true"]').count() > 0
                or self._page.locator('rich-textarea').count() > 0
            )
        except Exception:
            return False

    def _do_login(self, email: str, password: str) -> None:
        if not email or not password:
            logger.warning("gemini: no Google credentials — session-only mode")
            return
        page = self._page
        try:
            # Google sign-in flow
            page.goto("https://accounts.google.com/signin", wait_until="domcontentloaded", timeout=20_000)
            time.sleep(random.uniform(1.5, 2.5))

            email_loc = page.locator('input[type="email"]').first
            email_loc.wait_for(state="visible", timeout=10_000)
            email_loc.click()
            self._human_type(email_loc, email)
            time.sleep(0.5)
            page.keyboard.press("Enter")
            time.sleep(random.uniform(1.5, 2.5))

            pwd_loc = page.locator('input[type="password"]').first
            pwd_loc.wait_for(state="visible", timeout=10_000)
            pwd_loc.click()
            self._human_type(pwd_loc, password)
            time.sleep(0.5)
            page.keyboard.press("Enter")
            time.sleep(3.5)

            # Navigate to Gemini after login
            page.goto(self._CHAT_URL, wait_until="domcontentloaded", timeout=20_000)
            time.sleep(2.5)

            self._save_session()
            logger.info("gemini: login ok")
        except Exception as exc:
            logger.warning("gemini: login failed: %r", exc)

    def _send_prompt(self, prompt: str) -> str:
        page = self._page

        if "gemini.google.com" not in (page.url or ""):
            page.goto(self._CHAT_URL, wait_until="domcontentloaded", timeout=25_000)
            time.sleep(random.uniform(2.0, 3.0))

        # Find Gemini's rich-textarea
        input_loc = page.locator('rich-textarea [contenteditable="true"], [contenteditable="true"]').first
        input_loc.wait_for(state="visible", timeout=15_000)
        input_loc.click()
        time.sleep(random.uniform(0.3, 0.7))

        # Type via keyboard (Gemini's contenteditable is tricky with direct set)
        # Select all + type overwrites previous content
        page.keyboard.press("Control+a")
        time.sleep(0.2)
        page.keyboard.press("Delete")
        time.sleep(0.2)
        # Type the prompt (short-enough for this approach)
        input_loc.type(prompt, delay=5)  # fast but human-ish
        time.sleep(0.5)

        # Send
        try:
            send_btn = page.locator('[aria-label*="Send message"], button[aria-label*="Send"]').first
            send_btn.wait_for(state="visible", timeout=5_000)
            send_btn.click()
        except Exception:
            page.keyboard.press("Enter")

        time.sleep(1.5)

        response_text = self._wait_for_stable_text(
            'model-response .markdown, model-response p, '
            '[class*="response-content"], [data-test-id="response"]'
        )

        self._save_session()
        return response_text


# ── Qwen web UI ───────────────────────────────────────────────────────────────

class PlaywrightQwenClient(PlaywrightLLMBase):
    """
    Interacts with chat.qwenlm.ai — Alibaba's Qwen3 flagship model.
    Login via Google OAuth (same Google account credentials as Gemini).

    Default model: qwen3-235b-a22b (largest free MoE, thinking mode).
    Override: QWEN_MODEL env var.

    ENV:
        QWEN_EMAIL      — Google account email
        QWEN_PASSWORD   — Google account password
        QWEN_MODEL      — model slug (default: qwen3-235b-a22b)
        QWEN_THINKING   — "1" (default) | "0" to disable extended thinking
    """

    CLIENT_NAME = "playwright_qwen"
    SESSION_NAME = "qwen"
    _ERROR_PREFIX = "playwright_qwen_error:"
    _CRED_PREFIX = "QWEN"

    _CHAT_URL = "https://chat.qwenlm.ai"
    _LOGIN_URL = "https://chat.qwenlm.ai"

    # Latest flagship model as of 2026-05
    _DEFAULT_MODEL = "qwen3-235b-a22b"

    def __init__(self) -> None:
        super().__init__()
        # Qwen3-235B with thinking mode is slow — allow much more time than the default
        self._timeout_sec = float(os.getenv("PLAYWRIGHT_QWEN_TIMEOUT_SEC", "300"))
        self._rate_sec = float(os.getenv("PLAYWRIGHT_QWEN_RATE_SEC", "5.0"))
        self._model = os.getenv("QWEN_MODEL", self._DEFAULT_MODEL).strip()
        self._thinking = os.getenv("QWEN_THINKING", "1") == "1"

    def _is_logged_in(self) -> bool:
        try:
            cur = self._page.url or ""
            if "qwenlm.ai" not in cur:
                self._page.goto(self._CHAT_URL, wait_until="domcontentloaded", timeout=25_000)
                time.sleep(2.5)
            # accounts.google.com in URL → not logged in
            if "accounts.google.com" in (self._page.url or ""):
                return False
            # "Stay logged out" or "Log in" button visible → guest landing page, NOT logged in
            page_text = ""
            try:
                page_text = self._page.evaluate("document.body ? document.body.innerText : ''")
            except Exception:
                pass
            if any(marker in page_text for marker in ("Stay logged out", "Login or sign up", "Log in\nSign up")):
                return False
            # User avatar / conversation sidebar / New Chat button → truly logged in
            logged_in_indicators = (
                self._page.locator(
                    '[data-testid*="avatar"], [aria-label*="New chat"], '
                    'button:has-text("New chat"), button:has-text("New conversation"), '
                    '[class*="sidebar"], [class*="conversation-list"]'
                ).count() > 0
            )
            return logged_in_indicators
        except Exception:
            return False

    def _google_oauth(self, email: str, password: str, page: Any) -> None:
        """Complete Google OAuth flow on the current page (accounts.google.com)."""
        try:
            email_loc = page.locator('input[type="email"]').first
            email_loc.wait_for(state="visible", timeout=10_000)
            email_loc.click()
            time.sleep(random.uniform(0.3, 0.6))
            self._human_type(email_loc, email)
            time.sleep(random.uniform(0.4, 0.8))

            next_btn = page.locator('button:has-text("Next"), #identifierNext').first
            next_btn.click()
            time.sleep(random.uniform(1.5, 2.5))

            pwd_loc = page.locator('input[type="password"]').first
            pwd_loc.wait_for(state="visible", timeout=10_000)
            pwd_loc.click()
            time.sleep(random.uniform(0.3, 0.6))
            self._human_type(pwd_loc, password)
            time.sleep(random.uniform(0.4, 0.8))

            pwd_next = page.locator('button:has-text("Next"), #passwordNext').first
            pwd_next.click()
            time.sleep(3.5)
        except Exception as exc:
            logger.warning("qwen: google_oauth step failed: %r", exc)

    def _do_login(self, email: str, password: str) -> None:
        if not email or not password:
            logger.warning("qwen: no credentials — session-only mode")
            return
        page = self._page
        try:
            page.goto(self._CHAT_URL, wait_until="domcontentloaded", timeout=25_000)
            time.sleep(random.uniform(1.5, 2.5))

            # Look for sign-in / login button
            sign_in = page.locator(
                'button:has-text("Sign in"), button:has-text("Log in"), '
                'a:has-text("Sign in"), [data-testid*="login"], [data-testid*="signin"]'
            ).first
            if sign_in.is_visible(timeout=5_000):
                sign_in.click()
                time.sleep(1.5)

            # Look for "Continue with Google" button
            google_btn = page.locator(
                'button:has-text("Google"), [aria-label*="Google"], '
                'button:has-text("Continue with Google")'
            ).first

            if google_btn.is_visible(timeout=5_000):
                # Google OAuth may open as popup or redirect
                try:
                    with page.expect_popup(timeout=8_000) as popup_info:
                        google_btn.click()
                    popup = popup_info.value
                    popup.wait_for_load_state("domcontentloaded", timeout=15_000)
                    self._google_oauth(email, password, popup)
                    # Wait for popup to close (OAuth complete)
                    try:
                        popup.wait_for_event("close", timeout=15_000)
                    except Exception:
                        pass
                except Exception:
                    # No popup → same-tab redirect
                    google_btn.click()
                    time.sleep(2.0)
                    if "accounts.google.com" in (page.url or ""):
                        self._google_oauth(email, password, page)
                        # Wait for redirect back to Qwen
                        try:
                            page.wait_for_url("*qwenlm.ai*", timeout=15_000)
                        except Exception:
                            pass
            else:
                # Direct email/password login fallback
                email_loc = page.locator('input[type="email"], input[name="email"]').first
                if email_loc.is_visible(timeout=3_000):
                    email_loc.click()
                    self._human_type(email_loc, email)
                    time.sleep(0.5)
                    pwd_loc = page.locator('input[type="password"]').first
                    pwd_loc.wait_for(state="visible", timeout=5_000)
                    pwd_loc.click()
                    self._human_type(pwd_loc, password)
                    page.keyboard.press("Enter")
                    time.sleep(3.0)

            time.sleep(2.0)
            self._save_session()
            logger.info("qwen: login ok model=%s", self._model)

        except Exception as exc:
            logger.warning("qwen: login failed: %r", exc)

    def _select_model(self) -> None:
        """Select the configured model if a model switcher is present."""
        page = self._page
        try:
            # Typical model selector: dropdown button showing current model name
            model_btn = page.locator(
                '[data-testid*="model"], button[class*="model"], '
                '[aria-label*="model"], button:has-text("Qwen")'
            ).first
            if not model_btn.is_visible(timeout=3_000):
                return
            model_btn.click()
            time.sleep(0.8)

            # Click the target model option
            target = page.locator(
                f'[data-testid*="{self._model}"], '
                f'li:has-text("{self._model}"), '
                f'button:has-text("{self._model}"), '
                f'[role="option"]:has-text("235B"), '
                f'[role="menuitem"]:has-text("235B")'
            ).first
            if target.is_visible(timeout=3_000):
                target.click()
                time.sleep(0.6)
                logger.info("qwen: model selected = %s", self._model)
        except Exception:
            pass  # Selector may vary; default model is usually the latest anyway

    def _toggle_thinking(self) -> None:
        """Enable/disable extended thinking mode if toggle is present."""
        if not self._thinking:
            return
        page = self._page
        try:
            thinking_btn = page.locator(
                'button[aria-label*="thinking"], button:has-text("Think"), '
                '[data-testid*="thinking"]'
            ).first
            if thinking_btn.is_visible(timeout=2_000):
                # Only click if currently OFF (aria-pressed=false or similar)
                pressed = thinking_btn.get_attribute("aria-pressed") or ""
                if pressed.lower() in ("false", "0", ""):
                    thinking_btn.click()
                    time.sleep(0.4)
        except Exception:
            pass

    def _send_prompt(self, prompt: str) -> str:
        page = self._page

        if "qwenlm.ai" not in (page.url or ""):
            page.goto(self._CHAT_URL, wait_until="domcontentloaded", timeout=25_000)
            time.sleep(random.uniform(1.5, 2.5))

        # Select latest model and enable thinking
        self._select_model()
        self._toggle_thinking()

        # New conversation to avoid context bleed
        try:
            new_btn = page.locator(
                'button:has-text("New chat"), button:has-text("New conversation"), '
                '[aria-label*="new chat"], [data-testid*="new-chat"]'
            ).first
            if new_btn.is_visible(timeout=2_000):
                new_btn.click()
                time.sleep(0.8)
        except Exception:
            pass

        # Find and fill input
        input_loc = page.locator('textarea, [contenteditable="true"]').first
        input_loc.wait_for(state="visible", timeout=15_000)
        input_loc.click()
        time.sleep(random.uniform(0.3, 0.6))

        # Paste via JS for textarea, keyboard for contenteditable
        tag = input_loc.evaluate("el => el.tagName.toLowerCase()")
        if tag == "textarea":
            self._paste_text("textarea", prompt)
        else:
            page.keyboard.press("Control+a")
            time.sleep(0.1)
            page.keyboard.press("Delete")
            time.sleep(0.1)
            input_loc.type(prompt, delay=3)
        time.sleep(0.5)

        # Submit
        try:
            send_btn = page.locator(
                'button[aria-label*="Send"], button[type="submit"], '
                '[data-testid*="send"]'
            ).first
            send_btn.wait_for(state="visible", timeout=5_000)
            send_btn.click()
        except Exception:
            page.keyboard.press("Enter")

        time.sleep(1.5)

        # Wait for response — Qwen3 streams through a <think> block first,
        # then emits the final JSON answer.
        # Single shared deadline so we don't burn full timeout on each selector.
        _RESP_SELECTORS = [
            '.markdown-body', '[class*="markdown-body"]',
            '[class*="markdown"]', '[class*="message-content"]',
            '[class*="chat-message"]', '[class*="assistant"]',
            'article', 'main p',
        ]
        STABLE_NEEDED = 4
        POLL_SEC = 2.0
        deadline = time.monotonic() + self._timeout_sec
        last_json_text = ""
        stable_streak = 0
        response_text = ""

        while time.monotonic() < deadline:
            # Only treat text as candidate if it parses as dict with "risk" key.
            # This excludes the user's prompt (it contains { but isn't valid parsed JSON).
            json_candidate = ""
            for sel in _RESP_SELECTORS:
                try:
                    els = page.locator(sel).all()
                    if els:
                        t = els[-1].inner_text().strip()
                        if t and "{" in t:
                            parsed = _extract_json_obj(t)
                            if parsed and ("risk" in parsed or "risk_score" in parsed or "event_type" in parsed):
                                json_candidate = t
                                break
                except Exception:
                    pass

            if json_candidate:
                if json_candidate == last_json_text:
                    stable_streak += 1
                    if stable_streak >= STABLE_NEEDED:
                        response_text = json_candidate
                        break
                else:
                    stable_streak = 0
                    last_json_text = json_candidate
            else:
                stable_streak = 0
            time.sleep(POLL_SEC)

        # If we timed out but last seen JSON was valid, use it anyway
        if not response_text and last_json_text:
            response_text = last_json_text

        # Debug: dump full page text if we still have no JSON
        if not response_text or "{" not in response_text:
            try:
                page_text = page.evaluate("document.body ? document.body.innerText : ''")
                dump_path = "/tmp/qwen_page_debug.txt"
                with open(dump_path, "w") as f:
                    f.write(page_text[:8000])
                logger.warning("qwen: no JSON in response — page dump → %s", dump_path)
                if "{" in page_text:
                    response_text = page_text
            except Exception:
                pass

        self._save_session()
        return response_text


# ── Round-robin wrapper ───────────────────────────────────────────────────────

class RoundRobinPlaywrightClient:
    """
    Distributes requests across Playwright clients in round-robin order.
    On each call starts from the next client in the ring; if that client
    returns an error (confidence=0), falls through to the remaining ones.
    If all fail, returns the last error result.
    """

    def __init__(self, clients: list[PlaywrightLLMBase]) -> None:
        self._clients = clients
        self._idx = 0
        self._lock = threading.Lock()

    def analyze(self, *, title: str, url: str, source: str, summary: str = "") -> dict[str, Any]:
        if not self._clients:
            return {
                "risk": 0.0, "surprise": 0.0, "confidence": 0.0,
                "tags": [], "primary_tag": "", "summary": "all_llms_failed",
            }
        with self._lock:
            start = self._idx % len(self._clients)
            self._idx += 1

        ordered = self._clients[start:] + self._clients[:start]
        last: dict[str, Any] | None = None
        for client in ordered:
            res = client.analyze(title=title, url=url, source=source, summary=summary)
            last = res
            if res.get("confidence", 0) > 0:
                return res
        return last or {
            "risk": 0.0, "surprise": 0.0, "confidence": 0.0,
            "tags": [], "primary_tag": "", "summary": "all_llms_failed",
        }


# ── Factory ───────────────────────────────────────────────────────────────────

def build_playwright_clients() -> list[PlaywrightLLMBase]:
    """
    Return enabled playwright LLM clients in priority order.
    Skips silently when playwright is not installed.
    """
    try:
        import playwright  # noqa: F401
    except ImportError:
        logger.warning("playwright not installed — skipping browser-based LLM clients")
        return []

    clients: list[PlaywrightLLMBase] = []

    # Priority 1: Qwen3-235B (largest free MoE, Google OAuth)
    if os.getenv("USE_PLAYWRIGHT_QWEN", "1") == "1":
        clients.append(PlaywrightQwenClient())

    # Priority 2: DeepSeek (strong, dedicated login)
    if os.getenv("USE_PLAYWRIGHT_DEEPSEEK", "1") == "1":
        clients.append(PlaywrightDeepSeekClient())

    # Priority 3: ChatGPT (free tier, session-based)
    if os.getenv("USE_PLAYWRIGHT_CHATGPT", "1") == "1":
        clients.append(PlaywrightChatGPTClient())

    # Priority 4: Gemini web UI (opt-in, Google login required)
    if os.getenv("USE_PLAYWRIGHT_GEMINI", "0") == "1":
        clients.append(PlaywrightGeminiClient())

    logger.info("playwright LLM clients ready: %s", [c.CLIENT_NAME for c in clients])
    return clients
