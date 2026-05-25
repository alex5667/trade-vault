"""
tools/test_playwright_llm.py
────────────────────────────
Тест Playwright LLM клиентов на одной тестовой новости.
Запускает клиентов по цепочке: Qwen3 → DeepSeek → ChatGPT → API fallback.

Usage:
    cd python-worker
    QWEN_EMAIL=… QWEN_PASSWORD=… python -m tools.test_playwright_llm
    # или через make env:
    env $(cat ../news-pipeline.env | grep -v '^#' | xargs) python -m tools.test_playwright_llm
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("test_playwright_llm")

# ── Test news item ────────────────────────────────────────────────────────────
TEST_TITLE = (
    "Federal Reserve cuts interest rates by 50bps in emergency move "
    "as inflation drops to 1.8% — Bitcoin surges 8% to $98,500"
)
TEST_URL   = "https://coindesk.com/markets/2026/05/fed-rate-cut-bitcoin-surge"
TEST_SOURCE = "coindesk"
TEST_SUMMARY = (
    "The Federal Reserve announced an unexpected 50bps rate cut, "
    "citing below-target inflation and slowing economic growth. "
    "Risk assets rallied sharply; Bitcoin hit $98,500 within minutes."
)


def _pp(result: dict) -> str:
    return json.dumps(result, ensure_ascii=False, indent=2)


def run() -> None:
    log.info("=" * 70)
    log.info("TEST NEWS: %s", TEST_TITLE)
    log.info("=" * 70)

    # ── 1. Playwright LLM clients ─────────────────────────────────────────────
    os.environ.setdefault("PLAYWRIGHT_HEADLESS", "1")
    os.environ.setdefault("PLAYWRIGHT_BROWSER_SESSIONS", "/tmp/playwright_sessions_test")
    os.environ.setdefault("PLAYWRIGHT_LLM_TIMEOUT_SEC", "120")

    from news_pipeline.playwright_llm_client import (
        PlaywrightChatGPTClient,
        PlaywrightDeepSeekClient,
        PlaywrightQwenClient,
    )

    # Each client now runs inside its own ThreadPoolExecutor(max_workers=1)
    # so sync_playwright event loops never conflict between clients.
    clients_to_try = [
        ("Qwen3-235B (chat.qwenlm.ai)",  PlaywrightQwenClient),
        ("DeepSeek   (chat.deepseek.com)", PlaywrightDeepSeekClient),
        ("ChatGPT    (chatgpt.com free)",  PlaywrightChatGPTClient),
    ]

    _ERR_MARKERS = (
        "playwright_qwen_error:", "playwright_deepseek_error:",
        "playwright_chatgpt_error:", "no_json:", "executor_timeout",
        "all_llms_failed",
    )

    result = None
    for label, ClientClass in clients_to_try:
        log.info("─── Trying: %s", label)
        client = ClientClass()
        t0 = time.monotonic()
        try:
            res = client.analyze(
                title=TEST_TITLE,
                url=TEST_URL,
                source=TEST_SOURCE,
                summary=TEST_SUMMARY,
            )
        except Exception as exc:
            log.warning("  EXCEPTION: %r", exc)
            continue
        elapsed = time.monotonic() - t0

        summary = res.get("summary", "")
        is_error = any(m in summary for m in _ERR_MARKERS)

        log.info("  elapsed=%.1fs  risk=%.2f  confidence=%.2f  tags=%s",
                 elapsed, res.get("risk", 0), res.get("confidence", 0), res.get("tags"))
        log.info("  summary=%s", summary[:160])

        # Check debug dump if written
        dump = "/tmp/qwen_page_debug.txt"
        if os.path.exists(dump):
            with open(dump) as f:
                snippet = f.read(600)
            log.info("  [page dump] %s", snippet.replace("\n", " ")[:400])
            os.remove(dump)

        if not is_error and res.get("confidence", 0) > 0:
            log.info("✓ SUCCESS with %s", label)
            result = {"client": label, "elapsed_sec": round(elapsed, 2), **res}
            break
        else:
            log.warning("  → got error/empty, trying next client")

    # ── 2. API fallback if all browser clients failed ─────────────────────────
    if result is None:
        log.info("─── Trying API fallback: Gemini")
        from news_pipeline.llm_client import GeminiHTTPClient
        t0 = time.monotonic()
        try:
            res = GeminiHTTPClient().analyze(
                title=TEST_TITLE, url=TEST_URL, source=TEST_SOURCE, summary=TEST_SUMMARY
            )
            elapsed = time.monotonic() - t0
            result = {"client": "Gemini API", "elapsed_sec": round(elapsed, 2), **res}
            log.info("✓ Gemini API ok  risk=%.2f  tags=%s", res.get("risk", 0), res.get("tags"))
        except Exception as exc:
            log.error("Gemini API also failed: %r", exc)

    # ── 3. Print final result ─────────────────────────────────────────────────
    log.info("=" * 70)
    if result:
        log.info("FINAL RESULT:")
        print(_pp(result))
    else:
        log.error("ALL CLIENTS FAILED — no result")
    log.info("=" * 70)


if __name__ == "__main__":
    run()
