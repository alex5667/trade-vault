# python-worker/news_pipeline/llm_client.py
from __future__ import annotations

import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

# NOTE: This file stays dependency-free (urllib) on purpose.
# Many deployments want the smallest possible image for python-worker.


class LLMClient:
    def analyze(self, *, title: str, url: str, source: str) -> Dict[str, Any]:
        raise NotImplementedError


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _clamp01(x: float) -> float:
    return _clamp(x, 0.0, 1.0)


def _extract_json_obj(text: str) -> Optional[dict]:
    """Extract the first JSON object from LLM output.

    Gemini иногда возвращает JSON внутри markdown или с префиксом/суффиксом.
    Мы пытаемся:
      1) json.loads(text)
      2) find first {...} span and json.loads(span)
    """
    text = (text or "").strip()
    if not text:
        return None

    # 1) direct JSON
    try:
        v = json.loads(text)
        return v if isinstance(v, dict) else None
    except Exception:
        pass

    # 2) find a plausible object span
    i = text.find("{")
    j = text.rfind("}")
    if i >= 0 and j > i:
        frag = text[i : j + 1]
        try:
            v = json.loads(frag)
            return v if isinstance(v, dict) else None
        except Exception:
            return None

    return None


class _TokenBucket:
    """Very small in-process rate limiter.

    It is NOT a distributed limiter. For multi-replica, prefer Redis-based limiter.
    """

    def __init__(self, rpm: float) -> None:
        self.capacity = max(1.0, float(rpm))
        self.tokens = self.capacity
        self.fill_rate = self.capacity / 60.0  # tokens per second
        self.last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last
            self.last = now

            self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return

            # Need to wait for (1-token)/fill_rate seconds.
            need = (1.0 - self.tokens) / max(1e-6, self.fill_rate)
        time.sleep(need)


class GeminiHTTPClient(LLMClient):
    """Gemini REST generateContent client.

    Endpoint:
      POST https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent
      headers: x-goog-api-key: <key>

    Response shape:
      candidates[0].content.parts[0].text  (best-effort)
    """

    def __init__(self) -> None:
        self.api_key = os.getenv("GEMINI_API_KEY", "").strip()
        self.model = os.getenv("GEMINI_MODEL", "gemini-1.5-pro").strip()
        self.timeout_sec = float(os.getenv("GEMINI_TIMEOUT_SEC", "10"))
        self.max_retries = int(os.getenv("GEMINI_RETRIES", "2"))
        self.temperature = float(os.getenv("GEMINI_TEMPERATURE", "0.2"))
        self.max_tokens = int(os.getenv("GEMINI_MAX_TOKENS", "256"))

        # Best-effort local rpm limiter (prevents self-throttling storms).
        rpm = float(os.getenv("GEMINI_RPM", "0") or "0")
        self._limiter = _TokenBucket(rpm) if rpm > 0 else None

        # Allowed tag vocabulary (keep in sync with your tags.py)
        self.allowed_tags = {
            "cpi", "ppi", "fomc", "fed_speech", "nfp", "rates", "inflation",
            "risk_off", "risk_on", "earnings", "geopolitics", "crypto_reg",
            "exchange", "hack", "etf", "liquidation", "macro",
        }

    def analyze(self, *, title: str, url: str, source: str) -> Dict[str, Any]:
        if not self.api_key:
            # fail-open
            return {"risk": 0.0, "surprise": 0.0, "tags": [], "primary_tag": "", "confidence": 0.0, "summary": ""}

        if self._limiter is not None:
            self._limiter.acquire()

        prompt = (
            "Return ONLY compact JSON object with keys:\n"
            "risk (0..1 float), surprise (-1..1 float), confidence (0..1 float),\n"
            "tags (array of strings from allowed set),\n"
            "primary_tag (string from same set), summary (<=160 chars).\n\n"
            f"allowed_tags={sorted(self.allowed_tags)}\n"
            f"source={source}\nurl={url}\ntitle={title}\n"
        )

        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
        body = json.dumps(
            {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": self.temperature, "maxOutputTokens": self.max_tokens},
            }
        ).encode("utf-8")

        last_err = ""
        for attempt in range(self.max_retries + 1):
            try:
                req = urllib.request.Request(
                    url=endpoint,
                    data=body,
                    headers={"Content-Type": "application/json", "x-goog-api-key": self.api_key},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")

                data = json.loads(raw)
                text = ""
                try:
                    text = data["candidates"][0]["content"]["parts"][0]["text"]
                except Exception:
                    # fallback: try other common shapes
                    try:
                        text = data["candidates"][0]["content"]["parts"][0].get("text", "")
                    except Exception:
                        text = raw[:512]

                obj = _extract_json_obj(text) or {}

                risk = float(obj.get("risk", 0.0) or 0.0)
                surprise = float(obj.get("surprise", 0.0) or 0.0)
                conf = float(obj.get("confidence", 0.0) or 0.0)
                summary = str(obj.get("summary") or "")[:200]

                tags = obj.get("tags") or []
                if not isinstance(tags, list):
                    tags = []
                tags = [t for t in tags if isinstance(t, str) and t in self.allowed_tags]

                primary_tag = obj.get("primary_tag") or ""
                if not isinstance(primary_tag, str) or primary_tag not in self.allowed_tags:
                    primary_tag = ""

                return {
                    "risk": _clamp01(risk),
                    "surprise": _clamp(surprise, -1.0, 1.0),
                    "confidence": _clamp01(conf),
                    "tags": tags,
                    "primary_tag": primary_tag,
                    "summary": summary,
                }

            except urllib.error.HTTPError as e:
                last_err = f"HTTPError {e.code}"
                # retry on 429/5xx
                if e.code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                    # exponential backoff with jitter
                    time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
                    continue
            except Exception as e:
                last_err = str(e)
                if attempt < self.max_retries:
                    time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
                    continue

        return {
            "risk": 0.0,
            "surprise": 0.0,
            "confidence": 0.0,
            "tags": [],
            "primary_tag": "",
            "summary": f"llm_error:{last_err}"[:200],
        }
