# python-worker/news_pipeline/llm_client.py
from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

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

    # strip <think> blocks from reasoning models
    if "<think>" in text:
        parts = text.split("</think>")
        text = parts[-1].strip()

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
            "cpi", "ppi", "fomc", "fed_speech", "nfp", "rates", "inflation"
            "risk_off", "risk_on", "earnings", "geopolitics", "crypto_reg"
            "exchange", "hack", "etf", "liquidation", "macro"
        }

    def analyze(self, *, title: str, url: str, source: str, summary: str = "") -> Dict[str, Any]:
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
            f"source={source}\nurl={url}\ntitle={title}\nsummary={summary}\n"
        )

        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
        body = json.dumps(
            {
                "contents": [{"parts": [{"text": prompt}]}]
                "generationConfig": {"temperature": self.temperature, "maxOutputTokens": self.max_tokens}
            }
        ).encode("utf-8")

        last_err = ""
        for attempt in range(self.max_retries + 1):
            try:
                req = urllib.request.Request(
                    url=endpoint
                    data=body
                    headers={"Content-Type": "application/json", "x-goog-api-key": self.api_key}
                    method="POST"
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
                    "risk": _clamp01(risk)
                    "surprise": _clamp(surprise, -1.0, 1.0)
                    "confidence": _clamp01(conf)
                    "tags": tags
                    "primary_tag": primary_tag
                    "summary": summary
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
            "risk": 0.0
            "surprise": 0.0
            "confidence": 0.0
            "tags": []
            "primary_tag": ""
            "summary": f"llm_error:{last_err}"[:200]
        }


class NvidiaDeepSeekClient(LLMClient):
    """Nvidia Integrate API — DeepSeek-v3.2 (OpenAI-compatible endpoint).

    ENV:
      NVIDIA_DEEPSEEK_API_KEY  — API key (default: hardcoded fallback)
      NVIDIA_DEEPSEEK_MODEL    — model name (default: deepseek-ai/deepseek-v3.2)

    Endpoint: https://integrate.api.nvidia.com/v1/chat/completions
    Auth:     Authorization: Bearer <key>
    """

    def __init__(self) -> None:
        self.api_key = os.getenv("NVIDIA_DEEPSEEK_API_KEY", "").strip()
        self.model = os.getenv("NVIDIA_DEEPSEEK_MODEL", "deepseek-ai/deepseek-v3.2").strip()
        self.timeout_sec = float(os.getenv("GEMINI_TIMEOUT_SEC", "15"))
        self.max_retries = int(os.getenv("GEMINI_RETRIES", "2"))

        self.allowed_tags = {
            "cpi", "ppi", "fomc", "fed_speech", "nfp", "rates", "inflation"
            "risk_off", "risk_on", "earnings", "geopolitics", "crypto_reg"
            "exchange", "hack", "etf", "liquidation", "macro"
        }

    def analyze(self, *, title: str, url: str, source: str, summary: str = "") -> Dict[str, Any]:
        if not self.api_key:
            return {"risk": 0.0, "surprise": 0.0, "tags": [], "primary_tag": "", "confidence": 0.0, "summary": ""}

        prompt = (
            "Return ONLY a compact JSON object with keys:\n"
            "risk (0..1 float), surprise (-1..1 float), confidence (0..1 float),\n"
            "tags (array of strings from allowed set),\n"
            "primary_tag (string from same set), summary (<=160 chars).\n\n"
            f"allowed_tags={sorted(self.allowed_tags)}\n"
            f"source={source}\nurl={url}\ntitle={title}\nsummary={summary}\n"
        )

        endpoint = "https://integrate.api.nvidia.com/v1/chat/completions"
        body = json.dumps(
            {
                "model": self.model
                "messages": [{"role": "user", "content": prompt}]
                "max_tokens": 512
                "temperature": 0.2
                "top_p": 0.95
                "stream": False
            }
        ).encode("utf-8")

        last_err = ""
        for attempt in range(self.max_retries + 1):
            try:
                req = urllib.request.Request(
                    url=endpoint
                    data=body
                    headers={
                        "Content-Type": "application/json"
                        "Authorization": f"Bearer {self.api_key}"
                        "Accept": "application/json"
                    }
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")

                data = json.loads(raw)
                text = ""
                try:
                    text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                except Exception:
                    text = raw[:512]

                obj = _extract_json_obj(text) or {}

                risk = float(obj.get("risk", 0.0) or 0.0)
                surprise = float(obj.get("surprise", 0.0) or 0.0)
                conf = float(obj.get("confidence", 0.0) or 0.0)
                summary_out = str(obj.get("summary") or "")[:200]

                tags = obj.get("tags") or []
                if not isinstance(tags, list):
                    tags = []
                tags = [t for t in tags if isinstance(t, str) and t in self.allowed_tags]

                primary_tag = obj.get("primary_tag") or ""
                if not isinstance(primary_tag, str) or primary_tag not in self.allowed_tags:
                    primary_tag = ""

                return {
                    "risk": _clamp01(risk)
                    "surprise": _clamp(surprise, -1.0, 1.0)
                    "confidence": _clamp01(conf)
                    "tags": tags
                    "primary_tag": primary_tag
                    "summary": summary_out
                }

            except urllib.error.HTTPError as e:
                last_err = f"HTTPError {e.code}"
                if e.code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                    time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
                    continue
            except Exception as e:
                last_err = str(e)
                if attempt < self.max_retries:
                    time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
                    continue

        return {
            "risk": 0.0
            "surprise": 0.0
            "confidence": 0.0
            "tags": []
            "primary_tag": ""
            "summary": f"deepseek_error:{last_err}"[:200]
        }


class NvidiaQwenClient(LLMClient):
    """Nvidia Integrate API Client (Qwen Fallback)."""

    def __init__(self) -> None:
        self.api_key = os.getenv("NVIDIA_API_KEY", "").strip()
        self.model = os.getenv("NVIDIA_MODEL", "qwen/qwen3.5-397b-a17b").strip()
        self.timeout_sec = float(os.getenv("GEMINI_TIMEOUT_SEC", "15"))
        self.max_retries = int(os.getenv("GEMINI_RETRIES", "1")) # Меньше ретраев для фолбека

        self.allowed_tags = {
            "cpi", "ppi", "fomc", "fed_speech", "nfp", "rates", "inflation"
            "risk_off", "risk_on", "earnings", "geopolitics", "crypto_reg"
            "exchange", "hack", "etf", "liquidation", "macro"
        }

    def analyze(self, *, title: str, url: str, source: str, summary: str = "") -> Dict[str, Any]:
        if not self.api_key:
            return {"risk": 0.0, "surprise": 0.0, "tags": [], "primary_tag": "", "confidence": 0.0, "summary": ""}

        prompt = (
            "Return ONLY a compact JSON object with keys:\n"
            "risk (0..1 float), surprise (-1..1 float), confidence (0..1 float),\n"
            "tags (array of strings from allowed set),\n"
            "primary_tag (string from same set), summary (<=160 chars).\n\n"
            f"allowed_tags={sorted(self.allowed_tags)}\n"
            f"source={source}\nurl={url}\ntitle={title}\nsummary={summary}\n"
        )

        endpoint = "https://integrate.api.nvidia.com/v1/chat/completions"
        body = json.dumps(
            {
                "model": self.model
                "messages": [{"role": "user", "content": prompt}]
                "max_tokens": 512
                "temperature": 0.2, # Немного детерминированности
                "top_p": 0.95
                "top_k": 20
                "stream": False
                # "chat_template_kwargs": {"enable_thinking": True},  # Отключаем thinking для JSON ответа
            }
        ).encode("utf-8")

        last_err = ""
        for attempt in range(self.max_retries + 1):
            try:
                req = urllib.request.Request(
                    url=endpoint
                    data=body
                    headers={
                        "Content-Type": "application/json"
                        "Authorization": f"Bearer {self.api_key}"
                        "Accept": "application/json"
                    }
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")

                data = json.loads(raw)
                text = ""
                try:
                    text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                except Exception:
                    text = raw[:512]

                obj = _extract_json_obj(text) or {}

                risk = float(obj.get("risk", 0.0) or 0.0)
                surprise = float(obj.get("surprise", 0.0) or 0.0)
                conf = float(obj.get("confidence", 0.0) or 0.0)
                summary_out = str(obj.get("summary") or "")[:200]

                tags = obj.get("tags") or []
                if not isinstance(tags, list):
                    tags = []
                tags = [t for t in tags if isinstance(t, str) and t in self.allowed_tags]

                primary_tag = obj.get("primary_tag") or ""
                if not isinstance(primary_tag, str) or primary_tag not in self.allowed_tags:
                    primary_tag = ""

                return {
                    "risk": _clamp01(risk)
                    "surprise": _clamp(surprise, -1.0, 1.0)
                    "confidence": _clamp01(conf)
                    "tags": tags
                    "primary_tag": primary_tag
                    "summary": summary_out
                }

            except urllib.error.HTTPError as e:
                last_err = f"HTTPError {e.code}"
                if e.code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                    time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
                    continue
            except Exception as e:
                last_err = str(e)
                if attempt < self.max_retries:
                    time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
                    continue

        return {
            "risk": 0.0
            "surprise": 0.0
            "confidence": 0.0
            "tags": []
            "primary_tag": ""
            "summary": f"nv_qwen_error:{last_err}"[:200]
        }


class OllamaDeepSeekClient(LLMClient):
    """Local Ollama instance fallback (target: deepseek-r1).

    Assumes endpoint at OLLAMA_BASE_URL (default: http://host.docker.internal:11434).
    """

    def __init__(self) -> None:
        self.base_url = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434").rstrip("/")
        self.model = os.getenv("NEWS_FALLBACK_OLLAMA_MODEL", "deepseek-r1:14b").strip()
        self.timeout_sec = float(os.getenv("OLLAMA_TIMEOUT_SEC", "30"))
        self.max_retries = int(os.getenv("OLLAMA_RETRIES", "1"))

        self.allowed_tags = {
            "cpi", "ppi", "fomc", "fed_speech", "nfp", "rates", "inflation"
            "risk_off", "risk_on", "earnings", "geopolitics", "crypto_reg"
            "exchange", "hack", "etf", "liquidation", "macro"
        }

    def analyze(self, *, title: str, url: str, source: str, summary: str = "") -> Dict[str, Any]:
        prompt = (
            "Return ONLY a compact JSON object with keys:\n"
            "risk (0..1 float), surprise (-1..1 float), confidence (0..1 float),\n"
            "tags (array of strings from allowed set),\n"
            "primary_tag (string from same set), summary (<=160 chars).\n\n"
            f"allowed_tags={sorted(self.allowed_tags)}\n"
            f"source={source}\nurl={url}\ntitle={title}\nsummary={summary}\n"
        )

        endpoint = f"{self.base_url}/api/chat"
        body = json.dumps({
            "model": self.model
            "messages": [{"role": "user", "content": prompt}]
            "stream": False
            "options": {
                "temperature": 0.2
            }
        }).encode("utf-8")

        last_err = ""
        for attempt in range(self.max_retries + 1):
            try:
                req = urllib.request.Request(
                    url=endpoint
                    data=body
                    headers={"Content-Type": "application/json", "Accept": "application/json"}
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")

                data = json.loads(raw)
                text = ""
                try:
                    text = data.get("message", {}).get("content", "")
                except Exception:
                    text = raw[:512]

                obj = _extract_json_obj(text) or {}

                risk = float(obj.get("risk", 0.0) or 0.0)
                surprise = float(obj.get("surprise", 0.0) or 0.0)
                conf = float(obj.get("confidence", 0.0) or 0.0)
                summary_out = str(obj.get("summary") or "")[:200]

                tags = obj.get("tags") or []
                if not isinstance(tags, list):
                    tags = []
                tags = [t for t in tags if isinstance(t, str) and t in self.allowed_tags]

                primary_tag = obj.get("primary_tag") or ""
                if not isinstance(primary_tag, str) or primary_tag not in self.allowed_tags:
                    primary_tag = ""

                return {
                    "risk": _clamp01(risk)
                    "surprise": _clamp(surprise, -1.0, 1.0)
                    "confidence": _clamp01(conf)
                    "tags": tags
                    "primary_tag": primary_tag
                    "summary": summary_out
                }

            except urllib.error.HTTPError as e:
                last_err = f"HTTPError {e.code}"
                if e.code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                    time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
                    continue
            except Exception as e:
                last_err = str(e)
                if attempt < self.max_retries:
                    time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
                    continue

        return {
            "risk": 0.0
            "surprise": 0.0
            "confidence": 0.0
            "tags": []
            "primary_tag": ""
            "summary": f"ollama_error:{last_err}"[:200]
        }


class OllamaMinipcClient(LLMClient):
    """Local Ollama instance fallback on Minipc (target: qwen2.5:7b).

    Assumes endpoint at OLLAMA_MINIPC_URL (default: http://192.168.0.121:11434).
    """

    def __init__(self) -> None:
        self.base_url = os.getenv("OLLAMA_MINIPC_URL", "http://192.168.0.121:11434").rstrip("/")
        self.model = os.getenv("NEWS_FALLBACK_MINIPC_MODEL", "qwen2.5:7b").strip()
        self.timeout_sec = float(os.getenv("OLLAMA_TIMEOUT_SEC", "30"))
        self.max_retries = int(os.getenv("OLLAMA_RETRIES", "1"))

        self.allowed_tags = {
            "cpi", "ppi", "fomc", "fed_speech", "nfp", "rates", "inflation"
            "risk_off", "risk_on", "earnings", "geopolitics", "crypto_reg"
            "exchange", "hack", "etf", "liquidation", "macro"
        }

    def analyze(self, *, title: str, url: str, source: str, summary: str = "") -> Dict[str, Any]:
        prompt = (
            "Return ONLY a compact JSON object with keys:\n"
            "risk (0..1 float), surprise (-1..1 float), confidence (0..1 float),\n"
            "tags (array of strings from allowed set),\n"
            "primary_tag (string from same set), summary (<=160 chars).\n\n"
            f"allowed_tags={sorted(self.allowed_tags)}\n"
            f"source={source}\nurl={url}\ntitle={title}\nsummary={summary}\n"
        )

        endpoint = f"{self.base_url}/api/chat"
        body = json.dumps({
            "model": self.model
            "messages": [{"role": "user", "content": prompt}]
            "stream": False
            "options": {
                "temperature": 0.2
            }
        }).encode("utf-8")

        last_err = ""
        for attempt in range(self.max_retries + 1):
            try:
                req = urllib.request.Request(
                    url=endpoint
                    data=body
                    headers={"Content-Type": "application/json", "Accept": "application/json"}
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")

                data = json.loads(raw)
                text = ""
                try:
                    text = data.get("message", {}).get("content", "")
                except Exception:
                    text = raw[:512]

                obj = _extract_json_obj(text) or {}

                risk = float(obj.get("risk", 0.0) or 0.0)
                surprise = float(obj.get("surprise", 0.0) or 0.0)
                conf = float(obj.get("confidence", 0.0) or 0.0)
                summary_out = str(obj.get("summary") or "")[:200]

                tags = obj.get("tags") or []
                if not isinstance(tags, list):
                    tags = []
                tags = [t for t in tags if isinstance(t, str) and t in self.allowed_tags]

                primary_tag = obj.get("primary_tag") or ""
                if not isinstance(primary_tag, str) or primary_tag not in self.allowed_tags:
                    primary_tag = ""

                return {
                    "risk": _clamp01(risk)
                    "surprise": _clamp(surprise, -1.0, 1.0)
                    "confidence": _clamp01(conf)
                    "tags": tags
                    "primary_tag": primary_tag
                    "summary": summary_out
                }

            except urllib.error.HTTPError as e:
                last_err = f"HTTPError {e.code}"
                if e.code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                    time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
                    continue
            except Exception as e:
                last_err = str(e)
                if attempt < self.max_retries:
                    time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
                    continue

        return {
            "risk": 0.0
            "surprise": 0.0
            "confidence": 0.0
            "tags": []
            "primary_tag": ""
            "summary": f"ollama_minipc_error:{last_err}"[:200]
        }


class FallbackLLMClient(LLMClient):
    """
    Класс, принимающий несколько клиентов: сначала пробует primary
    при ошибке пробует следующий по цепочке.
    """

    # Маркеры ошибок всех клиентов
    _ERR_MARKERS = ("llm_error:", "nv_qwen_error:", "nv_kimi_error:", "deepseek_error:", "ollama_error:", "ollama_minipc_error:")

    def __init__(self, clients: list[LLMClient]) -> None:
        self.clients = clients

    @classmethod
    def build_default(cls) -> "FallbackLLMClient":
        """Создать цепочку из всех доступных клиентов в том порядке, в котором они пробуются."""
        return cls([
            GeminiHTTPClient()
            NvidiaQwenClient()
            NvidiaKimiClient()
            NvidiaDeepSeekClient()
            OllamaDeepSeekClient()
            OllamaMinipcClient()
        ])

    def analyze(self, *, title: str, url: str, source: str, summary: str = "") -> Dict[str, Any]:
        last_res = None
        for client in self.clients:
            client_name = type(client).__name__
            t0 = time.monotonic()
            res = client.analyze(title=title, url=url, source=source, summary=summary)
            elapsed = time.monotonic() - t0
            last_res = res
            s = res.get("summary", "")
            # Если ни один маркер ошибки не найден — успех
            if not any(m in s for m in self._ERR_MARKERS):
                logger.info("llm_fallback ok client=%s elapsed=%.2fs", client_name, elapsed)
                return res
            # ошибка — пробуем следующий LLM
            logger.warning("llm_fallback fail client=%s elapsed=%.2fs err=%s", client_name, elapsed, s[:120])
            time.sleep(0.5)

        logger.error("llm_fallback all_failed title=%s", title[:80])
        return last_res or {
            "risk": 0.0
            "surprise": 0.0
            "confidence": 0.0
            "tags": []
            "primary_tag": ""
            "summary": "all_llms_failed"
        }


class NvidiaKimiClient(LLMClient):
    """Nvidia Integrate API Client (Kimi-k2.5 Fallback)."""

    def __init__(self) -> None:
        self.api_key = os.getenv("NVIDIA_API_KEY", "").strip()
        self.model = os.getenv("NVIDIA_MODEL_KIMI", "moonshotai/kimi-k2.5").strip()
        self.timeout_sec = float(os.getenv("GEMINI_TIMEOUT_SEC", "15"))
        self.max_retries = int(os.getenv("GEMINI_RETRIES", "1"))

        self.allowed_tags = {
            "cpi", "ppi", "fomc", "fed_speech", "nfp", "rates", "inflation"
            "risk_off", "risk_on", "earnings", "geopolitics", "crypto_reg"
            "exchange", "hack", "etf", "liquidation", "macro"
        }

    def analyze(self, *, title: str, url: str, source: str, summary: str = "") -> Dict[str, Any]:
        if not self.api_key:
            return {"risk": 0.0, "surprise": 0.0, "tags": [], "primary_tag": "", "confidence": 0.0, "summary": ""}

        prompt = (
            "Return ONLY a compact JSON object with keys:\n"
            "risk (0..1 float), surprise (-1..1 float), confidence (0..1 float),\n"
            "tags (array of strings from allowed set),\n"
            "primary_tag (string from same set), summary (<=160 chars).\n\n"
            f"allowed_tags={sorted(self.allowed_tags)}\n"
            f"source={source}\nurl={url}\ntitle={title}\nsummary={summary}\n"
        )

        endpoint = "https://integrate.api.nvidia.com/v1/chat/completions"
        body = json.dumps(
            {
                "model": self.model
                "messages": [{"role": "user", "content": prompt}]
                "max_tokens": 512
                "temperature": 1.00
                "top_p": 1.00
                "stream": False
                "chat_template_kwargs": {"thinking": True}
            }
        ).encode("utf-8")

        last_err = ""
        for attempt in range(self.max_retries + 1):
            try:
                req = urllib.request.Request(
                    url=endpoint
                    data=body
                    headers={
                        "Content-Type": "application/json"
                        "Authorization": f"Bearer {self.api_key}"
                        "Accept": "application/json"
                    }
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")

                data = json.loads(raw)
                text = ""
                try:
                    text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                except Exception:
                    text = raw[:512]

                obj = _extract_json_obj(text) or {}

                risk = float(obj.get("risk", 0.0) or 0.0)
                surprise = float(obj.get("surprise", 0.0) or 0.0)
                conf = float(obj.get("confidence", 0.0) or 0.0)
                summary_out = str(obj.get("summary") or "")[:200]

                tags = obj.get("tags") or []
                if not isinstance(tags, list):
                    tags = []
                tags = [t for t in tags if isinstance(t, str) and t in self.allowed_tags]

                primary_tag = obj.get("primary_tag") or ""
                if not isinstance(primary_tag, str) or primary_tag not in self.allowed_tags:
                    primary_tag = ""

                return {
                    "risk": _clamp01(risk)
                    "surprise": _clamp(surprise, -1.0, 1.0)
                    "confidence": _clamp01(conf)
                    "tags": tags
                    "primary_tag": primary_tag
                    "summary": summary_out
                }

            except urllib.error.HTTPError as e:
                last_err = f"HTTPError {e.code}"
                if e.code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                    time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
                    continue
            except Exception as e:
                last_err = str(e)
                if attempt < self.max_retries:
                    time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
                    continue

        return {
            "risk": 0.0
            "surprise": 0.0
            "confidence": 0.0
            "tags": []
            "primary_tag": ""
            "summary": f"nv_kimi_error:{last_err}"[:200]
        }
