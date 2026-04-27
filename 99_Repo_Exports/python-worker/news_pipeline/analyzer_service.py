from __future__ import annotations

import logging
import json
import time
from typing import Any, Dict, List, Optional, Protocol, Tuple

import redis

from .models import NewsRawItem, NewsAnalysisCompact
from .redis_streams import ensure_group, xreadgroup_block, xack, xadd_trim
from .tags import tags_to_mask, pick_primary_tag
from .utils import now_ms, safe_float
from . import config
from .llm_client import GeminiClient, LLMClient


log = logging.getLogger("news-analyzer")


class LLMClient(Protocol):
    def analyze_news(self, item: NewsRawItem) -> Dict[str, Any]:
        """
        Вернуть dict (тяжёлое):
        {
          "summary": "...",
          "risk": 0..1,
          "surprise": -1..1,
          "tags": ["cpi","risk_off"],
          "confidence": 0..1,
          "symbol_hints": ["BTCUSDT"] (optional),
          "notes": ...
        }
        """
        ...


class GeminiClient:
    """
    Реальная интеграция зависит от твоей библиотеки.
    Вариант 1: google-generativeai
    Вариант 2: google-genai (новый SDK)
    Я оставляю тонкий слой, который легко заменить.
    """

    def __init__(self, api_key: str, model: str) -> None:
        self.api_key = api_key
        self.model = model

    def analyze_news(self, item: NewsRawItem) -> Dict[str, Any]:
        if not self.api_key:
            # fail-open: если ключа нет — отдаём нейтральный анализ
            return {"summary": "", "risk": 0.0, "surprise": 0.0, "tags": [], "confidence": 0.0}

        # ---- ПСЕВДОКОД (вставь свой SDK) ----
        # prompt = f"..."
        # resp = gemini.generate_content(prompt)
        # parsed = json.loads(resp.text) ...
        #
        # Ниже — безопасный stub:
        return {
            "summary": item.title[:240],
            "risk": 0.2,
            "surprise": 0.0,
            "tags": ["macro"],
            "confidence": 0.5,
        }


def _analysis_key(uid: str) -> str:
    return f"news:analysis:{uid}"


class NewsAnalyzerService:
    """
    ConsumerGroup:
      - читает news:raw
      - делает LLM-анализ
      - тяжёлое кладёт в Redis key news:analysis:<uid> (TTL дни)
      - компактное кладёт в stream news:analysis (для feature store)

    Важно:
      - ack только после записи результатов
      - on error: DLQ + ack (или не ack, но тогда будет висеть pending)
    """

    def __init__(
        self,
        r: redis.Redis,
        llm: LLMClient,
        consumer: str = "analyzer-1",
        block_ms: int = 5000,
        batch: int = 10,
        dlq_stream: str = "news:dlq",
    ) -> None:
        self.r = r
        self.llm = llm
        self.consumer = consumer
        self.block_ms = block_ms
        self.batch = batch
        self.dlq_stream = dlq_stream
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    def run_forever(self) -> None:
        ensure_group(self.r, config.NEWS_RAW_STREAM, config.NEWS_ANALYZER_GROUP, mkstream=True)
        log.info("news-analyzer started consumer=%s", self.consumer)

        while not self._stop:
            items = xreadgroup_block(
                self.r,
                config.NEWS_RAW_STREAM,
                config.NEWS_ANALYZER_GROUP,
                consumer=self.consumer,
                count=self.batch,
                block_ms=self.block_ms,
            )
            if not items:
                continue

            for _stream, msgs in items:
                for msg_id, fields in msgs.items():
                    try:
                        raw = NewsRawItem.from_stream_fields(fields)
                        if not raw.uid:
                            xack(self.r, config.NEWS_RAW_STREAM, config.NEWS_ANALYZER_GROUP, msg_id)
                            continue

                        heavy = self.llm.analyze_news(raw)
                        tags = heavy.get("tags") or []
                        if isinstance(tags, str):
                            tags = [t for t in tags.split(",") if t]
                        if not isinstance(tags, list):
                            tags = []

                        risk = float(heavy.get("risk") or 0.0)
                        risk = max(0.0, min(1.0, risk))

                        surprise = float(heavy.get("surprise") or 0.0)
                        surprise = max(-1.0, min(1.0, surprise))

                        confidence = float(heavy.get("confidence") or 0.0)
                        confidence = max(0.0, min(1.0, confidence))

                        # tags -> mask + primary
                        mask = tags_to_mask(tags)
                        primary = pick_primary_tag(tags)

                        # тяжёлое в key
                        key = _analysis_key(raw.uid)
                        heavy_payload = {
                            "uid": raw.uid,
                            "source": raw.source,
                            "url": raw.url,
                            "title": raw.title,
                            "ts_ms": raw.ts_ms,
                            "symbols": raw.symbols,
                            "analysis": heavy,
                        }
                        self.r.set(key, json.dumps(heavy_payload, ensure_ascii=False))
                        self.r.expire(key, int(config.NEWS_ANALYSIS_KEY_TTL_SEC))

                        compact = NewsAnalysisCompact(
                            uid=raw.uid,
                            ts_ms=raw.ts_ms or now_ms(),
                            symbols=raw.symbols,
                            risk=risk,
                            surprise=surprise,
                            tags_mask=mask,
                            primary_tag_id=primary,
                            confidence=confidence,
                            news_ref=key,
                        )
                        xadd_trim(
                            self.r,
                            config.NEWS_ANALYSIS_STREAM,
                            compact.to_stream_fields(),
                            maxlen=config.NEWS_MAXLEN,
                        )

                        # ack после успеха
                        xack(self.r, config.NEWS_RAW_STREAM, config.NEWS_ANALYZER_GROUP, msg_id)

                    except Exception as e:
                        log.exception("analyze failed msg_id=%s err=%s", msg_id, e)
                        try:
                            # DLQ (компактно)
                            xadd_trim(
                                self.r,
                                self.dlq_stream,
                                {
                                    "src_stream": config.NEWS_RAW_STREAM,
                                    "msg_id": str(msg_id),
                                    "err": str(e)[:512],
                                    "ts_ms": str(now_ms()),
                                },
                                maxlen=config.NEWS_MAXLEN,
                            )
                        except Exception:
                            pass
                        # чтобы не зависало в pending — ack
                        try:
                            xack(self.r, config.NEWS_RAW_STREAM, config.NEWS_ANALYZER_GROUP, msg_id)
                        except Exception:
                            pass
