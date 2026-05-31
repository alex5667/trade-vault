import json
import time
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import redis  # pip install redis

log = logging.getLogger("news-analyzer")


# --- Tags mapping (битовые флаги) ---
# ВАЖНО: фиксируйте порядок, не меняйте индексы (иначе backtest/история сломаются).
TAG_BITS: Dict[str, int] = {
    "fed": 0,
    "fomc": 1,
    "cpi": 2,
    "ppi": 3,
    "jobs": 4,
    "gdp": 5,
    "rates": 6,
    "risk_off": 7,
    "risk_on": 8,
    "war": 9,
    "regulation": 10,
    "exchange": 11,
    "hack": 12,
    "etf": 13,
    "earnings": 14,
    "macro": 15,
    # ... расширяйте, но не переиспользуйте биты
}

def tags_to_mask(tags: List[str]) -> int:
    m = 0
    for t in tags:
        b = TAG_BITS.get(str(t).strip().lower())
        if b is not None:
            m |= (1 << b)
    return m

def grade_to_id(grade: str) -> int:
    g = (grade or "").upper()
    if g == "CRITICAL": return 3
    if g == "HIGH": return 2
    if g == "MED": return 1
    return 0

def safe_float(x: Any, d: float = 0.0) -> float:
    try: return float(x)
    except Exception: return d

def stable_uid(*parts: str) -> str:
    # совпадает с вашим python models.py (sha256 + separator)
    import hashlib
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()[:24]


# --- LLM интерфейс ---
class LLMClient:
    def analyze_news(self, *, title: str, summary: str, source: str, url: str) -> Dict[str, Any]:
        """
        Должен вернуть минимум:
          risk_score: 0..1
          surprise_score: -1..1
          grade: LOW|MED|HIGH|CRITICAL
          narrative_tags: list[str]
          summary: short
        """
        raise NotImplementedError


@dataclass(frozen=True)
class AnalyzerConfig:
    redis_url: str = "redis://redis-worker-1:6379/0"
    stream_in: str = "news:raw"
    stream_out: str = "news:analysis"
    group: str = "news-analyzer"
    consumer: str = "news-analyzer-1"

    block_ms: int = 5000
    count: int = 50

    dedupe_ttl_sec: int = 7 * 24 * 3600
    heavy_ttl_sec: int = 7 * 24 * 3600

    # fail-open деградация
    max_pending_before_degrade: int = 5000


class NewsAnalyzer:
    def __init__(self, cfg: AnalyzerConfig, r: redis.Redis, llm: LLMClient) -> None:
        self.cfg = cfg
        self.r = r
        self.llm = llm

    def ensure_group(self) -> None:
        try:
            self.r.xgroup_create(self.cfg.stream_in, self.cfg.group, id="$", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" in str(e):
                return
            raise

    def run_forever(self) -> None:
        self.ensure_group()
        log.info("started stream=%s group=%s consumer=%s", self.cfg.stream_in, self.cfg.group, self.cfg.consumer)

        while True:
            # Можно мониторить backlog через XPENDING (дорого делать часто).
            # Делаем мягко: раз в N циклов или по таймеру.
            msgs = self.r.xreadgroup(
                groupname=self.cfg.group,
                consumername=self.cfg.consumer,
                streams={self.cfg.stream_in: ">"},
                count=self.cfg.count,
                block=self.cfg.block_ms,
            )
            if not msgs:
                continue

            for stream_name, entries in msgs:
                for msg_id, fields in entries:
                    try:
                        self._handle_one(msg_id, fields)
                    except Exception as e:
                        # fail-open: не держим pending навечно
                        log.exception("handle failed id=%s err=%s", msg_id, e)
                        try:
                            self.r.xack(self.cfg.stream_in, self.cfg.group, msg_id)
                        except Exception:
                            pass

    def _handle_one(self, msg_id: str, fields: Dict[str, Any]) -> None:
        # decode_responses=True → fields уже str->str
        uid = str(fields.get("uid") or "")
        title = str(fields.get("title") or "")
        url = str(fields.get("url") or "")
        source = str(fields.get("source") or "")
        summary = str(fields.get("summary") or "")

        if not uid or not title:
            self.r.xack(self.cfg.stream_in, self.cfg.group, msg_id)
            return

        # --- Идемпотентность: один raw.uid -> один анализ ---
        dedupe_key = f"news:analysis:done:{uid}"
        if not self.r.set(dedupe_key, "1", nx=True, ex=self.cfg.dedupe_ttl_sec):
            self.r.xack(self.cfg.stream_in, self.cfg.group, msg_id)
            return

        # --- Деградация при огромном backlog (опционально) ---
        # Если у вас бывают spikes, можно включить реальную проверку XPENDING.
        # Здесь оставляю "точку расширения":
        degrade = False

        if degrade:
            out = {"risk_score": 0.0, "surprise_score": 0.0, "grade": "LOW", "narrative_tags": [], "summary": ""}
        else:
            out = self.llm.analyze_news(title=title, summary=summary, source=source, url=url)

        risk = safe_float(out.get("risk_score"), 0.0)
        surprise = safe_float(out.get("surprise_score"), 0.0)
        grade = str(out.get("grade") or "LOW").upper()
        tags = list(out.get("narrative_tags") or [])
        short = str(out.get("summary") or "")[:400]

        tags_mask = tags_to_mask(tags)
        primary_tag_id = self._pick_primary_tag(tags)

        grade_id = grade_to_id(grade)

        analysis_uid = stable_uid("analysis", uid, grade, str(int(risk * 1000)), str(tags_mask))

        heavy_key = f"news:analysis:{analysis_uid}"
        heavy_obj = {
            "analysis_uid": analysis_uid,
            "raw_uid": uid,
            "ts_ms": int(time.time() * 1000),
            "risk": risk,
            "surprise": surprise,
            "grade": grade,
            "tags": tags[:16],
            "summary": short,
            "source": source,
            "url": url,
            # сюда можно складывать всё "тяжёлое": объяснение, цепочки, цитаты, full json
            "llm": out,
        }

        pipe = self.r.pipeline(transaction=False)

        # тяжёлое → отдельный key
        pipe.set(heavy_key, json.dumps(heavy_obj, ensure_ascii=False, separators=(",", ":")), ex=self.cfg.heavy_ttl_sec)

        # компакт → stream
        pipe.xadd(self.cfg.stream_out, {
            "uid": analysis_uid,
            "raw_uid": uid,
            "analyzed_ts_ms": str(int(time.time() * 1000)),
            "risk": str(risk),
            "surprise": str(surprise),
            "grade_id": str(grade_id),
            "tags_mask": str(tags_mask),
            "primary_tag_id": str(primary_tag_id),
            "ref": heavy_key,
        }, maxlen=200000, approximate=True)

        # ack вход
        pipe.xack(self.cfg.stream_in, self.cfg.group, msg_id)

        pipe.execute()

    def _pick_primary_tag(self, tags: List[str]) -> int:
        # простая стратегия: первый распознанный тег
        for t in tags:
            b = TAG_BITS.get(str(t).strip().lower())
            if b is not None:
                return b + 1  # +1 чтобы 0 означал "none"
        return 0
