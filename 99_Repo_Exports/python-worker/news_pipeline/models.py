# python-worker/news_pipeline/models.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ---- Tag bitmask (uint64) ----
# Набор фиксированный. Не переименовывайте биты без миграции бэктеста.
TAG_NONE               = 0
TAG_HIGH_IMPACT_MACRO  = 1 << 0   # FOMC/CPI/NFP/ECB/BOE/...
TAG_CRYPTO_REGULATION  = 1 << 1
TAG_EXCHANGE_OUTAGE    = 1 << 2
TAG_SECURITY_INCIDENT  = 1 << 3
TAG_ETF_FLOWS          = 1 << 4
TAG_EARNINGS           = 1 << 5
TAG_WAR_GEO            = 1 << 6
TAG_RISK_OFF           = 1 << 7
TAG_RISK_ON            = 1 << 8

# Можно расширять до 64 бит.


@dataclass(frozen=True, slots=True)
class NewsFeatures:
    """
    То, что попадает в ctx.news (строго компактно).
    """
    # 0..1: «опасность/важность» (чем выше — тем больше шанс ухудшить качество сигнала)
    risk: float = 0.0

    # -1..+1: «неожиданность»/surprise (условный скор)
    surprise: float = 0.0

    # uint64 bitmask
    tags_mask: int = 0

    # int id (свой словарь), либо 0 если нет
    primary_tag: int = 0

    # ref на heavy JSON (news:analysis:<uid>)
    ref: str = ""

    # ts обновления агрегата (epoch ms)
    updated_ts: int = 0

    # --- Calendar части (чтобы tick-loop делал 1 attach в одно место) ---
    # seconds до ближайшего high-impact события по asset_class
    event_tminus_sec: int = 10**9
    event_grade_id: int = 0
    event_ref: str = ""


@dataclass(frozen=True, slots=True)
class NewsRawItem:
    """
    Нормализованное сырьё от ingestor.
    Хранится в Redis Stream news:raw (полями).
    """
    uid: str
    source: str
    title: str
    url: str
    ts_ms: int
    # символ/asset_class может быть пустым: analyzer/feature-store дополнят
    symbol: str = ""
    asset_class: str = ""


@dataclass(frozen=True, slots=True)
class NewsAnalysis:
    """
    Результат LLM. В stream кладём компакт, heavy JSON — в key news:analysis:<uid>.
    """
    uid: str
    ts_ms: int
    symbol: str
    asset_class: str

    risk: float
    surprise: float
    tags_mask: int
    primary_tag: int

    # короткая версия (для отладки), но НЕ обязана быть всегда
    summary: str = ""


@dataclass(frozen=True, slots=True)
class NewsAnalysisCompact:
    """
    Компактная версия анализа для stream и feature store.
    Хранится в Redis Stream news:analysis.
    """
    uid: str
    ts_ms: int
    symbols: list[str]  # список символов, связанных с новостью
    risk: float
    surprise: float
    tags_mask: int
    primary_tag_id: int
    confidence: float
    news_ref: str  # ссылка на тяжелое хранилище news:analysis:<uid>

    def to_stream_fields(self) -> dict[str, str]:
        """Конвертирует в поля Redis Stream (все строки)."""
        return {
            "uid": self.uid
            "ts_ms": str(self.ts_ms)
            "symbols": ",".join(self.symbols)
            "risk": f"{self.risk:.6f}"
            "surprise": f"{self.surprise:.6f}"
            "tags_mask": str(self.tags_mask)
            "primary_tag_id": str(self.primary_tag_id)
            "confidence": f"{self.confidence:.6f}"
            "news_ref": self.news_ref
        }

    @classmethod
    def from_stream_fields(cls, fields: dict[str, str]) -> NewsAnalysisCompact:
        """Создает объект из полей Redis Stream."""
        symbols = fields.get("symbols", "").split(",") if fields.get("symbols") else []
        symbols = [s for s in symbols if s]  # убираем пустые

        return cls(
            uid=fields.get("uid", "")
            ts_ms=int(fields.get("ts_ms", "0"))
            symbols=symbols
            risk=float(fields.get("risk", "0.0"))
            surprise=float(fields.get("surprise", "0.0"))
            tags_mask=int(fields.get("tags_mask", "0"))
            primary_tag_id=int(fields.get("primary_tag_id", "0"))
            confidence=float(fields.get("confidence", "0.0"))
            news_ref=fields.get("news_ref", "")
        )


@dataclass(frozen=True, slots=True)
class CalendarEvent:
    """
    Календарное событие из Redis Stream calendar:events.
    """
    event_id: str
    title: str
    ts_ms: int
    grade_id: int  # 0..4 - важность события
    currency: str = ""  # валюта
    region: str = ""    # регион
    symbols: list[str] = None  # связанные символы

    def __post_init__(self):
        if self.symbols is None:
            object.__setattr__(self, 'symbols', [])

    def to_stream_fields(self) -> dict[str, str]:
        """Конвертирует в поля Redis Stream."""
        return {
            "event_id": self.event_id
            "title": self.title
            "ts_ms": str(self.ts_ms)
            "grade_id": str(self.grade_id)
            "currency": self.currency
            "region": self.region
            "symbols": ",".join(self.symbols)
        }

    @classmethod
    def from_stream_fields(cls, fields: dict[str, str]) -> CalendarEvent:
        """Создает объект из полей Redis Stream."""
        symbols = fields.get("symbols", "").split(",") if fields.get("symbols") else []
        symbols = [s for s in symbols if s]

        return cls(
            event_id=fields.get("event_id", "")
            title=fields.get("title", "")
            ts_ms=int(fields.get("ts_ms", "0"))
            grade_id=int(fields.get("grade_id", "0"))
            currency=fields.get("currency", "")
            region=fields.get("region", "")
            symbols=symbols
        )
