from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

from domain.evidence_keys import MetaKeys

SCHEMA_VERSION = 1

@dataclass(frozen=True)
class OutboxEnvelope:
    """
    Единый конверт для outbox.

    Почему не храним "как есть" в отдельных полях stream:
      - Redis Stream поля — строки/байты; сложные структуры надо сериализовать.
      - Но ключевые поля (id/kind/symbol/ts) держим отдельно для лёгкой фильтрации/дебага.

    Обязательные поля: signal_id, ts_ms, kind, symbol.
    Опциональные с дефолтами: trace_id, source, ingest_time_ms, quality_flags.
    event_id генерируется автоматически через make_envelope() если не задан.
    """
    signal_id: str
    ts_ms: int
    kind: str
    symbol: str
    # ── Observability / tracing (Optional с дефолтами) ────────────────────────
    event_id: str | None = None
    source: str = "python-worker"
    ingest_time_ms: int | None = None
    process_time_ms: int | None = None
    trace_id: str | None = None
    quality_flags: list[str] | None = None
    # ── Signal fields ─────────────────────────────────────────────────────────
    side: str | None = None
    raw_score: float | None = None
    final_score: float | None = None
    confidence_pct: float | None = None
    payload: dict[str, Any] | None = None
    # schema_version = meta_schema_version propagated from confirmations_engine.build().
    # Default SCHEMA_VERSION keeps backward-compat when caller does not pass it.
    schema_version: int = SCHEMA_VERSION

    @classmethod
    def make_envelope(cls, **kwargs) -> OutboxEnvelope:
        if "event_id" not in kwargs:
            kwargs["event_id"] = str(uuid.uuid4())

        if "ingest_time_ms" not in kwargs or kwargs["ingest_time_ms"] is None:
            payload = kwargs.get("payload")
            if isinstance(payload, dict) and "redis_read_time_ms" in payload:
                kwargs["ingest_time_ms"] = int(payload["redis_read_time_ms"])
            else:
                kwargs["ingest_time_ms"] = int(time.time() * 1000)

        # Accept meta_schema_version as an alias for schema_version so that
        # confirmations_engine.build() can propagate the feature-set version
        # without changing the field name on the envelope dataclass.
        if "meta_schema_version" in kwargs and "schema_version" not in kwargs:
            kwargs["schema_version"] = int(kwargs.pop(MetaKeys.SCHEMA_VERSION) or 1)
        elif "meta_schema_version" in kwargs:
            kwargs.pop(MetaKeys.SCHEMA_VERSION)  # discard duplicate

        return cls(**kwargs)

    def to_stream_fields(self) -> dict[str, str]:
        """
        Превратить envelope в поля для XADD.

        ВАЖНО:
          - payload сериализуем в JSON одной строкой.
          - Все значения приводим к строкам — совместимо с redis-py и фейковыми реализациями в тестах.
          - trace_id / event_id / ingest_time_ms генерируются на лету если не заданы.
        """
        event_id = self.event_id or str(uuid.uuid4())
        ingest_time_ms = self.ingest_time_ms if self.ingest_time_ms is not None else int(time.time() * 1000)
        process_time_ms = self.process_time_ms if self.process_time_ms is not None else int(time.time() * 1000)
        trace_id = self.trace_id or event_id

        d: dict[str, Any] = {
            "schema_version": self.schema_version,
            "event_id": event_id,
            "source": self.source,
            "signal_id": self.signal_id,
            "event_time_ms": int(self.ts_ms),
            "ts_ms": int(self.ts_ms),  # backward compat alias
            "ingest_time_ms": ingest_time_ms,
            "process_time_ms": process_time_ms,
            "kind": self.kind,
            "symbol": self.symbol,
            "trace_id": trace_id,
            "quality_flags": json.dumps(self.quality_flags or [], separators=(",", ":")),
        }
        if self.side is not None:
            d["side"] = self.side
        if self.raw_score is not None:
            d["raw_score"] = float(self.raw_score)
        if self.final_score is not None:
            d["final_score"] = float(self.final_score)
        if self.confidence_pct is not None:
            d["confidence_pct"] = float(self.confidence_pct)

        payload = self.payload or {}
        d["payload_json"] = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

        # Redis Stream поля как str
        return {k: str(v) for k, v in d.items()}


# Модульный алиас для обратной совместимости:
# from core.outbox_envelope import make_envelope
make_envelope = OutboxEnvelope.make_envelope
