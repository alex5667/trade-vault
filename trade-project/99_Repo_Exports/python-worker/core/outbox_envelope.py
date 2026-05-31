from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

from domain.evidence_keys import MetaKeys

# Protocol version of the outbox envelope.
# Bumped to 2 to match meta.payload_schema="outbox_envelope:v2" emitted by
# services/outbox/envelope_builder.py. The on-wire shape did not change;
# the bump activates dual-read defaults so v1 producers (still in-flight at
# rollout time) remain accepted by SignalDispatcher without an env override.
SCHEMA_VERSION = 2

# Older protocol versions the dispatcher still accepts by default during the
# v1→v2 migration window. Drop entries here once `dispatcher_schema_version_total
# {schema_version="1"}` is stable at zero for ≥48h.
LEGACY_SCHEMA_VERSIONS: tuple[int, ...] = (1,)

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

    schema_version vs feature_schema_version:
      schema_version          — envelope/protocol version (gated by dispatcher
                                via ACCEPTED_SCHEMA_VERSIONS). NEVER set this
                                from ML feature-set version.
      feature_schema_version  — ML feature-set version (e.g. v13_of=13,
                                v14_of=14, v15_of=15). Propagated from
                                confirmations_engine.build() for replay/audit,
                                NOT for dispatcher gating.
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
    # Protocol version — gated by dispatcher ACCEPTED_SCHEMA_VERSIONS.
    schema_version: int = SCHEMA_VERSION
    # ML feature-set version (propagated, not gated). Default 0 means "unset".
    feature_schema_version: int = 0

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

        # Backward-compat: `meta_schema_version` is an ML feature-set version.
        # Historically it was misrouted into protocol `schema_version`, which
        # silently DLQ'd envelopes once ML schema diverged from 1 (e.g. v13_of=13).
        # Now route it into `feature_schema_version` only; protocol stays at SCHEMA_VERSION.
        if "meta_schema_version" in kwargs:
            try:
                fsv = int(kwargs.pop(MetaKeys.SCHEMA_VERSION) or 0)
            except (TypeError, ValueError):
                fsv = 0
            kwargs.setdefault("feature_schema_version", fsv)

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
        if self.feature_schema_version:
            d["feature_schema_version"] = int(self.feature_schema_version)
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
