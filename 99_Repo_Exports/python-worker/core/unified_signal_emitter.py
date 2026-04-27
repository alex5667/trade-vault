from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import time
from typing import Any, Dict, List, Optional, Mapping

from core.outbox_envelope import OutboxEnvelope, make_envelope
from core.outbox_writer import OutboxWriter, EmitResult
from core.redis_keys import RedisStreams as RS


class UnifiedSignalEmitter:
    """
    Надёжная модель publish/outbox:

    1) Всегда пишем в outbox (Redis Stream) — это "источник истины" для downstream.
    2) Downstream consumer (в другом процессе) читает outbox и публикует в WS/TG/etc.
    3) Idempotent по signal_id: один сигнал -> одна запись в outbox, даже при ретраях/дубликатах.

    ВАЖНО:
    - emit() НЕ обязан "публиковать" куда-то ещё.
    - emit() обязан быть дешёвым и безопасным: записал в stream -> ок.
    """

    def __init__(
        self,
        redis,
        logger,
        metrics=None,
        source: str = "python-worker",
        outbox_stream: str = RS.SIGNAL_OUTBOX,
        dedup_ttl_s: int = 24 * 60 * 60,
    ):
        self.redis = redis
        self.logger = logger
        self.metrics = metrics
        self.source = source

        # OutboxWriter берёт на себя:
        # - дедуп по signal_id (SET NX)
        # - retries
        # - "placeholder" чтобы не терять сообщения при гонках/ошибках
        self._outbox = OutboxWriter(
            redis=redis,
            logger=logger,
            metrics=metrics,
            stream_name=outbox_stream,
            dedup_ttl_s=dedup_ttl_s,
        )

    def emit(
        self,
        *,
        signal_id: str,
        kind: str,
        symbol: str,
        side: Optional[str] = None,
        raw_score: Optional[float] = None,
        final_score: Optional[float] = None,
        confidence_pct: Optional[float] = None,
        payload: Optional[Dict[str, Any]] = None,
        labels: Optional[Mapping[str, Any]] = None,
        ts_event_ms: Optional[int] = None,
        ingest_time_ms: Optional[int] = None,
        trace_id: Optional[str] = None,
        quality_flags: Optional[List[str]] = None,
        source: Optional[str] = None,
        meta_schema_version: Optional[int] = None,
    ) -> EmitResult:
        """
        Записать сигнал в outbox.

        Новые параметры контракта:
          trace_id:      сквозной ID для трейсинга Go→Python→NestJS.
                         Если не передан — генерируется автоматически.
          quality_flags: список флагов DQ от детекторов (["stale_tick", ...]).
          source:        имя продюсера; переопределяет self.source для этого события.

        labels:
          - Дополнительные метки/тэги/диагностика от детектора/валидаторов/скорера.
          - Важно: labels всегда кладём в payload["labels"] (dict).
          - Это позволяет downstream не парсить "tags строкой", а работать со структурой.

        Idempotency:
          - key = f"outbox:dedup:{signal_id}"
          - если уже есть -> duplicate=True, emit ok, но повторно не пишем.
        """
        if not signal_id:
            # Без signal_id нельзя сделать idempotency.
            # Это лучше fail-closed: пусть вызывающая сторона починит генерацию sid.
            self._m_inc("outbox.emit.missing_signal_id")
            return EmitResult(ok=False, written=False, duplicate=False, entry_id=None)

        ts_ms = int(ts_event_ms or get_ny_time_millis())
        p: Dict[str, Any] = dict(payload or {})

        # Гарантируем структурные labels:
        # - payload.setdefault("labels", {}) -> dict
        # - затем update(labels)
        if labels:
            cur = p.get("labels")
            if not isinstance(cur, dict):
                cur = {}
            cur.update(dict(labels))
            p["labels"] = cur

        env = make_envelope(
            signal_id=str(signal_id),
            source=source or self.source,
            kind=str(kind),
            symbol=str(symbol),
            ts_ms=ts_ms,
            ingest_time_ms=ingest_time_ms,
            trace_id=trace_id,
            side=str(side) if side is not None else None,
            raw_score=float(raw_score) if raw_score is not None else None,
            final_score=float(final_score) if final_score is not None else None,
            confidence_pct=float(confidence_pct) if confidence_pct is not None else None,
            quality_flags=quality_flags,
            payload=p,
            meta_schema_version=int(meta_schema_version) if meta_schema_version is not None else None,
        )

        res = self._outbox.write(env)
        if res.ok:
            if res.duplicate:
                self._m_inc("outbox.emit.duplicate")
            else:
                self._m_inc("outbox.emit.written")
        else:
            self._m_inc("outbox.emit.failed")
        return res

    def _m_inc(self, name: str, v: int = 1) -> None:
        if not self.metrics:
            return
        try:
            # поддерживаем разные интерфейсы метрик (duck-typing)
            if hasattr(self.metrics, "inc"):
                self.metrics.inc(name, v)
            elif hasattr(self.metrics, "counter"):
                self.metrics.counter(name, v)
        except Exception:
            pass
