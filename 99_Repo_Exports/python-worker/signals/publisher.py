from __future__ import annotations

import os
from typing import Any, Optional

import math


class SignalPublisher:
    """
    Строит payload и публикует сигнал ниже по потоку.

    ВАЖНО (единый источник правды):
      - Этот класс НЕ ДОЛЖЕН вычислять или записывать payload["confidence"].
      - confidence_pct — это калиброванная/нормализованная UI метрика, которая должна создаваться
        ТОЛЬКО в UnifiedSignalPipeline (или одном центральном слое пайплайна).

    Обоснование:
      - Предотвращает расхождения между unified/legacy путями.
      - Устраняет баги "двойной нормализации" (confidence/100 дважды и т.д.).
      - Делает аудиты согласованными: payload всегда содержит final_score; confidence
        всегда вычисляется в одном месте.
    """

    def __init__(self, emitter, logger):
        self._emitter = emitter
        self._logger = logger
        # fail-open: если кто-то забудет проставить confidence выше (не unified path),
        # мы НЕ считаем его здесь, а только помечаем.
        self._warn_missing_conf = os.getenv("PUBLISHER_WARN_MISSING_CONFIDENCE", "1").lower() not in {"0","false","no"}

    @staticmethod
    def _is_finite(x: Any) -> bool:
        try:
            v = float(x)
        except Exception:
            return False
        return math.isfinite(v)

    def build_payload(
        self,
        *,
        kind: str,
        side: int,
        symbol: str,
        ts: int,
        price: float,
        raw_score: float,
        final_score: float,
        # ПРИМЕЧАНИЕ: сохранено для обратной совместимости со старыми вызывающими кодами.
        # НЕ ДОЛЖНО использоваться для записи payload["confidence"].
        # Новый контракт: publisher полностью игнорирует это поле.
        confidence: Optional[float] = None,
        signal_id: str,
        level_price: Optional[float] = None,
        level_key: Optional[str] = None,
        reasons: Optional[list[str]] = None,
        parts: Optional[dict[str, Any]] = None,
        **extra: Any,
    ) -> dict[str, Any]:
        # Payload ДОЛЖЕН быть стабильным и минимальным.
        # confidence намеренно исключен здесь.
        payload: dict[str, Any] = {
            "kind": kind,
            "side": side,
            "symbol": symbol,
            "ts": ts,
            "price": price,
            "raw_score": raw_score,
            "final_score": final_score,
            "signal_id": signal_id,
        }

        # Optional fields
        if level_price is not None:
            payload["level_price"] = level_price
        if level_key is not None:
            payload["level_key"] = level_key
        if reasons:
            payload["reasons"] = list(reasons)
        if parts:
            payload["parts"] = dict(parts)

        # Сохраняем extra поля, но избегаем случайного внедрения "confidence".
        # Если какой-то вызывающий код все еще передает confidence через **extra, мы удаляем его здесь.
        if "confidence" in extra:
            extra.pop("confidence", None)

        # Защита: убеждаемся, что float-подобные поля конечны; fail-open отбрасывая плохие значения.
        # (НЕ вызывать ошибку здесь; publisher не является слоем строгой валидации.)
        for k in ("price", "raw_score", "final_score", "level_price"):
            if k in payload and payload[k] is not None and not self._is_finite(payload[k]):
                payload.pop(k, None)

        payload.update(extra)
        # NOTE: намеренно НЕ кладём "confidence" сюда.
        return payload

    def publish(self, payload: dict[str, Any]) -> None:
        """
        Delegate publishing to self._emitter (SignalOutboxPublisher or compatible).

        Publisher не должен вычислять confidence. Только сигнализируем, если его нет.
        """
        if "confidence" not in payload and self._warn_missing_conf:
            try:
                payload.setdefault("labels", {})
                if isinstance(payload["labels"], dict):
                    payload["labels"]["missing_confidence_fail_open"] = 1
            except Exception:
                pass

        if self._emitter is None:
            self._logger.warning("SignalPublisher._emitter is None — signal dropped for %s", payload.get("symbol"))
            return

        # Delegate to emitter. If emitter is a SignalOutboxPublisher, it expects
        # named args (source/strategy/symbol/side/kind/level_key/ts_ms/envelope).
        # If emitter has a generic publish(signal: dict) method, use that.
        if hasattr(self._emitter, "publish") and callable(self._emitter.publish):
            try:
                import inspect
                sig = inspect.signature(self._emitter.publish)
                params = list(sig.parameters.keys())
                # If emitter.publish expects keyword-only args (SignalOutboxPublisher pattern)
                if "envelope" in params:
                    self._emitter.publish(
                        source=str(payload.get("source", "unified")),
                        strategy=str(payload.get("strategy", "unified")),
                        symbol=str(payload.get("symbol", "unknown")),
                        side=str(payload.get("side", "UNKNOWN")).upper(),
                        kind=str(payload.get("kind", "ENTRY")),
                        level_key=str(payload.get("level_key", "")),
                        ts_ms=int(payload.get("ts", 0) or payload.get("ts_ms", 0) or 0),
                        envelope=payload,
                    )
                else:
                    # Generic dict-based publish
                    self._emitter.publish(payload)
            except Exception as e:
                self._logger.error("SignalPublisher._emitter.publish() failed: %s", e, exc_info=True)
        else:
            self._logger.warning("SignalPublisher._emitter has no publish() method — signal dropped")

