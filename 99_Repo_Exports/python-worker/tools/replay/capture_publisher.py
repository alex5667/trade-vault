from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class CapturePublisher:
    """
    Publisher для record&replay:
    - ничего не шлёт наружу
    - складывает payload'ы в память (signals list)
    - умеет писать jsonl на диск (для golden files)

    Почему publisher, а не emitter:
    - актуальная UnifiedSignalPipeline требует dependency 'publisher'
      (SignalPublisher), поэтому replay должен подменять именно его.
    """
    logger: Any
    signals: list[dict[str, Any]] = field(default_factory=list)

    def publish(self, payload: dict[str, Any]) -> bool:
        # fail-open: не валим replay из-за плохого payload
        try:
            if not isinstance(payload, dict):
                payload = {"_bad_payload_type": str(type(payload))}
            self.signals.append(payload)
            return True
        except Exception as e:  # pragma: no cover
            try:
                self.logger.exception(f"CapturePublisher.publish failed: {e}")
            except Exception:
                pass
            return False

    # Некоторые реализации SignalPublisher могли использовать другое имя метода.
    def publish_signal(self, payload: dict[str, Any]) -> bool:  # pragma: no cover
        return self.publish(payload)

    def dump_jsonl(self, path: str) -> None:
        """
        Golden file: один сигнал = один JSON.
        """
        with open(path, "w", encoding="utf-8") as f:
            for s in self.signals:
                f.write(json.dumps(s, ensure_ascii=False, sort_keys=True) + "\n")
