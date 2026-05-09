from __future__ import annotations

"""
Интеграционные тесты 6.2: "record & replay" каркас.

ВАЖНО:
  - этот тест проверяет что реплей-фреймворк стабилен
  - что golden-репорт (counts + score percentiles) совпадает
  - что контрольные события совпадают после нормализации payload

Чтобы подключить РЕАЛЬНЫЙ handler:
  - добавьте python-worker/handlers/replay_factory.py с функцией create_adapter()
  - и сделайте отдельный integration test, который использует вашу фабрику и реальную запись из /tmp.
"""

import json
from pathlib import Path
from typing import Any

from replay.outbox_capture import OutboxCapture
from replay.replay_runner import replay_jsonl
from replay.report import build_report, normalize_signal_payload

FIX_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "replay"


class DemoReplaySystem:
    """
    Демонстрационная система (для CI), чтобы тесты 6.2 были "живыми" прямо сейчас.
    НЕ заменяет реальные integration tests на CryptoOrderFlowHandler.

    Логика:
      - вход: ctx payload (json dict)
      - если z_delta >= 3 => kind="breakout"
      - если obi >= 2 => kind="obi_spike"
      - final_score = raw_score * conf_factor
    """

    def __init__(self) -> None:
        self.outbox = OutboxCapture()

    def process_ctx(self, ctx_payload: dict[str, Any]) -> None:
        sym = (ctx_payload.get("symbol", "TEST"))
        side = (ctx_payload.get("side", "buy"))
        ts = int(ctx_payload.get("ts", 0) or 0)
        price = float(ctx_payload.get("price", 100.0) or 100.0)
        raw = float(ctx_payload.get("raw_score", 1.0) or 1.0)
        conf = float(ctx_payload.get("conf_factor", 0.5) or 0.5)
        z = float(ctx_payload.get("z_delta", 0.0) or 0.0)
        obi = float(ctx_payload.get("obi", 0.0) or 0.0)

        if z >= 3.0:
            kind = "breakout"
        elif obi >= 2.0:
            kind = "obi_spike"
        else:
            return

        final = raw * conf
        self.outbox.publish(
            {
                "kind": kind,
                "side": side,
                "symbol": sym,
                "ts": ts,
                "price": price,
                "raw_score": raw,
                "final_score": final,
                "confidence": min(100.0, max(0.0, abs(final) * 50.0)),
                "level_price": ctx_payload.get("level_price"),
                "reason_code": "OK",
            }
        )

    # ticks replay в demo не используем
    def process_tick(self, payload: dict[str, Any]) -> None:
        return


def _demo_adapter():
    return DemoReplaySystem()


def test_replay_ctx_matches_golden_report_and_samples() -> None:
    inp = FIX_DIR / "ctx_sample.jsonl"
    golden = FIX_DIR / "golden_ctx_sample.json"
    assert inp.exists(), f"missing fixture: {inp}"
    assert golden.exists(), f"missing golden: {golden}"

    adapter = _demo_adapter()
    outbox = replay_jsonl(adapter=adapter, path=str(inp), type_filter="ctx", max_events=None)
    report = build_report(outbox.items)

    g = json.loads(golden.read_text(encoding="utf-8"))
    assert report.counts_by_kind == g["counts_by_kind"]
    assert report.score_p50_by_kind == g["score_p50_by_kind"]
    assert report.score_p95_by_kind == g["score_p95_by_kind"]

    # golden samples: контрольные события по индексам
    samples = g.get("samples", [])
    norm = [normalize_signal_payload(x) for x in outbox.items]
    for s in samples:
        idx = int(s["index"])
        assert 0 <= idx < len(norm)
        assert norm[idx] == s["payload_norm"]
