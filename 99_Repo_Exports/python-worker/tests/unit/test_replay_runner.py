from __future__ import annotations

import os
import tempfile

from replay.jsonl import JsonlWriter
from replay.outbox_capture import OutboxCapture
from replay.replay_runner import replay_jsonl


class FakeAdapter:
    def __init__(self) -> None:
        self.outbox = OutboxCapture()

    def process_ctx(self, ctx_payload: dict) -> None:
        # deterministic: emit one signal per ctx
        self.outbox.publish(
            {
                "kind": "breakout",
                "side": "up",
                "symbol": ctx_payload.get("symbol"),
                "ts": ctx_payload.get("ts"),
                "final_score": float(ctx_payload.get("z_delta", 0.0) or 0.0),
            }
        )


def test_replay_jsonl_ctx() -> None:
    fd, path = tempfile.mkstemp(suffix=".jsonl")
    os.close(fd)
    try:
        w = JsonlWriter(path)
        w.write({"type": "ctx", "payload": {"symbol": "BTCUSDT", "ts": 1, "z_delta": 3.0}})
        w.write({"type": "ctx", "payload": {"symbol": "BTCUSDT", "ts": 2, "z_delta": 4.0}})
        w.close()

        ad = FakeAdapter()
        outbox = replay_jsonl(adapter=ad, path=path, type_filter="ctx")
        assert len(outbox.items) == 2
        assert outbox.items[0]["final_score"] == 3.0
        assert outbox.items[1]["final_score"] == 4.0
    finally:
        try:
            os.remove(path)
        except Exception:
            pass
