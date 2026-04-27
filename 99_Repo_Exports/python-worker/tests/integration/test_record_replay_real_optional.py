from __future__ import annotations

"""
REAL record&replay integration test (optional/manual).

How to run locally:
  export REPLAY_FACTORY="python_worker.handlers.replay_factory:create_adapter"
  export REPLAY_INPUT="/tmp/replay_ctx.jsonl"   # or ticks jsonl
  export REPLAY_TYPE="ctx"                      # ctx or tick
  export REPLAY_GOLDEN="/tmp/golden.json"       # optional

  pytest -q -k record_replay_real_optional

Notes:
  - This test is skipped in CI by default (needs your factory + a recording).
  - Use REPLAY_STABLE_SIGNAL_ID=1 for strict golden comparisons.
"""

import importlib
import json
import os
import pytest

from replay.replay_runner import replay_jsonl
from replay.report import build_report, normalize_signal_payload


def _load_factory(spec: str):
    mod_name, fn_name = spec.split(":")
    mod = importlib.import_module(mod_name)
    return getattr(mod, fn_name)


@pytest.mark.skipif(not os.getenv("REPLAY_FACTORY"), reason="REPLAY_FACTORY not set")
@pytest.mark.skipif(not os.getenv("REPLAY_INPUT"), reason="REPLAY_INPUT not set")
def test_record_replay_real_optional() -> None:
    factory = _load_factory(os.environ["REPLAY_FACTORY"])
    adapter = factory()

    inp = os.environ["REPLAY_INPUT"]
    typ = os.getenv("REPLAY_TYPE", "ctx").strip().lower()
    assert typ in {"ctx", "tick"}

    outbox = replay_jsonl(adapter=adapter, path=inp, type_filter=typ, max_events=None)
    rep = build_report(outbox.items)

    # If golden provided, compare.
    gpath = os.getenv("REPLAY_GOLDEN", "").strip()
    if gpath:
        with open(gpath, "r", encoding="utf-8") as fh:
            g = json.load(fh)
        assert rep.counts_by_kind == g["counts_by_kind"]
        assert rep.score_p50_by_kind == g["score_p50_by_kind"]
        assert rep.score_p95_by_kind == g["score_p95_by_kind"]

        # control samples (by index) — normalized payload
        norm = [normalize_signal_payload(x) for x in outbox.items]
        for s in g.get("samples", []):
            idx = int(s["index"])
            assert 0 <= idx < len(norm)
            assert norm[idx] == s["payload_norm"]
