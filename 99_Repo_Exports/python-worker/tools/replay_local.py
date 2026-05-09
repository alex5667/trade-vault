from __future__ import annotations

"""
CLI: replay JSONL (ctx/tick) through adapter, output a summary report.

Example:
  python -m tools.replay_local \
    --in /tmp/replay.jsonl --type ctx \
    --factory python_worker.handlers.replay_factory:create_adapter \
    --golden /tmp/golden.json
"""

import argparse
import importlib
import json
from typing import Any

from replay.replay_runner import replay_jsonl
from replay.report import build_report, normalize_signal_payload


def _load_factory(spec: str):
    mod_name, fn_name = spec.split(":")
    mod = importlib.import_module(mod_name)
    return getattr(mod, fn_name)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input JSONL")
    ap.add_argument("--type", default="ctx", choices=["ctx", "tick"], help="Record type to replay")
    ap.add_argument("--factory", required=True, help="factory spec: module:function")
    ap.add_argument("--max", type=int, default=0, help="max events (0=all)")
    ap.add_argument("--golden", default="", help="optional golden json to compare")
    ap.add_argument("--print_samples", type=int, default=0, help="print first N normalized signals")
    args = ap.parse_args()

    factory = _load_factory(args.factory)
    adapter: Any = factory()

    max_events = None if args.max <= 0 else int(args.max)
    outbox = replay_jsonl(adapter=adapter, path=args.inp, type_filter=args.type, max_events=max_events)
    signals = list(getattr(outbox, "items", []) or [])

    rep = build_report(signals)
    print("counts_by_kind:", json.dumps(rep.counts_by_kind, ensure_ascii=False))
    print("score_p50_by_kind:", json.dumps(rep.score_p50_by_kind, ensure_ascii=False))
    print("score_p95_by_kind:", json.dumps(rep.score_p95_by_kind, ensure_ascii=False))

    if args.print_samples and args.print_samples > 0:
        n = min(int(args.print_samples), len(signals))
        for i in range(n):
            print("sample", i, json.dumps(normalize_signal_payload(signals[i]), ensure_ascii=False))

    if args.golden:
        with open(args.golden, encoding="utf-8") as fh:
            g = json.load(fh)
        assert rep.counts_by_kind == g["counts_by_kind"]
        assert rep.score_p50_by_kind == g["score_p50_by_kind"]
        assert rep.score_p95_by_kind == g["score_p95_by_kind"]

        norm = [normalize_signal_payload(x) for x in signals]
        for s in g.get("samples", []):
            idx = int(s["index"])
            assert 0 <= idx < len(norm)
            assert norm[idx] == s["payload_norm"]

        print("GOLDEN OK")


if __name__ == "__main__":
    main()
