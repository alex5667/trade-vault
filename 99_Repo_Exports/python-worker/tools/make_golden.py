from __future__ import annotations

"""
CLI: build golden JSON from recorded SIGNALS JSONL.

Input:
  JSONL with {"type":"signal","payload":{...}} lines.

Output golden:
  {
    counts_by_kind: {...},
    score_p50_by_kind: {...},
    score_p95_by_kind: {...},
    samples: [{index, payload_norm}, ...]
  }
"""

import argparse
import json
from typing import Any, Dict, List

from replay.jsonl import iter_jsonl
from replay.report import build_report, normalize_signal_payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input JSONL (type=signal expected)")
    ap.add_argument("--out", required=True, help="Output golden json")
    ap.add_argument("--samples", type=int, default=3, help="How many sample indexes to store")
    ap.add_argument("--sample_step", type=int, default=0, help="If >0, take samples each N signals")
    args = ap.parse_args()

    signals: List[Dict[str, Any]] = []
    for rec in iter_jsonl(args.inp):
        if str(rec.get("type", "")) != "signal":
            continue
        p = rec.get("payload", None)
        if isinstance(p, dict):
            signals.append(p)

    rep = build_report(signals)
    g: Dict[str, Any] = {
        "counts_by_kind": rep.counts_by_kind,
        "score_p50_by_kind": rep.score_p50_by_kind,
        "score_p95_by_kind": rep.score_p95_by_kind,
        "samples": [],
    }

    norm = [normalize_signal_payload(x) for x in signals]
    if args.sample_step and args.sample_step > 0:
        step = int(args.sample_step)
        for i in range(0, len(norm), step):
            g["samples"].append({"index": i, "payload_norm": norm[i]})
    else:
        for i in range(min(int(args.samples), len(norm))):
            g["samples"].append({"index": i, "payload_norm": norm[i]})

    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(g, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
