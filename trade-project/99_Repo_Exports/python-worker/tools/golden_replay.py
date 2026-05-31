from __future__ import annotations

import argparse
import importlib
from collections.abc import Callable
from typing import Any

from core.replay_io import iter_ndjson, topdiff, write_ndjson


def import_callable(path: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Import callable by 'module:function'."""
    if ":" not in path:
        raise ValueError("--runner must be module:function")
    mod_name, fn_name = path.split(":", 1)
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, fn_name)
    if not callable(fn):
        raise TypeError(f"runner {path} is not callable")
    return fn  # type: ignore


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="NDJSON inputs captured from online pipeline")
    ap.add_argument("--runner", required=True, help="module:function that maps input dict -> output dict")
    ap.add_argument("--write-baseline", default="", help="Write outputs NDJSON to this path")
    ap.add_argument("--compare-baseline", default="", help="Compare against baseline outputs NDJSON")
    ap.add_argument("--keys", default="decision,score,ml_prob", help="Comma keys to compare")
    ap.add_argument("--limit", type=int, default=0, help="Limit rows for quick runs (0=all)")
    args = ap.parse_args()

    runner = import_callable(args.runner)

    outs: list[dict[str, Any]] = []
    for i, row in enumerate(iter_ndjson(args.inputs)):
        if args.limit and i >= args.limit:
            break
        out = runner(row) or {}
        out["_i"] = i
        outs.append(out)

    if args.write_baseline:
        write_ndjson(args.write_baseline, outs)

    if args.compare_baseline:
        base_rows = list(iter_ndjson(args.compare_baseline))
        keys = [k.strip() for k in args.keys.split(",") if k.strip()]

        # Backwards compatibility for older baselines
        for r in base_rows:
            if "ok" in r and "decision" not in r:
                r["decision"] = r["ok"]
            if "ml_prob" not in r:
                r["ml_prob"] = 0.0

        n_changed, diffs = topdiff(base_rows, outs, keys=keys, top_k=20)
        print(f"rows={min(len(base_rows), len(outs))} changed={n_changed}")
        for d in diffs:
            print(f"i={d.idx} key={d.key} base={d.a} cur={d.b}")
        if n_changed > 0:
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

