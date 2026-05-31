from __future__ import annotations

import argparse
import json
from typing import Any


def load_ndjson(path: str) -> list[dict[str, Any]]:
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="outp", required=True)
    args = ap.parse_args()

    events = load_ndjson(args.inp)
    events.sort(key=lambda e: int(e.get("ts_ms", 0)))

    # This script is intentionally minimal: it expects you to embed of_confirm into raw signals
    # during replay of your existing pipeline.
    #
    # If you want a pure-engine replay, we’ll wire runtime proxies next.
    out = []
    for e in events:
        if e.get("type") == "of_confirm":
            out.append(e)

    with open(args.outp, "w", encoding="utf-8") as f:
        for e in out:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
