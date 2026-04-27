from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List


KEEP = ("v", "symbol", "regime", "ts_ms", "src", "n", "eff_quote_th", "min_quote_delta", "state_hash")


def load_ndjson(path: str) -> List[Dict[str, Any]]:
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            p = row.get("payload")
            if isinstance(p, str):
                p = json.loads(p)
            out.append(p)
    return out


def r4(x: Any) -> Any:
    try:
        return round(float(x), 6)
    except Exception:
        return x


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="outp", required=True)
    args = ap.parse_args()

    rows = load_ndjson(args.inp)
    out = []
    for p in rows:
        d = {}
        for k in KEEP:
            if k not in p:
                continue
            if k in ("eff_quote_th", "min_quote_delta"):
                d[k] = r4(p[k])
            else:
                d[k] = p[k]
        out.append(d)

    out.sort(key=lambda x: (int(x.get("ts_ms", 0)), x.get("symbol", ""), x.get("regime", "")))
    with open(args.outp, "w", encoding="utf-8") as f:
        for x in out:
            f.write(json.dumps(x, ensure_ascii=False, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
