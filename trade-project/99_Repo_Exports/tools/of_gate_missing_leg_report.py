"""Top missing confirmation legs from Strong OF Gate metrics.

Aggregates Redis Stream entries (default: metrics:of_gate) and reports which leg is
missing most often when ok=0 and have<need.

Prefers field 'miss_leg'. Fallback parses 'reason' for token 'miss:<leg>'.

Run:
  python -m tools.of_gate_missing_leg_report --hours 24 --top 20

Env:
  REDIS_URL (default: redis://localhost:6379/0)
  OF_GATE_METRICS_STREAM (default: metrics:of_gate)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections import Counter, defaultdict
from typing import Dict, Iterator, Tuple

try:
    import redis  # type: ignore
except Exception as e:  # pragma: no cover
    redis = None
    _REDIS_IMPORT_ERROR = e

MISS_RE = re.compile(r"(?:^|\\|)miss:([a-zA-Z0-9_\\-]+)")


def _decode(x) -> str:
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="replace")
    return str(x)


def _safe_int(x, default: int = 0) -> int:
    try:
        if x is None:
            return default
        s = str(x).strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default


def _entry_ts_ms(entry_id: str) -> int:
    try:
        return int(entry_id.split("-", 1)[0])
    except Exception:
        return 0


def _extract_miss_leg(fields: Dict[str, str]) -> str:
    miss_leg = (fields.get("miss_leg") or "").strip()
    if miss_leg:
        return miss_leg
    reason = (fields.get("reason") or "").strip()
    if reason:
        m = MISS_RE.search(reason)
        if m:
            return m.group(1)
    return ""


def _is_relevant(fields: Dict[str, str]) -> bool:
    ok = _safe_int(fields.get("ok"), 0)
    have = _safe_int(fields.get("have"), 0)
    need = _safe_int(fields.get("need"), 0)
    return ok == 0 and need > 0 and have < need


def iter_of_gate_stream(
    r: "redis.Redis",
    stream: str,
    min_ts_ms: int,
    max_entries: int,
    batch_size: int,
) -> Iterator[Tuple[str, int, Dict[str, str]]]:
    """Iterate stream from newest to oldest until min_ts_ms or max_entries."""
    last_max = "+"
    scanned = 0
    while True:
        entries = r.xrevrange(stream, max=last_max, min="-", count=batch_size)
        if not entries:
            break
        last_entry_id = None
        for entry_id_b, kv in entries:
            entry_id = _decode(entry_id_b)
            last_entry_id = entry_id
            fields = {_decode(k): _decode(v) for k, v in kv.items()}
            ts_ms = _safe_int(fields.get("ts_ms"), 0)
            if ts_ms <= 0:
                ts_ms = _entry_ts_ms(entry_id)
            if ts_ms < min_ts_ms:
                return
            yield entry_id, ts_ms, fields
            scanned += 1
            if max_entries and scanned >= max_entries:
                return
        if last_entry_id is None:
            break
        last_max = f"({last_entry_id}"  # exclusive


def main() -> int:
    if redis is None:  # pragma: no cover
        print(f"ERROR: python package 'redis' is required: {_REDIS_IMPORT_ERROR}", file=sys.stderr)
        return 2

    ap = argparse.ArgumentParser(description="Top missing OF-gate legs from metrics stream")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--stream", default=os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate"))
    ap.add_argument("--hours", type=float, default=24.0)
    ap.add_argument("--min-ts-ms", type=int, default=0)
    ap.add_argument("--max-entries", type=int, default=200000)
    ap.add_argument("--batch-size", type=int, default=2000)
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--by-symbol", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    now_ms = int(time.time() * 1000)
    min_ts_ms = args.min_ts_ms if args.min_ts_ms > 0 else int(now_ms - args.hours * 3600 * 1000)

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)

    overall = Counter()
    per_symbol = defaultdict(Counter)
    scanned = 0
    considered = 0
    missing = 0

    for _eid, _ts, fields in iter_of_gate_stream(
        r, args.stream, min_ts_ms=min_ts_ms, max_entries=args.max_entries, batch_size=args.batch_size
    ):
        scanned += 1
        if not _is_relevant(fields):
            continue
        considered += 1
        leg = _extract_miss_leg(fields)
        if not leg:
            missing += 1
            continue
        overall[leg] += 1
        if args.by_symbol:
            sym = (fields.get("symbol") or "").strip() or "UNKNOWN"
            per_symbol[sym][leg] += 1

    if args.json:
        import json

        out = {
            "stream": args.stream,
            "min_ts_ms": min_ts_ms,
            "scanned": scanned,
            "considered": considered,
            "missing_miss_leg": missing,
            "top": overall.most_common(args.top),
        }
        if args.by_symbol:
            out["by_symbol"] = {k: v.most_common(args.top) for k, v in per_symbol.items()}
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0

    print(f"stream={args.stream} window_start_ts_ms={min_ts_ms} scanned={scanned} considered={considered}")
    if considered == 0:
        print("No relevant entries found (ok=0 and have<need).")
        return 0

    total = sum(overall.values())
    if total == 0:
        print("No miss_leg info found. Ensure producer emits miss_leg or reason contains miss:<leg>.")
    else:
        print(f"\nTop missing legs (top={min(args.top, len(overall))}):")
        for leg, cnt in overall.most_common(args.top):
            pct = (cnt / total) * 100.0 if total else 0.0
            print(f"  {leg:24s} {cnt:8d}  ({pct:5.1f}%)")

    if missing:
        print(f"\nWARNING: could not determine miss_leg for {missing}/{considered} relevant entries")

    if args.by_symbol and per_symbol:
        print("\nPer-symbol top missing legs:")
        for sym in sorted(per_symbol.keys()):
            c = per_symbol[sym]
            if not c:
                continue
            sym_total = sum(c.values())
            print(f"\n[{sym}] total={sym_total}")
            for leg, cnt in c.most_common(min(10, args.top)):
                pct = (cnt / sym_total) * 100.0 if sym_total else 0.0
                print(f"  {leg:24s} {cnt:8d}  ({pct:5.1f}%)")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

