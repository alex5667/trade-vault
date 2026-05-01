from __future__ import annotations
"""Export OFInputsV1 records from Redis Stream to NDJSON.

Why:
  Golden replay should run on deterministic inputs captured at decision time.
  The system writes OFInputsV1 as raw JSON into a Redis Stream field `payload`.

Defaults (per codebase/docker-compose):
  - stream name: env OF_INPUTS_STREAM or "signals:of:inputs"
  - field name : env OF_INPUTS_STREAM_FIELD or "payload"

Usage:
  cd python-worker
  python -m tools.export_of_inputs_ndjson --redis-url redis://localhost:6379/0 --out /tmp/of_inputs.ndjson
  python -m tools.export_of_inputs_ndjson --start-id 0-0 --max-records 500000 --batch 2000

Notes:
  - Writes *one JSON per line* (valid NDJSON).
  - Validates JSON by default (skips invalid payloads, counts errors).
  - Deterministic ordering: uses XRANGE in increasing ID order.
"""


import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Tuple


try:
    # redis-py
    from redis import Redis
except Exception as exc:  # pragma: no cover
    Redis = None  # type: ignore


DEFAULT_STREAM = "signals:of:inputs"
FALLBACK_STREAM = "stream:of:inputs"
DEFAULT_FIELD = "payload"


def _env_stream_name() -> str:
    s = (os.getenv("OF_INPUTS_STREAM") or "").strip()
    if s:
        return s
    # historical fallback
    return DEFAULT_STREAM


def _env_field_name() -> str:
    f = (os.getenv("OF_INPUTS_STREAM_FIELD") or "").strip()
    return f or DEFAULT_FIELD


def _json_is_valid(s: str) -> bool:
    try:
        json.loads(s)
        return True
    except Exception:
        return False


def _next_min_id_exclusive(last_id: str) -> str:
    """Return exclusive XRANGE min bound: "(<id>"."""
    last_id = str(last_id or "").strip()
    if not last_id:
        return "-"
    return f"({last_id}"


def iter_stream_payloads(
    *,
    r: "Redis",
    stream: str,
    field: str,
    start_id: str,
    end_id: str,
    batch: int,
    max_records: int,
    validate_json: bool,
    stderr: Optional[object] = None,
) -> Iterator[Tuple[str, str]]:
    """Yield (redis_id, payload_str) from XRANGE in order.

    This function is isolated for unit testing (can be fed a stub Redis).
    """
    if stderr is None:
        stderr = sys.stderr

    emitted = 0
    cur_min = start_id
    while True:
        if max_records > 0 and emitted >= max_records:
            return

        count = batch
        if max_records > 0:
            count = min(count, max_records - emitted)

        # redis-py XRANGE: returns List[Tuple[id, Dict[field, value]]]
        items: List[Tuple[str, Dict[str, str]]] = r.xrange(stream, min=cur_min, max=end_id, count=count)
        if not items:
            return

        for rid, kv in items:
            if max_records > 0 and emitted >= max_records:
                return
            payload = kv.get(field)
            if payload is None:
                print(f"WARN: missing field '{field}' in stream entry id={rid}", file=stderr)
                continue
            if validate_json and not _json_is_valid(payload):
                print(f"WARN: invalid JSON in stream entry id={rid} (skipped)", file=stderr)
                continue
            emitted += 1
            yield (rid, payload)

        # advance cursor to avoid repeating last element
        cur_min = _next_min_id_exclusive(items[-1][0])


@dataclass
class ExportStats:
    stream: str
    field: str
    start_id: str
    end_id: str
    batch: int
    max_records: int
    written: int


def export_to_ndjson(
    *,
    redis_url: str,
    stream: str,
    field: str,
    out_path: str,
    start_id: str = "-",
    end_id: str = "+",
    batch: int = 2000,
    max_records: int = 0,
    validate_json: bool = True,
) -> ExportStats:
    if Redis is None:
        raise RuntimeError("redis-py is not available; install 'redis' package")

    stream = stream or DEFAULT_STREAM
    field = field or DEFAULT_FIELD
    if not out_path:
        raise ValueError("out_path is required")

    r = Redis.from_url(redis_url, decode_responses=True)

    written = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for _rid, payload in iter_stream_payloads(
            r=r,
            stream=stream,
            field=field,
            start_id=start_id,
            end_id=end_id,
            batch=batch,
            max_records=max_records,
            validate_json=validate_json,
        ):
            # Ensure NDJSON: one object per line
            f.write(payload)
            f.write("\n")
            written += 1

    return ExportStats(
        stream=str(stream),
        field=str(field),
        start_id=str(start_id),
        end_id=str(end_id),
        batch=int(batch),
        max_records=int(max_records),
        written=int(written),
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Export OFInputsV1 from Redis Stream to NDJSON")
    p.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    p.add_argument("--stream", default=_env_stream_name(), help="Redis stream name")
    p.add_argument("--field", default=_env_field_name(), help="Field name containing raw JSON")
    p.add_argument("--out", required=True, help="Output NDJSON file path")
    p.add_argument("--start-id", default="-", help="XRANGE min id (inclusive). Use '-' for beginning")
    p.add_argument("--end-id", default="+", help="XRANGE max id (inclusive). Use '+' for end")
    p.add_argument("--batch", type=int, default=2000, help="XRANGE count per batch")
    p.add_argument("--max-records", type=int, default=0, help="Stop after N written records (0=all)")
    p.add_argument("--no-validate", action="store_true", help="Do not validate JSON payload")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    stream = args.stream
    # convenience fallback if someone set legacy stream but it's empty
    # (we do NOT auto-switch; we only hint)
    try:
        stats = export_to_ndjson(
            redis_url=args.redis_url,
            stream=stream,
            field=args.field,
            out_path=args.out,
            start_id=args.start_id,
            end_id=args.end_id,
            batch=args.batch,
            max_records=args.max_records,
            validate_json=not args.no_validate,
        )
    except Exception as exc:
        print(f"ERROR: export failed: {exc}", file=sys.stderr)
        print(
            f"HINT: stream defaults to {DEFAULT_STREAM} (env OF_INPUTS_STREAM). "
            f"Legacy fallback is {FALLBACK_STREAM}.",
            file=sys.stderr,
        )
        return 2

    print(
        f"OK: wrote {stats.written} records to {args.out} "
        f"from stream={stats.stream} field={stats.field} start={stats.start_id} end={stats.end_id}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

