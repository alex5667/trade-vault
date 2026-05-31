#!/usr/bin/env python3
"""Regenerate tests/fixtures/signals_of_inputs_golden.json from a live Redis.

Pulls the latest entry from `signals:of:inputs` on redis-worker-1 and saves
its payload as the test fixture. Run when:
  - Schema changes deliberately (v14_of → v15_of, etc.).
  - Coverage floors in test_signals_of_inputs_golden.py need bumping after
    a sustained coverage improvement.

Env overrides:
  REDIS_WORKER_HOST   (default: localhost)
  REDIS_WORKER_PORT   (default: 63791)
  OF_INPUTS_STREAM    (default: signals:of:inputs)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import redis  # type: ignore


def main() -> int:
    host = os.getenv("REDIS_WORKER_HOST", "localhost")
    port = int(os.getenv("REDIS_WORKER_PORT", "63791"))
    stream = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")

    r = redis.Redis(host=host, port=port, decode_responses=True, socket_connect_timeout=3)
    items = r.xrevrange(stream, count=1)
    if not items:
        print(f"ERROR: stream '{stream}' empty on {host}:{port}", file=sys.stderr)
        return 1

    xid, fields = items[0]
    raw = fields.get("payload") or fields.get("data") or next(iter(fields.values()), "")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"ERROR: payload not valid JSON: {e}", file=sys.stderr)
        return 1

    target = Path(__file__).parent / "fixtures" / "signals_of_inputs_golden.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w") as f:
        json.dump({"_stream_id": xid, "payload": data}, f, indent=2, sort_keys=True)

    n_ind = len(data.get("indicators", {}))
    print(f"saved {target} ({n_ind} indicator keys, stream id {xid})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
