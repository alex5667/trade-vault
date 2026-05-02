from __future__ import annotations
"""OF Inputs DLQ/Quarantine drilldown (P96).

Prints quick triage info (top dq_code / err_prefix, sample payloads).

Env:
- REDIS_URL (required)
- OF_INPUTS_DLQ_STREAM (default: stream:dlq:of_inputs)
- OF_INPUTS_QUARANTINE_STREAM (default: quarantine:signals:of:inputs)

Usage:
  python -m orderflow_services.of_inputs_dlq_drilldown_p96 --dlq 200 --quarantine 200 --samples 3,
""",
import argparse
import json
import os
from collections import Counter
from typing import Any, Dict, List, Tuple

import redis.asyncio as aioredis


def _json_loads(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s)
    except Exception:
        return {"_raw": s}


async def _read_tail(r: aioredis.Redis, stream: str, count: int) -> List[Tuple[str, Dict[str, str]]]:
    try:
        items = await r.xrevrange(stream, max='+', min='-', count=count)
        # items: List[(id, {field:value})]
        return [(str(iid), {str(k): str(v) for k, v in fields.items()}) for iid, fields in items]
    except Exception:
        return []


def _extract_ctx(fields: Dict[str, str]) -> Dict[str, Any]:
    payload = fields.get('payload') or ''
    ctx = _json_loads(payload)
    return ctx


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--dlq', type=int, default=200)
    ap.add_argument('--quarantine', type=int, default=200)
    ap.add_argument('--samples', type=int, default=3)
    args = ap.parse_args()

    redis_url = os.environ.get('REDIS_URL')
    if not redis_url:
        raise SystemExit('REDIS_URL is required')

    dlq_stream = os.environ.get('OF_INPUTS_DLQ_STREAM', 'stream:dlq:of_inputs')
    q_stream = os.environ.get('OF_INPUTS_QUARANTINE_STREAM', 'quarantine:signals:of:inputs')

    r = aioredis.from_url(redis_url, decode_responses=True)

    dlq_items = await _read_tail(r, dlq_stream, args.dlq)
    q_items = await _read_tail(r, q_stream, args.quarantine)

    print(f'DLQ stream: {dlq_stream}  (tail={len(dlq_items)})')
    dlq_prefix = Counter()
    dlq_dq = Counter()
    for _id, fields in dlq_items:
        ctx = _extract_ctx(fields)
        dlq_prefix[str(ctx.get('err_prefix', 'na'))] += 1
        dq = str(ctx.get('dq_code', '') or 'na')
        dlq_dq[dq] += 1

    print('Top err_prefix:', dlq_prefix.most_common(10))
    print('Top dq_code:', dlq_dq.most_common(10))

    if args.samples > 0 and dlq_items:
        print('\nDLQ samples:')
        for _id, fields in dlq_items[: args.samples]:
            ctx = _extract_ctx(fields)
            print(f'- id={_id} symbol={ctx.get("symbol")} dq={ctx.get("dq_code")} err_prefix={ctx.get("err_prefix")}')
            payload = ctx.get('payload')
            if isinstance(payload, str) and len(payload) > 500:
                payload = payload[:500] + '...'
            print('  payload:', payload)

    print(f'\nQuarantine stream: {q_stream}  (tail={len(q_items)})')
    q_dq = Counter()
    for _id, fields in q_items:
        ctx = _extract_ctx(fields)
        q_dq[str(ctx.get('dq_code', 'na'))] += 1

    print('Top dq_code:', q_dq.most_common(10))

    if args.samples > 0 and q_items:
        print('\nQuarantine samples:')
        for _id, fields in q_items[: args.samples]:
            ctx = _extract_ctx(fields)
            print(f'- id={_id} symbol={ctx.get("symbol")} dq={ctx.get("dq_code")} attempt={ctx.get("attempt_version")} published={ctx.get("published_version")}')
            print('  missing_fields:', ctx.get('missing_fields'))
            print('  book_age_ms:', ctx.get('book_age_ms'))


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
