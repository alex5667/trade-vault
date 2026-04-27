"""OF Inputs DLQ/Quarantine exporter (P96).

Purpose:
- Expose basic Redis Streams health for OFInputs DLQ + quarantine streams:
  len, age (sec), last_id_ts_ms.

Notes:
- Keep label cardinality low (only `stream`).
- Tolerates missing streams.

Env:
- REDIS_URL (required)
- OF_INPUTS_DLQ_EXPORTER_PORT (default: 9158)
- OF_INPUTS_DLQ_EXPORTER_REFRESH_SEC (default: 10)
- OF_INPUTS_DLQ_STREAMS (default: "stream:dlq:of_inputs,quarantine:signals:of:inputs")

Run:
  python -m orderflow_services.of_inputs_dlq_exporter_v1
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import os
import time
from typing import Dict, List, Tuple

import redis.asyncio as aioredis
from prometheus_client import Gauge, start_http_server


G_UP = Gauge('of_inputs_dlq_exporter_up', '1 if exporter loop is running')
G_POLL_TS_MS = Gauge('of_inputs_dlq_exporter_poll_ts_ms', 'Last poll timestamp (ms)')
G_LEN = Gauge('of_inputs_dlq_len', 'Redis stream length', ['stream'])
G_LAST_ID_TS_MS = Gauge('of_inputs_dlq_last_id_ts_ms', 'Last entry timestamp derived from stream id', ['stream'])
G_AGE_SEC = Gauge('of_inputs_dlq_age_sec', 'Age in seconds: now - last_id_ts_ms/1000', ['stream'])


def _parse_stream_id_ts_ms(stream_id: str) -> int:
    # Redis stream id is "<ms>-<seq>"
    try:
        return int(stream_id.split('-', 1)[0])
    except Exception:
        return 0


async def _xinfo_len_last_id(r: aioredis.Redis, stream: str) -> Tuple[int, str]:
    try:
        info = await r.xinfo_stream(stream)
        length = int(info.get('length', 0) or 0)
        last = info.get('last-generated-id') or '0-0'
        return length, str(last)
    except Exception:
        # stream might not exist yet
        return 0, '0-0'


async def main() -> None:
    redis_url = os.environ.get('REDIS_URL')
    if not redis_url:
        raise SystemExit('REDIS_URL is required')

    port = int(os.environ.get('OF_INPUTS_DLQ_EXPORTER_PORT', '9158'))
    refresh = float(os.environ.get('OF_INPUTS_DLQ_EXPORTER_REFRESH_SEC', '10'))
    streams_raw = os.environ.get('OF_INPUTS_DLQ_STREAMS', 'stream:dlq:of_inputs,quarantine:signals:of:inputs')
    streams: List[str] = [s.strip() for s in streams_raw.split(',') if s.strip()]

    start_http_server(port)

    r = aioredis.from_url(redis_url, decode_responses=True)

    while True:
        G_UP.set(1)
        now_ms = get_ny_time_millis()
        G_POLL_TS_MS.set(now_ms)

        for stream in streams:
            length, last_id = await _xinfo_len_last_id(r, stream)
            last_ts_ms = _parse_stream_id_ts_ms(last_id)
            age_sec = 0.0
            if last_ts_ms > 0:
                age_sec = max(0.0, (now_ms - last_ts_ms) / 1000.0)

            G_LEN.labels(stream=stream).set(length)
            G_LAST_ID_TS_MS.labels(stream=stream).set(last_ts_ms)
            G_AGE_SEC.labels(stream=stream).set(age_sec)

        await asyncio.sleep(refresh)


if __name__ == '__main__':
    asyncio.run(main())
