#!/usr/bin/env python3
from __future__ import annotations
"""of_gate_dlq_exporter_v1.py

Prometheus exporter for Redis DLQ streams related to OF-gate metrics.

Purpose
- Make DLQ growth/non-zero visible (alerts + dashboards), separate from normal ok_rate/archiver signals.

Reads Redis Streams:
- stream:dlq:of_gate_metrics
- stream:dlq:of_gate_quarantine

Exports (labels: stream)
- of_gate_dlq_len
- of_gate_dlq_first_id_ms           (oldest entry id prefix)
- of_gate_dlq_last_id_ms            (newest entry id prefix)
- of_gate_dlq_oldest_age_sec        (now - first_id_ms)
- of_gate_dlq_age_sec               (now - last_id_ms)  [kept for backward compatibility]

Top-K diagnostics (labels: stream, err_prefix)
- of_gate_dlq_err_prefix_total      (count within the sampled window)

Also exports:
- of_gate_dlq_exporter_up
- of_gate_dlq_exporter_poll_ts_ms
- of_gate_dlq_exporter_errors_total

ENV
  REDIS_URL (required)
  OF_GATE_DLQ_EXPORTER_PORT (default 9154)
  OF_GATE_DLQ_EXPORTER_REFRESH_SEC (default 10)
  OF_GATE_DLQ_STREAMS (default "stream:dlq:of_gate_metrics,stream:dlq:of_gate_quarantine")

  # top err_prefix sampling
  OF_GATE_DLQ_EXPORTER_SAMPLE_LIMIT (default 2000)
  OF_GATE_DLQ_EXPORTER_TOPK (default 10)
  OF_GATE_DLQ_EXPORTER_ERR_PREFIX_MAXLEN (default 64)

Notes
- Missing stream is treated as len=0.
- first_id_ms/last_id_ms=0 for empty/missing stream.
- DLQ oldest age is more useful than newest age for backlog health.
"""

from utils.time_utils import get_ny_time_millis

import os
import signal
import time
from collections import Counter
from typing import Dict, List, Set, Tuple

from prometheus_client import Counter as PromCounter, Gauge, start_http_server  # type: ignore


def _now_ms() -> int:
    return get_ny_time_millis()


def _parse_streams(env_val: str) -> List[str]:
    items = [x.strip() for x in (env_val or "").split(",")]
    return [x for x in items if x]


def _id_to_ms(stream_id: str) -> int:
    # Redis Stream ID: "<ms>-<seq>"
    try:
        ms_s = (stream_id or "").split("-", 1)[0]
        return int(ms_s)
    except Exception:
        return 0


def _err_prefix(err: str, maxlen: int) -> str:
    s = (err or "").strip()
    if not s:
        return "(empty)"
    # prefer a stable prefix: "parse_error" / "pg_batch_error" / etc
    p = s.split(":", 1)[0].strip()
    if not p:
        p = s.split(" ", 1)[0].strip()
    if not p:
        return "(empty)"
    return p[:maxlen]


# Gauges
of_gate_dlq_exporter_up = Gauge("of_gate_dlq_exporter_up", "DLQ exporter loop running (1/0)")
of_gate_dlq_exporter_poll_ts_ms = Gauge(
    "of_gate_dlq_exporter_poll_ts_ms", "last successful poll time (epoch ms)"
)

of_gate_dlq_len = Gauge("of_gate_dlq_len", "DLQ stream length", ["stream"])
of_gate_dlq_first_id_ms = Gauge(
    "of_gate_dlq_first_id_ms", "DLQ stream oldest entry id prefix (epoch ms)", ["stream"]
)
of_gate_dlq_last_id_ms = Gauge(
    "of_gate_dlq_last_id_ms", "DLQ stream newest entry id prefix (epoch ms)", ["stream"]
)

# Backward-compatibility: this is newest age, not backlog age
of_gate_dlq_age_sec = Gauge(
    "of_gate_dlq_age_sec", "age of the newest DLQ entry in seconds", ["stream"]
)

of_gate_dlq_oldest_age_sec = Gauge(
    "of_gate_dlq_oldest_age_sec", "age of the oldest DLQ entry in seconds", ["stream"]
)

# Top err_prefix from sampled window
of_gate_dlq_err_prefix_total = Gauge(
    "of_gate_dlq_err_prefix_total", "DLQ err_prefix counts in sampled window", ["stream", "err_prefix"]
)

# Errors
of_gate_dlq_exporter_errors_total = PromCounter(
    "of_gate_dlq_exporter_errors_total", "DLQ exporter errors"
)


class Exporter:
    def __init__(self) -> None:
        self.running = True
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

        self.redis_url = os.getenv("REDIS_URL", "").strip()
        if not self.redis_url:
            raise RuntimeError("REDIS_URL is required")

        self.streams = _parse_streams(
            os.getenv(
                "OF_GATE_DLQ_STREAMS",
                "stream:dlq:of_gate_metrics,stream:dlq:of_gate_quarantine",
            )
        )
        if not self.streams:
            self.streams = ["stream:dlq:of_gate_metrics", "stream:dlq:of_gate_quarantine"]

        self.refresh_sec = int(os.getenv("OF_GATE_DLQ_EXPORTER_REFRESH_SEC", "10"))

        self.sample_limit = int(os.getenv("OF_GATE_DLQ_EXPORTER_SAMPLE_LIMIT", "2000"))
        self.topk = int(os.getenv("OF_GATE_DLQ_EXPORTER_TOPK", "10"))
        self.err_prefix_maxlen = int(os.getenv("OF_GATE_DLQ_EXPORTER_ERR_PREFIX_MAXLEN", "64"))

        # Track last label-sets to remove stale topK series
        self._last_err_labels: Set[Tuple[str, str]] = set()

        # Lazy import to avoid hard dependency in unit environments
        import redis  # type: ignore

        self.redis = redis.Redis.from_url(
            self.redis_url,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )

    def _stop(self, *_args) -> None:
        self.running = False

    def _poll_one(self, key: str) -> Tuple[int, int, int, Counter]:
        """Returns (len, first_id_ms, last_id_ms, err_prefix_counter)."""
        try:
            n = int(self.redis.xlen(key) or 0)
        except Exception:
            n = 0

        first_ms = 0
        last_ms = 0
        err_counter: Counter = Counter()

        if n > 0:
            try:
                # Oldest
                rows = self.redis.xrange(key, min="-", max="+", count=1)
                if rows:
                    sid = rows[0][0]
                    first_ms = _id_to_ms(str(sid))
            except Exception:
                first_ms = 0
            try:
                # Newest
                rows = self.redis.xrevrange(key, max="+", min="-", count=1)
                if rows:
                    sid = rows[0][0]
                    last_ms = _id_to_ms(str(sid))
            except Exception:
                last_ms = 0

            # Sample newest N entries to compute top err_prefix
            try:
                lim = max(0, min(self.sample_limit, n))
                if lim > 0 and self.topk > 0:
                    rows = self.redis.xrevrange(key, max="+", min="-", count=lim)
                    for _sid, fields in rows:
                        if not isinstance(fields, dict):
                            continue
                        err = str(fields.get("err") or fields.get("error") or "")
                        p = _err_prefix(err, self.err_prefix_maxlen)
                        err_counter[p] += 1
            except Exception:
                # ignore sampling errors
                pass

        return n, first_ms, last_ms, err_counter

    def _set_top_err_prefix(self, stream: str, err_counter: Counter) -> None:
        new_labels: Set[Tuple[str, str]] = set()
        # emit only topK
        for prefix, cnt in err_counter.most_common(max(0, self.topk)):
            lbl = (stream, str(prefix))
            new_labels.add(lbl)
            of_gate_dlq_err_prefix_total.labels(stream=stream, err_prefix=str(prefix)).set(float(cnt))

        # remove stale labels from previous iterations
        for old in list(self._last_err_labels):
            if old[0] == stream and old not in new_labels:
                try:
                    of_gate_dlq_err_prefix_total.remove(old[0], old[1])
                except Exception:
                    pass

        # update global label-set
        self._last_err_labels = {x for x in self._last_err_labels if x[0] != stream} | new_labels

    def loop(self) -> None:
        while self.running:
            of_gate_dlq_exporter_up.set(1.0)
            now_ms = _now_ms()
            try:
                for s in self.streams:
                    n, first_ms, last_ms, err_counter = self._poll_one(s)
                    of_gate_dlq_len.labels(stream=s).set(float(n))
                    of_gate_dlq_first_id_ms.labels(stream=s).set(float(first_ms))
                    of_gate_dlq_last_id_ms.labels(stream=s).set(float(last_ms))

                    if last_ms > 0:
                        of_gate_dlq_age_sec.labels(stream=s).set(float(max(0, now_ms - last_ms) / 1000.0))
                    else:
                        of_gate_dlq_age_sec.labels(stream=s).set(float("nan"))

                    if first_ms > 0:
                        of_gate_dlq_oldest_age_sec.labels(stream=s).set(float(max(0, now_ms - first_ms) / 1000.0))
                    else:
                        of_gate_dlq_oldest_age_sec.labels(stream=s).set(float("nan"))

                    self._set_top_err_prefix(s, err_counter)

                of_gate_dlq_exporter_poll_ts_ms.set(float(now_ms))

            except Exception:
                of_gate_dlq_exporter_errors_total.inc()
                of_gate_dlq_exporter_up.set(0.0)

            time.sleep(max(1, self.refresh_sec))


def main() -> int:
    port = int(os.getenv("OF_GATE_DLQ_EXPORTER_PORT", "9154"))
    start_http_server(port)
    exp = Exporter()
    exp.loop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
