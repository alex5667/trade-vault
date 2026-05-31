#!/usr/bin/env python3
"""ml_confirm p_edge vs actual WR poller (item 2 of 2026-05-23 stop-bleed).

Reads `trades:closed` stream, joins with the predicted `p_edge` carried in
the trade payload (or fetched via decision snapshot), buckets the trade by
its predicted p_edge bucket (10 bins: 0.0-0.1, 0.1-0.2, ..., 0.9-1.0), and
publishes a rolling-window WR gauge per `(kind, bucket)` pair.

Wired metrics (defined in services/observability/metrics_registry.py):
    ml_confirm_p_edge_vs_actual_wr{kind, bucket}
    ml_confirm_p_edge_vs_actual_n{kind, bucket}

Alert rule lives in `prometheus/alerts_stop_bleed_2026_05_23.yml`
(MLConfirmPEdgeMiscalibrated). Fires when actual_WR < 0.5 × midpoint(bucket)
with n>=20 over a 30m window.

ENV
---
PEDGE_WR_POLLER_PORT          Prometheus port (default 9863)
PEDGE_WR_POLLER_WINDOW_MIN    Rolling window size in minutes (default 30)
PEDGE_WR_POLLER_INTERVAL_S    Refresh interval (default 30)
PEDGE_WR_POLLER_STREAM        Source stream (default trades:closed)
PEDGE_WR_POLLER_GROUP         Consumer group (default pedge-wr-poller)
PEDGE_WR_POLLER_CONSUMER      Consumer name (default pedge-wr-poller-1)
PEDGE_WR_POLLER_BATCH         XREADGROUP COUNT (default 100)

Status: SKELETON. Reads trades:closed but the p_edge join + bucket
publishing path needs verification against the actual outbox envelope
shape in production. Do NOT deploy without:
  1. Confirming `trades:closed` carries `p_edge` (or join via decision:{sid})
  2. Picking the right `kind` label (currently uses payload.kind)
  3. Adding a docker-compose entry under python-workers
"""
from __future__ import annotations

import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("pedge_wr_poller")


_BUCKETS: tuple[tuple[float, float, str], ...] = (
    (0.0, 0.1, "0.0-0.1"),
    (0.1, 0.2, "0.1-0.2"),
    (0.2, 0.3, "0.2-0.3"),
    (0.3, 0.4, "0.3-0.4"),
    (0.4, 0.5, "0.4-0.5"),
    (0.5, 0.6, "0.5-0.6"),
    (0.6, 0.7, "0.6-0.7"),
    (0.7, 0.8, "0.7-0.8"),
    (0.8, 0.9, "0.8-0.9"),
    (0.9, 1.01, "0.9-1.0"),
)


def _bucket_of(p: float) -> str:
    for lo, hi, label in _BUCKETS:
        if lo <= p < hi:
            return label
    return "na"


@dataclass
class _Window:
    """Rolling window of (ts_ms, win) tuples per (kind, bucket)."""
    items: deque = field(default_factory=deque)

    def add(self, ts_ms: int, win: bool) -> None:
        self.items.append((ts_ms, 1 if win else 0))

    def prune(self, cutoff_ms: int) -> None:
        while self.items and self.items[0][0] < cutoff_ms:
            self.items.popleft()

    def stats(self) -> tuple[int, float]:
        n = len(self.items)
        if n == 0:
            return 0, 0.0
        wins = sum(x for _, x in self.items)
        return n, wins / n


class PEdgeWRPoller:
    def __init__(self, redis_client: Any) -> None:
        self.r = redis_client
        self.stream = os.getenv("PEDGE_WR_POLLER_STREAM", "trades:closed")
        self.group = os.getenv("PEDGE_WR_POLLER_GROUP", "pedge-wr-poller")
        self.consumer = os.getenv("PEDGE_WR_POLLER_CONSUMER", "pedge-wr-poller-1")
        self.batch = int(os.getenv("PEDGE_WR_POLLER_BATCH", "100"))
        self.window_min = int(os.getenv("PEDGE_WR_POLLER_WINDOW_MIN", "30"))
        self.interval_s = float(os.getenv("PEDGE_WR_POLLER_INTERVAL_S", "30"))
        # state: (kind, bucket) → _Window
        self._windows: dict[tuple[str, str], _Window] = {}

    def _ensure_group(self) -> None:
        try:
            self.r.xgroup_create(self.stream, self.group, id="$", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                log.warning("xgroup_create failed: %s", e)

    def _extract(self, fields: dict) -> tuple[str, float, bool, int] | None:
        """Map a trades:closed message to (kind, p_edge, win, ts_ms).

        Schema varies — this is a best-effort extraction. Returns None when
        we can't compute a bucket (missing p_edge).
        """
        try:
            # Stream message fields are bytes; payload is typically a JSON blob
            # under "data" or "payload" or scattered as flat fields.
            blob = (
                fields.get(b"data")
                or fields.get(b"payload")
                or fields.get("data")
                or fields.get("payload")
            )
            if blob:
                if isinstance(blob, bytes):
                    blob = blob.decode("utf-8", "ignore")
                obj = json.loads(blob)
            else:
                obj = {k.decode() if isinstance(k, bytes) else k:
                       (v.decode() if isinstance(v, bytes) else v)
                       for k, v in fields.items()}
            p_edge = obj.get("p_edge") or obj.get("ml_p_edge")
            if p_edge is None:
                return None
            p_edge = float(p_edge)
            kind = str(obj.get("kind") or obj.get("signal_kind") or "of:unknown")
            pnl = obj.get("pnl_net") or obj.get("pnl_pct") or obj.get("r_multiple") or 0
            win = float(pnl) > 0
            ts_ms = int(obj.get("ts_ms") or obj.get("close_ts_ms") or time.time() * 1000)
            return kind, p_edge, win, ts_ms
        except Exception:
            return None

    def _publish(self) -> None:
        try:
            from services.observability.metrics_registry import (
                ml_confirm_p_edge_vs_actual_n,
                ml_confirm_p_edge_vs_actual_wr,
            )
        except Exception:
            return
        cutoff = int(time.time() * 1000) - self.window_min * 60 * 1000
        for (kind, bucket), win in self._windows.items():
            win.prune(cutoff)
            n, wr = win.stats()
            try:
                if ml_confirm_p_edge_vs_actual_n is not None:
                    ml_confirm_p_edge_vs_actual_n.labels(kind=kind, bucket=bucket).set(n)
                if ml_confirm_p_edge_vs_actual_wr is not None:
                    ml_confirm_p_edge_vs_actual_wr.labels(kind=kind, bucket=bucket).set(wr)
            except Exception:
                pass

    def run_forever(self) -> None:
        self._ensure_group()
        log.info("pedge_wr_poller started stream=%s group=%s window_min=%d",
                 self.stream, self.group, self.window_min)
        last_publish = 0.0
        while True:
            try:
                resp = self.r.xreadgroup(
                    self.group, self.consumer,
                    {self.stream: ">"},
                    count=self.batch, block=int(self.interval_s * 1000),
                )
                for _stream, msgs in (resp or []):
                    ack_ids = []
                    for msg_id, fields in msgs:
                        extracted = self._extract(fields)
                        if extracted is not None:
                            kind, p_edge, win, ts_ms = extracted
                            bucket = _bucket_of(p_edge)
                            key = (kind, bucket)
                            self._windows.setdefault(key, _Window()).add(ts_ms, win)
                        ack_ids.append(msg_id)
                    if ack_ids:
                        try:
                            self.r.xack(self.stream, self.group, *ack_ids)
                        except Exception:
                            pass
            except Exception as e:
                log.warning("xreadgroup loop error: %s", e)
                time.sleep(1.0)

            now = time.time()
            if (now - last_publish) >= self.interval_s:
                self._publish()
                last_publish = now


def main() -> None:
    import sys
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    try:
        from prometheus_client import start_http_server
        port = int(os.getenv("PEDGE_WR_POLLER_PORT", "9863"))
        start_http_server(port)
        log.info("prometheus on :%d", port)
    except Exception as e:
        log.warning("prometheus startup failed: %s", e)

    from core.redis_client import get_redis_client
    r = get_redis_client()
    poller = PEdgeWRPoller(r)
    poller.run_forever()


if __name__ == "__main__":
    main()
