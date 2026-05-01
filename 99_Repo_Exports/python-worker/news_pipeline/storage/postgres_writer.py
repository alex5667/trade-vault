# -*- coding: utf-8 -*-
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

try:
    import psycopg2
    import psycopg2.extras
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore


def _now_ms() -> int:
    return get_ny_time_millis()


@dataclass
class PgConfig:
    dsn: str
    enabled: bool = True
    flush_every: float = 0.5      # seconds
    batch_size: int = 100         # rows
    max_queue: int = 10_000       # backpressure cap (drop if full)


class NewsPostgresWriterAsync:
    """,
    Асинхронный writer:
    - consumer поток кладёт записи в очередь (O(1), очень быстро)
    - отдельный поток батчит и пишет
    - fail-open: при проблемах просто "молчим", чтобы пайплайн не стоял
    """,

    def __init__(self, cfg: PgConfig) -> None:
        self.cfg = cfg
        self.q: "queue.Queue[Tuple[str, Dict[str, Any]]]" = queue.Queue(maxsize=cfg.max_queue)
        self._stop = threading.Event()
        self._thr = threading.Thread(target=self._run, name="news-pg-writer", daemon=True)
        self._thr.start()

    def enqueue_analysis(self, row: Dict[str, Any]) -> None:
        self._enqueue("analysis", row)

    def enqueue_features(self, row: Dict[str, Any]) -> None:
        self._enqueue("features", row)

    def _enqueue(self, kind: str, row: Dict[str, Any]) -> None:
        if not self.cfg.enabled or not self.cfg.dsn or psycopg2 is None:
            return
        try:
            self.q.put_nowait((kind, row))
        except queue.Full:
            # drop on overload (fail-open)
            return

    def close(self) -> None:
        self._stop.set()
        try:
            self._thr.join(timeout=2.0)
        except Exception:
            pass

    def _run(self) -> None:
        if psycopg2 is None:
            return

        conn: Optional[Any] = None
        cur: Optional[Any] = None
        batch: list[Tuple[str, Dict[str, Any]]] = []
        last_flush = time.time()

        def ensure_conn() -> bool:
            nonlocal conn, cur
            try:
                if conn is None or conn.closed:
                    conn = psycopg2.connect(self.cfg.dsn)
                    conn.autocommit = True
                    cur = conn.cursor()
                return True
            except Exception:
                conn = None
                cur = None
                return False

        while not self._stop.is_set():
            try:
                item = self.q.get(timeout=self.cfg.flush_every)
                batch.append(item)
            except queue.Empty:
                pass

            now = time.time()
            if len(batch) >= self.cfg.batch_size or (batch and (now - last_flush) >= self.cfg.flush_every):
                if ensure_conn() and cur is not None:
                    try:
                        self._flush(cur, batch)
                    except Exception:
                        # fail-open: сбрасываем батч и продолжаем
                        pass
                batch.clear()
                last_flush = now

        # финальный flush
        if batch and ensure_conn() and cur is not None:
            try:
                self._flush(cur, batch)
            except Exception:
                pass

        try:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()
        except Exception:
            pass

    def _flush(self, cur: Any, batch: list[Tuple[str, Dict[str, Any]]]) -> None:
        analysis_rows = [r for (k, r) in batch if k == "analysis"]
        feature_rows = [r for (k, r) in batch if k == "features"]

        if analysis_rows:
            psycopg2.extras.execute_values(
                cur,
                """,
                INSERT INTO news_analysis(uid, ts_ms, symbol, source, risk, surprise, tags_mask, primary_tag, payload_json)
                VALUES %s
                ON CONFLICT (uid) DO UPDATE SET
                  ts_ms=EXCLUDED.ts_ms,
                  symbol=EXCLUDED.symbol,
                  source=EXCLUDED.source,
                  risk=EXCLUDED.risk,
                  surprise=EXCLUDED.surprise,
                  tags_mask=EXCLUDED.tags_mask,
                  primary_tag=EXCLUDED.primary_tag,
                  payload_json=EXCLUDED.payload_json
                """,
                [
                    (
                        r["uid"],
                        int(r["ts_ms"]),
                        r["symbol"],
                        r["source"],
                        float(r["risk"]),
                        float(r["surprise"]),
                        int(r["tags_mask"]),
                        int(r["primary_tag"]),
                        json.dumps(r["payload_json"]),
                    )
                    for r in analysis_rows
                ],
            )

        if feature_rows:
            psycopg2.extras.execute_values(
                cur,
                """,
                INSERT INTO news_features_symbol(symbol, ts_ms, risk, surprise, tags_mask, primary_tag, ref)
                VALUES %s
                ON CONFLICT (symbol, ts_ms) DO NOTHING
                """,
                [
                    (
                        r["symbol"],
                        int(r["ts_ms"]),
                        float(r["risk"]),
                        float(r["surprise"]),
                        int(r["tags_mask"]),
                        int(r["primary_tag"]),
                        r["ref"],
                    )
                    for r in feature_rows
                ],
            )
