from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

# This module is intentionally compatible with BOTH psycopg2 and psycopg (v3).
# Your current python-worker container ships psycopg2-binary, but we keep v3
# support to avoid future migrations being painful.

try:  # psycopg v3
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

try:  # psycopg2
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore


# ---------------------------------------------------------------------------
# DDL (Timescale friendly, but works in vanilla Postgres)
# ---------------------------------------------------------------------------

DDL_NEWS_ANALYSIS = """
CREATE TABLE IF NOT EXISTS news_analysis (
  uid           TEXT   NOT NULL,
  symbol        TEXT   NOT NULL,
  ts_ms         BIGINT NOT NULL,
  source        TEXT   NOT NULL,
  risk          DOUBLE PRECISION NOT NULL,
  surprise      DOUBLE PRECISION NOT NULL,
  tags_mask     BIGINT NOT NULL,
  primary_tag   INTEGER NOT NULL,
  payload_json  JSONB  NOT NULL,
  inserted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(uid, symbol)
);
CREATE INDEX IF NOT EXISTS news_analysis_ts_idx ON news_analysis (ts_ms DESC);
CREATE INDEX IF NOT EXISTS news_analysis_symbol_ts_idx ON news_analysis (symbol, ts_ms DESC);
"""

DDL_NEWS_FEATURES_SYMBOL = """
CREATE TABLE IF NOT EXISTS news_features_symbol (
  symbol      TEXT   NOT NULL,
  ts_ms       BIGINT NOT NULL,
  risk        DOUBLE PRECISION NOT NULL,
  surprise    DOUBLE PRECISION NOT NULL,
  tags_mask   BIGINT NOT NULL,
  primary_tag INTEGER NOT NULL,
  ref         TEXT   NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(symbol, ts_ms)
);
CREATE INDEX IF NOT EXISTS news_features_symbol_ts_idx ON news_features_symbol (ts_ms DESC);
"""

DDL_CALENDAR_EVENTS = """
CREATE TABLE IF NOT EXISTS calendar_events (
  uid            TEXT   PRIMARY KEY,
  event_ts_ms    BIGINT NOT NULL,
  ingested_ts_ms BIGINT NOT NULL,
  country        TEXT   NOT NULL,
  currency       TEXT   NOT NULL,
  title          TEXT   NOT NULL,
  importance     INTEGER NOT NULL,
  forecast       TEXT   NOT NULL,
  previous       TEXT   NOT NULL,
  unit           TEXT   NOT NULL,
  source         TEXT   NOT NULL,
  payload_json   JSONB  NOT NULL,
  inserted_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS calendar_events_event_ts_idx ON calendar_events (event_ts_ms DESC);
CREATE INDEX IF NOT EXISTS calendar_events_currency_ts_idx ON calendar_events (currency, event_ts_ms DESC);
"""

# Time series of next-event features per scope (asset_class).
# Scope examples: "fx", "metals", "crypto"
DDL_CALENDAR_FEATURES_SCOPE = """
CREATE TABLE IF NOT EXISTS calendar_features_scope (
  scope            TEXT   NOT NULL,
  ts_ms            BIGINT NOT NULL,
  next_event_ts_ms BIGINT NOT NULL,
  event_grade_id   INTEGER NOT NULL,
  event_ref        TEXT   NOT NULL,
  event_tminus_sec INTEGER NOT NULL,
  inserted_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(scope, ts_ms)
);
CREATE INDEX IF NOT EXISTS calendar_features_scope_ts_idx ON calendar_features_scope (ts_ms DESC);
CREATE INDEX IF NOT EXISTS calendar_features_scope_scope_ts_idx ON calendar_features_scope (scope, ts_ms DESC);
"""


def _jsonb(v: Any) -> str:
    if isinstance(v, str):
        # already JSON string
        return v
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "{}"


class NewsPostgresWriter:
    """Small batch writer for news + calendar storage.

    Key properties:
    - Safe to use from worker threads (internal lock)
    - Fail-open: callers should wrap in try/except; this writer raises on DB errors
      but does not affect Redis path.
    - Uses simple executemany batches to minimize overhead.

    Tables:
      - news_analysis (raw)
      - news_features_symbol (aggregates snapshots)
      - calendar_events (raw)
      - calendar_features_scope (aggregates snapshots)
    """

    def __init__(self, *, dsn: str, batch_size: int = 200) -> None:
        self.dsn = dsn
        self.batch_size = int(batch_size)

        self._q_lock = threading.Lock()
        self._q_analysis: List[Tuple] = []
        self._q_features: List[Tuple] = []

        # calendar is low frequency; we insert immediately but keep methods here.
        self._last_connect_err_ms = 0

    # ---------------- Connection helpers ----------------

    def _connect(self):
        if psycopg is not None:
            return psycopg.connect(self.dsn)
        if psycopg2 is not None:
            return psycopg2.connect(self.dsn)
        raise RuntimeError("No Postgres driver installed (psycopg2 or psycopg)")

    def ensure_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(DDL_NEWS_ANALYSIS)
                cur.execute(DDL_NEWS_FEATURES_SYMBOL)
                cur.execute(DDL_CALENDAR_EVENTS)
                cur.execute(DDL_CALENDAR_FEATURES_SCOPE)
            conn.commit()

    # ---------------- News: queue & flush ----------------

    def enqueue_analysis(
        self,
        *,
        uid: str,
        symbol: str,
        ts_ms: int,
        source: str,
        risk: float,
        surprise: float,
        tags_mask: int,
        primary_tag: int,
        payload_json: Dict[str, Any],
    ) -> None:
        row = (
            uid,
            symbol,
            int(ts_ms),
            source,
            float(risk),
            float(surprise),
            int(tags_mask),
            int(primary_tag),
            _jsonb(payload_json),
        )
        with self._q_lock:
            self._q_analysis.append(row)
            if len(self._q_analysis) >= self.batch_size:
                # best-effort flush; if fails caller may retry later
                self.flush_analysis()

    def enqueue_feature(
        self,
        *,
        symbol: str,
        ts_ms: int,
        risk: float,
        surprise: float,
        tags_mask: int,
        primary_tag: int,
        ref: str,
    ) -> None:
        row = (
            symbol,
            int(ts_ms),
            float(risk),
            float(surprise),
            int(tags_mask),
            int(primary_tag),
            str(ref),
        )
        with self._q_lock:
            self._q_features.append(row)
            if len(self._q_features) >= self.batch_size:
                self.flush_features()

    def flush_analysis(self) -> None:
        with self._q_lock:
            rows = self._q_analysis
            self._q_analysis = []
        if not rows:
            return
        self._insert_news_analysis(rows)

    def flush_features(self) -> None:
        with self._q_lock:
            rows = self._q_features
            self._q_features = []
        if not rows:
            return
        self._insert_news_features(rows)

    def flush_all(self) -> None:
        # Callers use this periodically; we keep it lightweight.
        self.flush_analysis()
        self.flush_features()

    def _insert_news_analysis(self, rows: Sequence[Tuple]) -> None:
        sql = """
        INSERT INTO news_analysis
          (uid, symbol, ts_ms, source, risk, surprise, tags_mask, primary_tag, payload_json)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (uid, symbol) DO UPDATE
          SET risk=EXCLUDED.risk,
              surprise=EXCLUDED.surprise,
              tags_mask=EXCLUDED.tags_mask,
              primary_tag=EXCLUDED.primary_tag,
              payload_json=EXCLUDED.payload_json,
              ts_ms=EXCLUDED.ts_ms,
              inserted_at=now();
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                if psycopg2 is not None and conn.__class__.__module__.startswith("psycopg2"):
                    psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
                else:
                    cur.executemany(sql, rows)
            conn.commit()

    def _insert_news_features(self, rows: Sequence[Tuple]) -> None:
        sql = """
        INSERT INTO news_features_symbol
          (symbol, ts_ms, risk, surprise, tags_mask, primary_tag, ref)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol, ts_ms) DO NOTHING;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                if psycopg2 is not None and conn.__class__.__module__.startswith("psycopg2"):
                    psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
                else:
                    cur.executemany(sql, rows)
            conn.commit()

    # ---------------- Calendar: immediate inserts ----------------

    def insert_calendar_event(
        self,
        *,
        uid: str,
        event_ts_ms: int,
        ingested_ts_ms: int,
        country: str,
        currency: str,
        title: str,
        importance: int,
        forecast: str,
        previous: str,
        unit: str,
        source: str,
        payload_json: Dict[str, Any],
    ) -> None:
        sql = """
        INSERT INTO calendar_events
          (uid, event_ts_ms, ingested_ts_ms, country, currency, title, importance,
           forecast, previous, unit, source, payload_json)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (uid) DO UPDATE
          SET event_ts_ms=EXCLUDED.event_ts_ms,
              ingested_ts_ms=EXCLUDED.ingested_ts_ms,
              country=EXCLUDED.country,
              currency=EXCLUDED.currency,
              title=EXCLUDED.title,
              importance=EXCLUDED.importance,
              forecast=EXCLUDED.forecast,
              previous=EXCLUDED.previous,
              unit=EXCLUDED.unit,
              source=EXCLUDED.source,
              payload_json=EXCLUDED.payload_json,
              inserted_at=now();
        """
        row = (
            uid,
            int(event_ts_ms),
            int(ingested_ts_ms),
            country,
            currency,
            title,
            int(importance),
            forecast,
            previous,
            unit,
            source,
            _jsonb(payload_json),
        )
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, row)
            conn.commit()

    def insert_calendar_feature_scope(
        self,
        *,
        scope: str,
        ts_ms: int,
        next_event_ts_ms: int,
        event_grade_id: int,
        event_ref: str,
        event_tminus_sec: int,
    ) -> None:
        sql = """
        INSERT INTO calendar_features_scope
          (scope, ts_ms, next_event_ts_ms, event_grade_id, event_ref, event_tminus_sec)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (scope, ts_ms) DO NOTHING;
        """
        row = (scope, int(ts_ms), int(next_event_ts_ms), int(event_grade_id), str(event_ref), int(event_tminus_sec))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, row)
            conn.commit()
