from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

DDL_NEWS_ANALYSIS = """
CREATE TABLE IF NOT EXISTS news_analysis (
  uid           TEXT NOT NULL,
  symbol        TEXT NOT NULL,
  ts_ms         BIGINT NOT NULL,
  source        TEXT NOT NULL,
  risk          DOUBLE PRECISION NOT NULL,
  surprise      DOUBLE PRECISION NOT NULL,
  tags_mask     BIGINT NOT NULL,
  primary_tag   INTEGER NOT NULL,
  payload_json  JSONB NOT NULL,
  inserted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(uid, symbol)
);
CREATE INDEX IF NOT EXISTS news_analysis_ts_idx ON news_analysis (ts_ms DESC);
CREATE INDEX IF NOT EXISTS news_analysis_symbol_ts_idx ON news_analysis (symbol, ts_ms DESC);
"""

DDL_NEWS_FEATURES = """
CREATE TABLE IF NOT EXISTS news_features_symbol (
  symbol      TEXT NOT NULL,
  ts_ms       BIGINT NOT NULL,
  risk        DOUBLE PRECISION NOT NULL,
  surprise    DOUBLE PRECISION NOT NULL,
  tags_mask   BIGINT NOT NULL,
  primary_tag INTEGER NOT NULL,
  ref         TEXT NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(symbol, ts_ms)
);
CREATE INDEX IF NOT EXISTS news_features_symbol_ts_idx ON news_features_symbol (ts_ms DESC);
"""

DDL_CAL_EVENTS = """
CREATE TABLE IF NOT EXISTS calendar_events (
  uid            TEXT PRIMARY KEY,
  event_ts_ms    BIGINT NOT NULL,
  ingested_ts_ms BIGINT NOT NULL,
  country        TEXT NOT NULL,
  currency       TEXT NOT NULL,
  title          TEXT NOT NULL,
  importance     INTEGER NOT NULL,
  grade_id       INTEGER NOT NULL,
  forecast       TEXT NOT NULL,
  previous       TEXT NOT NULL,
  unit           TEXT NOT NULL,
  source         TEXT NOT NULL,
  payload_json   JSONB NOT NULL,
  inserted_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS calendar_events_event_ts_idx ON calendar_events (event_ts_ms DESC);
CREATE INDEX IF NOT EXISTS calendar_events_currency_ts_idx ON calendar_events (currency, event_ts_ms DESC);
"""

# Time series of next-event features per scope (asset_class).
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

class NewsPostgresWriter:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._pool = None
        self._init_pool()

    @staticmethod
    def from_env() -> NewsPostgresWriter:
        trading_pw = os.getenv("TRADING_PASSWORD", "trading_password")
        dsn = (
            (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))
            or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("POSTGRES_DSN"))
            or f"postgresql://trading:{trading_pw}@scanner-postgres:5432/scanner_analytics"
        )
        print(f"DEBUG: NewsPostgresWriter using DSN: {dsn}, PG_DSN arg: {os.getenv('PG_DSN')}", flush=True)
        return NewsPostgresWriter(dsn=dsn)

    def _init_pool(self):
        max_retries = 10
        for i in range(max_retries):
            try:
                if i > 0:
                    print(f"DEBUG: Attempt {i+1}/{max_retries} connecting to {self.dsn.split('@')[-1] if '@' in self.dsn else 'DB'}...", flush=True)
                self._pool = SimpleConnectionPool(1, 10, self.dsn)
                return
            except psycopg2.OperationalError as e:
                print(f"WARN: Postgres connection failed (Attempt {i+1}/{max_retries}): {e}", flush=True)
                if i == max_retries - 1:
                    print(f"ERROR: Failed to connect after {max_retries} attempts. DSN: {self.dsn}", flush=True)
                    raise e

                sleep_time = min(2 * (2 ** i), 30)
                print(f"WARN: Retrying in {sleep_time}s...", flush=True)
                time.sleep(sleep_time)

    @contextmanager
    def _conn(self):
        conn = self._pool.getconn()
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def ensure_schema(self) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'news_analysis'")
                if cur.fetchone() is None:
                    cur.execute(DDL_NEWS_ANALYSIS)
                    cur.execute(DDL_NEWS_FEATURES)
                    cur.execute(DDL_CAL_EVENTS)
                    cur.execute(DDL_CALENDAR_FEATURES_SCOPE)
            conn.commit()

    def insert_news_analysis(
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
        payload_json: str | dict[str, Any],
    ) -> None:
        obj = payload_json
        if isinstance(payload_json, str):
            try:
                obj = json.loads(payload_json)
            except Exception:
                obj = {"raw": payload_json}

        q = """
        INSERT INTO news_analysis(uid, symbol, ts_ms, source, risk, surprise, tags_mask, primary_tag, payload_json)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (uid, symbol) DO UPDATE SET
          risk=EXCLUDED.risk,
          surprise=EXCLUDED.surprise,
          tags_mask=EXCLUDED.tags_mask,
          primary_tag=EXCLUDED.primary_tag,
          payload_json=EXCLUDED.payload_json,
          ts_ms=EXCLUDED.ts_ms,
          inserted_at=now();
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (uid, symbol, int(ts_ms), source, float(risk), float(surprise),
                                int(tags_mask), int(primary_tag), psycopg2.extras.Json(obj)))
            conn.commit()

    def insert_news_features_symbol(
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
        q = """
        INSERT INTO news_features_symbol(symbol, ts_ms, risk, surprise, tags_mask, primary_tag, ref)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol, ts_ms) DO NOTHING;
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (symbol, int(ts_ms), float(risk), float(surprise),
                                int(tags_mask), int(primary_tag), ref))
            conn.commit()

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
        grade_id: int,
        forecast: str,
        previous: str,
        unit: str,
        source: str,
        payload_json: str | dict[str, Any],
    ) -> None:
        if isinstance(payload_json, str):
            try:
                obj = json.loads(payload_json) if payload_json else {}
            except Exception:
                obj = {"raw": payload_json}
        else:
            obj = payload_json or {}

        q = """
        INSERT INTO calendar_events(uid, event_ts_ms, ingested_ts_ms, country, currency, title, importance, grade_id,
                                   forecast, previous, unit, source, payload_json)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (uid) DO UPDATE SET
          event_ts_ms=EXCLUDED.event_ts_ms,
          ingested_ts_ms=EXCLUDED.ingested_ts_ms,
          country=EXCLUDED.country,
          currency=EXCLUDED.currency,
          title=EXCLUDED.title,
          importance=EXCLUDED.importance,
          grade_id=EXCLUDED.grade_id,
          forecast=EXCLUDED.forecast,
          previous=EXCLUDED.previous,
          unit=EXCLUDED.unit,
          source=EXCLUDED.source,
          payload_json=EXCLUDED.payload_json,
          inserted_at=now();
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (uid, int(event_ts_ms), int(ingested_ts_ms), country, currency,
                                title[:512], int(importance), int(grade_id),
                                forecast[:128], previous[:128], unit[:32], source[:64],
                                psycopg2.extras.Json(obj)))
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
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (scope, int(ts_ms), int(next_event_ts_ms), int(event_grade_id), str(event_ref), int(event_tminus_sec)))
            conn.commit()
