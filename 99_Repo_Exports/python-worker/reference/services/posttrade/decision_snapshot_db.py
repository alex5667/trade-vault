"""DB adapters for decision_snapshot table: SQLite (tests/dev) and Postgres/Timescale (prod).

We intentionally keep the interface tiny and explicit:
- `ensure_schema()` is best-effort (OK if the table already exists).
- `upsert_decision_snapshots(rows)` performs batched idempotent writes.

Why BIGINT epoch-ms time:
- Matches the system-wide time contract (event-time is epoch-ms).
- Avoids timezone drift and conversion bugs.
- TimescaleDB supports BIGINT time for hypertables.

Dependencies:
- Postgres: prefers `psycopg` (v3) or `psycopg2`.
  If none are installed, Postgres adapter will raise a clear error.
- Tests use SQLite via stdlib `sqlite3`.
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

Row = Dict[str, Any]


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _to_float(v: Any) -> Optional[float]:
    try:
        f = float(v)
    except Exception:
        return None
    if f != f:  # NaN
        return None
    return f


def _to_text_array(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v if x is not None]
    # tolerate comma-separated string
    s = str(v).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _json_dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "{}"


@dataclass
class SQLiteDecisionSnapshotDB:
    """SQLite adapter used for integration smoke tests.

    Notes:
    - SQLite doesn't have JSONB or TEXT[]; we store them as TEXT.
    - `ts_decision_ms` remains BIGINT to match production schema.
    """

    conn: sqlite3.Connection

    def ensure_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS decision_snapshot (
              ts_decision_ms INTEGER NOT NULL,
              sid TEXT NOT NULL,
              symbol TEXT NOT NULL,
              venue TEXT NOT NULL,
              session TEXT NOT NULL,
              tf TEXT NOT NULL,
              kind TEXT NOT NULL,
              side TEXT NOT NULL,
              direction TEXT NOT NULL,

              decision_bid REAL,
              decision_ask REAL,
              decision_mid REAL,
              decision_spread_bps REAL,

              decision_depth_bid_5 REAL,
              decision_depth_ask_5 REAL,
              decision_depth_bid_20 REAL,
              decision_depth_ask_20 REAL,

              decision_book_slope_bid REAL,
              decision_book_slope_ask REAL,
              decision_dws_bps REAL,

              decision_ofi_norm REAL,
              decision_expected_slippage_bps REAL,
              decision_exec_risk_norm REAL,

              decision_price REAL,

              tca_ready INTEGER NOT NULL DEFAULT 0,
              book_sanity_flags TEXT NOT NULL DEFAULT '[]',

              schema_version INTEGER NOT NULL DEFAULT 1,
              producer TEXT NOT NULL DEFAULT '',
              ts_insert_ms INTEGER NOT NULL DEFAULT 0,

              extra TEXT,

              UNIQUE (sid, ts_decision_ms)
            );
            """
        )
        self.conn.commit()

    def upsert_decision_snapshots(self, rows: Sequence[Row]) -> int:
        if not rows:
            return 0
        cur = self.conn.cursor()
        sql = (
            """
            INSERT INTO decision_snapshot (
              ts_decision_ms, sid, symbol, venue, session, tf, kind, side, direction,
              decision_bid, decision_ask, decision_mid, decision_spread_bps,
              decision_depth_bid_5, decision_depth_ask_5, decision_depth_bid_20, decision_depth_ask_20,
              decision_book_slope_bid, decision_book_slope_ask, decision_dws_bps,
              decision_ofi_norm, decision_expected_slippage_bps, decision_exec_risk_norm,
              decision_price,
              tca_ready, book_sanity_flags,
              schema_version, producer, ts_insert_ms,
              extra
            ) VALUES (
              :ts_decision_ms, :sid, :symbol, :venue, :session, :tf, :kind, :side, :direction,
              :decision_bid, :decision_ask, :decision_mid, :decision_spread_bps,
              :decision_depth_bid_5, :decision_depth_ask_5, :decision_depth_bid_20, :decision_depth_ask_20,
              :decision_book_slope_bid, :decision_book_slope_ask, :decision_dws_bps,
              :decision_ofi_norm, :decision_expected_slippage_bps, :decision_exec_risk_norm,
              :decision_price,
              :tca_ready, :book_sanity_flags,
              :schema_version, :producer, :ts_insert_ms,
              :extra
            )
            ON CONFLICT(sid, ts_decision_ms) DO UPDATE SET
              symbol=excluded.symbol,
              venue=excluded.venue,
              session=excluded.session,
              tf=excluded.tf,
              kind=excluded.kind,
              side=excluded.side,
              direction=excluded.direction,
              decision_bid=excluded.decision_bid,
              decision_ask=excluded.decision_ask,
              decision_mid=excluded.decision_mid,
              decision_spread_bps=excluded.decision_spread_bps,
              decision_depth_bid_5=excluded.decision_depth_bid_5,
              decision_depth_ask_5=excluded.decision_depth_ask_5,
              decision_depth_bid_20=excluded.decision_depth_bid_20,
              decision_depth_ask_20=excluded.decision_depth_ask_20,
              decision_book_slope_bid=excluded.decision_book_slope_bid,
              decision_book_slope_ask=excluded.decision_book_slope_ask,
              decision_dws_bps=excluded.decision_dws_bps,
              decision_ofi_norm=excluded.decision_ofi_norm,
              decision_expected_slippage_bps=excluded.decision_expected_slippage_bps,
              decision_exec_risk_norm=excluded.decision_exec_risk_norm,
              decision_price=excluded.decision_price,
              tca_ready=excluded.tca_ready,
              book_sanity_flags=excluded.book_sanity_flags,
              schema_version=excluded.schema_version,
              producer=excluded.producer,
              ts_insert_ms=excluded.ts_insert_ms,
              extra=excluded.extra;
            """
        )
        params = []
        for r in rows:
            p = dict(r)
            p["tca_ready"] = 1 if bool(r.get("tca_ready")) else 0
            p["book_sanity_flags"] = _json_dumps(_to_text_array(r.get("book_sanity_flags")))
            p["extra"] = _json_dumps(r.get("extra")) if r.get("extra") is not None else None
            params.append(p)
        cur.executemany(sql, params)
        self.conn.commit()
        return len(rows)


@dataclass
class PostgresDecisionSnapshotDB:
    """Postgres/Timescale adapter.

    Uses psycopg v3 if available, otherwise psycopg2.

    Note: schema management (CREATE TABLE + hypertable) is intentionally separated.
    In prod you should apply `decision_snapshot_timescale.sql` via migrations/psql.
    `ensure_schema()` here is a best-effort fallback for dev/staging.
    """

    dsn: str

    def _connect(self):
        try:
            import psycopg  # type: ignore
            return psycopg.connect(self.dsn)
        except Exception:
            try:
                import psycopg2  # type: ignore
                return psycopg2.connect(self.dsn)
            except Exception as e:
                raise RuntimeError(
                    "Postgres driver not available. Install psycopg (v3) or psycopg2."
                ) from e

    def ensure_schema(self) -> None:
        conn = self._connect()
        try:
            cur = conn.cursor()
            # Minimal schema (no hypertable here; do it in SQL migration for Timescale)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS decision_snapshot (
                  ts_decision_ms BIGINT NOT NULL,
                  sid TEXT NOT NULL,
                  symbol TEXT NOT NULL,
                  venue TEXT NOT NULL DEFAULT 'binance',
                  session TEXT NOT NULL DEFAULT '',
                  tf TEXT NOT NULL DEFAULT '',
                  kind TEXT NOT NULL DEFAULT '',
                  side TEXT NOT NULL DEFAULT '',
                  direction TEXT NOT NULL DEFAULT '',

                  decision_bid DOUBLE PRECISION NULL,
                  decision_ask DOUBLE PRECISION NULL,
                  decision_mid DOUBLE PRECISION NULL,
                  decision_spread_bps DOUBLE PRECISION NULL,

                  decision_depth_bid_5 DOUBLE PRECISION NULL,
                  decision_depth_ask_5 DOUBLE PRECISION NULL,
                  decision_depth_bid_20 DOUBLE PRECISION NULL,
                  decision_depth_ask_20 DOUBLE PRECISION NULL,

                  decision_book_slope_bid DOUBLE PRECISION NULL,
                  decision_book_slope_ask DOUBLE PRECISION NULL,
                  decision_dws_bps DOUBLE PRECISION NULL,

                  decision_ofi_norm DOUBLE PRECISION NULL,
                  decision_expected_slippage_bps DOUBLE PRECISION NULL,
                  decision_exec_risk_norm DOUBLE PRECISION NULL,

                  decision_price DOUBLE PRECISION NULL,

                  tca_ready BOOLEAN NOT NULL DEFAULT FALSE,
                  book_sanity_flags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],

                  schema_version INTEGER NOT NULL DEFAULT 1,
                  producer TEXT NOT NULL DEFAULT '',
                  ts_insert_ms BIGINT NOT NULL DEFAULT 0,

                  extra JSONB NULL,

                  UNIQUE (sid, ts_decision_ms)
                );
                """
            )
            conn.commit()
        finally:
            conn.close()

    def upsert_decision_snapshots(self, rows: Sequence[Row]) -> int:
        if not rows:
            return 0
        conn = self._connect()
        try:
            cur = conn.cursor()
            sql = (
                """
                INSERT INTO decision_snapshot (
                  ts_decision_ms, sid, symbol, venue, session, tf, kind, side, direction,
                  decision_bid, decision_ask, decision_mid, decision_spread_bps,
                  decision_depth_bid_5, decision_depth_ask_5, decision_depth_bid_20, decision_depth_ask_20,
                  decision_book_slope_bid, decision_book_slope_ask, decision_dws_bps,
                  decision_ofi_norm, decision_expected_slippage_bps, decision_exec_risk_norm,
                  decision_price,
                  tca_ready, book_sanity_flags,
                  schema_version, producer, ts_insert_ms,
                  extra
                ) VALUES (
                  %(ts_decision_ms)s, %(sid)s, %(symbol)s, %(venue)s, %(session)s, %(tf)s, %(kind)s, %(side)s, %(direction)s,
                  %(decision_bid)s, %(decision_ask)s, %(decision_mid)s, %(decision_spread_bps)s,
                  %(decision_depth_bid_5)s, %(decision_depth_ask_5)s, %(decision_depth_bid_20)s, %(decision_depth_ask_20)s,
                  %(decision_book_slope_bid)s, %(decision_book_slope_ask)s, %(decision_dws_bps)s,
                  %(decision_ofi_norm)s, %(decision_expected_slippage_bps)s, %(decision_exec_risk_norm)s,
                  %(decision_price)s,
                  %(tca_ready)s, %(book_sanity_flags)s,
                  %(schema_version)s, %(producer)s, %(ts_insert_ms)s,
                  %(extra)s
                )
                ON CONFLICT (sid, ts_decision_ms) DO UPDATE SET
                  symbol=excluded.symbol,
                  venue=excluded.venue,
                  session=excluded.session,
                  tf=excluded.tf,
                  kind=excluded.kind,
                  side=excluded.side,
                  direction=excluded.direction,
                  decision_bid=excluded.decision_bid,
                  decision_ask=excluded.decision_ask,
                  decision_mid=excluded.decision_mid,
                  decision_spread_bps=excluded.decision_spread_bps,
                  decision_depth_bid_5=excluded.decision_depth_bid_5,
                  decision_depth_ask_5=excluded.decision_depth_ask_5,
                  decision_depth_bid_20=excluded.decision_depth_bid_20,
                  decision_depth_ask_20=excluded.decision_depth_ask_20,
                  decision_book_slope_bid=excluded.decision_book_slope_bid,
                  decision_book_slope_ask=excluded.decision_book_slope_ask,
                  decision_dws_bps=excluded.decision_dws_bps,
                  decision_ofi_norm=excluded.decision_ofi_norm,
                  decision_expected_slippage_bps=excluded.decision_expected_slippage_bps,
                  decision_exec_risk_norm=excluded.decision_exec_risk_norm,
                  decision_price=excluded.decision_price,
                  tca_ready=excluded.tca_ready,
                  book_sanity_flags=excluded.book_sanity_flags,
                  schema_version=excluded.schema_version,
                  producer=excluded.producer,
                  ts_insert_ms=excluded.ts_insert_ms,
                  extra=excluded.extra;
                """
            )
            params = []
            for r in rows:
                p = dict(r)
                p["tca_ready"] = bool(r.get("tca_ready"))
                p["book_sanity_flags"] = _to_text_array(r.get("book_sanity_flags"))
                # psycopg2: pass JSON as string; psycopg3 handles dict as json automatically sometimes
                extra = r.get("extra")
                p["extra"] = extra if isinstance(extra, (dict, list)) else (json.loads(extra) if isinstance(extra, str) and extra.strip().startswith(("{","[")) else extra)
                params.append(p)
            cur.executemany(sql, params)
            conn.commit()
            return len(rows)
        finally:
            conn.close()
