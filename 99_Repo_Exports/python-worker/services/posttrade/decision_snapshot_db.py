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
              is_virtual INTEGER NOT NULL DEFAULT 0,

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
              is_virtual,
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
              :is_virtual,
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
              is_virtual=excluded.is_virtual,
              extra=excluded.extra;
            """
        )
        params = []
        for r in rows:
            p = dict(r)
            p["tca_ready"] = 1 if bool(r.get("tca_ready")) else 0
            p["book_sanity_flags"] = _json_dumps(_to_text_array(r.get("book_sanity_flags")))
            p["is_virtual"] = 1 if bool(r.get("is_virtual")) else 0
            p["extra"] = _json_dumps(r.get("extra")) if r.get("extra") is not None else None
            params.append(p)
        cur.executemany(sql, params)
        self.conn.commit()
        return len(rows)


def _make_json_wrapper(conn: Any):
    """Return a callable that wraps a JSON string for the active psycopg driver.

    psycopg3: psycopg.types.json.Jsonb — marks the value as JSONB for the binary
              protocol; no SQL-level ::jsonb cast required.
    psycopg2: psycopg2.extras.Json — same purpose for the text protocol.
    fallback:  identity (passes raw string — works only if a ::jsonb cast is present).

    Both wrappers accept a pre-serialised JSON *string* (not a dict/list).
    """
    module = type(conn).__module__
    if module.startswith("psycopg") and not module.startswith("psycopg2"):
        # psycopg v3 path
        try:
            from psycopg.types.json import Jsonb  # type: ignore
            return Jsonb
        except ImportError:
            pass
    # psycopg2 path (or psycopg2 used as fallback)
    try:
        from psycopg2.extras import Json  # type: ignore
        return Json
    except ImportError:
        pass
    # Last-resort identity: caller must ensure SQL has ::jsonb cast if this path is hit.
    return lambda v: v


@dataclass
class PostgresDecisionSnapshotDB:
    """Postgres/Timescale adapter.

    Uses psycopg v3 if available, otherwise psycopg2.

    Note: schema management (CREATE TABLE + hypertable) is intentionally separated.
    In prod you should apply `decision_snapshot_timescale.sql` via migrations/psql.
    `ensure_schema()` here is a best-effort fallback for dev/staging.
    """

    dsn: str

    _driver_mod: Any = None  # resolved lazily
    _driver_name: str = ""   # "psycopg" or "psycopg2"
    _conn_pool: Any = None

    def _get_connection(self):
        import queue
        if self._conn_pool is None:
            self._conn_pool = queue.Queue(maxsize=10)
        
        # Drain the pool of any already closed or broken connections.
        while not self._conn_pool.empty():
            try:
                conn = self._conn_pool.get_nowait()
                
                # Proactive health check: check 'closed' attribute AND try a dummy query.
                if not getattr(conn, "closed", True):
                    try:
                        # Minimal ping to ensure the server hasn't dropped us.
                        with conn.cursor() as cur:
                            cur.execute("SELECT 1")
                        return conn
                    except Exception:
                        # Connection is dead (server closed it, or network failure).
                        try:
                            conn.close()
                        except Exception:
                            pass
            except queue.Empty:
                break
        
        # Pool empty or all stale; create fresh connection.
        return self._connect()

    def _put_connection(self, conn):
        import queue
        if self._conn_pool is None:
            self._conn_pool = queue.Queue(maxsize=10)
        if getattr(conn, "closed", True):
            return
        try:
            self._conn_pool.put_nowait(conn)
        except queue.Full:
            try:
                conn.close()
            except Exception:
                pass

    def _resolve_driver(self):
        """Import psycopg v3 or psycopg2 once; raise clearly if neither installed."""
        if self._driver_mod is not None:
            return self._driver_mod
        try:
            import psycopg  # type: ignore
            self._driver_mod = psycopg
            self._driver_name = "psycopg"
            return psycopg
        except ImportError:
            pass
        try:
            import psycopg2  # type: ignore
            self._driver_mod = psycopg2
            self._driver_name = "psycopg2"
            return psycopg2
        except ImportError:
            raise RuntimeError(
                "Postgres driver not available. Install psycopg (v3) or psycopg2."
            )

    def _connect(self, *, _max_retries: int = 5, _base_delay: float = 1.0):
        """Connect with retry + exponential backoff for transient PG errors.
        Distinguishes between missing driver (fatal) and connection errors (retryable).
        """
        import logging
        import time

        log = logging.getLogger("decision_snapshot_db")
        driver = self._resolve_driver()
        
        last_err: Exception | None = None
        for attempt in range(1, _max_retries + 1):
            try:
                return driver.connect(
                    self.dsn,
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5
                )
            except Exception as exc:
                # Driver was loaded, but connection failed (OperationalError, etc.)
                last_err = exc
                if attempt < _max_retries:
                    delay = _base_delay * (2 ** (attempt - 1))
                    log.warning(
                        "Postgres (%s) connect attempt %d/%d failed: %s. Retrying in %.1fs",
                        self._driver_name,
                        attempt,
                        _max_retries,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
        
        # If we reach here, we exhausted retries
        raise RuntimeError(
            f"Postgres connection via {self._driver_name} failed after {_max_retries} attempts. "
            f"Last error: {last_err}"
        ) from last_err

    def ensure_schema(self) -> None:
        conn = self._get_connection()
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
                  is_virtual BOOLEAN NOT NULL DEFAULT FALSE,

                  extra JSONB NULL,

                  UNIQUE (sid, ts_decision_ms)
                );
                """
            )
            conn.commit()
            self._put_connection(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            raise

    def upsert_decision_snapshots(self, rows: Sequence[Row]) -> int:
        if not rows:
            return 0
        
        import logging
        log = logging.getLogger("decision_snapshot_db")
        driver = self._resolve_driver()
        
        # We retry for OperationalError (lost connection).
        # Data errors (UniqueConstraintError, etc.) are NOT retryable by simple reconnection.
        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            conn = self._get_connection()
            try:
                cur = conn.cursor()
                # Detect driver once per call to choose the correct JSON wrapper.
                _json_wrapper = _make_json_wrapper(conn)
                
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
                      is_virtual,
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
                      %(is_virtual)s,
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
                      is_virtual=excluded.is_virtual,
                      extra=excluded.extra;
                    """
                )
                
                params = []
                for r in rows:
                    p = dict(r)
                    p["tca_ready"] = bool(r.get("tca_ready"))
                    p["book_sanity_flags"] = _to_text_array(r.get("book_sanity_flags"))
                    extra = r.get("extra")
                    if extra is None:
                        p["extra"] = None
                    else:
                        raw_json = extra if isinstance(extra, str) else _json_dumps(extra)
                        p["extra"] = _json_wrapper(raw_json)
                    params.append(p)
                
                cur.executemany(sql, params)
                conn.commit()
                self._put_connection(conn)
                return len(rows)
            
            except driver.OperationalError as e:
                # Lost connection. Discard and retry with backoff.
                try:
                    conn.close()
                except Exception:
                    pass
                if attempt < max_attempts:
                    import time
                    delay = 1.0 * (2 ** (attempt - 1))
                    log.warning(
                        "Postgres OperationalError on attempt %d/%d. Retrying in %.1fs: %s",
                        attempt, max_attempts, delay, e
                    )
                    time.sleep(delay)
                    continue
                # Exhausted retries
                raise
            
            except Exception:
                # For non-operational errors (logic, data, unique constraint), 
                # we rollback if possible and raise immediately.
                try:
                    conn.rollback()
                except Exception:
                    pass
                self._put_connection(conn)
                raise
