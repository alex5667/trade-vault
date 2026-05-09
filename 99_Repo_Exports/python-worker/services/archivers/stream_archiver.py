from utils.time_utils import get_ny_time_millis

"""
Stream Archiver Service - Production-grade Redis Streams -> PostgreSQL archival

Архивирует критически важные streams для долгосрочного анализа:
1) stream:trade:entry_audit -> entry_policy_audit table
2) events:trades -> position_events table
3) signals:confidence:scores -> signal_confidence_scores table (high-frequency)
4) metrics:of_gate -> of_gate_metrics table (P3: per-event архив ok/ok_soft/reason_code)
5) quarantined:metrics:of_gate -> of_gate_metrics_quarantine table (P3: "грязь" отдельно)

Ключевые features:
- Consumer Group с XREADGROUP/XACK для at-least-once processing + идемпотентность по stream_id
- Batch insert (configurable) для производительности
- XAUTOCLAIM для recovery pending messages (missed consumers)
- DLQ (Dead Letter Queue) для failed messages
- Deterministic timestamp: payload.ts_ms/ts_event_ms или fallback на stream_id timestamp
- Isolation-ready: один и тот же код можно запускать отдельными контейнерами (noisy neighbor guard)
- P78: best-effort writes status to Redis hashes for Prometheus exporter observability

Примечание по формату сообщений:
- upstream обычно пишет JSON строку в поле "payload" (AsyncSignalPublisher) или "data" (legacy)
- archiver поддерживает оба варианта.

P3 ENV flags:
  OF_GATE_METRICS_ARCHIVE_ENABLED=1        — enable metrics:of_gate archiver
  OF_GATE_QUARANTINE_ARCHIVE_ENABLED=1     — enable quarantined:metrics:of_gate archiver
  OF_GATE_METRICS_AUTO_MIGRATE=1           — auto create of_gate_metrics table
  OF_GATE_QUARANTINE_AUTO_MIGRATE=1        — auto create of_gate_metrics_quarantine table
  OF_GATE_METRICS_ROLLUPS_AUTO_MIGRATE=0   — auto create CAGG / retention (Timescale)

P78 ENV keys:
  OF_GATE_ARCHIVER_METRICS_KEY             — Redis hash for metrics archiver status
  OF_GATE_QUARANTINE_ARCHIVER_METRICS_KEY  — Redis hash for quarantine archiver status
"""

import asyncio
import contextlib
import datetime as dt
import json
import math
import os
from dataclasses import dataclass
from typing import Any

import psycopg2
import redis.asyncio as aioredis
from psycopg2.extras import execute_values

from core.redis_keys import RedisStreams as RS
from utils.task_manager import safe_create_task


def env(name: str, default: str) -> str:
    """Env helper with empty string handling"""
    v = os.getenv(name)
    return v if v else default


def env_int(name: str, default: int) -> int:
    """Env int helper"""
    v = os.getenv(name)
    return int(v) if v else default


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def parse_int_set(csv: str, default: set[int]) -> set[int]:
    if not csv:
        return set(default)
    out: set[int] = set()
    for part in str(csv).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except Exception:
            continue
    return out or set(default)


def pick_dsn() -> str:
    """
    DSN precedence:
    1) ARCHIVER_PG_DSN (override for isolation / separate container)
    2) ANALYTICS_DB_DSN (canonical source)
    3) TRADES_DB_DSN (analytics legacy fallback)
    4) DATABASE_URL
    5) PG_DSN
    """
    return (
        os.getenv("ARCHIVER_PG_DSN")
        or os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or os.getenv("DATABASE_URL")
        or os.getenv("PG_DSN")
        or ""
    )


def safe_float(x: Any) -> float | None:
    """Safe float conversion + finite check"""
    try:
        if x is None:
            return None
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None


def safe_int(x: Any) -> int | None:
    """Safe int conversion"""
    try:
        return None if x is None else int(x)
    except Exception:
        return None


def ts_ms_from_stream_id(stream_id: str) -> int:
    """Extract timestamp in milliseconds from Redis stream ID: '<ms>-<seq>'"""
    return int(stream_id.split("-", 1)[0])


def coalesce_ts_ms(payload: dict[str, Any], stream_id: str) -> int:
    """
    Deterministic timestamp extraction:
    1) payload ts_ms/ts_event_ms/ts/timestamp_ms
    2) stream_id timestamp
    """
    for k in ("ts_ms", "ts_event_ms", "ts", "timestamp_ms"):
        v = safe_int(payload.get(k))
        if v is not None:
            return v
    return ts_ms_from_stream_id(stream_id)


def parse_meta_json(meta: Any) -> dict[str, Any] | None:
    """Parse meta field (may be JSON string). Returns dict or None."""
    if meta is None:
        return None
    if isinstance(meta, dict):
        return meta
    if isinstance(meta, str):
        try:
            return json.loads(meta)
        except Exception:
            return {"_raw": meta[:2000]}
    return {"_raw": str(meta)[:2000]}


def parse_stream_payload(fields: dict[str, Any]) -> dict[str, Any]:
    """Decode stream message fields to payload dict.

    Supports:
    - legacy "data" JSON
    - modern "payload" JSON
    - fallback to raw fields
    """
    raw = fields.get("data")
    if raw is None:
        raw = fields.get("payload")
    if raw is None:
        return dict(fields)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "ignore")
    if isinstance(raw, str):
        return json.loads(raw)
    return dict(fields)


@dataclass
class PgCfg:
    dsn: str


class PgWriter:
    """PostgreSQL writer with batch insert + idempotency"""

    def __init__(self, cfg: PgCfg):
        self.cfg = cfg

    @contextlib.contextmanager
    def _conn(self):
        conn = psycopg2.connect(self.cfg.dsn)
        try:
            yield conn
        finally:
            with contextlib.suppress(Exception):
                conn.rollback()
            conn.close()

    def ensure_signal_confidence_scores_table(self) -> None:
        """Create signal_confidence_scores table (+ hypertable if Timescale present)."""
        check_ddl = "SELECT 1 FROM information_schema.tables WHERE table_name = 'signal_confidence_scores'"
        ddl = """
        CREATE TABLE IF NOT EXISTS signal_confidence_scores (
          stream_id TEXT NOT NULL,
          ts_ms BIGINT NOT NULL,
          ts TIMESTAMPTZ NOT NULL,
          sid TEXT NOT NULL,
          symbol TEXT NOT NULL,
          schema_version INT NOT NULL,
          producer TEXT NOT NULL,
          confidence_raw DOUBLE PRECISION NOT NULL,
          confidence_final DOUBLE PRECISION,
          evidence_json JSONB NOT NULL,
          context_json JSONB,
          PRIMARY KEY (stream_id, ts)
        );
        """
        idx = """
        CREATE INDEX IF NOT EXISTS signal_confidence_scores_symbol_ts_idx
          ON signal_confidence_scores (symbol, ts DESC);
        CREATE INDEX IF NOT EXISTS signal_confidence_scores_sid_ts_idx
          ON signal_confidence_scores (sid, ts DESC);
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(check_ddl)
                if cur.fetchone() is None:
                    cur.execute(ddl)
                    try:
                        # If TimescaleDB is installed, make it a hypertable.
                        cur.execute("SELECT create_hypertable('signal_confidence_scores','ts', if_not_exists => TRUE);")
                    except Exception:
                        # Fail-open: table works as plain Postgres.
                        conn.rollback()
                    cur.execute(idx)
            conn.commit()

    # ------------------------------------------------------------------
    # of_gate_metrics / of_gate_metrics_quarantine (P3: per-event archival)
    # ------------------------------------------------------------------
    #
    # Replaces P2-era aggregated-stats of_gate_metrics schema.
    # Now stores one row per of_gate evaluation event:
    #   - of_gate_metrics: metrics:of_gate stream (ok/ok_soft/reason_code rows)
    #   - of_gate_metrics_quarantine: quarantined:metrics:of_gate (DQ-flagged rows)
    #
    # Timescale CAGG (rollups) computed separately via ensure_of_gate_metrics_rollups_timescale().
    # Enable archival via: OF_GATE_METRICS_ARCHIVE_ENABLED=1 / OF_GATE_QUARANTINE_ARCHIVE_ENABLED=1

    def ensure_of_gate_metrics_tables(self) -> None:
        """Create of_gate_metrics and of_gate_metrics_quarantine tables (+ hypertables if Timescale present).

        Idempotent: safe to call on every startup.
        Also migrates old single-column-PK schemas to the current schema (ADD COLUMN IF NOT EXISTS).
        """
        ddl_metrics = """
        CREATE TABLE IF NOT EXISTS of_gate_metrics (
          stream_id TEXT NOT NULL,
          ts_ms BIGINT NOT NULL,
          ts TIMESTAMPTZ NOT NULL,
          symbol TEXT NOT NULL,
          scenario_v4 TEXT NOT NULL,
          schema_version INT NOT NULL,
          ok SMALLINT NOT NULL,
          ok_soft SMALLINT NOT NULL,
          missing_legs JSONB,
          reason_code TEXT NOT NULL,
          payload_json JSONB NOT NULL,
          PRIMARY KEY (stream_id, ts)
        );
        """
        ddl_q = """
        CREATE TABLE IF NOT EXISTS of_gate_metrics_quarantine (
          stream_id TEXT NOT NULL,
          ts_ms BIGINT NOT NULL,
          ts TIMESTAMPTZ NOT NULL,
          source_stream TEXT NOT NULL,
          symbol TEXT,
          scenario_v4 TEXT,
          schema_version INT,
          ok SMALLINT,
          ok_soft SMALLINT,
          dq_code TEXT NOT NULL,
          err TEXT,
          payload_json JSONB NOT NULL,
          PRIMARY KEY (stream_id, ts)
        );
        """
        # Migrate old quarantine schema: add missing columns if the table already exists
        # with the previous single-PK layout (src_stream / src_stream_id style).
        migrate_q = """
        DO $$
        BEGIN
          ALTER TABLE of_gate_metrics_quarantine ADD COLUMN IF NOT EXISTS source_stream TEXT;
          ALTER TABLE of_gate_metrics_quarantine ADD COLUMN IF NOT EXISTS symbol TEXT;
          ALTER TABLE of_gate_metrics_quarantine ADD COLUMN IF NOT EXISTS scenario_v4 TEXT;
          ALTER TABLE of_gate_metrics_quarantine ADD COLUMN IF NOT EXISTS schema_version INT;
          ALTER TABLE of_gate_metrics_quarantine ADD COLUMN IF NOT EXISTS ok SMALLINT;
          ALTER TABLE of_gate_metrics_quarantine ADD COLUMN IF NOT EXISTS ok_soft SMALLINT;
        EXCEPTION WHEN others THEN
          NULL;
        END $$;
        """
        # Each index in its own exception-safe block to survive schema drift
        idx_sql = """
        DO $$ BEGIN CREATE INDEX IF NOT EXISTS of_gate_metrics_symbol_ts_idx
          ON of_gate_metrics (symbol, ts DESC);
        EXCEPTION WHEN others THEN NULL; END $$;

        DO $$ BEGIN CREATE INDEX IF NOT EXISTS of_gate_metrics_scenario_ts_idx
          ON of_gate_metrics (scenario_v4, ts DESC);
        EXCEPTION WHEN others THEN NULL; END $$;

        DO $$ BEGIN CREATE INDEX IF NOT EXISTS of_gate_metrics_reason_ts_idx
          ON of_gate_metrics (reason_code, ts DESC);
        EXCEPTION WHEN others THEN NULL; END $$;

        DO $$ BEGIN CREATE INDEX IF NOT EXISTS of_gate_q_dq_code_ts_idx
          ON of_gate_metrics_quarantine (dq_code, ts DESC);
        EXCEPTION WHEN others THEN NULL; END $$;

        DO $$ BEGIN CREATE INDEX IF NOT EXISTS of_gate_q_symbol_ts_idx
          ON of_gate_metrics_quarantine (symbol, ts DESC);
        EXCEPTION WHEN others THEN NULL; END $$;
        """
        hypertable_sql = """
        DO $$
        BEGIN
          PERFORM create_hypertable('of_gate_metrics','ts', if_not_exists => TRUE);
        EXCEPTION WHEN others THEN
          NULL;
        END $$;

        DO $$
        BEGIN
          PERFORM create_hypertable('of_gate_metrics_quarantine','ts', if_not_exists => TRUE);
        EXCEPTION WHEN others THEN
          NULL;
        END $$;
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl_metrics)
                cur.execute(ddl_q)
                cur.execute(migrate_q)
                cur.execute(hypertable_sql)
                cur.execute(idx_sql)
            conn.commit()


    def ensure_of_gate_metrics_rollups_timescale(self) -> None:
        """Create continuous aggregates + retention policy (Timescale). Safe to run multiple times.

        No-op (returns silently) if Timescale not installed or insufficient permissions.
        Views created:
          - of_gate_ok_rate_5m: 5-minute rollup per symbol/scenario_v4
          - of_gate_ok_rate_1h: 1-hour rollup per symbol/scenario_v4
        Retention: 30 days for raw tables.
        """
        ddl = """
        DO $$
        BEGIN
          -- Continuous aggregate 5m (symbol x scenario rollup)
          BEGIN
            EXECUTE $$
            CREATE MATERIALIZED VIEW IF NOT EXISTS of_gate_ok_rate_5m
            WITH (timescaledb.continuous) AS
            SELECT
              time_bucket('5 minutes', ts) AS bucket,
              symbol,
              scenario_v4,
              count(*)::bigint AS eligible_count,
              sum(ok)::bigint AS ok_hard_count,
              sum(ok_soft)::bigint AS ok_soft_count,
              CASE WHEN count(*) = 0 THEN NULL ELSE (sum(ok)::numeric / count(*)::numeric) END AS ok_rate_strict,
              CASE WHEN count(*) = 0 THEN NULL ELSE ((sum(ok)+sum(ok_soft))::numeric / count(*)::numeric) END AS ok_rate_soft,
              CASE WHEN (sum(ok)+sum(ok_soft)) = 0 THEN NULL ELSE (sum(ok_soft)::numeric / (sum(ok)+sum(ok_soft))::numeric) END AS soft_share
            FROM of_gate_metrics
            GROUP BY 1,2,3;
            $$;
          EXCEPTION WHEN others THEN
            NULL;
          END;

          -- Continuous aggregate 1h (symbol x scenario rollup)
          BEGIN
            EXECUTE $$
            CREATE MATERIALIZED VIEW IF NOT EXISTS of_gate_ok_rate_1h
            WITH (timescaledb.continuous) AS
            SELECT
              time_bucket('1 hour', ts) AS bucket,
              symbol,
              scenario_v4,
              count(*)::bigint AS eligible_count,
              sum(ok)::bigint AS ok_hard_count,
              sum(ok_soft)::bigint AS ok_soft_count,
              CASE WHEN count(*) = 0 THEN NULL ELSE (sum(ok)::numeric / count(*)::numeric) END AS ok_rate_strict,
              CASE WHEN count(*) = 0 THEN NULL ELSE ((sum(ok)+sum(ok_soft))::numeric / count(*)::numeric) END AS ok_rate_soft,
              CASE WHEN (sum(ok)+sum(ok_soft)) = 0 THEN NULL ELSE (sum(ok_soft)::numeric / (sum(ok)+sum(ok_soft))::numeric) END AS soft_share
            FROM of_gate_metrics
            GROUP BY 1,2,3;
            $$;
          EXCEPTION WHEN others THEN
            NULL;
          END;

          -- Policies: safe if functions exist (fail silently if not Timescale)
          BEGIN
            PERFORM add_continuous_aggregate_policy('of_gate_ok_rate_5m', start_offset => INTERVAL '1 day', end_offset => INTERVAL '5 minutes', schedule_interval => INTERVAL '5 minutes');
          EXCEPTION WHEN undefined_function THEN
            NULL;
          END;
          BEGIN
            PERFORM add_continuous_aggregate_policy('of_gate_ok_rate_1h', start_offset => INTERVAL '7 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour');
          EXCEPTION WHEN undefined_function THEN
            NULL;
          END;

          -- Retention policies (30 days raw)
          BEGIN
            PERFORM add_retention_policy('of_gate_metrics', INTERVAL '30 days');
          EXCEPTION WHEN undefined_function THEN
            NULL;
          END;
          BEGIN
            PERFORM add_retention_policy('of_gate_metrics_quarantine', INTERVAL '30 days');
          EXCEPTION WHEN undefined_function THEN
            NULL;
          END;

        EXCEPTION WHEN others THEN
          -- Timescale not installed or insufficient permissions: ignore.
          NULL;
        END $$;
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    def ensure_of_gate_tables(self) -> None:
        """Create of_gate_metrics and of_gate_metrics_quarantine tables (+ hypertables if Timescale present)."""
        ddl = """
        CREATE TABLE IF NOT EXISTS of_gate_metrics (
          stream_id TEXT PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          ts TIMESTAMPTZ NOT NULL,
          symbol TEXT NOT NULL,
          schema_version INT NOT NULL DEFAULT 1,
          scenario_v4 TEXT NOT NULL DEFAULT 'unknown',
          ok INT NOT NULL DEFAULT 0,
          ok_soft INT NOT NULL DEFAULT 0,
          reason_code TEXT NOT NULL DEFAULT 'na',
          missing_legs JSONB,
          payload_json JSONB NOT NULL,
        );
        CREATE INDEX IF NOT EXISTS of_gate_metrics_symbol_ts_idx
          ON of_gate_metrics (symbol, ts DESC);
        CREATE INDEX IF NOT EXISTS of_gate_metrics_scenario_ts_idx
          ON of_gate_metrics (scenario_v4, ts DESC);
        """
        ddl_q = """
        CREATE TABLE IF NOT EXISTS of_gate_metrics_quarantine (
          stream_id TEXT PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          ts TIMESTAMPTZ NOT NULL,
          src_stream TEXT,
          src_stream_id TEXT,
          dq_code TEXT,
          err TEXT,
          payload_json JSONB
        );
        CREATE INDEX IF NOT EXISTS of_gate_metrics_quarantine_ts_idx
          ON of_gate_metrics_quarantine (ts DESC);
        CREATE INDEX IF NOT EXISTS of_gate_metrics_quarantine_dq_ts_idx
          ON of_gate_metrics_quarantine (dq_code, ts DESC);
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
                cur.execute(ddl_q)
                try:
                    cur.execute("SELECT create_hypertable('of_gate_metrics','ts', if_not_exists => TRUE);")
                    cur.execute("SELECT create_hypertable('of_gate_metrics_quarantine','ts', if_not_exists => TRUE);")
                except Exception:
                    conn.rollback()
            conn.commit()

    def insert_entry_audit(self, rows: list[tuple[Any, ...]]) -> int:
        """Batch insert entry_policy_audit with ON CONFLICT (stream_id) DO NOTHING"""
        if not rows:
            return 0
        sql = """
        INSERT INTO entry_policy_audit (
          stream_id, ts_ms, ts,
          sid, symbol, tf, strategy, source,
          decision, arm, ab_group, scenario, regime,
          of_confirm_score, coh, leader_conf,
          spread_z, pressure_sps, obi_age_ms,
          payload_json
        ) VALUES %s
        ON CONFLICT (stream_id) DO NOTHING
        """
        with self._conn() as conn, conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        return len(rows)

    def insert_position_events(self, rows: list[tuple[Any, ...]]) -> int:
        """Batch insert position_events with ON CONFLICT (stream_id) DO NOTHING"""
        if not rows:
            return 0
        sql = """
        INSERT INTO position_events (
          stream_id, ts_ms, ts,
          position_id, sid, symbol,
          event_type, meta_json, payload_json
        ) VALUES %s
        ON CONFLICT (stream_id) DO NOTHING
        """
        with self._conn() as conn, conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        return len(rows)

    def insert_signal_confidence_scores(self, rows: list[tuple[Any, ...]]) -> int:
        """Batch insert signal_confidence_scores with ON CONFLICT (stream_id, ts) DO NOTHING"""
        if not rows:
            return 0
        sql = """
        INSERT INTO signal_confidence_scores (
          stream_id, ts_ms, ts,
          sid, symbol,
          schema_version, producer,
          confidence_raw, confidence_final,
          evidence_json, context_json
        ) VALUES %s
        ON CONFLICT (stream_id, ts) DO NOTHING
        """
        with self._conn() as conn, conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        return len(rows)

    def insert_of_gate_metrics(self, rows: list[tuple[Any, ...]]) -> int:
        """Batch insert of_gate_metrics per-event rows (idempotent).

        Schema: (stream_id, ts_ms, ts, symbol, scenario_v4, schema_version,
                 ok, ok_soft, missing_legs, reason_code, payload_json)
        """
        if not rows:
            return 0
        sql = """
        INSERT INTO of_gate_metrics (
          stream_id, ts_ms, ts,
          symbol, scenario_v4,
          schema_version,
          ok, ok_soft,
          missing_legs,
          reason_code,
          payload_json
        ) VALUES %s
        ON CONFLICT (stream_id, ts) DO NOTHING
        """
        with self._conn() as conn, conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        return len(rows)

    def insert_of_gate_metrics_quarantine(self, rows: list[tuple[Any, ...]]) -> int:
        """Batch insert of_gate_metrics_quarantine (idempotent).

        Schema: (stream_id, ts_ms, ts, source_stream, symbol, scenario_v4,
                 schema_version, ok, ok_soft, dq_code, err, payload_json)
        """
        if not rows:
            return 0
        sql = """
        INSERT INTO of_gate_metrics_quarantine (
          stream_id, ts_ms, ts,
          source_stream,
          symbol, scenario_v4,
          schema_version,
          ok, ok_soft,
          dq_code,
          err,
          payload_json
        ) VALUES %s
        ON CONFLICT (stream_id, ts) DO NOTHING
        """
        with self._conn() as conn, conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        return len(rows)





    def ensure_trade_kpi_liqmap_v1_table(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS trade_kpi_liqmap_v1 (
          stream_id TEXT NOT NULL,
          ts_ms BIGINT NOT NULL,
          ts TIMESTAMPTZ NOT NULL,
          trade_id TEXT NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          regime TEXT NOT NULL,
          sl_hit_near_liqmap_peak SMALLINT,
          tp1_anchored SMALLINT,
          tp1_anchored_and_hit SMALLINT,
          sl_liqmap_peak_dist_bps DOUBLE PRECISION,
          sl_liqmap_peak_usd DOUBLE PRECISION,
          liqmap_kpi JSONB NOT NULL,
          payload_json JSONB NOT NULL,
          PRIMARY KEY (stream_id, ts)
        );
        """
        idx = """
        CREATE INDEX IF NOT EXISTS trade_kpi_liqmap_v1_symbol_ts_idx
          ON trade_kpi_liqmap_v1 (symbol, ts DESC);
        CREATE INDEX IF NOT EXISTS trade_kpi_liqmap_v1_trade_id_ts_idx
          ON trade_kpi_liqmap_v1 (trade_id, ts DESC);
        CREATE INDEX IF NOT EXISTS trade_kpi_liqmap_v1_liqmap_kpi_gin
          ON trade_kpi_liqmap_v1 USING GIN (liqmap_kpi jsonb_path_ops);
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
                try:
                    cur.execute("SELECT create_hypertable('trade_kpi_liqmap_v1','ts', if_not_exists => TRUE);")
                except Exception:
                    conn.rollback()
                cur.execute(idx)
            conn.commit()

    def insert_trade_kpi_liqmap_v1(self, rows) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO trade_kpi_liqmap_v1 (
          stream_id, ts_ms, ts,
          trade_id, symbol, side, regime,
          sl_hit_near_liqmap_peak, tp1_anchored, tp1_anchored_and_hit,
          sl_liqmap_peak_dist_bps, sl_liqmap_peak_usd,
          liqmap_kpi, payload_json
        ) VALUES %s
        ON CONFLICT (stream_id, ts) DO NOTHING
        """
        with self._conn() as conn, conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(cur, sql, rows, page_size=2000)
        return len(rows)

class StreamArchiver:
    """Main archiver service"""

    def __init__(self, r: aioredis.Redis, pg: PgWriter):
        self.r = r
        self.pg = pg

        # Stream names
        self.entry_stream = env("TRADE_ENTRY_AUDIT_STREAM", "stream:trade:entry_audit")
        self.events_stream = env("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES)
        self.conf_scores_stream = env("CONF_SCORES_STREAM", "signals:confidence:scores")

        self.post_sl_stream = env("POST_SL_STREAM", "trades:post_sl")
        self.post_sl_liqmap_enabled = env_int("POST_SL_LIQMAP_KPI_ARCHIVE_ENABLED", 0) == 1
        self.post_sl_liqmap_cg = env("POST_SL_LIQMAP_KPI_CG", "post_sl_liqmap_kpi_archiver")
        self.post_sl_liqmap_consumer = env("POST_SL_LIQMAP_KPI_CONSUMER", "archiver_post_sl_1")
        self.post_sl_liqmap_batch = env_int("POST_SL_LIQMAP_KPI_BATCH", 2000)
        self.post_sl_liqmap_block_ms = env_int("POST_SL_LIQMAP_KPI_BLOCK_MS", 1000)
        self.post_sl_liqmap_min_idle = env_int("POST_SL_LIQMAP_KPI_MIN_IDLE_MS", 60000)
        self.post_sl_liqmap_dlq = env("POST_SL_LIQMAP_KPI_DLQ_STREAM", "stream:dlq:post_sl_liqmap_kpi")
        self.post_sl_liqmap_auto_migrate = env_bool("POST_SL_LIQMAP_KPI_AUTO_MIGRATE", True)
        self.post_sl_liqmap_status_hash = env("POST_SL_LIQMAP_KPI_ARCHIVER_STATUS_HASH", "metrics:post_sl_liqmap_kpi_archiver")



        # OF gate metrics streams
        self.of_gate_metrics_stream = env("OF_GATE_METRICS_STREAM", RS.OF_GATE_METRICS)
        self.of_gate_quarantine_stream = env("OF_GATE_QUARANTINE_STREAM", RS.OF_GATE_METRICS_QUARANTINE)

        # P3: OF-gate streams (per-event archival)
        self.of_gate_stream = env("OF_GATE_METRICS_STREAM", RS.OF_GATE_METRICS)
        self.of_gate_q_stream = env("OF_GATE_QUARANTINE_STREAM", RS.OF_GATE_METRICS_QUARANTINE)

        # Enable flags
        self.of_gate_enabled = env_int("OF_GATE_METRICS_ARCHIVE_ENABLED", 0) == 1
        self.of_gate_quarantine_enabled = env_int("OF_GATE_QUARANTINE_ARCHIVE_ENABLED", 0) == 1

        self.entry_enabled = env_int("ENTRY_AUDIT_ARCHIVE_ENABLED", 1) == 1
        self.events_enabled = env_int("POSITION_EVENTS_ARCHIVE_ENABLED", 1) == 1
        self.conf_scores_enabled = env_int("CONFIDENCE_SCORES_ARCHIVE_ENABLED", 0) == 1
        # P3 enable flags (default off — opt-in)
        self.of_gate_enabled = env_int("OF_GATE_METRICS_ARCHIVE_ENABLED", 0) == 1
        self.of_gate_quarantine_enabled = env_int("OF_GATE_QUARANTINE_ARCHIVE_ENABLED", 0) == 1

        # Entry audit consumer config
        self.entry_cg = env("ENTRY_AUDIT_CG", "entry_audit_archiver")
        self.entry_consumer = env("ENTRY_AUDIT_CONSUMER", "archiver_1")
        self.entry_batch = env_int("ENTRY_AUDIT_BATCH", 500)
        self.entry_block_ms = env_int("ENTRY_AUDIT_BLOCK_MS", 2000)
        self.entry_min_idle = env_int("ENTRY_AUDIT_MIN_IDLE_MS", 60000)
        self.entry_dlq = env("ENTRY_AUDIT_DLQ_STREAM", "stream:dlq:entry_audit")

        # Position events consumer config
        self.events_cg = env("POSITION_EVENTS_CG", "position_events_archiver")
        self.events_consumer = env("POSITION_EVENTS_CONSUMER", "archiver_1")
        self.events_batch = env_int("POSITION_EVENTS_BATCH", 500)
        self.events_block_ms = env_int("POSITION_EVENTS_BLOCK_MS", 2000)
        self.events_min_idle = env_int("POSITION_EVENTS_MIN_IDLE_MS", 60000)
        self.events_dlq = env("POSITION_EVENTS_DLQ_STREAM", "stream:dlq:position_events")

        # Confidence scores consumer config (HF)
        self.conf_scores_cg = env("CONF_SCORES_CG", "conf_scores_archiver")
        self.conf_scores_consumer = env("CONF_SCORES_CONSUMER", "archiver_scores_1")
        self.conf_scores_batch = env_int("CONF_SCORES_BATCH", 5000)
        self.conf_scores_block_ms = env_int("CONF_SCORES_BLOCK_MS", 1000)
        self.conf_scores_min_idle = env_int("CONF_SCORES_MIN_IDLE_MS", 60000)
        self.conf_scores_dlq = env("CONF_SCORES_DLQ_STREAM", "stream:dlq:conf_scores")
        self.conf_scores_auto_migrate = env_bool("CONF_SCORES_AUTO_MIGRATE", False)
        self.conf_scores_store_context = env_bool("CONF_SCORES_STORE_CONTEXT", True)
        self.conf_scores_store_evidence = env_bool("CONF_SCORES_STORE_EVIDENCE", True)

        # P3: OF-gate metrics consumer config
        self.of_gate_cg = env("OF_GATE_METRICS_CG", "of_gate_metrics_archiver")
        self.of_gate_consumer = env("OF_GATE_METRICS_CONSUMER", "archiver_of_gate_1")
        self.of_gate_batch = env_int("OF_GATE_METRICS_BATCH", 5000)
        self.of_gate_block_ms = env_int("OF_GATE_METRICS_BLOCK_MS", 1000)
        self.of_gate_min_idle = env_int("OF_GATE_METRICS_MIN_IDLE_MS", 60000)
        self.of_gate_dlq = env("OF_GATE_METRICS_DLQ_STREAM", "stream:dlq:of_gate_metrics")
        self.of_gate_auto_migrate = env_bool("OF_GATE_METRICS_AUTO_MIGRATE", False)
        self.of_gate_rollups_auto_migrate = env_bool("OF_GATE_METRICS_ROLLUPS_AUTO_MIGRATE", False)

        # P3: OF-gate quarantine consumer config
        self.of_gate_q_cg = env("OF_GATE_QUARANTINE_CG", "of_gate_quarantine_archiver")
        self.of_gate_q_consumer = env("OF_GATE_QUARANTINE_CONSUMER", "archiver_of_gate_q_1")
        self.of_gate_q_batch = env_int("OF_GATE_QUARANTINE_BATCH", 5000)
        self.of_gate_q_block_ms = env_int("OF_GATE_QUARANTINE_BLOCK_MS", 1000)
        self.of_gate_q_min_idle = env_int("OF_GATE_QUARANTINE_MIN_IDLE_MS", 60000)
        self.of_gate_q_dlq = env("OF_GATE_QUARANTINE_DLQ_STREAM", "stream:dlq:of_gate_quarantine")
        self.of_gate_q_auto_migrate = env_bool("OF_GATE_QUARANTINE_AUTO_MIGRATE", False)

        # P78: Redis hash keys for archiver status (Prometheus exporter reads these)
        # Best-effort writes: written after each batch, never block the hot path.
        self.of_gate_archiver_metrics_key = env(
            "OF_GATE_ARCHIVER_METRICS_KEY", "metrics:of_gate_metrics_archiver"
        )
        self.of_gate_q_archiver_metrics_key = env(
            "OF_GATE_QUARANTINE_ARCHIVER_METRICS_KEY", "metrics:of_gate_quarantine_archiver"
        )

        # OF gate consumer config
        self.of_gate_cg = env("OF_GATE_METRICS_CG", "of_gate_metrics_archiver")
        self.of_gate_consumer = env("OF_GATE_METRICS_CONSUMER", "archiver_of_gate_1")
        self.of_gate_batch = env_int("OF_GATE_METRICS_BATCH", 5000)
        self.of_gate_block_ms = env_int("OF_GATE_METRICS_BLOCK_MS", 1000)
        self.of_gate_min_idle = env_int("OF_GATE_METRICS_MIN_IDLE_MS", 60000)
        self.of_gate_dlq = env("OF_GATE_METRICS_DLQ_STREAM", "stream:dlq:of_gate_metrics")

        self.of_gate_quarantine_cg = env("OF_GATE_QUARANTINE_CG", "of_gate_quarantine_archiver")
        self.of_gate_quarantine_consumer = env("OF_GATE_QUARANTINE_CONSUMER", "archiver_of_gate_q_1")
        self.of_gate_quarantine_batch = env_int("OF_GATE_QUARANTINE_BATCH", 5000)
        self.of_gate_quarantine_block_ms = env_int("OF_GATE_QUARANTINE_BLOCK_MS", 1000)
        self.of_gate_quarantine_min_idle = env_int("OF_GATE_QUARANTINE_MIN_IDLE_MS", 60000)
        self.of_gate_quarantine_dlq = env("OF_GATE_QUARANTINE_DLQ_STREAM", "stream:dlq:of_gate_quarantine")

        self.of_gate_auto_migrate = env_bool("OF_GATE_METRICS_AUTO_MIGRATE", False)
        self.of_gate_quarantine_auto_migrate = env_bool("OF_GATE_QUARANTINE_AUTO_MIGRATE", False)

        # Status hashes for exporter
        self.of_gate_status_hash = env("OF_GATE_METRICS_ARCHIVER_STATUS_HASH", "metrics:of_gate_metrics_archiver")
        self.of_gate_quarantine_status_hash = env(
            "OF_GATE_QUARANTINE_ARCHIVER_STATUS_HASH", "metrics:of_gate_quarantine_archiver"
        )

        # Schema acceptance (rollback hook)
        self.conf_schema_accepted = parse_int_set(env("CONF_SCHEMA_VERSION_ACCEPTED", "1"), {1})

        # Event type filter (empty = all events)
        types_csv = env("POSITION_EVENTS_TYPES", "")
        self.events_types = {t.strip() for t in types_csv.split(",") if t.strip()}

    async def ensure_group(self, stream: str, group: str) -> None:
        """Create consumer group if not exists."""
        import redis.exceptions
        while True:
            try:
                await self.r.xgroup_create(name=stream, groupname=group, id="0-0", mkstream=True)
                break
            except (redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError) as e:
                print(f"⏳ Redis is loading or unavailable, waiting to create group {group} for {stream}... ({e})")
                import asyncio
                await asyncio.sleep(2.0)
                continue
            except Exception as e:
                err_str = str(e).upper()
                if "BUSYGROUP" in err_str:
                    break
                if "LOADING" in err_str:
                    print(f"⏳ Redis is loading, waiting to create group {group} for {stream}...")
                    import asyncio
                    await asyncio.sleep(2.0)
                    continue
                raise

    async def dlq(self, dlq_stream: str, stream: str, stream_id: str, err: str, payload: dict[str, Any]) -> None:
        """Write failed message to Dead Letter Queue"""
        await self.r.xadd(
            dlq_stream,
            {
                "stream": stream,
                "stream_id": stream_id,
                "err": err[:500],
                "payload": json.dumps(payload, ensure_ascii=False)[:4000],
            },
            maxlen=200000,
            approximate=True,
        )

    # ------------------------------------------------------------------
    # P78: best-effort Redis hash status reporter for Prometheus exporter
    # ------------------------------------------------------------------

    async def _bump_archiver_metrics(
        self,
        key: str,
        last_stream_id: str,
        inserted: int,
        errors: int,
    ) -> None:
        """Write last-run status to Redis hash (best-effort, non-blocking).

        Fields written:
          last_run_ts_ms: current epoch ms
          last_stream_id: last processed Redis stream ID
          archival_lag_seconds: seconds between now and last_stream_id timestamp
          inserted_total: cumulative inserts (HINCRBY)
          error_total:    cumulative errors (HINCRBY)
        """
        try:
            now_ms = int(dt.datetime.now(tz=dt.UTC).timestamp() * 1000)
            # Compute archival lag from stream ID timestamp (ms part before '-')
            lag_seconds = 0.0
            try:
                stream_ts_ms = int(last_stream_id.split("-", 1)[0])
                lag_seconds = max(0.0, (now_ms - stream_ts_ms) / 1000.0)
            except (ValueError, IndexError):
                pass
            pipe = self.r.pipeline()
            pipe.hset(key, mapping={
                "last_run_ts_ms": now_ms,
                "last_stream_id": last_stream_id,
                "archival_lag_seconds": f"{lag_seconds:.1f}",
            })
            if inserted:
                pipe.hincrby(key, "inserted_total", inserted)
            if errors:
                pipe.hincrby(key, "error_total", errors)
            await pipe.execute()
        except Exception:
            # P78: best-effort — never fail the archiver due to metrics reporting
            pass

    def entry_row(self, stream_id: str, payload: dict[str, Any]) -> tuple[Any, ...]:
        """Parse entry_policy_audit payload into DB row"""
        ts_ms = coalesce_ts_ms(payload, stream_id)
        ts = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=dt.UTC)

        decision = (
            payload.get("decision")
            or payload.get("result")
            or payload.get("policy_decision")
            or "UNKNOWN"
        )
        return (
            stream_id, ts_ms, ts,
            payload.get("sid"),
            payload.get("symbol"),
            payload.get("tf"),
            payload.get("strategy"),
            payload.get("source"),
            str(decision),
            payload.get("arm") or payload.get("ab_arm"),
            payload.get("group") or payload.get("ab_group"),
            payload.get("scenario"),
            payload.get("regime"),
            safe_float(payload.get("of_confirm_score")),
            safe_float(payload.get("coh")),
            safe_float(payload.get("leader_conf") or payload.get("leader_conf_score")),
            safe_float(payload.get("spread_z")),
            safe_float(payload.get("pressure_sps")),
            safe_int(payload.get("obi_age_ms")),
            json.dumps(payload, ensure_ascii=False),
        )

    def event_row(self, stream_id: str, payload: dict[str, Any]) -> tuple[Any, ...]:
        """Parse position_events payload into DB row."""
        ts_ms = coalesce_ts_ms(payload, stream_id)
        ts = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=dt.UTC)

        position_id = payload.get("position_id") or payload.get("order_id")
        event_type = (payload.get("event_type") or "UNKNOWN")
        meta_json = parse_meta_json(payload.get("meta"))

        return (
            stream_id, ts_ms, ts,
            position_id,
            payload.get("sid"),
            payload.get("symbol"),
            event_type,
            json.dumps(meta_json, ensure_ascii=False) if meta_json is not None else None,
            json.dumps(payload, ensure_ascii=False),
        )

    def conf_score_row(self, stream_id: str, payload: dict[str, Any]) -> tuple[Any, ...]:
        """Parse signals:confidence:scores payload into DB row."""
        ts_ms = coalesce_ts_ms(payload, stream_id)
        ts = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=dt.UTC)

        schema_version = safe_int(payload.get("schema_version")) or 1
        if schema_version not in self.conf_schema_accepted:
            raise ValueError(f"schema_version_not_accepted:{schema_version}")

        sid = payload.get("sid") or payload.get("signal_id") or ""
        symbol = payload.get("symbol") or ""
        producer = payload.get("producer") or payload.get("service") or "unknown"

        conf_raw = safe_float(payload.get("confidence_raw"))
        conf_final = safe_float(payload.get("confidence_final"))
        if conf_raw is None:
            conf_raw = safe_float(payload.get("confidence")) or 0.0
        if conf_final is None:
            conf_final = safe_float(payload.get("confidence"))

        evidence_map = payload.get("evidence_map")
        if evidence_map is None:
            evidence_map = payload.get("evidence")
        if not isinstance(evidence_map, dict):
            evidence_map = {}

        ctx = payload.get("context") if isinstance(payload.get("context"), dict) else None
        if ctx is None and self.conf_scores_store_context:
            # Keep a small subset to avoid bloating JSONB
            ctx = {
                "market_mode": payload.get("market_mode"),
                "data_health": payload.get("data_health"),
                "tf": payload.get("tf"),
                "session": payload.get("session"),
            }

        return (
            stream_id, ts_ms, ts,
            str(sid), symbol,
            int(schema_version), str(producer),
            float(conf_raw), float(conf_final) if conf_final is not None else None,
            json.dumps(evidence_map, ensure_ascii=False) if self.conf_scores_store_evidence else "{}",
            json.dumps(ctx, ensure_ascii=False) if (ctx is not None) else None,
        )

    # ------------------------------------------------------------------
    # P3: OF-gate per-event row builders
    # ------------------------------------------------------------------

    def of_gate_row(self, stream_id: str, payload: dict[str, Any]) -> tuple[Any, ...]:
        """Parse metrics:of_gate payload into of_gate_metrics DB row (per-event archival).

        P3 schema: one row per evaluation event (ok/ok_soft/reason_code/missing_legs).
        Deterministic timestamp: payload.ts_ms -> payload.ts_event_ms -> stream_id ms.
        """
        ts_ms = coalesce_ts_ms(payload, stream_id)
        ts = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=dt.UTC)

        symbol = (payload.get("symbol") or "")
        scenario_v4 = str(payload.get("scenario_v4") or payload.get("scenario") or "na")
        schema_version = safe_int(payload.get("schema_version")) or 1

        ok = safe_int(payload.get("ok"))
        ok_soft = safe_int(payload.get("ok_soft"))
        ok = 0 if ok is None else ok
        ok_soft = 0 if ok_soft is None else ok_soft

        # missing_legs: normalize to JSON-serializable form
        missing_legs = payload.get("missing_legs")
        if isinstance(missing_legs, str):
            try:
                missing_legs_json = json.loads(missing_legs)
            except Exception:
                missing_legs_json = {"_raw": missing_legs[:2000]}
        else:
            missing_legs_json = missing_legs if isinstance(missing_legs, (dict, list)) else None

        reason_code = str(payload.get("reason_code") or payload.get("reason") or "na")

        return (
            stream_id, ts_ms, ts,
            symbol, scenario_v4,
            int(schema_version),
            int(ok), int(ok_soft),
            json.dumps(missing_legs_json, ensure_ascii=False) if missing_legs_json is not None else None,
            reason_code,
            json.dumps(payload, ensure_ascii=False),
        )

    def of_gate_quarantine_row(self, source_stream: str, stream_id: str, payload: dict[str, Any]) -> tuple[Any, ...]:
        """Parse quarantined:metrics:of_gate payload into of_gate_metrics_quarantine row.

        DQ-flagged (dirty) rows are archived separately to avoid polluting ok_rate rollups.
        The dq_code field identifies why the row was quarantined (schema mismatch, bad timestamp, etc).
        """
        ts_ms = coalesce_ts_ms(payload, stream_id)
        ts = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=dt.UTC)

        symbol = payload.get("symbol")
        scenario_v4 = payload.get("scenario_v4") or payload.get("scenario")
        schema_version = safe_int(payload.get("schema_version"))
        ok = safe_int(payload.get("ok"))
        ok_soft = safe_int(payload.get("ok_soft"))

        # DQ code: reason why this row was quarantined
        dq_code = str(
            payload.get("dq_code")
            or payload.get("dq_why")
            or payload.get("why")
            or payload.get("err")
            or "dq_unknown"
        )
        err = payload.get("err")
        err = str(err)[:500] if err is not None else None

        return (
            stream_id, ts_ms, ts,
            source_stream,
            symbol if symbol is not None else None,
            str(scenario_v4) if scenario_v4 is not None else None,
            int(schema_version) if schema_version is not None else None,
            int(ok) if ok is not None else None,
            int(ok_soft) if ok_soft is not None else None,
            dq_code[:120],
            err,
            json.dumps(payload, ensure_ascii=False),
        )



    async def _status_bump(self, key: str, inserted_delta: int, last_stream_ts_ms: int, err: str | None = None) -> None:
        try:
            now_ms = get_ny_time_millis()
            p = self.r.pipeline()
            p.hset(key, mapping={
                "last_run_ts_ms": str(now_ms),
                "last_stream_ts_ms": str(last_stream_ts_ms),
                "ok": "0" if err else "1",
            })
            if inserted_delta:
                p.hincrby(key, "inserted_total", int(inserted_delta))
            if err:
                p.hincrby(key, "error_total", 1)
                p.hset(key, "last_error", str(err)[:200])
            await p.execute()
        except Exception:
            return

    async def _read_new(self, stream: str, group: str, consumer: str, count: int, block_ms: int):
        try:
            return await self.r.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=count,
                block=block_ms,
            )
        except Exception as e:
            if "NOGROUP" in str(e).upper():
                print(f"⚠️ NOGROUP error detected, recreating consumer group {group} for stream {stream}...")
                await self.ensure_group(stream, group)
                return []
            raise

    async def _claim_pending(self, stream: str, group: str, consumer: str, min_idle_ms: int, count: int):
        try:
            _next_start, msgs, _deleted = await self.r.xautoclaim(
                name=stream,
                groupname=group,
                consumername=consumer,
                min_idle_time=min_idle_ms,
                start_id="0-0",
                count=count,
            )
            return msgs
        except Exception as e:
            if "NOGROUP" in str(e).upper():
                print(f"⚠️ NOGROUP error detected in xautoclaim, recreating consumer group {group} for stream {stream}...")
                await self.ensure_group(stream, group)
            return []

    async def consume_entry_audit(self) -> None:
        await self.ensure_group(self.entry_stream, self.entry_cg)
        loop = asyncio.get_running_loop()

        while True:
            pending = await self._claim_pending(
                self.entry_stream,
                self.entry_cg,
                self.entry_consumer,
                self.entry_min_idle,
                self.entry_batch,
            )
            if pending:
                msgs = pending
            else:
                resp = await self._read_new(
                    self.entry_stream,
                    self.entry_cg,
                    self.entry_consumer,
                    self.entry_batch,
                    self.entry_block_ms,
                )
                if not resp:
                    continue
                _, msgs = resp[0]

            rows: list[tuple[Any, ...]] = []
            ack_ids: list[str] = []

            for mid, fields in msgs:
                try:
                    payload = parse_stream_payload(fields)
                    rows.append(self.entry_row(mid, payload))
                    ack_ids.append(mid)
                except Exception as e:
                    await self.dlq(self.entry_dlq, self.entry_stream, mid, f"parse_error:{e}", {"fields": fields})
                    await self.r.xack(self.entry_stream, self.entry_cg, mid)

            if not rows:
                continue

            try:
                await loop.run_in_executor(None, self.pg.insert_entry_audit, rows)
                await self.r.xack(self.entry_stream, self.entry_cg, *ack_ids)
            except Exception:
                # We intentionally DO NOT send batch errors to DLQ (they are connection errors / poison pill batches)
                # Instead, we let the batch stay un-acked in standard PEL, to be reclaimed on next run using XAUTOCLAIM.
                await asyncio.sleep(1.0)

    async def consume_position_events(self) -> None:
        await self.ensure_group(self.events_stream, self.events_cg)
        loop = asyncio.get_running_loop()

        while True:
            pending = await self._claim_pending(
                self.events_stream,
                self.events_cg,
                self.events_consumer,
                self.events_min_idle,
                self.events_batch,
            )
            if pending:
                msgs = pending
            else:
                resp = await self._read_new(
                    self.events_stream,
                    self.events_cg,
                    self.events_consumer,
                    self.events_batch,
                    self.events_block_ms,
                )
                if not resp:
                    continue
                _, msgs = resp[0]

            rows: list[tuple[Any, ...]] = []
            ack_ids: list[str] = []

            for mid, fields in msgs:
                try:
                    payload = parse_stream_payload(fields)
                    et = (payload.get("event_type") or "UNKNOWN")
                    if self.events_types and et not in self.events_types:
                        await self.r.xack(self.events_stream, self.events_cg, mid)
                        continue
                    rows.append(self.event_row(mid, payload))
                    ack_ids.append(mid)
                except Exception as e:
                    await self.dlq(self.events_dlq, self.events_stream, mid, f"parse_error:{e}", {"fields": fields})
                    await self.r.xack(self.events_stream, self.events_cg, mid)

            if not rows:
                continue

            try:
                await loop.run_in_executor(None, self.pg.insert_position_events, rows)
                await self.r.xack(self.events_stream, self.events_cg, *ack_ids)
            except Exception:
                # We intentionally DO NOT send batch errors to DLQ (they are connection errors / poison pill batches)
                # Instead, we let the batch stay un-acked in standard PEL, to be reclaimed on next run using XAUTOCLAIM.
                await asyncio.sleep(1.0)

    async def consume_confidence_scores(self) -> None:
        await self.ensure_group(self.conf_scores_stream, self.conf_scores_cg)
        loop = asyncio.get_running_loop()

        while True:
            pending = await self._claim_pending(
                self.conf_scores_stream,
                self.conf_scores_cg,
                self.conf_scores_consumer,
                self.conf_scores_min_idle,
                self.conf_scores_batch,
            )
            if pending:
                msgs = pending
            else:
                resp = await self._read_new(
                    self.conf_scores_stream,
                    self.conf_scores_cg,
                    self.conf_scores_consumer,
                    self.conf_scores_batch,
                    self.conf_scores_block_ms,
                )
                if not resp:
                    continue
                _, msgs = resp[0]

            rows: list[tuple[Any, ...]] = []
            ack_ids: list[str] = []

            for mid, fields in msgs:
                try:
                    payload = parse_stream_payload(fields)
                    rows.append(self.conf_score_row(mid, payload))
                    ack_ids.append(mid)
                except Exception as e:
                    await self.dlq(self.conf_scores_dlq, self.conf_scores_stream, mid, f"parse_error:{e}", {"fields": fields})
                    await self.r.xack(self.conf_scores_stream, self.conf_scores_cg, mid)

            if not rows:
                continue

            try:
                await loop.run_in_executor(None, self.pg.insert_signal_confidence_scores, rows)
                await self.r.xack(self.conf_scores_stream, self.conf_scores_cg, *ack_ids)
            except Exception:
                # We intentionally DO NOT send batch errors to DLQ (they are connection errors / poison pill batches)
                # Instead, we let the batch stay un-acked in standard PEL, to be reclaimed on next run using XAUTOCLAIM.
                await asyncio.sleep(1.0)

    # ------------------------------------------------------------------
    # P3: OF-gate metrics + quarantine consumers
    # P78: status bumped to Redis hashes after each batch for Prometheus exporter
    # ------------------------------------------------------------------
        if (self.of_gate_enabled or self.of_gate_quarantine_enabled) and (self.of_gate_auto_migrate or self.of_gate_quarantine_auto_migrate):
            await loop.run_in_executor(None, self.pg.ensure_of_gate_tables)

    async def consume_of_gate_metrics(self) -> None:
        """Consume metrics:of_gate stream and archive per-event rows to of_gate_metrics table.

        P3: per-event archival (one row per ok/ok_soft evaluation).
        P78: writes last_run_ts_ms / inserted_total / error_total to Redis hash
             OF_GATE_ARCHIVER_METRICS_KEY for the Prometheus exporter (best-effort).

        Uses XREADGROUP consumer group with XAUTOCLAIM for PEL recovery.
        Failed rows go to DLQ (never block the hot path).
        Enable: OF_GATE_METRICS_ARCHIVE_ENABLED=1
        """
        await self.ensure_group(self.of_gate_stream, self.of_gate_cg)
        loop = asyncio.get_running_loop()

        while True:
            pending = await self._claim_pending(
                self.of_gate_stream,
                self.of_gate_cg,
                self.of_gate_consumer,
                self.of_gate_min_idle,
                self.of_gate_batch,
            )
            if pending:
                msgs = pending
            else:
                resp = await self._read_new(
                    self.of_gate_stream,
                    self.of_gate_cg,
                    self.of_gate_consumer,
                    self.of_gate_batch,
                    self.of_gate_block_ms,
                )
                if not resp:
                    continue
                _, msgs = resp[0]

            rows: list[tuple[Any, ...]] = []
            ack_ids: list[str] = []
            parse_errors = 0
            last_seen_mid = ""

            for mid, fields in msgs:
                last_seen_mid = mid
                try:
                    payload = parse_stream_payload(fields)
                    rows.append(self.of_gate_row(mid, payload))
                    ack_ids.append(mid)
                except Exception as e:
                    parse_errors += 1
                    await self.dlq(self.of_gate_dlq, self.of_gate_stream, mid, f"parse_error:{e}", {"fields": fields})
                    await self.r.xack(self.of_gate_stream, self.of_gate_cg, mid)

            if not rows:
                # P78: bump errors even if no rows inserted (all parse-failed)
                if last_seen_mid and parse_errors:
                    await self._bump_archiver_metrics(
                        self.of_gate_archiver_metrics_key, last_seen_mid, inserted=0, errors=parse_errors
                    )
                continue

            try:
                await loop.run_in_executor(None, self.pg.insert_of_gate_metrics, rows)
                await self.r.xack(self.of_gate_stream, self.of_gate_cg, *ack_ids)
                # P78: bump success metrics
                await self._bump_archiver_metrics(
                    self.of_gate_archiver_metrics_key,
                    ack_ids[-1] if ack_ids else last_seen_mid,
                    inserted=len(rows),
                    errors=parse_errors,
                )
            except Exception:
                parse_errors += 1
                # We intentionally DO NOT send batch errors to DLQ (they are connection errors / poison pill batches)
                # Instead, we let the batch stay un-acked in standard PEL, to be reclaimed on next run using XAUTOCLAIM.
                # P78: bump error metrics on DB failure
                await self._bump_archiver_metrics(
                    self.of_gate_archiver_metrics_key,
                    ack_ids[-1] if ack_ids else last_seen_mid,
                    inserted=0,
                    errors=1,
                )
                await asyncio.sleep(1.0)

    async def consume_of_gate_quarantine(self) -> None:
        """Consume quarantined:metrics:of_gate stream and archive rows to of_gate_metrics_quarantine.

        P3: DQ (dirty/quarantined) rows archived separately from clean metrics.
            This isolates bad data from ok_rate rollups.
        P78: writes status to Redis hash OF_GATE_QUARANTINE_ARCHIVER_METRICS_KEY (best-effort).

        Enable: OF_GATE_QUARANTINE_ARCHIVE_ENABLED=1
        """
        await self.ensure_group(self.of_gate_q_stream, self.of_gate_q_cg)
        loop = asyncio.get_running_loop()

        while True:
            pending = await self._claim_pending(
                self.of_gate_q_stream,
                self.of_gate_q_cg,
                self.of_gate_q_consumer,
                self.of_gate_q_min_idle,
                self.of_gate_q_batch,
            )
            if pending:
                msgs = pending
            else:
                resp = await self._read_new(
                    self.of_gate_q_stream,
                    self.of_gate_q_cg,
                    self.of_gate_q_consumer,
                    self.of_gate_q_batch,
                    self.of_gate_q_block_ms,
                )
                if not resp:
                    continue
                _, msgs = resp[0]

            rows: list[tuple[Any, ...]] = []
            ack_ids: list[str] = []
            parse_errors = 0
            last_seen_mid = ""

            for mid, fields in msgs:
                last_seen_mid = mid
                try:
                    payload = parse_stream_payload(fields)
                    rows.append(self.of_gate_quarantine_row(self.of_gate_q_stream, mid, payload))
                    ack_ids.append(mid)
                except Exception as e:
                    parse_errors += 1
                    await self.dlq(self.of_gate_q_dlq, self.of_gate_q_stream, mid, f"parse_error:{e}", {"fields": fields})
                    await self.r.xack(self.of_gate_q_stream, self.of_gate_q_cg, mid)

            if not rows:
                # P78: bump errors even if no rows inserted (all parse-failed)
                if last_seen_mid and parse_errors:
                    await self._bump_archiver_metrics(
                        self.of_gate_q_archiver_metrics_key, last_seen_mid, inserted=0, errors=parse_errors
                    )
                continue

            try:
                await loop.run_in_executor(None, self.pg.insert_of_gate_metrics_quarantine, rows)
                await self.r.xack(self.of_gate_q_stream, self.of_gate_q_cg, *ack_ids)
                # P78: bump success metrics
                await self._bump_archiver_metrics(
                    self.of_gate_q_archiver_metrics_key,
                    ack_ids[-1] if ack_ids else last_seen_mid,
                    inserted=len(rows),
                    errors=parse_errors,
                )
            except Exception:
                parse_errors += 1
                # We intentionally DO NOT send batch errors to DLQ (they are connection errors / poison pill batches)
                # Instead, we let the batch stay un-acked in standard PEL, to be reclaimed on next run using XAUTOCLAIM.
                # P78: bump error metrics on DB failure
                await self._bump_archiver_metrics(
                    self.of_gate_q_archiver_metrics_key,
                    ack_ids[-1] if ack_ids else last_seen_mid,
                    inserted=0,
                    errors=1,
                )
                await asyncio.sleep(1.0)


    def post_sl_liqmap_kpi_row(self, stream_id, payload):
        ts_ms = coalesce_ts_ms(payload, stream_id)
        import datetime as dt
        import json
        ts = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=dt.UTC)
        trade_id = str(payload.get("trade_id") or payload.get("id") or "").strip()
        symbol = (payload.get("symbol") or "").strip().upper()
        side = (payload.get("side") or "").strip().upper()
        regime = str(payload.get("regime") or payload.get("market_regime") or "unknown").strip().lower()
        if not trade_id or not symbol or not side:
            raise ValueError(f"missing_required_fields trade_id={trade_id} symbol={symbol} side={side}")
        liqmap_kpi = {}
        for k, v in payload.items():
            if isinstance(k, str) and k.startswith("liqmap_"):
                liqmap_kpi[k] = v
        for k in (
            "sl_hit_near_liqmap_peak", "sl_liqmap_peak_dist_bps", "sl_liqmap_peak_usd",
            "tp1_anchored", "tp1_anchored_and_hit", "liqmap_levels_applied",
            "liqmap_tp1_adj_bps", "liqmap_sl_adj_bps", "liqmap_levels_reason"
        ):
            if k in payload:
                liqmap_kpi[k] = payload.get(k)
        sl_hit = safe_int(payload.get("sl_hit_near_liqmap_peak"))
        tp1_anchored = safe_int(payload.get("tp1_anchored"))
        tp1_hit = safe_int(payload.get("tp1_anchored_and_hit"))
        sl_peak_dist_bps = safe_float(payload.get("sl_liqmap_peak_dist_bps"))
        sl_peak_usd = safe_float(payload.get("sl_liqmap_peak_usd"))
        return (
            stream_id, ts_ms, ts, trade_id, symbol, side, regime,
            sl_hit, tp1_anchored, tp1_hit, sl_peak_dist_bps, sl_peak_usd,
            json.dumps(liqmap_kpi, ensure_ascii=False), json.dumps(payload, ensure_ascii=False)
        )

    async def consume_post_sl_liqmap_kpi(self) -> None:
        import asyncio
        await self.ensure_group(self.post_sl_stream, self.post_sl_liqmap_cg)
        loop = asyncio.get_running_loop()
        while True:
            pending = await self._claim_pending(
                self.post_sl_stream, self.post_sl_liqmap_cg, self.post_sl_liqmap_consumer,
                self.post_sl_liqmap_min_idle, self.post_sl_liqmap_batch)
            if pending:
                msgs = pending
            else:
                resp = await self._read_new(
                    self.post_sl_stream, self.post_sl_liqmap_cg, self.post_sl_liqmap_consumer,
                    self.post_sl_liqmap_batch, self.post_sl_liqmap_block_ms)
                if not resp:
                    continue
                _, msgs = resp[0]
            rows = []
            ack_ids = []
            for mid, fields in msgs:
                try:
                    payload = parse_stream_payload(fields)
                    rows.append(self.post_sl_liqmap_kpi_row(mid, payload))
                    ack_ids.append(mid)
                except Exception as e:
                    await self.dlq(self.post_sl_liqmap_dlq, self.post_sl_stream, mid, f"parse_error:{e}", {"fields": str(fields)[:2000]})
                    await self.r.xack(self.post_sl_stream, self.post_sl_liqmap_cg, mid)
            if not rows:
                continue
            try:
                await loop.run_in_executor(None, self.pg.insert_trade_kpi_liqmap_v1, rows)
                await self.r.xack(self.post_sl_stream, self.post_sl_liqmap_cg, *ack_ids)
            except Exception:
                # We intentionally DO NOT send batch errors to DLQ (they are connection errors / poison pill batches)
                # Instead, we let the batch stay un-acked in standard PEL, to be reclaimed on next run using XAUTOCLAIM.
                import asyncio
                await asyncio.sleep(1.0)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()

        # Retry loop for initial DDL and Postgres readiness check
        for attempt in range(6):
            try:
                # Auto-DDL for existing streams
                if self.conf_scores_enabled and self.conf_scores_auto_migrate:
                    await loop.run_in_executor(None, self.pg.ensure_signal_confidence_scores_table)

                # P3: Auto-DDL for of_gate_metrics / quarantine tables
                if (self.of_gate_enabled or self.of_gate_quarantine_enabled) and (
                    self.of_gate_auto_migrate or self.of_gate_quarantine_auto_migrate
                ):
                    await loop.run_in_executor(None, self.pg.ensure_of_gate_metrics_tables)

                # P3: Auto-DDL for Timescale CAGG rollups (only if explicitly enabled)
                if self.of_gate_enabled and self.of_gate_rollups_auto_migrate:
                    await loop.run_in_executor(None, self.pg.ensure_of_gate_metrics_rollups_timescale)

                if self.post_sl_liqmap_enabled and self.post_sl_liqmap_auto_migrate:
                    await loop.run_in_executor(None, self.pg.ensure_trade_kpi_liqmap_v1_table)

                break # Success
            except Exception as e:
                err_str = str(e).lower()
                if "could not translate host name" in err_str or "connection refused" in err_str:
                    if attempt < 5:
                        print(f"⏳ Postgres is unavailable, waiting before executing DDL... ({e})")
                        await asyncio.sleep(2.0)
                        continue
                raise


        tasks = []

        if self.post_sl_liqmap_enabled:
            tasks.append(safe_create_task(self.consume_post_sl_liqmap_kpi()))

        if self.entry_enabled:
            tasks.append(safe_create_task(self.consume_entry_audit()))
        if self.events_enabled:
            tasks.append(safe_create_task(self.consume_position_events()))
        if self.conf_scores_enabled:
            tasks.append(safe_create_task(self.consume_confidence_scores()))
        if self.of_gate_enabled:
            tasks.append(safe_create_task(self.consume_of_gate_metrics()))
        if self.of_gate_quarantine_enabled:
            tasks.append(safe_create_task(self.consume_of_gate_quarantine()))

        if not tasks:
            raise RuntimeError("No archivers enabled via ENV")
        await asyncio.gather(*tasks)


async def main() -> None:
    redis_url = env("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True)

    dsn = pick_dsn()
    if not dsn:
        raise RuntimeError("Missing ARCHIVER_PG_DSN/TRADES_DB_DSN/DATABASE_URL/PG_DSN")
    pg = PgWriter(PgCfg(dsn=dsn))

    svc = StreamArchiver(r, pg)
    await svc.run()


if __name__ == "__main__":
    asyncio.run(main())
