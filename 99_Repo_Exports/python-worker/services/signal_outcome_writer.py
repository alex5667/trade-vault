# services/signal_outcome_writer.py
"""
SignalOutcomeWriter — двойная персистенция signal outcomes.

Пишет SignalOutcome записи в:
  1) Redis Stream `signals:outcomes` — для real-time downstream consumers
  2) TimescaleDB `signal_outcomes` — для аналитики и ML-тренировки

Fail-open: ни один вызов emit/persist не должен роняться — только WARNING.
Вызывается из trade_monitor через _db_executor.submit() (background thread).
"""
from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any, Optional

log = logging.getLogger("signal_outcome_writer")

# Configurable via ENV
_REDIS_STREAM_KEY = os.getenv("SIGNAL_OUTCOMES_STREAM", "signals:outcomes")
_REDIS_STREAM_MAXLEN = int(os.getenv("SIGNAL_OUTCOMES_STREAM_MAXLEN", "100000"))
_DB_ENABLED = os.getenv("SIGNAL_OUTCOMES_DB_ENABLED", "1") == "1"

# Singleton
_instance: Optional["SignalOutcomeWriter"] = None
_lock = threading.Lock()


class SignalOutcomeWriter:
    """Двойная персистенция signal outcomes: Redis Stream + TimescaleDB."""

    def __init__(self) -> None:
        self._redis = None  # lazy
        self._dsn: str = ""
        self._db_pool = None  # lazy

    def _get_redis(self):
        """Lazy init Redis — берём из core.redis_client (тот же пул что trade_monitor)."""
        if self._redis is None:
            try:
                from core.redis_client import get_redis
                self._redis = get_redis()
            except Exception as e:
                log.warning("signal_outcome_writer: Redis init failed: %s", e)
        return self._redis

    def _get_dsn(self) -> str:
        """Lazy DSN — берём из analytics_db (тот же DSN что trades_closed)."""
        if not self._dsn:
            self._dsn = os.getenv(
                "TRADES_DB_DSN"
                "postgresql://postgres:postgres@localhost:5432/scanner_analytics"
            )
        return self._dsn

    # ------------------------------------------------------------------
    # Redis Stream
    # ------------------------------------------------------------------
    def emit_to_redis(self, outcome: Any) -> bool:
        """
        XADD в signals:outcomes stream.

        Args:
            outcome: SignalOutcome с методом .to_dict()

        Returns:
            True если XADD успешен, False при ошибке.
        """
        try:
            r = self._get_redis()
            if r is None:
                return False

            data = outcome.to_dict()
            r.xadd(
                _REDIS_STREAM_KEY
                data
                maxlen=_REDIS_STREAM_MAXLEN
                approximate=True
            )
            return True
        except Exception as e:
            log.warning("⚠️ signal_outcome XADD failed (fail-open): %s", e)
            return False

    # ------------------------------------------------------------------
    # TimescaleDB
    # ------------------------------------------------------------------
    def persist_to_db(self, outcome: Any) -> bool:
        """
        INSERT в signal_outcomes таблицу (TimescaleDB hypertable).

        ON CONFLICT (order_id) DO NOTHING — идемпотентно.

        Returns:
            True если INSERT успешен, False при ошибке.
        """
        if not _DB_ENABLED:
            return False

        if not getattr(self, "_schema_checked", False):
            try:
                from services.analytics_db import get_conn
                with get_conn() as conn, conn.cursor() as cur:
                    # Check if columns exists before attempting to add
                    # This avoids some potential lock issues or noise in logs
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name='signal_outcomes' AND column_name='trace_id';
                    """)
                    if not cur.fetchone():
                        log.info("signal_outcome_writer: adding trace_id column to signal_outcomes")
                        cur.execute("ALTER TABLE signal_outcomes ADD COLUMN IF NOT EXISTS trace_id TEXT DEFAULT '';")
                    
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name='signal_outcomes' AND column_name='event_id';
                    """)
                    if not cur.fetchone():
                        log.info("signal_outcome_writer: adding event_id column to signal_outcomes")
                        cur.execute("ALTER TABLE signal_outcomes ADD COLUMN IF NOT EXISTS event_id TEXT DEFAULT '';")
                    
                    conn.commit()
                    self._schema_checked = True
            except Exception as e:
                log.warning("⚠️ signal_outcome DB auto-migration failed: %s", e)
                # We do NOT set _schema_checked to True here so we can retry on next insert
                # But we should avoid spamming logs, maybe a small backoff or flag is needed
                # For now, just not setting the flag is enough to retry.

        sql = """
            INSERT INTO signal_outcomes (
                ts, sid, order_id, symbol, strategy, source, tf, direction
                entry_price, entry_ts_ms, sl, tp1_price, atr, entry_tag, regime, scenario
                exit_price, exit_ts_ms, pnl_net, pnl_gross, fees
                r_multiple, one_r_money, risk_usd
                close_reason
                tp1_hit, tp2_hit, tp3_hit
                trailing_started, trailing_active, trailing_moves, duration_ms
                mfe_pnl, mae_pnl, giveback, missed_profit
                is_virtual, meta_enforce_cov_bucket, trace_id, event_id
            ) VALUES (
                to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s, %s
                %s, %s, %s, %s, %s, %s, %s, %s
                %s, %s, %s, %s, %s
                %s, %s, %s
                %s
                %s, %s, %s
                %s, %s, %s, %s
                %s, %s, %s, %s
                %s, %s, %s, %s
            )
            ON CONFLICT (order_id, ts) DO NOTHING
        """

        params = (
            outcome.exit_ts_ms, outcome.sid, outcome.order_id, outcome.symbol
            outcome.strategy, outcome.source, outcome.tf, outcome.direction
            outcome.entry_price, outcome.entry_ts_ms
            outcome.sl, outcome.tp1_price, outcome.atr
            outcome.entry_tag, outcome.regime, outcome.scenario
            outcome.exit_price, outcome.exit_ts_ms
            outcome.pnl_net, outcome.pnl_gross, outcome.fees
            outcome.r_multiple, outcome.one_r_money, outcome.risk_usd
            outcome.close_reason
            outcome.tp1_hit, outcome.tp2_hit, outcome.tp3_hit
            outcome.trailing_started, outcome.trailing_active
            outcome.trailing_moves, outcome.duration_ms
            outcome.mfe_pnl, outcome.mae_pnl
            outcome.giveback, outcome.missed_profit
            outcome.is_virtual, outcome.meta_enforce_cov_bucket
            getattr(outcome, "trace_id", ""), getattr(outcome, "event_id", "")
        )

        try:
            from services.analytics_db import get_conn
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()
            return True
        except Exception as e:
            log.warning("⚠️ signal_outcome DB persist failed (fail-open): %s", e)
            return False

    # ------------------------------------------------------------------
    # Combined
    # ------------------------------------------------------------------
    def emit(self, outcome: Any) -> None:
        """
        Emit outcome to both Redis Stream and TimescaleDB.

        Fail-open: каждый канал обрабатывается независимо.
        Никогда не бросает исключение.
        """
        try:
            redis_ok = self.emit_to_redis(outcome)
            if redis_ok:
                log.debug(
                    "✅ signal_outcome emitted to Redis | sid=%s r_mult=%.2f is_win=%s"
                    outcome.sid, outcome.r_multiple, outcome.is_win
                )
        except Exception as e:
            log.warning("⚠️ signal_outcome Redis emit unexpected error: %s", e)

        try:
            db_ok = self.persist_to_db(outcome)
            if db_ok:
                log.debug(
                    "✅ signal_outcome persisted to DB | sid=%s order_id=%s"
                    outcome.sid, outcome.order_id
                )
        except Exception as e:
            log.warning("⚠️ signal_outcome DB persist unexpected error: %s", e)


def get_signal_outcome_writer() -> SignalOutcomeWriter:
    """Singleton accessor (thread-safe, lazy init)."""
    global _instance
    if _instance is None:
        with _lock:
            if _instance is None:
                _instance = SignalOutcomeWriter()
                log.info(
                    "✅ SignalOutcomeWriter initialized | stream=%s maxlen=%d db_enabled=%s"
                    _REDIS_STREAM_KEY, _REDIS_STREAM_MAXLEN, _DB_ENABLED
                )
    return _instance
