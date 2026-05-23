from __future__ import annotations

import logging
from datetime import UTC

import psycopg2
import psycopg2.extras

from .signal_snapshot import SignalSnapshot


class SignalLogger:
    """
    Логирование сигналов с L3-метриками в TimescaleDB.
    """

    def __init__(self, dsn: str, min_pool_size: int = 1, max_pool_size: int = 10):
        self.dsn = dsn
        self.pool = psycopg2.pool.SimpleConnectionPool(
            min_pool_size, max_pool_size, dsn
        )
        self.logger = logging.getLogger("SignalLogger")

    def log_signal(self, snapshot: SignalSnapshot) -> bool:
        """
        Логировать сигнал в базу данных.
        """
        try:
            conn = self.pool.getconn()
            try:
                with conn.cursor() as cur:
                    self._insert_signal(cur, snapshot)
                conn.commit()
                self.logger.debug(f"Logged signal {snapshot.signal_id}")
                return True
            finally:
                self.pool.putconn(conn)
        except Exception as e:
            self.logger.error(f"Failed to log signal {snapshot.signal_id}: {e}")
            return False

    def _insert_signal(self, cur, snapshot: SignalSnapshot) -> None:
        """Вставка сигнала в базу данных (таблица signals)."""

        # Преобразуем snapshot в dict
        data = snapshot.to_dict()

        # Map snapshot fields to signals table schema
        # Required fields: signal_id, ts_signal, symbol, side, setup_type, price_at_signal, final_score
        try:
            import uuid
            from datetime import datetime

            # Generate signal_id if not present
            signal_id = data.get('signal_id') or str(uuid.uuid4())

            # Parse timestamp
            ts_signal = data.get('ts') or data.get('ts_signal') or datetime.now(UTC)
            if isinstance(ts_signal, (int, float)):
                # Convert epoch ms to datetime
                ts_signal = datetime.fromtimestamp(ts_signal / 1000.0, tz=UTC)

            # Extract required fields
            symbol = data.get('symbol', 'UNKNOWN')
            side = data.get('side') or data.get('direction', 'UNKNOWN')
            setup_type = data.get('setup_type') or data.get('signal_family') or data.get('kind', 'unknown')
            price_at_signal = float(data.get('price') or data.get('entry') or data.get('price_at_signal', 0.0))
            final_score = float(data.get('final_score') or data.get('confidence', 0.0))

            # Optional fields
            meta = data.get('meta', {})
            def _get_f(key, fallback_key=None, fallback_meta=None):
                val = data.get(key)
                if val is not None: return val
                if fallback_key and data.get(fallback_key) is not None: return data.get(fallback_key)
                if fallback_meta and meta.get(fallback_meta) is not None: return meta.get(fallback_meta)
                if meta.get(key) is not None: return meta.get(key)
                return None

            def _first_not_none(*keys):
                for k in keys:
                    v = _get_f(k)
                    if v is not None:
                        return v
                return None

            # atr_1m: data keys may be 'atr_1m', 'atr_14' (from SignalSnapshot.to_dict), or 'atr'
            atr_1m = _first_not_none('atr_1m', 'atr_14', 'atr')
            # atr_5m: data key 'atr_5m' only
            atr_5m = _get_f('atr_5m')
            session = data.get('session')
            regime = data.get('regime')
            # delta_spike_z: direct key or fallback to delta_z
            delta_spike_z = _first_not_none('delta_spike_z', 'delta_z')
            # obi: SignalSnapshot.to_dict() produces 'obi_avg_20'; fallback to 'obi' or 'obi_avg'
            obi = _first_not_none('obi', 'obi_avg_20', 'obi_avg')
            # weak_progress: SignalSnapshot.to_dict() produces 'weak_progress_ratio'
            weak_progress = _first_not_none('weak_progress', 'weak_progress_ratio')
            tick_size = _get_f('tick_size')
            contract_size = _get_f('contract_size')

            # Store full snapshot in raw_ctx
            raw_ctx = data

            sql = """
            INSERT INTO signals (
                signal_id,
                ts_signal,
                symbol,
                side,
                setup_type,
                price_at_signal,
                final_score,
                atr_1m,
                atr_5m,
                session,
                regime,
                delta_spike_z,
                obi,
                weak_progress,
                tick_size,
                contract_size,
                raw_ctx
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (signal_id) DO NOTHING;
            """

            cur.execute(sql, (
                signal_id,
                ts_signal,
                symbol,
                side.upper(),
                setup_type,
                price_at_signal,
                final_score,
                atr_1m,
                atr_5m,
                session,
                regime,
                delta_spike_z,
                obi,
                weak_progress,
                tick_size,
                contract_size,
                psycopg2.extras.Json(raw_ctx)
            ))
        except Exception as e:
            self.logger.error(f"Failed to insert signal {data.get('signal_id', 'unknown')}: {e}")
            raise

    def get_recent_signals(
        self,
        symbol: str | None = None,
        family: str | None = None,
        limit: int = 100
    ) -> list[dict]:
        """
        Получить недавние сигналы для анализа.
        """
        try:
            conn = self.pool.getconn()
            try:
                with conn.cursor() as cur:
                    conditions = ["ts_signal >= now() - interval '30 days'"]
                    params = []

                    if symbol:
                        conditions.append("symbol = %s")
                        params.append(symbol)

                    if family:
                        conditions.append("setup_type = %s")
                        params.append(family)

                    sql = f"""
                    SELECT * FROM signals
                    WHERE {' AND '.join(conditions)}
                    ORDER BY ts_signal DESC
                    LIMIT %s
                    """

                    params.append(limit)
                    cur.execute(sql, params)

                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()

                    return [dict(zip(columns, row)) for row in rows]
            finally:
                self.pool.putconn(conn)
        except Exception as e:
            self.logger.error(f"Failed to fetch recent signals: {e}")
            return []

    def cleanup_old_signals(self, days_to_keep: int = 90) -> int:
        """
        Очистить старые сигналы (старше days_to_keep дней).
        Возвращает количество удаленных записей.
        """
        try:
            conn = self.pool.getconn()
            try:
                with conn.cursor() as cur:
                    sql = """
                    DELETE FROM signals
                    WHERE ts_signal < now() - interval '%s days'
                    """

                    cur.execute(sql, (days_to_keep,))
                    deleted_count = cur.rowcount
                    conn.commit()

                    self.logger.info(f"Cleaned up {deleted_count} old signals")
                    return deleted_count
            finally:
                self.pool.putconn(conn)
        except Exception as e:
            self.logger.error(f"Failed to cleanup old signals: {e}")
            return 0
