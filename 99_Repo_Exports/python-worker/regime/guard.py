from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg2
import redis
from psycopg2 import pool

from .models import BaselineQuantiles
from .rolling_stats import RollingWindowStats
from .state import RegimeState, Status

Key = tuple[str, str, str, str]  # (venue, symbol, timeframe, family)


@dataclass
class BaselineConfig:
    hit_rate: BaselineQuantiles
    expectancy_r: BaselineQuantiles


class RegimeGuardService:
    """
    Online-контроль по signal family:
      - rolling expectancy_R, hit-rate, drawdown по R;
      - сравнение с baseline-квантилями;
      - смена режима: active/degraded/disabled;
      - запись состояния в Timescale + Redis.
    """

    def __init__(
        self,
        pg_dsn: str,
        redis_dsn: str,
        window_size: int = 100,
        baseline_horizon_days: int = 180,
        disable_dd_mult: float = 1.5,      # во сколько раз dd хуже лимита — сразу disable
        degrade_dd_mult: float = 1.0,      # при каком уровне — degraded
        wr_safe_margin: float = 0.05,      # гистерезис по winrate
    ):
        self.pg_dsn = pg_dsn
        self.pg_pool = pool.ThreadedConnectionPool(1, 5, pg_dsn)
        self.redis = redis.from_url(redis_dsn, decode_responses=True)

        self.window_size = window_size
        self.baseline_horizon_days = baseline_horizon_days
        self.disable_dd_mult = disable_dd_mult
        self.degrade_dd_mult = degrade_dd_mult
        self.wr_safe_margin = wr_safe_margin

        self._stats: dict[Key, RollingWindowStats] = defaultdict(
            lambda: RollingWindowStats(window_size=window_size)
        )
        self._state: dict[Key, RegimeState] = {}

        self._baseline_cache: dict[Key, BaselineConfig] = {}

        # Internal logger
        import logging
        self.logger = logging.getLogger("regime_guard")

        # Новые поля для работы с baseline
        self.baseline_window_size = window_size  # используем то же окно
        self.baseline_horizon_days = baseline_horizon_days if 'baseline_horizon_days' in locals() else 180

    def _safe_read(self, query: str, params: tuple) -> tuple | None:
        """Safely execute a read query with rollback on error."""
        conn = None
        try:
            conn = self.pg_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"DB Read Error: {e}")
            return None
        finally:
            if conn:
                self.pg_pool.putconn(conn)

    def _safe_fetchall(self, query: str, params: tuple) -> list:
        """Safely execute a read query expecting multiple rows."""
        conn = None
        try:
            conn = self.pg_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"DB ReadAll Error: {e}")
            return []
        finally:
            if conn:
                self.pg_pool.putconn(conn)

    def _safe_write(self, query: str, params: tuple) -> None:
        """Safely execute a write query with commit/rollback."""
        conn = None
        try:
            conn = self.pg_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(query, params)
            conn.commit()
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"DB Write Error: {e}")
        finally:
            if conn:
                self.pg_pool.putconn(conn)

    # ---------- базовый key ----------

    @staticmethod
    def make_key(venue: str, symbol: str, timeframe: str, family: str) -> Key:
        return venue, symbol, timeframe, family

    # ---------- загрузка baseline из Timescale ----------

    def _load_baseline(self, key: Key) -> BaselineConfig | None:
        if key in self._baseline_cache:
            return self._baseline_cache[key]

        venue, symbol, timeframe, family = key

        # Загружаем обе метрики для данного family
        hit_rate = self._load_baseline_metric(symbol, family, "hit_rate")
        expectancy_r = self._load_baseline_metric(symbol, family, "expectancy_R")

        if not hit_rate or not expectancy_r:
            return None

        cfg = BaselineConfig(
            hit_rate=hit_rate,
            expectancy_r=expectancy_r,
        )
        self._baseline_cache[key] = cfg
        return cfg

    def _load_baseline_metric(self, symbol: str, family: str, metric: str) -> BaselineQuantiles | None:
        """Загружает квантили для одной метрики."""
        query = """
            SELECT p05, p10, p25, p50, p75, p90, p95, sample_size
            FROM signal_family_baseline
            WHERE symbol = %s AND family = %s AND metric = %s
              AND window_size = %s AND horizon_days = %s
        """
        params = (symbol, family, metric, self.window_size, self.baseline_horizon_days)

        row = self._safe_read(query, params)

        if not row:
            return None

        return BaselineQuantiles(
            p05=row[0], p10=row[1], p25=row[2], p50=row[3],
            p75=row[4], p90=row[5], p95=row[6], sample_size=row[7]
        )

    # ---------- публичный API: вызывается из SignalPerformanceTracker ----------

    def on_signal_closed(
        self,
        *,
        signal_id: str,
        family: str,
        venue: str,
        symbol: str,
        timeframe: str,
        r_value: float,
        closed_at: datetime,
    ) -> Callable[[], None] | None:
        """
        Вызывается при фактическом завершении сигнала (SL/TP/ручное закрытие).
        r_value = pnl / risk (в R).
        """
        if closed_at.tzinfo is None:
            closed_at = closed_at.replace(tzinfo=UTC)

        key = self.make_key(venue, symbol, timeframe, family)

        stats = self._stats[key]
        stats.add(r_value)

        baseline = self._load_baseline(key)
        if baseline is None:
            # пока нет baseline — режим не трогаем
            return None

        if stats.trades < 10:  # минимальное количество сделок для анализа
            return None

        wr = stats.hitrate
        exp_r = stats.expectancy_r

        state = self._state.get(key) or RegimeState(
            family=family,
            venue=venue,
            symbol=symbol,
            timeframe=timeframe,
        )

        # простая логика на основе baseline:
        # 1) если wr < wr_p10 → degraded
        # 2) если exp_r < exp_r_p10 → degraded
        # 3) если всё вернулось в норму → active

        new_status: Status = state.status
        reason = state.reason
        threshold_mult = state.threshold_mult
        disable_until = state.disable_until

        # Проверяем отклонения от baseline
        wr_below_baseline = baseline.hit_rate.p10 is not None and wr < baseline.hit_rate.p10
        exp_r_below_baseline = baseline.expectancy_r.p10 is not None and exp_r < baseline.expectancy_r.p10

        if wr_below_baseline or exp_r_below_baseline:
            if state.status != "disabled":
                new_status = "degraded"
                reason_parts = []
                if wr_below_baseline:
                    reason_parts.append(f"wr={wr:.3f} < baseline_p10={baseline.hit_rate_p10:.3f}")
                if exp_r_below_baseline:
                    reason_parts.append(f"exp_r={exp_r:.3f} < baseline_p10={baseline.expectancy_r_p10:.3f}")
                reason = f"degraded: {'; '.join(reason_parts)}"
                disable_until = None
                threshold_mult = 1.5  # пороги детектора *1.5
        else:
            # всё нормально, можно вернуть в active
            if state.status != "active":
                new_status = "active"
                reason = "metrics back to normal"
                disable_until = None
                threshold_mult = 1.0

        state.wr_window = wr
        state.exp_r_window = exp_r
        state.dd_r_window = 0.0  # больше не используем dd_r
        state.trades_window = stats.trades
        state.status = new_status
        state.reason = reason
        state.threshold_mult = threshold_mult
        state.disable_until = disable_until

        self._state[key] = state

        return self.get_persist_task(key, state, closed_at)

    # ---------- запись состояния в Timescale + Redis ----------

    def get_persist_task(self, key: Key, state: RegimeState, ts_state: datetime) -> Callable[[], None]:
        """Возвращает функцию для исполнения в ThreadPoolExecutor (non-blocking)"""
        def _task():
            try:
                self._persist_state_change_sync(key, state, ts_state)
            except Exception as e:
                self.logger.error(f"Async persist failed for {key}: {e}")
        return _task

    def _persist_state_change_sync(self, key: Key, state: RegimeState, ts_state: datetime) -> None:
        """
        Для простоты: пишем в БД и кладём актуальное состояние в Redis (как KV),
        чтобы ExecutionPlanner/детекторы могли читать быстрый снапшот.
        """
        venue, symbol, timeframe, family = key

        # Timescale
        query = """
            INSERT INTO signal_family_regime_state (
                ts_state, family, venue, symbol, timeframe,
                status, wr_window, exp_r_window, dd_r_window, trades_window,
                reason, disable_until, threshold_mult
            )
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s)
        """
        params = (
            ts_state,
            family,
            venue,
            symbol,
            timeframe,
            state.status,
            state.wr_window,
            state.exp_r_window,
            state.dd_r_window,
            state.trades_window,
            state.reason,
            state.disable_until,
            state.threshold_mult,
        )
        self._safe_write(query, params)

        # Redis — быстрый runtime-снапшот
        key_redis = f"signals:regime_state:{venue}:{symbol}:{timeframe}:{family}"
        value = {
            "status": state.status,
            "wr_window": state.wr_window,
            "exp_r_window": state.exp_r_window,
            "dd_r_window": state.dd_r_window,
            "trades_window": state.trades_window,
            "threshold_mult": state.threshold_mult,
            "disable_until": state.disable_until.isoformat() if state.disable_until else None,
        }
        self.redis.set(key_redis, json.dumps(value))

    # ---------- публичный API для чтения baseline ----------

    def load_baseline_for_family(
        self,
        symbol: str,
        family: str,
        window_size: int = 50,
        horizon_days: int = 180,
    ) -> dict[str, BaselineQuantiles]:
        """
        Читает baseline по двум метрикам для данного symbol+family.

        Returns:
            Словарь с BaselineQuantiles для 'hit_rate' и 'expectancy_R'
        """
        from .baseline_calc import BaselineQuantiles

        result: dict[str, BaselineQuantiles] = {}

        query = """
            SELECT metric,
                   p05, p10, p25, p50, p75, p90, p95,
                   sample_size
            FROM signal_family_baseline
            WHERE symbol = %s
              AND family = %s
              AND window_size = %s
              AND horizon_days = %s
              AND metric IN ('hit_rate', 'expectancy_R')
        """
        params = (symbol, family, window_size, horizon_days)

        rows = self._safe_fetchall(query, params)
        for row in rows:
                metric, p05, p10, p25, p50, p75, p90, p95, sample_size = row
                result[metric] = BaselineQuantiles(
                    p05=p05,
                    p10=p10,
                    p25=p25,
                    p50=p50,
                    p75=p75,
                    p90=p90,
                    p95=p95,
                    sample_size=sample_size,
                )
        return result
