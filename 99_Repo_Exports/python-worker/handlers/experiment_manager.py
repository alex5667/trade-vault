# handlers/experiment_manager.py

import time
import hashlib
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Literal, Any

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
    RealDictCursor = None

# Logger stub if common.log is missing
try:
    from common.log import logger
except ImportError:
    import logging
    logger = logging.getLogger("experiment_manager")

ExperimentStatus = Literal["draft", "running", "stopped", "completed"]
Variant = Literal["control", "treatment"]


@dataclass
class ExperimentSpec:
    """
    Спецификация эксперимента из базы данных
    """
    experiment_id: str
    filter_name: str
    signal_family: str
    direction: int        # +1/-1/0
    status: ExperimentStatus
    start_at_ms: int
    end_at_ms: Optional[int]
    target_metric: str
    config: Dict[str, Any]

    def is_active_for(self, now_ms: int, signal_family: str, direction: int) -> bool:
        """
        Проверяет, активен ли эксперимент для данного сигнала
        """
        if self.status != "running":
            return False
        if signal_family != self.signal_family:
            return False
        if self.direction != 0 and self.direction != int(direction):
            return False
        if now_ms < self.start_at_ms:
            return False
        if self.end_at_ms is not None and now_ms > self.end_at_ms:
            return False
        return True


class ExperimentManager:
    """
    Runtime-слой для A/B-экспериментов:

    - грузит активные эксперименты из Postgres
    - назначает вариант (control/treatment) по детерминированному хэшу
    - отдаёт info для логирования и применения фильтров.
    """

    def __init__(
        self
        pg_dsn: Optional[str] = None
        reload_interval_sec: int = 30
        logger=None
    ) -> None:
        """
        Args:
            pg_dsn: PostgreSQL DSN string. If None, uses PG_DSN env var
            reload_interval_sec: как часто перезагружать эксперименты из БД
            logger: логгер для отладки
        """
        self.pg_dsn = pg_dsn or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))

        self.reload_interval_sec = reload_interval_sec
        self.logger = logger

        self._last_reload_ts = 0.0
        self._experiments: Dict[str, ExperimentSpec] = {}

        # Первая загрузка
        self._reload(force=True)

    # ---------- публичный API ----------

    def assign_variant(
        self
        *
        now_ms: int
        symbol: str
        signal_family: str
        direction: int
        signal_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Назначает вариант эксперимента для сигнала.

        Возвращает:
          {
            "experiment_id": str
            "variant": "control"|"treatment"
            "filter_name": str
            "config": {...}
          }
        или None, если для данного семейства нет активного эксперимента.
        """
        self._maybe_reload()

        # для простоты: считаем, что для одного family в момент времени максимум 1 эксперимент
        active = [
            e for e in self._experiments.values()
            if e.is_active_for(now_ms, signal_family, direction)
        ]
        if not active:
            return None

        if len(active) > 1 and self.logger:
            self.logger.warning(
                "Multiple active experiments for family=%s, taking first"
                signal_family
            )

        exp = active[0]

        variant = self._choose_variant(
            experiment_id=exp.experiment_id
            symbol=symbol
            signal_family=signal_family
            signal_id=signal_id
        )

        return {
            "experiment_id": exp.experiment_id
            "variant": variant
            "filter_name": exp.filter_name
            "config": exp.config or {}
        }

    def get_active_experiments(self) -> List[ExperimentSpec]:
        """
        Возвращает список всех активных экспериментов (для отладки)
        """
        self._maybe_reload()
        return list(self._experiments.values())

    # ---------- внутренняя кухня ----------

    def _choose_variant(
        self
        *
        experiment_id: str
        symbol: str
        signal_family: str
        signal_id: int
    ) -> Variant:
        """
        Детерминированный split 50/50 по хэшу.
        Можно расширить до [0.8 control, 0.2 treatment] через config.
        """
        key = f"{experiment_id}:{symbol}:{signal_family}:{signal_id}"
        h = hashlib.sha256(key.encode("utf-8")).hexdigest()
        v = int(h[:8], 16) / 0xFFFFFFFF  # 0..1

        # можно подвигать пропорции через config эксперимента:
        # exp_config = self._experiments.get(experiment_id, {}).config
        # p_treatment = exp_config.get("p_treatment", 0.5)
        p_treatment = 0.5

        return "treatment" if v < p_treatment else "control"

    def _maybe_reload(self) -> None:
        """
        Перезагружает эксперименты из БД если прошло достаточно времени
        """
        now = time.time()
        if now - self._last_reload_ts < self.reload_interval_sec:
            return
        self._reload(force=False)
        self._last_reload_ts = now

    def _reload(self, *, force: bool) -> None:
        """
        Загружает эксперименты из базы данных
        """
        if psycopg2 is None or not self.pg_dsn:
            # Заглушка - если нет psycopg2 или DSN, просто возвращаем пустые эксперименты
            self._experiments = {}
            return

        conn = psycopg2.connect(self.pg_dsn)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select experiment_id, filter_name, signal_family, direction
                           status, extract(epoch from start_at)*1000 as start_at_ms
                           extract(epoch from end_at)*1000 as end_at_ms
                           target_metric, coalesce(config, '{}'::jsonb) as config
                    from signal_experiment
                    where status in ('running', 'draft')
                    """
                )
                rows = cur.fetchall()

            new_map: Dict[str, ExperimentSpec] = {}
            for r in rows:
                new_map[r["experiment_id"]] = ExperimentSpec(
                    experiment_id=r["experiment_id"]
                    filter_name=r["filter_name"]
                    signal_family=r["signal_family"]
                    direction=int(r["direction"])
                    status=r["status"]
                    start_at_ms=int(r["start_at_ms"])
                    end_at_ms=int(r["end_at_ms"]) if r["end_at_ms"] is not None else None
                    target_metric=r["target_metric"]
                    config=r["config"]
                )

            self._experiments = new_map
            if self.logger:
                self.logger.info(
                    "ExperimentManager reloaded %d experiments", len(new_map)
                )
        except Exception as e:
            if self.logger:
                self.logger.error("Failed to reload experiments: %s", e)
            # В случае ошибки не перезаписываем _experiments, оставляем старые
        finally:
            conn.close()


# Глобальный инстанс для использования в handlers
_experiment_manager_instance: Optional[ExperimentManager] = None


def get_experiment_manager() -> ExperimentManager:
    """
    Возвращает глобальный инстанс ExperimentManager (singleton pattern)
    """
    global _experiment_manager_instance
    if _experiment_manager_instance is None:
        _experiment_manager_instance = ExperimentManager()
    return _experiment_manager_instance
