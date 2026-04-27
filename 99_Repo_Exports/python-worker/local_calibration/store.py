# local_calibration/store.py
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Dict, List, Tuple

# Optional import for psycopg2
try:
    import psycopg2
    from psycopg2.extras import DictCursor
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    # Stub classes for when psycopg2 is not available
    class psycopg2:
        @staticmethod
        def connect(*args, **kwargs):
            raise ImportError("psycopg2 not available")

    class DictCursor:
        pass


ClusterMetricKey = Tuple[str, str, str, str]  # (symbol, session, regime, metric)


@dataclass
class ClusterMetricCfg:
    q90: float
    q95: float
    q98: float
    threshold: float
    cdf_points: List[dict]
    count_samples: int


class LocalCalibrationStore:
    """
    In-memory кэш калибровки.
    Можно обновлять при старте сервиса и периодически (hot reload).
    """

    def __init__(self) -> None:
        self._cfg: Dict[ClusterMetricKey, ClusterMetricCfg] = {}

    def load_from_db(self, dsn: str) -> None:
        """Загружает калибровку из базы данных"""
        if not PSYCOPG2_AVAILABLE:
            print("⚠️ psycopg2 not available, skipping database load")
            return
        conn = psycopg2.connect(dsn)
        try:
            sql = """
                SELECT
                    symbol,
                    session,
                    regime,
                    metric,
                    q90,
                    q95,
                    q98,
                    chosen_threshold,
                    count_samples,
                    cdf_points
                FROM signal_local_calibration
            """
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(sql)
                data: Dict[ClusterMetricKey, ClusterMetricCfg] = {}
                for r in cur:
                    key: ClusterMetricKey = (
                        r["symbol"],
                        r["session"],
                        r["regime"],
                        r["metric"],
                    )
                    cdf_points = r["cdf_points"]
                    if isinstance(cdf_points, str):
                        cdf_points = json.loads(cdf_points)

                    data[key] = ClusterMetricCfg(
                        q90=float(r["q90"]) if r["q90"] is not None else float("nan"),
                        q95=float(r["q95"]) if r["q95"] is not None else float("nan"),
                        q98=float(r["q98"]) if r["q98"] is not None else float("nan"),
                        threshold=float(r["chosen_threshold"])
                        if r["chosen_threshold"] is not None
                        else float("nan"),
                        cdf_points=list(cdf_points or []),
                        count_samples=int(r["count_samples"]),
                    )
            self._cfg = data
            print(f"Loaded {len(data)} calibration entries from database")
        finally:
            conn.close()

    def get_metric_cfg(
        self,
        symbol: str,
        session: str,
        regime: str,
        metric: str,
    ) -> ClusterMetricCfg | None:
        """Получает конфигурацию метрики для кластера"""
        key: ClusterMetricKey = (symbol, session, regime, metric)
        cfg = self._cfg.get(key)
        if cfg is not None:
            return cfg

        # fallback 1: без режима
        key2: ClusterMetricKey = (symbol, session, "mixed", metric)
        cfg = self._cfg.get(key2)
        if cfg is not None:
            return cfg

        # fallback 2: без сессии/режима
        key3: ClusterMetricKey = (symbol, "mixed", "mixed", metric)
        return self._cfg.get(key3)

    def get_cluster_metrics(
        self,
        symbol: str,
        session: str,
        regime: str,
    ) -> Dict[str, ClusterMetricCfg]:
        """Получает все метрики для кластера"""
        metrics = {}
        for metric in ["delta_spike_z", "obi", "weak_progress", "atr_quantile"]:
            cfg = self.get_metric_cfg(symbol, session, regime, metric)
            if cfg:
                metrics[metric] = cfg
        return metrics

    def is_empty(self) -> bool:
        """Проверяет, загружена ли калибровка"""
        return len(self._cfg) == 0


def eval_local_quantile(cdf_points: List[dict], x: float) -> float:
    """
    Вычисляет локальный квантиль по эмпирической CDF.
    cdf_points: [{value, q}, ...] отсортированные по value.
    """
    if not cdf_points:
        return float("nan")

    pts = sorted(cdf_points, key=lambda p: p["value"])
    if x <= pts[0]["value"]:
        return 0.0
    if x >= pts[-1]["value"]:
        return 1.0

    for i in range(1, len(pts)):
        if x <= pts[i]["value"]:
            x0 = pts[i - 1]["value"]
            q0 = pts[i - 1]["q"]
            x1 = pts[i]["value"]
            q1 = pts[i]["q"]
            if x1 == x0:
                return float(q1)
            t = (x - x0) / (x1 - x0)
            return float(q0 + t * (q1 - q0))
    return 1.0


# Глобальный инстанс для использования в сервисах
_global_store = LocalCalibrationStore()


def get_global_calibration_store() -> LocalCalibrationStore:
    """Получает глобальный инстанс стора калибровки"""
    return _global_store


def refresh_global_calibration(dsn: str) -> None:
    """Обновляет глобальную калибровку из базы данных"""
    _global_store.load_from_db(dsn)
