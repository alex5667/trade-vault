from __future__ import annotations

import json
from collections import defaultdict

# core/local_calibration.py
from dataclasses import dataclass

from .sessions import get_session_from_ts

# Типы данных
ClusterKey = tuple[str, str, str]  # (symbol, session, regime)
MetricName = str  # "delta_spike_z", "obi", "weak_progress", etc.

@dataclass
class SignalRow:
    """Строка данных сигнала для калибровки"""
    symbol: str
    session: str
    regime: str
    ts_utc: float
    delta_spike_z: float | None = None
    obi: float | None = None
    weak_progress: float | None = None
    atr_quantile: float | None = None
    pnl_r: float | None = None
    hit_tp: bool | None = None

@dataclass
class MetricCalibration:
    """Результаты калибровки для одной метрики в одном кластере"""
    q90: float
    q95: float
    q98: float
    chosen_threshold: float
    cdf_points: list[dict[str, float]]  # [{"value": float, "q": float}, ...]
    bucket_count: int

@dataclass
class ClusterCalibration:
    """Калибровка для одного кластера (symbol, session, regime)"""
    metrics: dict[MetricName, MetricCalibration]
    sample_count: int

class LocalCalibrationManager:
    """
    Менеджер локальной калибровки порогов по кластерам (symbol, session, regime).
    """

    def __init__(self):
        self.calibrations: dict[ClusterKey, ClusterCalibration] = {}
        self.min_cluster_samples = 300  # минимальное количество сигналов в кластере
        self.min_bucket_samples = 30    # минимальное количество сигналов в бакете

    def load_from_database(self, db_connection, lookback_days: int = 365) -> None:
        """
        Загружает данные из базы и строит калибровки.
        """
        rows = self._load_signal_rows(db_connection, lookback_days)
        clusters = self._build_clusters(rows)

        for cluster_key, cluster_rows in clusters.items():
            if len(cluster_rows) < self.min_cluster_samples:
                continue  # недостаточно данных

            # Хронологический сплит для предотвращения label peeking
            cluster_rows_sorted = sorted(cluster_rows, key=lambda r: r.ts_utc)
            split_idx = int(len(cluster_rows_sorted) * 0.8)
            train_rows = cluster_rows_sorted[:split_idx]
            val_rows = cluster_rows_sorted[split_idx:]

            # Калибровка исключительно на train данных
            calibration = self._calibrate_cluster(train_rows)
            self.calibrations[cluster_key] = calibration

    def _load_signal_rows(self, conn, lookback_days: int) -> list[SignalRow]:
        """
        Загружает строки сигналов из базы данных.
        Это упрощенная реализация - в реальности нужно использовать SQLAlchemy или psycopg2.
        """
        # Пример SQL запроса (адаптируйте под вашу схему)
        query = f"""
        SELECT
            symbol,
            ts as ts_utc,
            pattern_label,
            regime,
            delta_spike_z,
            obi,
            weak_progress,
            atr_quantile,
            pnl_r,
            hit_tp
        FROM signals
        WHERE ts >= NOW() - INTERVAL '{lookback_days} days'
        AND pnl_r IS NOT NULL  -- только завершенные сделки
        ORDER BY ts
        """

        # В реальности используйте ваш ORM/соединение
        # cursor = conn.cursor()
        # cursor.execute(query)
        # rows = cursor.fetchall()

        # Для демонстрации возвращаем пустой список
        # В реальной реализации здесь будет разбор результатов запроса
        return []

    def _build_clusters(self, rows: list[SignalRow]) -> dict[ClusterKey, list[SignalRow]]:
        """Группирует сигналы по кластерам (symbol, session, regime)"""
        clusters: dict[ClusterKey, list[SignalRow]] = defaultdict(list)

        for row in rows:
            # Если session/regime не заполнены, вычисляем
            if not row.session:
                row.session = get_session_from_ts(row.ts_utc)

            if not row.regime:
                # Для простоты используем заглушку
                # В реальности нужно использовать ваши метрики
                row.regime = "mixed"  # или вычислить на основе доступных данных

            key = (row.symbol, row.session, row.regime)
            clusters[key].append(row)

        return clusters

    def _calibrate_cluster(self, rows: list[SignalRow]) -> ClusterCalibration:
        """Выполняет калибровку для одного кластера"""
        metrics = {}

        # Калибровка delta_spike_z
        if any(r.delta_spike_z is not None for r in rows):
            delta_z_values = [r.delta_spike_z for r in rows if r.delta_spike_z is not None]
            pnl_values = [r.pnl_r or 0.0 for r in rows if r.delta_spike_z is not None]
            metrics["delta_spike_z"] = self._calibrate_metric(delta_z_values, pnl_values)

        # Аналогично для других метрик...
        # if any(r.obi is not None for r in rows):
        #     obi_values = [r.obi for r in rows if r.obi is not None]
        #     metrics["obi"] = self._calibrate_metric(obi_values, pnl_values)

        return ClusterCalibration(
            metrics=metrics,
            sample_count=len(rows)
        )

    def _calibrate_metric(self, metric_values: list[float], pnl_values: list[float]) -> MetricCalibration:
        """Калибрует одну метрику"""
        if not metric_values:
            return MetricCalibration(0, 0, 0, 0, [], 0)

        # Квантили
        q90 = self._quantile(metric_values, 0.9)
        q95 = self._quantile(metric_values, 0.95)
        q98 = self._quantile(metric_values, 0.98)

        # Бакеты по performance
        buckets = self._bucket_by_performance(metric_values, pnl_values)

        # Выбор оптимального порога
        chosen_threshold = self._choose_threshold_from_buckets(buckets)

        # CDF для локальных квантилей
        cdf_points = self._build_empirical_cdf(metric_values)

        return MetricCalibration(
            q90=q90,
            q95=q95,
            q98=q98,
            chosen_threshold=chosen_threshold,
            cdf_points=cdf_points,
            bucket_count=len(buckets)
        )

    def _quantile(self, xs: list[float], q: float) -> float:
        """Вычисляет квантиль"""
        if not xs:
            return 0.0
        xs_sorted = sorted(xs)
        k = int(q * (len(xs_sorted) - 1))
        return xs_sorted[k]

    def _bucket_by_performance(self, xs: list[float], ys: list[float], num_buckets: int = 5) -> list[dict]:
        """
        Разбивает на бакеты по значению метрики и считает средний результат в каждом бакете.
        """
        if len(xs) != len(ys):
            return []

        pairs = sorted(zip(xs, ys), key=lambda p: p[0])
        n = len(pairs)
        bucket_size = max(1, n // num_buckets)

        buckets = []
        for i in range(0, n, bucket_size):
            chunk = pairs[i : i + bucket_size]
            if not chunk:
                continue

            xs_chunk = [x for x, _ in chunk]
            ys_chunk = [y for _, y in chunk]

            buckets.append({
                "q_low": xs_chunk[0],
                "q_high": xs_chunk[-1],
                "mean_y": sum(ys_chunk) / len(ys_chunk),
                "count": len(xs_chunk),
            })

        return buckets

    def _choose_threshold_from_buckets(self, buckets: list[dict]) -> float:
        """Выбирает оптимальный порог из бакетов по performance"""
        if not buckets:
            return 0.0

        # Ищем бакеты с положительным результатом и достаточным количеством сделок
        candidates = [
            b for b in buckets
            if b["count"] >= self.min_bucket_samples and b["mean_y"] > 0
        ]

        if not candidates:
            # Если нет хороших бакетов, берем 95-й квантиль
            return max((b["q_high"] for b in buckets), default=0.0)

        # Выбираем бакет с максимальным q_low (самый "правый" хвост)
        best = max(candidates, key=lambda b: b["q_low"])
        return best["q_low"]

    def _build_empirical_cdf(self, xs: list[float], num_points: int = 101) -> list[dict[str, float]]:
        """Строит эмпирическую CDF для вычисления локальных квантилей"""
        if not xs:
            return []

        xs_sorted = sorted(xs)
        n = len(xs_sorted)

        points = []
        for i in range(num_points):
            q = i / (num_points - 1)
            k = int(q * (n - 1))
            points.append({
                "value": xs_sorted[k],
                "q": q
            })

        return points

    def get_calibration(self, symbol: str, session: str, regime: str) -> ClusterCalibration | None:
        """Получает калибровку для конкретного кластера"""
        key = (symbol, session, regime)
        return self.calibrations.get(key)

    def get_metric_calibration(self, symbol: str, session: str, regime: str, metric: str) -> MetricCalibration | None:
        """Получает калибровку для конкретной метрики в кластере"""
        cluster = self.get_calibration(symbol, session, regime)
        if cluster:
            return cluster.metrics.get(metric)
        return None

    def eval_local_quantile(self, cdf_points: list[dict[str, float]], x: float) -> float:
        """
        Вычисляет локальный квантиль для значения x используя CDF.
        """
        if not cdf_points:
            return 0.5  # нейтральное значение

        # Сортируем по value
        pts = sorted(cdf_points, key=lambda p: p["value"])

        # Граничные случаи
        if x <= pts[0]["value"]:
            return 0.0
        if x >= pts[-1]["value"]:
            return 1.0

        # Линейная интерполяция
        for i in range(1, len(pts)):
            if x <= pts[i]["value"]:
                x0, q0 = pts[i-1]["value"], pts[i-1]["q"]
                x1, q1 = pts[i]["value"], pts[i]["q"]

                if x1 == x0:
                    return q1

                t = (x - x0) / (x1 - x0)
                return q0 + t * (q1 - q0)

        return 1.0

    def save_to_json(self, filepath: str) -> None:
        """Сохраняет калибровки в JSON файл"""
        data = {}
        for key, calibration in self.calibrations.items():
            key_str = f"{key[0]}_{key[1]}_{key[2]}"
            data[key_str] = {
                "sample_count": calibration.sample_count,
                "metrics": {
                    metric_name: {
                        "q90": cal.q90,
                        "q95": cal.q95,
                        "q98": cal.q98,
                        "chosen_threshold": cal.chosen_threshold,
                        "cdf_points": cal.cdf_points,
                        "bucket_count": cal.bucket_count,
                    }
                    for metric_name, cal in calibration.metrics.items()
                }
            }

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

    def load_from_json(self, filepath: str) -> None:
        """Загружает калибровки из JSON файла"""
        with open(filepath) as f:
            data = json.load(f)

        for key_str, cal_data in data.items():
            symbol, session, regime = key_str.split('_', 2)
            key = (symbol, session, regime)

            metrics = {}
            for metric_name, metric_data in cal_data["metrics"].items():
                metrics[metric_name] = MetricCalibration(
                    q90=metric_data["q90"],
                    q95=metric_data["q95"],
                    q98=metric_data["q98"],
                    chosen_threshold=metric_data["chosen_threshold"],
                    cdf_points=metric_data["cdf_points"],
                    bucket_count=metric_data["bucket_count"],
                )

            self.calibrations[key] = ClusterCalibration(
                metrics=metrics,
                sample_count=cal_data["sample_count"]
            )
