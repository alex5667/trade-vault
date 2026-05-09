import logging
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

class AIOpsAnomalyExtractor:
    """
    Извлекает робастные аномалии (MAD Z-Score) из TimescaleDB для контекста LLM.
    Hard-capped до 50 метрик для предотвращения числовых галлюцинаций в 14B моделях.
    """

    def __init__(self, db_engine: Engine, top_n: int = 50, mad_threshold: float = 3.0):
        self.db_engine = db_engine
        self.top_n = top_n
        self.mad_threshold = mad_threshold

    def get_llm_payload(self) -> dict[str, Any]:
        """
        Возвращает готовый для интеграции в prompt JSON-ready словарь.
        """
        anomalies = self._calculate_anomalies()

        # Формируем итоговый payload для DeepSeek 14
        return {
            "aiops_context": {
                "aggregation_window": "60m",
                "mad_threshold": self.mad_threshold,
                "anomalies_found": len(anomalies),
                "metrics": anomalies
            }
        }

    def _calculate_anomalies(self) -> list[dict[str, Any]]:
        # Оптимизированный SQL-запрос к TimescaleDB.
        # time_bucket агрегирует сырые данные на лету, снижая нагрузку на сеть и Python.
        # ВНИМАНИЕ: Если таблица называется иначе, замените 'operational_metrics' и названия колонок
        sql = """
            SELECT
                metric_name,
                time_bucket('1 minute', ts) AS bucket,
                AVG(metric_value) AS val
            FROM
                metrics_raw
            WHERE
                ts >= NOW() - INTERVAL '60 minutes'
            GROUP BY 1, 2
            ORDER BY 1, 2
        """

        try:
            df = pd.read_sql(sql, self.db_engine)
        except Exception as e:
            logger.error(f"Ошибка чтения метрик из Timescale: {e}")
            return []

        if df.empty:
            logger.warning("Нет данных за последний час. Система в дауне или нет сэмплов?")
            return []

        # Pivot таблицы. Строки - время (60 корзин), столбцы - метрики.
        # ffill (forward fill) заполняет пропуски.
        pivot_df = df.pivot(index='bucket', columns='metric_name', values='val').ffill().fillna(0)

        # Защита от слишком коротких рядов (может быть на старте)
        if len(pivot_df) < 5:
            logger.warning("Недостаточно данных для расчета MAD (менее 5 бакетов).")
            return []

        # Векторизованный подсчет Median и MAD (без deprecated методов Pandas)
        medians = pivot_df.median()
        mads = (pivot_df - medians).abs().median()

        # Приводим MAD к sigma
        eps = 1e-9
        adjusted_mads = np.where(mads == 0, eps, mads * 1.4826)

        # Текущее состояние (последний бакет)
        current_values = pivot_df.iloc[-1]

        # Z-Score на базе MAD
        z_scores = (current_values - medians) / adjusted_mads

        results = pd.DataFrame({
            'metric': z_scores.index,
            'current_val': current_values.values,
            'median_baseline': medians.values,
            'z_score': z_scores.values
        })

        # Отсекаем шум <= mad_threshold
        anomalies = results[results['z_score'].abs() > self.mad_threshold].copy()

        # Сортировка по тяжести и срез до top_n (50)
        anomalies['abs_z'] = anomalies['z_score'].abs()
        top_anomalies = anomalies.sort_values(by='abs_z', ascending=False).head(self.top_n)

        output = []
        for _, row in top_anomalies.iterrows():
            output.append({
                "metric_name": row['metric'],
                "state": "CRITICAL_SPIKE" if row['z_score'] > 0 else "CRITICAL_DROP",
                "deviation_sigma": round(row['z_score'], 2),
                "current_value": round(row['current_val'], 4),
                "normal_baseline": round(row['median_baseline'], 4)
            })

        return output
