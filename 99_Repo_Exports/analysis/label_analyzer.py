# -*- coding: utf-8 -*-
"""
LabelAnalyzer — инструмент для анализа собранных меток/сигналов.
Поддерживает:
- Загрузку из Parquet
- Статистический анализ
- Визуализацию (опционально)
- Экспорт отчётов
"""

import functools
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

try:
    import pyarrow.dataset as ds

    _HAS_ARROW = True
except ImportError:
    _HAS_ARROW = False

logger = logging.getLogger(__name__)


class LabelAnalyzer:
    """
    Анализатор меток/сигналов из Parquet хранилища.
    """

    def __init__(self, labels_dir: str = "/data/labels") -> None:
        self.labels_dir = labels_dir

        if not os.path.exists(labels_dir):
            raise ValueError(f"Labels directory not found: {labels_dir}")

        if not _HAS_ARROW:
            raise ImportError("PyArrow required for LabelAnalyzer")

    def load_labels(
        self,
        symbol: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        min_confidence: float | None = None,
    ) -> pd.DataFrame:
        """
        Загружает метки из Parquet с фильтрацией.

        Args:
            symbol: фильтр по символу (например, "XAUUSD")
            start_date: начальная дата в формате "YYYY-MM-DD"
            end_date: конечная дата в формате "YYYY-MM-DD"
            min_confidence: минимальная confidence

        Returns:
            DataFrame с метками
        """
        dataset = ds.dataset(self.labels_dir, format="parquet", partitioning="hive")

        # Строим список фильтров и объединяем через AND
        filters: list[Any] = []

        if symbol:
            filters.append(ds.field("symbol") == symbol)

        if start_date:
            filters.append(ds.field("date") >= start_date)

        if end_date:
            filters.append(ds.field("date") <= end_date)

        combined_filter = functools.reduce(lambda a, b: a & b, filters) if filters else None

        # Загружаем данные
        table = dataset.to_table(filter=combined_filter)
        df = table.to_pandas()

        if df.empty:
            return df

        # Десериализация JSON-полей
        for json_col in ("metrics", "tp_levels"):
            if json_col in df.columns:
                df[json_col] = df[json_col].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else x
                )

        # Фильтр по confidence
        if min_confidence is not None:
            df = df[df["confidence"] >= min_confidence]

        logger.debug("Loaded %d labels (filters: symbol=%s, start=%s, end=%s, min_conf=%s)",
                     len(df), symbol, start_date, end_date, min_confidence)
        return df

    def get_stats(self, df: pd.DataFrame) -> dict[str, Any]:
        """
        Вычисляет статистику по меткам.

        Args:
            df: DataFrame с метками

        Returns:
            Словарь со статистикой
        """
        if df.empty:
            return {"error": "No data"}

        stats: dict[str, Any] = {
            "total_signals": len(df),
            "emitted_signals": int(df["emitted"].sum()) if "emitted" in df.columns else 0,
            "unique_symbols": df["symbol"].nunique() if "symbol" in df.columns else 0,
            "date_range": {
                "start": df["ts"].min() if "ts" in df.columns else None,
                "end": df["ts"].max() if "ts" in df.columns else None,
            },
            "by_side": df.groupby("side").size().to_dict() if "side" in df.columns else {},
            "by_source": df.groupby("source").size().to_dict() if "source" in df.columns else {},
            "confidence": {
                "mean": float(df["confidence"].mean()),
                "std": float(df["confidence"].std()),
                "min": float(df["confidence"].min()),
                "max": float(df["confidence"].max()),
                "median": float(df["confidence"].median()),
            }
            if "confidence" in df.columns
            else {},
            "lot_size": {
                "mean": float(df["lot"].mean()),
                "std": float(df["lot"].std()),
                "min": float(df["lot"].min()),
                "max": float(df["lot"].max()),
            }
            if "lot" in df.columns
            else {},
        }

        return stats

    def get_metrics_summary(self, df: pd.DataFrame) -> dict[str, Any]:
        """
        Анализирует метрики детекторов.

        Args:
            df: DataFrame с метками

        Returns:
            Сводка по метрикам
        """
        if df.empty or "metrics" not in df.columns:
            return {"error": "No metrics data"}

        # Извлекаем метрики в отдельные колонки
        metrics_df = pd.json_normalize(df["metrics"])

        summary: dict[str, Any] = {}

        for col in metrics_df.columns:
            if metrics_df[col].dtype in ("float64", "int64"):
                summary[col] = {
                    "mean": float(metrics_df[col].mean()),
                    "std": float(metrics_df[col].std()),
                    "min": float(metrics_df[col].min()),
                    "max": float(metrics_df[col].max()),
                }

        # Специальные метрики
        if "detector_source" in metrics_df.columns:
            summary["detector_usage"] = metrics_df["detector_source"].value_counts().to_dict()

        if "trigger" in metrics_df.columns:
            summary["trigger_rate"] = float(metrics_df["trigger"].mean())

        if "extreme" in metrics_df.columns:
            summary["extreme_rate"] = float(metrics_df["extreme"].mean())

        return summary

    def export_report(self, output_path: str, **kwargs: Any) -> str:
        """
        Экспортирует отчёт в JSON.

        Args:
            output_path: путь для сохранения
            **kwargs: параметры для load_labels()

        Returns:
            Путь к созданному файлу
        """
        df = self.load_labels(**kwargs)

        report: dict[str, Any] = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "filters": kwargs,
            "stats": self.get_stats(df),
            "metrics": self.get_metrics_summary(df),
            "sample_signals": df.head(10).to_dict("records") if not df.empty else [],
        }

        with open(output_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info("Report exported to %s", output_path)
        return output_path

    def get_recent_signals(self, hours: int = 24, symbol: str | None = None) -> pd.DataFrame:
        """
        Загружает недавние сигналы.

        Args:
            hours: за сколько часов
            symbol: фильтр по символу

        Returns:
            DataFrame с сигналами
        """
        now = datetime.now(timezone.utc)
        end_date = now.strftime("%Y-%m-%d")
        # Грузим минимум 2 дня, чтобы не пропустить данные при hours > 24
        lookback_days = max(2, (hours // 24) + 1)
        start_date = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        df = self.load_labels(symbol=symbol, start_date=start_date, end_date=end_date)

        if df.empty:
            return df

        # Точный фильтр по времени на уровне timestamps
        if "ts" in df.columns:
            cutoff = int((now - timedelta(hours=hours)).timestamp() * 1000)
            df = df[df["ts"] >= cutoff]

        return df.sort_values("ts", ascending=False) if "ts" in df.columns else df


# ============================================================================
# CLI интерфейс
# ============================================================================


def main() -> None:
    """CLI для анализа меток."""
    import argparse

    parser = argparse.ArgumentParser(description="Analyze signal labels from Parquet storage")
    parser.add_argument("--labels-dir", default="/data/labels", help="Labels directory")
    parser.add_argument("--symbol", help="Filter by symbol")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--min-confidence", type=float, help="Minimum confidence")
    parser.add_argument("--recent-hours", type=int, help="Show recent N hours")
    parser.add_argument("--export", help="Export report to JSON file")
    parser.add_argument("--show-sample", action="store_true", help="Show sample signals")

    args = parser.parse_args()

    try:
        analyzer = LabelAnalyzer(args.labels_dir)

        if args.recent_hours:
            df = analyzer.get_recent_signals(hours=args.recent_hours, symbol=args.symbol)
        else:
            df = analyzer.load_labels(
                symbol=args.symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                min_confidence=args.min_confidence,
            )

        print(f"\n📊 Loaded {len(df)} signals\n")

        if df.empty:
            print("No signals found with given filters.")
            return

        # Статистика
        stats = analyzer.get_stats(df)
        print("=== Statistics ===")
        print(json.dumps(stats, indent=2, default=str))

        # Метрики
        print("\n=== Detector Metrics ===")
        metrics = analyzer.get_metrics_summary(df)
        print(json.dumps(metrics, indent=2, default=str))

        # Примеры
        if args.show_sample:
            print("\n=== Sample Signals ===")
            print(df.head(5).to_string())

        # Экспорт
        if args.export:
            path = analyzer.export_report(
                args.export,
                symbol=args.symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                min_confidence=args.min_confidence,
            )
            print(f"\n✅ Report exported to: {path}")

    except Exception as e:  # noqa: BLE001
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
