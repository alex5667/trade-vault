"""
Analytics Package - Аналитический пакет 3.0.

Компоненты v1.0:
- repository.py - доступ к данным из Redis
- metrics.py - расчёт метрик (ROC/AUC, Precision/Recall)
- latency_decay.py - анализ latency и signal decay
- reporter.py - генерация отчётов
- parquet_sink.py - запись тайлов в Parquet
- tiles_service.py - фоновый сервис записи тайлов

Компоненты v2.0:
- dataset_export.py - экспорт партиционированных датасетов
- roc_store.py - хранение ROC кривых в Redis
- threshold_tuner.py - авто-подбор порога по ROC/AUC
- metrics_publisher.py - публикация метрик для Grafana
- telegram_reporter_ext.py - расширенные Telegram отчёты с графиками
- multi_publish_best_threshold.py - CLI для мульти-тюнинга
- nightly_pipeline.py - CLI для полного ночного прогона

Компоненты v3.0 (новые):
- svg_renderer.py - SVG генерация ROC/Confusion без PIL
- ab_compare.py - A/B сравнение с bootstrap доверительными интервалами
"""

__version__ = "3.0.0"

# Экспорт основных классов для удобного импорта
try:
    # Core modules
    from .repository import Repository, RepoConfig, Order, Signal
    from .metrics import calculate_roc_auc, roc_from_signals
    from .parquet_sink import ParquetSink

    # v2.0 modules
    from .threshold_tuner import ThresholdTuner
    from .roc_store import ROCStore
    from .metrics_publisher import MetricsPublisher
    from .telegram_reporter_ext import TelegramReporterExt

    # v3.0 modules
    from .svg_renderer import roc_svg, confusion_svg, save_svg

    __all__ = [
        # Core
        "Repository"
        "RepoConfig"
        "Order"
        "Signal"
        "calculate_roc_auc"
        "roc_from_signals"
        "ParquetSink"
        # v2.0
        "ThresholdTuner"
        "ROCStore"
        "MetricsPublisher"
        "TelegramReporterExt"
        # v3.0
        "roc_svg"
        "confusion_svg"
        "save_svg"
    ]
except ImportError as e:
    # Частичный импорт - некоторые модули могут быть недоступны
    import warnings
    warnings.warn(f"Partial import in analytics package: {e}")
    pass

