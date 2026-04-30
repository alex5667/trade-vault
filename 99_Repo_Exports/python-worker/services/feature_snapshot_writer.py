# services/feature_snapshot_writer.py
"""
FeatureSnapshotWriter — асинхронная fail-open персистенция снимков фичей
в гипертаблицу features_pit.

Необходима для сохранения time-point-correct фичей для ML без look-ahead bias.
Ошибки сохранения не должны ронять генерацию сигналов (fail-open паттерн).
"""
from __future__ import annotations

import json
import logging
import threading
from typing import Any, Dict, Optional

log = logging.getLogger("feature_snapshot_writer")

_instance: Optional["FeatureSnapshotWriter"] = None
_lock = threading.Lock()


class FeatureSnapshotWriter:
    """Асинхронная запись слепков метрик (ML features) в базу данных."""

    def __init__(self) -> None:
        pass

    def emit_to_db(self, symbol: str, ts_ms: int, feature_set: Dict[str, Any]) -> bool:
        """
        INSERT в features_pit таблицу.
        Идемпотентная ставка ON CONFLICT (symbol, ts) DO NOTHING.
        Выполняется в текущем потоке (в идеале из ThreadPool или как fallback).
        """
        sql = """
            INSERT INTO features_pit (symbol, ts, feature_set)
            VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (symbol, ts) DO NOTHING
        """
        
        # Конвертация типов для JSON (избежание float32 json errors)
        try:
            safe_features = json.loads(json.dumps(feature_set, default=str))
        except (TypeError, ValueError) as err:
            log.warning("⚠️ feature_snapshot format error: %s", err)
            safe_features = {}

        params = (
            symbol
            ts_ms
            json.dumps(safe_features)
        )

        try:
            from services.analytics_db import get_conn
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()
            return True
        except Exception as e:
            log.warning("⚠️ feature_snapshot DB persist failed (fail-open): %s", e)
            return False

    def emit_async(self, symbol: str, ts_ms: int, feature_set: Dict[str, Any]) -> None:
        """Асинхронно пишет в БД, чтобы не блокировать hot-path торговли."""
        def _task():
            success = self.emit_to_db(symbol, ts_ms, feature_set)
            if success:
                log.debug("✅ feature_snapshot persisted for %s at %s", symbol, ts_ms)

        # Fire and forget
        thread = threading.Thread(target=_task, daemon=True)
        thread.start()


def get_feature_snapshot_writer() -> FeatureSnapshotWriter:
    """Singleton accessor (thread-safe, lazy init)."""
    global _instance
    if _instance is None:
        with _lock:
            if _instance is None:
                _instance = FeatureSnapshotWriter()
                log.info("✅ FeatureSnapshotWriter initialized")
    return _instance
