"""
Parquet Sink - Универсальный писатель тайлов в Parquet.

Функции:
- Запись батчей данных в Parquet файлы
- Разбиение по тайлам: base_dir/{kind}/YYYY/MM/DD/HH/*.parquet
- Автоматическое создание директорий
- Поддержка PyArrow и FastParquet

Layout:
  /data/tiles/
    signals/YYYY/MM/DD/HH/*.parquet
    orders/YYYY/MM/DD/HH/*.parquet
    events/YYYY/MM/DD/HH/*.parquet
"""

from __future__ import annotations
import os
import time
import pathlib
import uuid
from typing import List, Dict, Any, Optional

import pandas as pd

from common.log import setup_logger

# Попытка импорта PyArrow (предпочтительно)
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _USE_PYARROW = True
except ImportError:
    _USE_PYARROW = False
    pa = None
    pq = None


class ParquetSink:
    """
    Универсальный писатель тайлов Parquet.
    
    Организует данные по структуре:
    base_dir/
      {kind}/
        YYYY/
          MM/
            DD/
              HH/
                {timestamp}-{uuid}.parquet
    
    Где kind: 'signals', 'orders', 'events', etc.
    """

    def __init__(self, base_dir: Optional[str] = None):
        """
        Инициализация Parquet Sink.
        
        Args:
            base_dir: Базовая директория для хранения тайлов
        """
        self.base_dir = base_dir or os.getenv("PARQUET_BASE_DIR", "/data/tiles")
        self.logger = setup_logger("ParquetSink")

        # Создаём базовую директорию
        pathlib.Path(self.base_dir).mkdir(parents=True, exist_ok=True)

        # Проверяем доступность движка
        if _USE_PYARROW:
            self.logger.info("✅ Используется PyArrow для записи Parquet")
            self.engine = "pyarrow"
        else:
            self.logger.warning("⚠️ PyArrow недоступен, используется FastParquet")
            self.engine = "fastparquet"

        self.logger.info(f"📁 Базовая директория тайлов: {self.base_dir}")

    def _tile_path(self, kind: str, ts: float) -> str:
        """
        Формирование пути к файлу тайла.
        
        Args:
            kind: Тип данных ('signals', 'orders', 'events')
            ts: Timestamp в секундах (Unix time)
            
        Returns:
            Полный путь к файлу
        """
        # Конвертируем timestamp в GMT структуру
        t = time.gmtime(ts)

        # Формируем путь: kind/YYYY/MM/DD/HH/
        path = (
            pathlib.Path(self.base_dir) /
            kind /
            f"{t.tm_year:04d}" /
            f"{t.tm_mon:02d}" /
            f"{t.tm_mday:02d}" /
            f"{t.tm_hour:02d}"
        )

        # Создаём директорию
        path.mkdir(parents=True, exist_ok=True)

        # Формируем имя файла: {timestamp}-{uuid}.parquet
        fname = f"{int(ts)}-{uuid.uuid4().hex[:8]}.parquet"

        return str(path / fname)

    def write_records(
        self,
        kind: str,
        rows: List[Dict[str, Any]],
        ts_field: str = "ts"
    ) -> Optional[str]:
        """
        Запись батча записей в Parquet файл.
        
        Args:
            kind: Тип данных ('signals', 'orders', 'events')
            rows: Список словарей с данными
            ts_field: Поле с timestamp для определения тайла
            
        Returns:
            Путь к созданному файлу или None
        """
        if not rows:
            self.logger.debug(f"Пустой батч для {kind}, пропуск записи")
            return None

        try:
            # Берём timestamp первой записи для определения тайла
            ts = float(rows[0].get(ts_field, time.time()))

            # Формируем путь
            out_path = self._tile_path(kind, ts)

            # Создаём DataFrame
            df = pd.DataFrame(rows)

            # Записываем в Parquet
            if _USE_PYARROW and pa and pq:
                # Используем PyArrow
                table = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_table(table, out_path)
            else:
                # Fallback на FastParquet
                df.to_parquet(out_path, engine="fastparquet", index=False)

            self.logger.info(
                f"✅ Записано {len(rows)} записей {kind} → {out_path}"
            )

            return out_path

        except Exception as e:
            self.logger.error(f"❌ Ошибка записи Parquet {kind}: {e}", exc_info=True)
            return None

    def write_batch(
        self,
        kind: str,
        batch: List[Dict[str, Any]],
        ts_field: str = "ts",
        batch_size: int = 1000
    ) -> List[str]:
        """
        Запись большого батча с разбиением на файлы.
        
        Args:
            kind: Тип данных
            batch: Большой список данных
            ts_field: Поле timestamp
            batch_size: Размер одного файла
            
        Returns:
            Список путей к созданным файлам
        """
        if not batch:
            return []

        files = []

        # Разбиваем на чанки
        for i in range(0, len(batch), batch_size):
            chunk = batch[i:i + batch_size]
            file_path = self.write_records(kind, chunk, ts_field)
            if file_path:
                files.append(file_path)

        self.logger.info(
            f"✅ Записано {len(batch)} записей {kind} в {len(files)} файлов"
        )

        return files

    def get_tile_stats(self) -> Dict[str, Any]:
        """Получение статистики по тайлам"""
        stats = {
            "base_dir": self.base_dir,
            "engine": self.engine,
            "kinds": {}
        }

        try:
            base_path = pathlib.Path(self.base_dir)

            for kind_dir in base_path.iterdir():
                if kind_dir.is_dir():
                    kind = kind_dir.name

                    # Подсчёт файлов
                    files = list(kind_dir.rglob("*.parquet"))
                    total_size = sum(f.stat().st_size for f in files)

                    stats["kinds"][kind] = {
                        "files": len(files),
                        "size_mb": round(total_size / 1024 / 1024, 2)
                    }

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения статистики: {e}")

        return stats

