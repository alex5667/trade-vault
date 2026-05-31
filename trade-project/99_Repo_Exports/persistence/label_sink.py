# -*- coding: utf-8 -*-
"""
ParquetLabelSink — быстрый проектор событий/меток в Parquet с тайловой партицией.
Партиции: symbol=..., date=YYYY-MM-DD, tile=HHmm(окно TileMinutes).
Писатель устойчив к отсутствию pyarrow — тогда делает CSV-фолбэк.

ENV:
  LABEL_PARQUET_DIR=/data/labels
  LABEL_TILE_MINUTES=15
"""

import os
import math
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

LABEL_PARQUET_DIR = os.getenv("LABEL_PARQUET_DIR", "/data/labels")
LABEL_TILE_MINUTES = int(os.getenv("LABEL_TILE_MINUTES", "15"))

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
    _HAS_ARROW = True
except Exception:
    import pandas as pd  # pandas почти всегда есть у вас
    _HAS_ARROW = False


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _tile_str(ts_ms: int) -> tuple:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(timezone.utc)
    date = dt.strftime("%Y-%m-%d")
    mins = dt.hour * 60 + dt.minute
    tile_idx = (mins // LABEL_TILE_MINUTES) * LABEL_TILE_MINUTES
    hh = tile_idx // 60
    mm = tile_idx % 60
    tile = f"{hh:02d}{mm:02d}"
    return date, tile


class ParquetLabelSink:
    """
    write(record: dict) принимает «плоский» dict; вложенные структуры превращает в JSON-строку.
    Рекомендуемая схема record:
      {
        "ts":  ... (ms),
        "symbol": "XAUUSD",
        "source": "hub|orderflow|ta",
        "side": "LONG|SHORT",
        "price": float,
        "sl": float,
        "tp_levels": [float,...],
        "lot": float,
        "confidence": float,
        "atr": float,
        "reason": str,
        "metrics": { "z_delta":..., "z_speed":..., "imbalance_score":..., ...},
        "emitted": true/false
      }
    """

    def __init__(self, root_dir: str = LABEL_PARQUET_DIR, tile_minutes: int = LABEL_TILE_MINUTES):
        self.root = root_dir
        self.tile_minutes = tile_minutes
        _ensure_dir(self.root)

    def _flatten(self, rec: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        for k, v in rec.items():
            if isinstance(v, (list, dict)):
                out[k] = json.dumps(v, ensure_ascii=False)
            else:
                out[k] = v
        return out

    def write(self, rec: Dict[str, Any]) -> str:
        if "ts" not in rec:
            raise ValueError("record must contain 'ts' (ms)")
        if "symbol" not in rec:
            raise ValueError("record must contain 'symbol'")

        sym = rec["symbol"]
        date, tile = _tile_str(int(rec["ts"]))
        flat = self._flatten(rec)

        if _HAS_ARROW:
            # Партиции как папки: symbol=..., date=..., tile=...
            part_dir = os.path.join(self.root, f"symbol={sym}", f"date={date}", f"tile={tile}")
            _ensure_dir(part_dir)
            # по одному файлу на запись — безопасно для мультипроцесса (можно объединять оффлайн)
            file_path = os.path.join(part_dir, f"{rec['ts']}-{sym}.parquet")

            table = pa.Table.from_pandas(pd.DataFrame([flat]))
            pq.write_table(table, file_path, compression="zstd", use_dictionary=True)
            return file_path
        else:
            part_dir = os.path.join(self.root, f"symbol={sym}", f"date={date}", f"tile={tile}")
            _ensure_dir(part_dir)
            file_path = os.path.join(part_dir, f"{rec['ts']}-{sym}.csv")
            pd.DataFrame([flat]).to_csv(file_path, index=False)
            return file_path

