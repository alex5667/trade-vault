"""
Dataset Export - Экспорт единого датасета сигналов и сделок в Parquet.

Функции:
- Объединение сигналов и ордеров в один датасет
- Партиционирование по symbol/strategy/year/month
- Поддержка PyArrow и FastParquet
- Вычисление производных полей (win, latency, etc)

Интеграция:
- Использует Repository для чтения данных
- Совместим с Signal Performance Tracker
- Готов для ML-анализа
"""

from __future__ import annotations
import os
import time
import pathlib
from typing import List, Dict, Any, Sequence, Optional

import pandas as pd

from common.log import setup_logger

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds
    _ARROW = True
except ImportError:
    _ARROW = False
    pa = None
    pq = None
    ds = None


logger = setup_logger("DatasetExport")

DATASET_DIR = os.getenv("DATASET_DIR", "/data/datasets")


def _ensure_dir(p: str):
    """Создание директории"""
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)


def _join_rows(orders: List, sig_by_id: Dict[str, Any], repo) -> List[Dict[str, Any]]:
    """
    Объединение ордеров и сигналов в единый датасет.
    
    Args:
        orders: Список Order объектов
        sig_by_id: Словарь {signal_id: Signal}
        repo: Repository для вычисления P/L
        
    Returns:
        Список словарей с объединёнными данными
    """
    rows = []
    
    for o in orders:
        s = sig_by_id.get(o.signal_id or "")

        # === Retrospective Dataset Masking (Phase 3/4) ===
        if s and getattr(s, "metadata", None):
            try:
                book_stale_ms = float(s.metadata.get("book_stale_ms") or 0.0)
                touch_traded_w = float(s.metadata.get("touch_traded_w") or 1.0)
                smt_coherence = float(s.metadata.get("smt_coherence") or 1.0)
                l3_taker_rate = float(s.metadata.get("l3_taker_rate") or 1.0)
                scenario = str(s.metadata.get("scenario") or "").lower()

                if book_stale_ms > 1000:
                    continue
                
                if scenario in ("breakout", "absorption") and touch_traded_w < 0.10:
                    continue
                    
                if "breakout" in scenario and smt_coherence < 0.70:
                    continue
                    
                if "absorption" in scenario and l3_taker_rate < 0.20:
                    continue
            except Exception as e:
                logger.warning(f"Error filtering signal {s.signal_id}: {e}")
                pass

        pnl = repo.compute_pnl_usd(o)
        
        # Определяем outcome
        win = 1 if (pnl is None or pnl > 0) else 0
        
        # Вычисляем latency (если доступно)
        tp1_latency = (o.tp1_time - o.entry_time) if (o.tp1_time and o.entry_time) else None
        tp2_latency = (o.tp2_time - o.entry_time) if (o.tp2_time and o.entry_time) else None
        tp3_latency = (o.tp3_time - o.entry_time) if (o.tp3_time and o.entry_time) else None
        sl_latency = (o.sl_time - o.entry_time) if (o.sl_time and o.entry_time) else None
        
        rows.append({
            # Order fields
            "order_id": o.order_id,
            "symbol": o.symbol,
            "timeframe": o.timeframe,
            "strategy": o.strategy,
            "source": o.source,
            "direction": o.direction,
            "lot": o.lot,
            "entry_price": o.entry_price,
            "entry_time": o.entry_time,
            "exit_price": o.exit_price,
            "exit_time": o.exit_time,
            "pnl_usd": pnl,
            "pnl_pct": o.pnl_pct,
            "result": o.result,

            # TP/SL levels and times
            "tp1_price": o.tp1_price,
            "tp1_time": o.tp1_time,
            "tp1_hit": int(o.tp1_hit),
            "tp1_latency": tp1_latency,

            "tp2_price": o.tp2_price,
            "tp2_time": o.tp2_time,
            "tp2_hit": int(o.tp2_hit),
            "tp2_latency": tp2_latency,

            "tp3_price": o.tp3_price,
            "tp3_time": o.tp3_time,
            "tp3_hit": int(o.tp3_hit),
            "tp3_latency": tp3_latency,

            "sl_price": o.sl_price,
            "sl_time": o.sl_time,
            "sl_latency": sl_latency,

            # TP→SL metrics
            "tp_before_sl": o.tp_before_sl,
            "close_reason": o.close_reason,

            # Signal fields
            "signal_id": s.signal_id if s else None,
            "signal_ts": s.ts if s else None,
            "signal_price": s.price if s else None,
            "signal_confidence": s.confidence if s else None,
            "signal_score": s.score if s else None,
            "signal_source": s.source if s else None,
            "signal_atr": s.atr if s else None,

            # NOTE: year/month/day/hour derived after DataFrame creation (vectorised)

            # Outcome flag
            "win": win,
        })
    
    return rows


def export_dataset(
    repo,
    orders: List,
    signals: List,
    out_name: Optional[str] = None
) -> str:
    """
    Экспорт датасета в один Parquet файл (непартиционированный).
    
    Args:
        repo: Repository объект
        orders: Список Order объектов
        signals: Список Signal объектов
        out_name: Имя выходного файла (опционально)
        
    Returns:
        Путь к созданному файлу
    """
    _ensure_dir(DATASET_DIR)
    
    sig_by_id = {s.signal_id: s for s in signals}
    rows = _join_rows(orders, sig_by_id, repo)
    df = pd.DataFrame(rows)

    # Derive partition columns vectorised (avoids per-row pd.to_datetime)
    _dt = pd.to_datetime(df["entry_time"], unit="s", utc=True, errors="coerce")
    df["year"] = _dt.dt.year
    df["month"] = _dt.dt.month
    df["day"] = _dt.dt.day
    df["hour"] = _dt.dt.hour

    if out_name is None:
        out_name = f"dataset_{int(time.time())}.parquet"
    
    out_path = os.path.join(DATASET_DIR, out_name)
    
    logger.info(f"📊 Экспорт датасета: {len(rows)} записей")
    
    if _ARROW and pa and pq:
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, out_path)
    else:
        df.to_parquet(out_path, engine="fastparquet", index=False)
    
    logger.info(f"✅ Датасет экспортирован: {out_path}")
    
    return out_path


def export_dataset_partitioned(
    repo,
    orders: List,
    signals: List,
    partition_cols: Sequence[str] = ("symbol", "strategy", "year", "month"),
    base_dir: Optional[str] = None
) -> str:
    """
    Экспорт датасета с партиционированием.
    
    Layout: base_dir/symbol=XAUUSD/strategy=orderflow/year=2025/month=11/*.parquet
    
    Args:
        repo: Repository объект
        orders: Список Order объектов
        signals: Список Signal объектов
        partition_cols: Колонки для партиционирования
        base_dir: Базовая директория (опционально)
        
    Returns:
        Путь к базовой директории датасета
    """
    base_dir = base_dir or os.getenv("DATASET_DIR", "/data/datasets_partitioned")
    _ensure_dir(base_dir)
    
    sig_by_id = {s.signal_id: s for s in signals}
    rows = _join_rows(orders, sig_by_id, repo)
    df = pd.DataFrame(rows)

    # Derive partition columns vectorised
    _dt = pd.to_datetime(df["entry_time"], unit="s", utc=True, errors="coerce")
    df["year"] = _dt.dt.year
    df["month"] = _dt.dt.month
    df["day"] = _dt.dt.day
    df["hour"] = _dt.dt.hour

    logger.info(f"📊 Экспорт партиционированного датасета: {len(rows)} записей")
    logger.info(f"📁 Партиции: {partition_cols}")
    
    if _ARROW and pa and pq:
        # Используем PyArrow для партиционирования
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(
            table,
            root_path=base_dir,
            partition_cols=list(partition_cols),
            existing_data_behavior="overwrite_or_ignore"
        )
    else:
        # Fallback: группируем и пишем вручную
        logger.warning("⚠️ PyArrow недоступен, используем ручное партиционирование")
        
        grouped = df.groupby(list(partition_cols), dropna=False)
        
        for keys, part in grouped:
            # Формируем путь
            if not isinstance(keys, tuple):
                keys = (keys,)
            
            rel_parts = []
            for col, val in zip(partition_cols, keys):
                rel_parts.append(f"{col}={val}")
            
            out_dir = os.path.join(base_dir, *rel_parts)
            _ensure_dir(out_dir)
            
            out_file = os.path.join(out_dir, f"part-{int(time.time())}.parquet")
            part.to_parquet(out_file, engine="fastparquet", index=False)
    
    logger.info(f"✅ Партиционированный датасет экспортирован: {base_dir}")
    
    return base_dir

