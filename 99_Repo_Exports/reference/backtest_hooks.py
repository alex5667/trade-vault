# backtest_hooks.py
"""
Backtest Hooks - реплей исторических тиков/книги из TimescaleDB или Parquet.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Dict, Optional, Tuple
import os
import time
import json
import math

# опциональные импорты — оборачиваем в try, чтобы контейнер не падал при отсутствии пакетов
try:
    import pandas as pd
except ImportError:
    pd = None
try:
    import psycopg2
except ImportError:
    psycopg2 = None

from common.log import setup_logger
from microstructure_spike_detector import MicrostructureSpikeDetector
from smart_cluster_analyzer import SmartClusterAnalyzer

log = setup_logger("backtest_hooks")

@dataclass
class ReplayTimeStats:
    """
    Statistics and counters for replay timing verification.
    """
    prev_ts: Optional[int] = None
    prev_wall: Optional[float] = None  # time.perf_counter()
    reorder_warn: int = 0
    reorder_severe: int = 0
    gap_warn: int = 0
    gap_severe: int = 0
    dup_ts: int = 0


@dataclass
class ReplayConfig:
    """
    Конфигурация для реплея исторических данных.
    
    Attributes:
        parquet_path: Путь к Parquet файлу (опционально)
        pg_dsn: PostgreSQL DSN строка (опционально)
        pg_table: Имя таблицы в PostgreSQL
        start_ms: Начальный timestamp в миллисекундах (опционально)
        end_ms: Конечный timestamp в миллисекундах (опционально)
        speed: Скорость реплея (0 = максимально быстро, 1.0 = реальное время, 2.0 = в 2 раза быстрее)
        chunk: Размер порции загрузки
        window_ticks: Размер окна для детекторов
        reorder_ms: Допустимое окно переупорядочивания ts (ms)
        max_gap_ms: Максимально допустимый разрыв между тиками (ms)
        drift_warn_ms: Порог предупреждения о дрейфе скорости реплея (ms)
    """
    parquet_path: Optional[str] = None
    pg_dsn: Optional[str] = None  # "postgresql://user:pass@host:5432/db"
    pg_table: str = "ticks_xauusd"  # (ts_ms bigint, bid double, ask double, last double, volume double)
    start_ms: Optional[int] = None
    end_ms: Optional[int] = None
    # Параметры реплея
    speed: float = 0.0      # 0 = максимально быстро; 1.0 = реальное время; 2.0 = в 2 раза быстрее
    chunk: int = 1000       # размер порции загрузки
    window_ticks: int = 300 # окно для детекторов

    # Timing verification (NEW)
    gap_warn_ms: int = int(os.getenv("REPLAY_GAP_WARN_MS", "2000"))
    gap_severe_ms: int = int(os.getenv("REPLAY_GAP_SEVERE_MS", "10000"))
    gap_severe_policy: str = os.getenv("REPLAY_GAP_SEVERE_POLICY", "log")  # "log" | "raise"
    drift_warn_ms: float = float(os.getenv("REPLAY_DRIFT_WARN_MS", "100.0"))
    log_sample_n: int = int(os.getenv("REPLAY_LOG_SAMPLE_N", "1000"))

def iter_ticks_from_parquet(cfg: ReplayConfig) -> Iterator[Dict]:
    """Итератор тиков из Parquet файла."""
    if pd is None:
        raise RuntimeError("pandas не установлен в контейнере (нужен для чтения Parquet)")
    
    df = pd.read_parquet(cfg.parquet_path)
    if df.empty:
        return

    if "ts" not in df.columns:
        raise ValueError(f"parquet ticks: missing required column 'ts' in {cfg.parquet_path}")

    # --- normalize ts -> ts_ms (epoch milliseconds) ---
    ts_col = df["ts"]
    scale = "ms"

    if pd.api.types.is_datetime64_any_dtype(ts_col):
        # view('int64') gives ns since epoch
        df["ts_ms"] = (ts_col.view("int64") // 1_000_000).astype("int64")
        scale = "datetime64[ns]"
    else:
        # Case B: numeric (may be ms/us/ns)
        x = ts_col.astype("int64")
        # find first non-null sample for scale detection
        sample = None
        try:
            valid_x = x.dropna()
            if not valid_x.empty:
                sample = int(valid_x.iloc[0])
        except Exception:
            pass

        if sample is None:
            raise ValueError("parquet ticks: cannot infer ts scale (no numeric samples)")

        a = abs(sample)
        if a >= 10_000_000_000_000_000:      # >= 1e16 -> ns
            df["ts_ms"] = (x // 1_000_000).astype("int64")
            scale = "ns"
        elif a >= 10_000_000_000_000:        # >= 1e13 -> us
            df["ts_ms"] = (x // 1_000).astype("int64")
            scale = "us"
        else:                                # ms
            df["ts_ms"] = x.astype("int64")
            scale = "ms"

    log.info("[replay] loaded %d rows from %s (ts_scale=%s)", len(df), cfg.parquet_path, scale)

    df = df.sort_values("ts_ms")
    if cfg.start_ms is not None:
        df = df[df["ts_ms"] >= cfg.start_ms]
    if cfg.end_ms is not None:
        df = df[df["ts_ms"] <= cfg.end_ms]
    
    for _, row in df.iterrows():
        yield {
            "ts": int(row["ts_ms"]),
            "bid": float(row.get("bid", 0.0)),
            "ask": float(row.get("ask", 0.0)),
            "last": float(row.get("last", 0.0)),
            "volume": float(row.get("volume", 0.0)),
            "flags": int(row.get("flags", 0))
        }

def iter_ticks_from_timescale(cfg: ReplayConfig) -> Iterator[Dict]:
    """Итератор тиков из TimescaleDB."""
    if psycopg2 is None:
        raise RuntimeError("psycopg2 не установлен в контейнере (нужен для доступа к Timescale)")
    
    conn = psycopg2.connect(cfg.pg_dsn)
    try:
        cur = conn.cursor(name="tick_stream_cur")
        try:
            q = f"""
                SELECT ts_ms, bid, ask, last, volume
                FROM {cfg.pg_table}
                WHERE (%s::bigint IS NULL OR ts_ms >= %s)
                  AND (%s::bigint IS NULL OR ts_ms <= %s)
                ORDER BY ts_ms ASC
            """
            cur.execute(q, (cfg.start_ms, cfg.start_ms, cfg.end_ms, cfg.end_ms))
            
            for rec in cur:
                ts, bid, ask, last, vol = rec
                yield {
                    "ts": int(ts),
                    "bid": float(bid or 0.0),
                    "ask": float(ask or 0.0),
                    "last": float(last or 0.0),
                    "volume": float(vol or 0.0),
                    "flags": 0
                }
        finally:
            cur.close()
    finally:
        conn.close()

def replay(cfg: ReplayConfig, on_step=None) -> List[Dict]:
    """
    Крутит поток, на каждом шаге считает micro/cluster и отдает callback-данные.
    
    Args:
        cfg: Конфигурация реплея
        on_step: Callback функция для каждого шага (result: Dict) -> None | bool
    
    Returns:
        Список снепшотов результатов (если не задан on_step)
    """
    src = None
    if cfg.parquet_path:
        src = iter_ticks_from_parquet(cfg)
    elif cfg.pg_dsn:
        src = iter_ticks_from_timescale(cfg)
    else:
        raise ValueError("Нужно указать parquet_path или pg_dsn")

    spike = MicrostructureSpikeDetector(maxlen=cfg.window_ticks)
    cluster = SmartClusterAnalyzer()
    buf: List[Dict] = []
    speed = cfg.speed

    st = ReplayTimeStats()

    for t in src:
        ts = int(t.get("ts") or 0)
        if ts <= 0:
            raise ValueError(f"replay: invalid ts={ts}")

        dt_ms = 0
        if st.prev_ts is not None:
            dt_ms = ts - st.prev_ts
            if dt_ms == 0:
                st.dup_ts += 1
                if st.dup_ts % cfg.log_sample_n == 1:
                    log.info("[replay] duplicated ts=%d (total dups=%d)", ts, st.dup_ts)
            elif dt_ms < 0:
                st.reorder_severe += 1
                raise ValueError(f"replay: strict monotonicity violation (dt={dt_ms}ms) at ts={ts}")
            elif dt_ms > cfg.gap_severe_ms:
                st.gap_severe += 1
                msg = f"[replay] SEVERE ts gap dt={dt_ms}ms > {cfg.gap_severe_ms}ms"
                log.error(msg)
                if cfg.gap_severe_policy == "raise":
                    raise ValueError(msg)
            elif dt_ms > cfg.gap_warn_ms:
                st.gap_warn += 1
                if st.gap_warn <= 10:
                    log.warning("[replay] ts gap dt=%dms > %dms", dt_ms, cfg.gap_warn_ms)

        # --- Speed logic (sleep) ---
        if speed and speed > 0:
            sleep_s = (dt_ms / 1000.0) / speed
            if sleep_s > 0:
                time.sleep(sleep_s)
            
            wall1 = time.perf_counter()

            # --- Drift monitor ---
            if st.prev_wall is not None and st.prev_ts is not None:
                wall_dt_ms = (wall1 - st.prev_wall) * 1000.0
                expected_wall_ms = (dt_ms / speed)
                drift_ms = wall_dt_ms - expected_wall_ms
                if abs(drift_ms) > cfg.drift_warn_ms:
                     # Add implicit counter to stats if not present, or just use a local one?
                     # Ideally add to ReplayTimeStats, but let's stick to simple modulo for now or just log.
                     # User suggested sampling. Let's use st.gap_warn (reusing field?) No, let's keep it simple.
                     # We can just log every Nth time or use a rate limit.
                     # Given strict requirements, let's just log it. 
                     # Wait, user said "st.prev_wall = wall1" correction is MUST.
                     # And "Optional: sample drift logs".
                     log.warning("[replay] speed drift %+.1fms (wall_dt=%.1f expected=%.1f)", 
                                drift_ms, wall_dt_ms, expected_wall_ms)
            
            st.prev_wall = wall1
        else:
            st.prev_wall = time.perf_counter()

        st.prev_ts = ts

        spike.feed_ticks([t])
        m = spike.compute()
        c = cluster.analyze_from_ticks(list(spike.window))

        res = {
            "ts": ts,
            "mid": (t["bid"] + t["ask"]) / 2 if t.get("bid") and t.get("ask") else (t.get("last") or 0.0),
            "micro": m,
            "cluster": c
        }
        
        if on_step:
            stop = on_step(res)
            if stop is True:
                break
        else:
            buf.append(res)
    
    return buf