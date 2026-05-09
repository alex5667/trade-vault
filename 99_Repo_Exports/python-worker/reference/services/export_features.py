#!/usr/bin/env python3
"""
Feature exporter for XAUUSD order flow.

Exports tick and book data from Redis Streams to Parquet or CSV format
for offline analysis, backtesting, and calibration.

Usage:
    python3 export_features.py --start "2025-10-25T10:00:00Z" \\
                                --end "2025-10-25T12:00:00Z" \\
                                --out features.parquet
"""

import argparse
import json
import os
from datetime import datetime

import redis

try:
    import pandas as pd
except ImportError:
    print("Error: pandas not installed. Run: pip install pandas pyarrow")
    exit(1)

# Опциональный GPU стек (cuDF/CuPy)
_GPU_AVAILABLE = False
try:
    import cudf  # type: ignore
    import cupy as cp  # type: ignore

    _GPU_AVAILABLE = True
except Exception:
    cudf = None  # type: ignore
    cp = None  # type: ignore
    _GPU_AVAILABLE = False

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from core.config import XAU_BOOK_STREAM, XAU_TICK_STREAM
from signals.featurizer import Rolling, make_features

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
TICK_STREAM = XAU_TICK_STREAM
BOOK_STREAM = XAU_BOOK_STREAM


def parse_time(s: str) -> int:
    """
    Parse time string to epoch milliseconds.
    
    Supports:
    - Epoch ms: "1729854000000"
    - ISO format: "2025-10-25T10:00:00Z"
    - ISO with timezone: "2025-10-25T10:00:00+00:00"
    
    Args:
        s: Time string
        
    Returns:
        Epoch milliseconds
        
    Raises:
        SystemExit: If time format is invalid
    """
    try:
        # Try as epoch ms first
        if s.isdigit():
            return int(s)

        # Try ISO format
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception as e:
        raise SystemExit(f"Invalid time format '{s}': {e}")


def xranges(
    r: redis.Redis,
    stream: str,
    start: str,
    end: str,
    count: int = 1000
):
    """
    Generator for reading Redis Stream in chunks.
    
    Args:
        r: Redis client
        stream: Stream name
        start: Start ID (e.g., "1729854000000-0")
        end: End ID (e.g., "1729857600000-999999")
        count: Chunk size
        
    Yields:
        (msg_id, fields) tuples
    """
    last = start

    while True:
        chunk = r.xrange(stream, min=last, max=end, count=count)
        if not chunk:
            break

        for mid, fields in chunk:
            last = mid
            yield mid, fields

        # Advance ID to avoid returning same last message
        parts = last.split("-")
        last = f"{parts[0]}-{int(parts[1]) + 1}"


def load_ticks(
    r: redis.Redis,
    start_ms: int,
    end_ms: int
) -> list[dict]:
    """
    Load ticks from Redis Stream.
    
    Args:
        r: Redis client
        start_ms: Start timestamp (ms)
        end_ms: End timestamp (ms)
        
    Returns:
        List of tick dictionaries
    """
    ticks = []
    start_id = f"{start_ms}-0"
    end_id = f"{end_ms}-999999"

    print(f"Loading ticks from {TICK_STREAM}...")
    for _, fields in xranges(r, TICK_STREAM, start_id, end_id):
        try:
            tick = json.loads(fields["data"])
            ticks.append(tick)
        except (KeyError, json.JSONDecodeError) as e:
            print(f"Warning: Failed to parse tick: {e}")
            continue

    print(f"Loaded {len(ticks)} ticks")
    return ticks


def load_books(
    r: redis.Redis,
    start_ms: int,
    end_ms: int
) -> dict[int, dict]:
    """
    Load order books from Redis Stream.
    
    Args:
        r: Redis client
        start_ms: Start timestamp (ms)
        end_ms: End timestamp (ms)
        
    Returns:
        Dictionary mapping timestamp to book snapshot
    """
    books_map = {}
    start_id = f"{start_ms}-0"
    end_id = f"{end_ms}-999999"

    print(f"Loading books from {BOOK_STREAM}...")
    for _, fields in xranges(r, BOOK_STREAM, start_id, end_id):
        try:
            book = json.loads(fields["data"])
            ts = int(book["ts"])
            books_map[ts] = book
        except (KeyError, json.JSONDecodeError) as e:
            print(f"Warning: Failed to parse book: {e}")
            continue

    print(f"Loaded {len(books_map)} book snapshots")
    return books_map


def find_nearest_book(
    books_map: dict[int, dict],
    ts: int,
    max_age_ms: int = 1000
) -> dict | None:
    """
    Find nearest book snapshot for given timestamp.
    
    Args:
        books_map: Dictionary of book snapshots
        ts: Target timestamp
        max_age_ms: Maximum age of book (ms)
        
    Returns:
        Book snapshot or None
    """
    # Exact match
    if ts in books_map:
        return books_map[ts]

    # Find closest earlier book within max_age
    candidates = [k for k in books_map if k <= ts and ts - k <= max_age_ms]
    if candidates:
        return books_map[max(candidates)]

    return None


def extract_features(
    ticks: list[dict],
    books_map: dict[int, dict],
    delta_window: int = 120,
    use_gpu: bool = False,
):
    """
    Extract features from ticks and books with optional GPU acceleration.
    
    ✅ ОПТИМИЗИРОВАНО: Использует батч обработку для OBI вычислений.
    
    Args:
        ticks: List of tick data
        books_map: Dictionary of book snapshots
        delta_window: Rolling window size for delta statistics
        
    Returns:
        DataFrame with features
    """
    print("Extracting features...")

    # ✅ GPU Support: используем batch processor для больших объемов
    use_batch = len(ticks) > 5000
    batch_processor = None

    if use_batch:
        try:
            from services.batch_processor import get_batch_processor
            batch_processor = get_batch_processor(batch_size=1000, use_gpu=True)
            print("🚀 Using GPU-accelerated batch processing")
        except ImportError:
            use_batch = False

    roll = Rolling(size=delta_window)
    rows = []

    # ✅ OBI Batch Optimization: накапливаем книги для батч обработки
    obi_books_buffer = []
    obi_ticks_indices = []
    obi_batch_size = 10  # Обрабатываем OBI батчами из 10+ книг

    # Обрабатываем батчами если много данных
    if use_batch and batch_processor:
        batch_size = 1000
        for batch_start in range(0, len(ticks), batch_size):
            batch = ticks[batch_start:batch_start + batch_size]

            for tick in batch:
                ts = int(tick["ts"])
                book = find_nearest_book(books_map, ts)

                # Накапливаем книги для батч OBI вычислений
                if book:
                    obi_books_buffer.append(book)
                    obi_ticks_indices.append(len(rows))

                feat = make_features(tick, book, roll)
                rows.append(feat)

            # ✅ Обрабатываем накопленные книги батчем для OBI
            if len(obi_books_buffer) >= obi_batch_size:
                try:
                    from signals.featurizer import obi_from_book_batch
                    obi_values = obi_from_book_batch(obi_books_buffer, depth=5)
                    # Обновляем OBI в уже созданных фичах
                    for idx, obi_val in zip(obi_ticks_indices, obi_values):
                        if idx < len(rows) and obi_val is not None:
                            rows[idx]["obi"] = obi_val
                except Exception:
                    pass  # Fallback уже использован в make_features
                obi_books_buffer.clear()
                obi_ticks_indices.clear()

            if (batch_start + len(batch)) % 10000 == 0:
                print(f"Processed {batch_start + len(batch)}/{len(ticks)} ticks...")
    else:
        # Обычная обработка с батч OBI оптимизацией
        for i, tick in enumerate(ticks):
            ts = int(tick["ts"])
            book = find_nearest_book(books_map, ts)

            # Накапливаем книги для батч OBI вычислений
            if book:
                obi_books_buffer.append(book)
                obi_ticks_indices.append(len(rows))

            feat = make_features(tick, book, roll)
            rows.append(feat)

            # ✅ Обрабатываем накопленные книги батчем для OBI
            if len(obi_books_buffer) >= obi_batch_size:
                try:
                    from signals.featurizer import obi_from_book_batch
                    obi_values = obi_from_book_batch(obi_books_buffer, depth=5)
                    # Обновляем OBI в уже созданных фичах
                    for idx, obi_val in zip(obi_ticks_indices, obi_values):
                        if idx < len(rows) and obi_val is not None:
                            rows[idx]["obi"] = obi_val
                except Exception:
                    pass  # Fallback уже использован в make_features
                obi_books_buffer.clear()
                obi_ticks_indices.clear()

            if (i + 1) % 10000 == 0:
                print(f"Processed {i + 1}/{len(ticks)} ticks...")

    # Обрабатываем оставшиеся книги
    if obi_books_buffer:
        try:
            from signals.featurizer import obi_from_book_batch
            obi_values = obi_from_book_batch(obi_books_buffer, depth=5)
            for idx, obi_val in zip(obi_ticks_indices, obi_values):
                if idx < len(rows) and obi_val is not None:
                    rows[idx]["obi"] = obi_val
        except Exception:
            pass

    if use_gpu and _GPU_AVAILABLE:
        try:
            df_gpu = cudf.DataFrame(rows)
            print(f"Extracted {len(df_gpu)} feature rows (GPU DataFrame)")
            return df_gpu
        except Exception:
            print("⚠️ GPU DataFrame failed, falling back to pandas")
    df_cpu = pd.DataFrame(rows)
    print(f"Extracted {len(df_cpu)} feature rows")

    return df_cpu


def main():
    """Main entry point."""
    ap = argparse.ArgumentParser(
        description="Export XAUUSD features from Redis Streams to Parquet/CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export last 2 hours
  python3 export_features.py --start "2025-10-25T10:00:00Z" \\
                              --end "2025-10-25T12:00:00Z" \\
                              --out features.parquet
  
  # Export using epoch timestamps
  python3 export_features.py --start 1729854000000 \\
                              --end 1729861200000 \\
                              --out features.csv
        """
    )
    ap.add_argument("--start", required=True, help="Start time (ms epoch or ISO)")
    ap.add_argument("--end", required=True, help="End time (ms epoch or ISO)")
    ap.add_argument("--out", required=True, help="Output file (.parquet or .csv)")
    ap.add_argument(
        "--delta-window",
        type=int,
        default=120,
        help="Delta rolling window size (default: 120)",
    )
    ap.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enable GPU acceleration for DataFrame output (cuDF + cuIO).",
    )
    args = ap.parse_args()

    # Parse timestamps
    start_ms = parse_time(args.start)
    end_ms = parse_time(args.end)

    if start_ms >= end_ms:
        raise SystemExit("Error: start time must be before end time")

    print(f"Time range: {start_ms} - {end_ms} ({(end_ms - start_ms) / 1000 / 60:.1f} minutes)")

    # Connect to Redis
    print(f"Connecting to Redis: {REDIS_URL}")
    r = redis.from_url(REDIS_URL, decode_responses=True)

    # Load data
    ticks = load_ticks(r, start_ms, end_ms)
    books_map = load_books(r, start_ms, end_ms)

    if not ticks:
        print("Warning: No ticks found in time range")
        return

    use_gpu = bool(args.use_gpu and _GPU_AVAILABLE)
    if args.use_gpu and not _GPU_AVAILABLE:
        print("⚠️ GPU requested but cuDF/CuPy not available, using CPU\n")
    elif use_gpu:
        print("🚀 GPU mode enabled (cuDF)\n")

    # Extract features
    df = extract_features(ticks, books_map, delta_window=args.delta_window, use_gpu=use_gpu)

    # Save
    print(f"Saving to {args.out}...")
    if args.out.endswith(".parquet"):
        df.to_parquet(args.out, index=False)
    else:
        df.to_csv(args.out, index=False)

    print(f"✅ Wrote {len(df)} rows to {args.out}")
    print(f"   Columns: {', '.join(df.columns)}")
    print(f"   Size: {os.path.getsize(args.out) / 1024:.1f} KB")


if __name__ == "__main__":
    main()

