#!/usr/bin/env python3
from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
build_edge_stack_train_dataset.py

Создает train dataset для edge_stack_v1 из metrics:ml_confirm + trades:closed.

Схема выходной строки JSONL:
{
  "ts_ms": int,
  "y": int (0/1),
  "direction": str (BUY/SELL),
  "scenario": str (trend/range/reversal/etc),
  "indicators": {
    "spread_bps": float,
    "expected_slippage_bps": float,
    "exec_risk_norm": float,
    "delta_z": float,
    "obi_z": float,
    "ofi_z": float,
    "liq_score": float,
    ...
  }
}

Лейбл y:
  r_mult = pnl / risk_usd
  y = 1 если r_mult >= R_MIN, иначе 0
  R_MIN по умолчанию: 0.20 (можно настроить через --r-min)

Usage:
  python3 -m tools.build_edge_stack_train_dataset \
    --since-hours 168 \
    --out /tmp/edge_train.jsonl \
    --r-min 0.20
"""

import argparse
import json
import os
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis
import contextlib


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _s(v: Any, d: str = "") -> str:
    return str(v) if v is not None else d


def _normalize_sid(raw_sid: Any, *, symbol: str, ts_ms: int) -> str:
    """Normalize sid to canonical: crypto-of:{SYMBOL}:{TS_MS}."""
    sym = (symbol or "").upper() or "NA"
    try:
        ts = int(ts_ms)
    except Exception:
        ts = 0
    s = (raw_sid or "")
    if s.startswith("crypto-of:"):
        head = s.split("|", 1)[0]
        parts = head.split(":", 2)
        if len(parts) == 3:
            sym2 = (parts[1] or sym).upper()
            try:
                ts2 = int(float(parts[2]))
            except Exception:
                ts2 = ts
            return f"crypto-of:{sym2}:{ts2}"
        return f"crypto-of:{sym}:{ts}"
    if "|" in s:
        try:
            p = s.split("|")
            sym2 = (p[0] or sym).upper()
            ts2 = int(float(p[1])) if len(p) > 1 else ts
            return f"crypto-of:{sym2}:{ts2}"
        except Exception:
            return f"crypto-of:{sym}:{ts}"
    if not s:
        return f"crypto-of:{sym}:{ts}"
    return s


def read_ml_confirm_metrics(
    r: redis.Redis,
    stream: str,
    since_ms: int,
    max_scan: int = 500_000,
) -> dict[str, dict[str, Any]]:
    """Читает metrics:ml_confirm и возвращает dict по sid."""
    by_sid: dict[str, dict[str, Any]] = {}
    scanned = 0
    last_id = f"{since_ms}-0"

    while scanned < max_scan:
        batch = r.xrange(stream, min=last_id, max="+", count=1000)
        if not batch:
            break

        for msg_id, fields in batch:
            scanned += 1
            if scanned >= max_scan:
                break

            # Извлекаем sid
            raw_sid = fields.get("sid") or fields.get("signal_id") or ""
            symbol = _s(fields.get("symbol"), "")
            ts_ms = _safe_int(fields.get("ts_ms"), 0)
            if not ts_ms:
                # Попробуем извлечь из msg_id
                try:
                    ts_ms = int(msg_id.split("-")[0])
                except Exception:
                    continue

            sid = _normalize_sid(raw_sid, symbol=symbol, ts_ms=ts_ms)
            if not sid or sid == "crypto-of:NA:0":
                continue

            # Извлекаем indicators из payload или полей
            indicators: dict[str, Any] = {}
            payload_str = fields.get("payload") or fields.get("indicators") or ""
            if payload_str and isinstance(payload_str, str):
                try:
                    payload = json.loads(payload_str)
                    if isinstance(payload, dict):
                        indicators = payload.get("indicators", {})
                        if not indicators:
                            indicators = payload
                except Exception:
                    pass

            # Если indicators не в payload, пробуем извлечь из полей
            if not indicators:
                for k in (
                    "spread_bps", "expected_slippage_bps", "exec_risk_norm",
                    "delta_z", "obi_z", "ofi_z", "liq_score",
                    "book_staleness_ms", "pressure", "rule_score",
                    "mae_r", "mfe_r", "adverse_proxy",
                ):
                    if k in fields:
                        v = fields.get(k)
                        if v is not None:
                            with contextlib.suppress(Exception):
                                indicators[k] = float(v) if "." in str(v) else int(v)

            direction = _s(fields.get("direction"), "").upper()
            scenario = _s(fields.get("scenario") or fields.get("scenario_v4"), "").lower()

            by_sid[sid] = {
                "ts_ms": ts_ms,
                "symbol": symbol,
                "direction": direction,
                "scenario": scenario,
                "indicators": indicators,
            }

        if batch:
            last_id = batch[-1][0]

    return by_sid


def read_trades_closed(
    r: redis.Redis,
    stream: str,
    since_ms: int,
    max_scan: int = 500_000,
) -> dict[str, dict[str, Any]]:
    """Читает trades:closed и возвращает dict по sid."""
    by_sid: dict[str, dict[str, Any]] = {}
    scanned = 0
    last_id = f"{since_ms}-0"

    while scanned < max_scan:
        batch = r.xrange(stream, min=last_id, max="+", count=1000)
        if not batch:
            break

        for msg_id, fields in batch:
            scanned += 1
            if scanned >= max_scan:
                break

            # Проверяем, что это POSITION_CLOSED
            event_type = _s(fields.get("event_type"), "").upper()
            if event_type not in ("POSITION_CLOSED", "CLOSE"):
                continue

            # Извлекаем sid
            raw_sid = fields.get("sid") or fields.get("signal_id") or ""
            symbol = _s(fields.get("symbol"), "")
            ts_ms = _safe_int(fields.get("ts_ms") or fields.get("exit_ts_ms"), 0)
            if not ts_ms:
                try:
                    ts_ms = int(msg_id.split("-")[0])
                except Exception:
                    continue

            sid = _normalize_sid(raw_sid, symbol=symbol, ts_ms=ts_ms)
            if not sid or sid == "crypto-of:NA:0":
                continue

            # Извлекаем pnl и risk_usd
            pnl = _safe_float(fields.get("pnl") or fields.get("pnl_net"), 0.0)
            risk_usd = _safe_float(fields.get("risk_usd") or fields.get("risk"), 0.0)

            # Если в payload
            payload_str = fields.get("payload")
            if payload_str and isinstance(payload_str, str):
                try:
                    payload = json.loads(payload_str)
                    if isinstance(payload, dict):
                        if pnl == 0.0:
                            pnl = _safe_float(payload.get("pnl") or payload.get("pnl_net"), pnl)
                        if risk_usd == 0.0:
                            risk_usd = _safe_float(payload.get("risk_usd") or payload.get("risk"), risk_usd)
                except Exception:
                    pass

            by_sid[sid] = {
                "pnl": pnl,
                "risk_usd": risk_usd,
                "r_mult": pnl / risk_usd if risk_usd > 0 else 0.0,
            }

        if batch:
            last_id = batch[-1][0]

    return by_sid


def build_dataset(
    ml_confirm_by_sid: dict[str, dict[str, Any]],
    trades_by_sid: dict[str, dict[str, Any]],
    r_min: float = 0.20,
) -> list[dict[str, Any]]:
    """Строит train dataset из joined данных."""
    dataset: list[dict[str, Any]] = []

    for sid, ml_data in ml_confirm_by_sid.items():
        trade_data = trades_by_sid.get(sid)
        if not trade_data:
            continue

        r_mult = trade_data.get("r_mult", 0.0)
        y = 1 if r_mult >= r_min else 0

        row = {
            "ts_ms": ml_data["ts_ms"],
            "y": y,
            "direction": ml_data["direction"],
            "scenario": ml_data["scenario"],
            "indicators": ml_data["indicators"],
        }

        dataset.append(row)

    # Сортируем по ts_ms для детерминизма
    dataset.sort(key=lambda x: int(x.get("ts_ms", 0)))

    return dataset


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build edge_stack_v1 train dataset from metrics:ml_confirm + trades:closed"
    )
    ap.add_argument(
        "--since-hours",
        type=float,
        default=168.0,
        help="Hours ago to start from (default: 168 = 7 days)",
    )
    ap.add_argument(
        "--out",
        type=str,
        required=True,
        help="Output JSONL file path",
    )
    ap.add_argument(
        "--r-min",
        type=float,
        default=0.20,
        help="Minimum R-multiple for y=1 label (default: 0.20)",
    )
    ap.add_argument(
        "--redis-url",
        type=str,
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        help="Redis URL",
    )
    ap.add_argument(
        "--ml-confirm-stream",
        type=str,
        default=os.getenv("ML_CONFIRM_METRICS_STREAM", RS.ML_CONFIRM_METRICS),
        help="ML confirm metrics stream",
    )
    ap.add_argument(
        "--trades-stream",
        type=str,
        default=os.getenv("TRADES_CLOSED_STREAM") or os.getenv("TRADE_EVENTS_STREAM", RS.TRADES_CLOSED),
        help="Trades closed stream",
    )
    ap.add_argument(
        "--max-scan",
        type=int,
        default=500_000,
        help="Max messages to scan per stream (default: 500000)",
    )
    args = ap.parse_args()

    since_ms = _now_ms() - int(float(args.since_hours) * 3600.0 * 1000.0)

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    print(f"Reading ml_confirm metrics from {args.ml_confirm_stream} (since {since_ms})...")
    ml_confirm_by_sid = read_ml_confirm_metrics(
        r, args.ml_confirm_stream, since_ms, max_scan=args.max_scan
    )
    print(f"  Found {len(ml_confirm_by_sid)} ml_confirm records")

    print(f"Reading trades closed from {args.trades_stream} (since {since_ms})...")
    trades_by_sid = read_trades_closed(
        r, args.trades_stream, since_ms, max_scan=args.max_scan
    )
    print(f"  Found {len(trades_by_sid)} closed trades")

    print(f"Building dataset (r_min={args.r_min})...")
    dataset = build_dataset(ml_confirm_by_sid, trades_by_sid, r_min=args.r_min)
    print(f"  Built {len(dataset)} training examples")

    # Статистика по лейблам
    y_1 = sum(1 for row in dataset if row.get("y") == 1)
    y_0 = len(dataset) - y_1
    print(f"  Labels: y=1: {y_1}, y=0: {y_0}")

    # Записываем JSONL
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for row in dataset:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

    print(f"✅ Written {len(dataset)} rows to {args.out}")


if __name__ == "__main__":
    main()

