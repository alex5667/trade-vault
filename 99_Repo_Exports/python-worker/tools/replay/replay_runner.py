from __future__ import annotations

"""
Record & Replay runner:
- читает jsonl записи ctx (одна строка = один ctx snapshot)
- прогоняет через UnifiedSignalPipeline.process(ctx)
- сохраняет результаты через CapturePublisher (один сигнал = один JSON)
"""

import json
import os
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Iterable, Optional, Sequence

from tools.replay.replay_factory import build_unified_pipeline_for_replay


def _to_ctx(obj: dict[str, Any]) -> Any:
    """
    Превращаем dict в объект с attribute-access.
    Это важно: пайплайн обычно работает через getattr(ctx, "...").
    """
    return SimpleNamespace(**obj)


def iter_ctx_jsonl(path: str) -> Iterable[Any]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            if not isinstance(d, dict):
                continue
            yield _to_ctx(d)


def _to_str(x: Any) -> str:
    try:
        if x is None:
            return ""
        if isinstance(x, (bytes, bytearray)):
            return x.decode("utf-8", "ignore")
        return str(x)
    except Exception:
        return ""


def iter_ctx_redis_streams(
    *,
    redis_url: str,
    stream: str,
    symbols_set: str,
    start_id: str = "0-0",
    count: int = 500,
    max_batches: int = 100000,
    symbols: Optional[Sequence[str]] = None,
) -> Iterable[Any]:
    """
    Reads ctx snapshots from Redis Streams.

    Split-streams friendly:
      - if stream contains '{sym}', expands per symbol from symbols_set (or explicit symbols)
      - else reads the stream directly

    Each record may have 'payload' JSON string or flat fields.
    Ordering is per symbol (no global merge).
    """
    import redis  # redis-py

    r = redis.Redis.from_url(redis_url, decode_responses=True)

    if "{sym}" in stream:
        if symbols:
            syms = [str(s) for s in symbols if str(s)]
        else:
            syms = sorted([_to_str(x) for x in (r.smembers(symbols_set) or set()) if _to_str(x)])
    else:
        syms = ["_single_"]

    for sym in syms:
        skey = stream.format(sym=sym) if sym != "_single_" else stream
        last = start_id
        for _ in range(max_batches):
            out = r.xread({skey: last}, count=count, block=0) or []
            if not out:
                break
            _sname, entries = out[0]
            if not entries:
                break
            for msg_id, fields in entries:
                last = _to_str(msg_id) or last
                d = dict(fields or {})
                p = d.get("payload")
                if isinstance(p, str) and p and (p.startswith("{") or p.startswith("[")):
                    try:
                        p0 = json.loads(p)
                        if isinstance(p0, dict):
                            yield _to_ctx(p0)
                            continue
                    except Exception:
                        pass
                yield _to_ctx(d)


@dataclass
class ReplayResult:
    processed: int
    published: int


def run_replay(
    *,
    input_jsonl: Optional[str],
    logger: Any,
    output_signals_jsonl: Optional[str] = None,
    # deps override:
    scoring_engine: Optional[Any] = None,
    regime_service: Optional[Any] = None,
    golden_logic: Optional[Any] = None,
    exec_filters: Optional[Any] = None,
    calibrator: Optional[Any] = None,
    # optional redis-stream source:
    redis_url: Optional[str] = None,
    redis_stream: Optional[str] = None,
    redis_symbols_set: str = os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols"),
    redis_start_id: str = "0-0",
) -> ReplayResult:
    bundle = build_unified_pipeline_for_replay(
        logger=logger,
        scoring_engine=scoring_engine,
        regime_service=regime_service,
        golden_logic=golden_logic,
        exec_filters=exec_filters,
        publisher=None,  # CapturePublisher by default
        calibrator=calibrator,
    )

    pipeline = bundle.pipeline
    publisher = bundle.publisher

    processed = 0
    if redis_stream:
        for ctx in iter_ctx_redis_streams(
            redis_url=str(redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")),
            stream=str(redis_stream),
            symbols_set=str(redis_symbols_set),
            start_id=str(redis_start_id),
        ):
            processed += 1
            pipeline.process(ctx)
    else:
        if not input_jsonl:
            raise ValueError("input_jsonl is required when redis_stream is not provided")
        for ctx in iter_ctx_jsonl(input_jsonl):
            processed += 1
            # UnifiedSignalPipeline в вашем проекте дергается как .process(ctx)
            pipeline.process(ctx)

    published = int(len(getattr(publisher, "signals", []) or []))
    if output_signals_jsonl and hasattr(publisher, "dump_jsonl"):
        publisher.dump_jsonl(output_signals_jsonl)

    return ReplayResult(processed=processed, published=published)
