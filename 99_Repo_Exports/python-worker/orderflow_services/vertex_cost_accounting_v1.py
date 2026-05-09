from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any

try:
    import redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore


@dataclass
class CostRecord:
    provider: str
    model_name: str
    request_id: str
    batch_id: str
    ts_ms: int
    input_chars: int
    output_chars: int
    estimated_cost_usd: float
    actual_cost_usd: float
    context_cache_ref: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def estimate_cost_usd(*, model_name: str, input_chars: int, output_chars: int) -> float:
    """Conservative local estimate for accounting/budget visibility.

    Pricing is intentionally env-driven because Vertex prices can change.
    Defaults are low and should be overridden in production.
    """,
    if "flash-lite" in model_name:
        in_per_mchar = float(os.getenv("VERTEX_FLASH_LITE_IN_USD_PER_MCHARS", "0.10") or 0.10)
        out_per_mchar = float(os.getenv("VERTEX_FLASH_LITE_OUT_USD_PER_MCHARS", "0.40") or 0.40)
    else:
        in_per_mchar = float(os.getenv("VERTEX_FLASH_IN_USD_PER_MCHARS", "0.30") or 0.30)
        out_per_mchar = float(os.getenv("VERTEX_FLASH_OUT_USD_PER_MCHARS", "1.20") or 1.20)
    return (max(0, input_chars) / 1_000_000.0) * in_per_mchar + (max(0, output_chars) / 1_000_000.0) * out_per_mchar


def record_cost(redis_url: str, rec: CostRecord) -> None:
    if redis is None:
        return
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        day = time.strftime("%Y%m%d", time.gmtime(int(rec.ts_ms) / 1000.0))
        hour = time.strftime("%Y%m%d%H", time.gmtime(int(rec.ts_ms) / 1000.0))
        last_key = os.getenv("ML_ANALYSIS_COST_LAST_HASH", "metrics:ml:analysis_cost:last")
        day_key = f"metrics:ml:analysis_cost:usd:{rec.provider}:{rec.model_name}:{day}"
        hour_key = f"metrics:ml:analysis_cost:reqs:{rec.provider}:{rec.model_name}:{hour}"
        stream = os.getenv("ML_ANALYSIS_COST_STREAM", "stream:ml:analysis_cost")
        p = r.pipeline()
        p.hset(last_key, mapping={
            "provider": rec.provider,
            "model_name": rec.model_name,
            "request_id": rec.request_id,
            "batch_id": rec.batch_id,
            "ts_ms": rec.ts_ms,
            "input_chars": rec.input_chars,
            "output_chars": rec.output_chars,
            "estimated_cost_usd": rec.estimated_cost_usd,
            "actual_cost_usd": rec.actual_cost_usd,
            "context_cache_ref": rec.context_cache_ref,
        })
        p.incrbyfloat(day_key, float(rec.actual_cost_usd or rec.estimated_cost_usd))
        p.expire(day_key, 7 * 86400)
        p.incr(hour_key, 1)
        p.expire(hour_key, 3 * 86400)
        p.xadd(stream, {
            "schema_version": 1,
            "provider": rec.provider,
            "model_name": rec.model_name,
            "request_id": rec.request_id,
            "batch_id": rec.batch_id,
            "ts_ms": rec.ts_ms,
            "input_chars": rec.input_chars,
            "output_chars": rec.output_chars,
            "estimated_cost_usd": rec.estimated_cost_usd,
            "actual_cost_usd": rec.actual_cost_usd,
            "context_cache_ref": rec.context_cache_ref,
            "payload_json": json.dumps(rec.to_dict(), ensure_ascii=False),
        }, maxlen=int(os.getenv("ML_ANALYSIS_COST_STREAM_MAXLEN", "100000") or 100000), approximate=True)
        p.execute()
    except Exception:
        return
