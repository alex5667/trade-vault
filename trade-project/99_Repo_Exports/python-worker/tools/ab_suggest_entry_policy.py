from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import redis

from common.redis_errors import retry_redis_operation
from services.entry_policy_ab_gate import norm_arm, regime_group
from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    return get_ny_time_millis()


def _j(s: str) -> dict[str, Any]:
    try:
        d = json.loads(s)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


@dataclass
class Stat:
    n: int = 0
    pnl_sum: float = 0.0
    wins: int = 0

    def add(self, pnl: float) -> None:
        self.n += 1
        self.pnl_sum += float(pnl)
        if pnl > 0:
            self.wins += 1

    @property
    def mean(self) -> float:
        return self.pnl_sum / self.n if self.n else 0.0

    @property
    def winrate(self) -> float:
        return self.wins / self.n if self.n else 0.0


def pick(stats: dict[str, Stat], min_n: int) -> str:
    best = "A"
    best_key = (-1e18, -1e18, -1)
    for arm, st in stats.items():
        if st.n < min_n:
            continue
        key = (st.mean, st.winrate, st.n)
        if key > best_key:
            best_key = key
            best = arm
    return best


def main() -> int:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    stream = os.getenv("AB_EVENTS_STREAM", RS.EVENTS_TRADES)
    window_ms = int(os.getenv("AB_WINDOW_MS", str(6 * 60 * 60 * 1000)))
    min_n = int(os.getenv("AB_MIN_N", "30"))
    max_scan = int(os.getenv("AB_MAX_SCAN", "8000"))

    r = redis.from_url(redis_url, decode_responses=True)
    now = _now_ms()
    cutoff = now - window_ms

    agg: defaultdict[str, defaultdict[str, Stat]] = defaultdict(lambda: defaultdict(Stat))

    # Retry xrevrange on BusyLoadingError (Redis loading dataset)
    entries = []
    last_id = "+"
    scanned = 0
    chunk_size = 1000

    while scanned < max_scan:
        batch = retry_redis_operation(
            operation=lambda: r.xrevrange(stream, max=last_id, min="-", count=chunk_size),
            operation_name="xrevrange_chunk",
            max_retries=15,
            base_delay=1.0,
            max_delay=30.0,
            logger_instance=logger,
        )
        if not batch:
            break

        for _id, fields in batch:
            # Skip overlap if any (though xrevrange is exclusive on min usually, here we move max)
            # Actually standard practice is to use '(' for exclusive range
            if _id == last_id:
                continue
            entries.append((_id, fields))

        scanned += len(batch)
        if len(batch) < chunk_size:
            break
        last_id = f"({batch[-1][0]}"  # exclusive for next iteration

    for _id, fields in entries:
        try:
            if (fields.get("event_type") or "") != "POSITION_CLOSED":
                continue
            ts = int(fields.get("ts") or 0)
            if ts <= 0 or ts < cutoff:
                continue
            pnl = float(fields.get("pnl") or 0.0)
            meta = _j(fields.get("meta") or "{}")
            arm = norm_arm((meta.get("ab_arm") or "A"))
            rg = (meta.get("regime") or "na")
            grp = regime_group(rg)
            agg[grp][arm].add(pnl)
        except Exception:
            continue

    winners = {g: pick(agg.get(g) or {}, min_n=min_n) for g in ("default", "thin")}

    ts_ms = _now_ms()
    sug_id = f"ab-entry-policy:{ts_ms}"
    suggestion = {
        "type": "entry_policy_suggestion",
        "id": sug_id,
        "ts_ms": ts_ms,
        "source": "ab_suggester",
        "window_ms": window_ms,
        "min_n": min_n,
        "proposed_sets": {
            "cfg:entry_policy:active_arm:default": winners.get("default", "A"),
            "cfg:entry_policy:active_arm:thin": winners.get("thin", "A"),
        },
        "stats": {
            g: {arm: {"n": st.n, "mean": st.mean, "winrate": st.winrate} for arm, st in (agg.get(g) or {}).items()}
            for g in ("default", "thin")
        },
    }

    key_prefix = os.getenv("AB_SUGGEST_KEY_PREFIX", "cfg:suggestions:entry_policy:")
    key = f"{key_prefix}{sug_id}"

    # Retry write operations on BusyLoadingError
    retry_redis_operation(
        operation=lambda: r.set(key, json.dumps(suggestion, ensure_ascii=False, separators=(",", ":"))),
        operation_name="set suggestion",
        max_retries=10,
        base_delay=1.0,
        max_delay=30.0,
        logger_instance=logger,
    )

    # Optional breadcrumb stream
    out_stream = os.getenv("AB_SUGGEST_STREAM", RS.AB_SUGGESTIONS)
    retry_redis_operation(
        operation=lambda: r.xadd(out_stream, {"payload": json.dumps(suggestion, ensure_ascii=False, separators=(",", ":"))}, maxlen=20000, approximate=True),
        operation_name="xadd suggestion",
        max_retries=10,
        base_delay=1.0,
        max_delay=30.0,
        logger_instance=logger,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
