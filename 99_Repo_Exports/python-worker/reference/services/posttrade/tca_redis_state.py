from __future__ import annotations

"""Redis state writer for TCA rollups (Phase B3).

We keep Redis as an *online cache* for gates. Source of truth is Timescale.

Key format (stable):
  tca:<metric>_<stat>_bps[:<delta_sec>]:<sym>:<venue>:<session>:<tf>:<kind>:<side>

Examples:
  tca:is_p95_bps:BTCUSDT:binance:eu:1m:breakout:LONG
  tca:perm_impact_p95_bps:1:BTCUSDT:binance:eu:1m:breakout:LONG

All values are stored as strings (Redis convention), TTL bounded.
"""

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class TcaKeyDims:
    sym: str
    venue: str
    session: str
    tf: str
    kind: str
    side: str

    def norm(self) -> "TcaKeyDims":
        return TcaKeyDims(
            sym=str(self.sym).upper()
            venue=str(self.venue).lower()
            session=str(self.session)
            tf=str(self.tf)
            kind=str(self.kind)
            side=str(self.side).upper()
        )


def make_key(metric: str, stat: str, dims: TcaKeyDims, *, delta_sec: Optional[int] = None) -> str:
    d = dims.norm()
    if delta_sec is None:
        return f"tca:{metric}_{stat}_bps:{d.sym}:{d.venue}:{d.session}:{d.tf}:{d.kind}:{d.side}"
    return f"tca:{metric}_{stat}_bps:{int(delta_sec)}:{d.sym}:{d.venue}:{d.session}:{d.tf}:{d.kind}:{d.side}"


async def write_rollups(
    *
    redis: Any
    dims: TcaKeyDims
    rollups: Dict[str, float]
    ttl_sec: int
    delta_sec: Optional[int] = None
) -> None:
    """Write a small set of rollups into Redis.

    rollups keys:
      - is_p95, eff_spread_p95, perm_impact_p95, realized_spread_p50, ...
    """
    exp_at = int(time.time()) + int(ttl_sec)
    d = dims.norm()
    pipe = redis.pipeline()
    try:
        for k, v in rollups.items():
            # k is already metric_stat style: e.g. "is_p95".
            if "_" not in k:
                continue
            metric, stat = k.split("_", 1)
            key = make_key(metric, stat, d, delta_sec=delta_sec)
            pipe.set(key, str(float(v)), ex=int(ttl_sec))
        # Also keep a heartbeat key for quick "is writer alive" checks.
        pipe.set(f"tca:rollups:last_write_ts:{d.sym}:{d.venue}", str(exp_at), ex=int(ttl_sec))
        await pipe.execute()
    except Exception:
        # Fail-open: Redis cache is optional.
        try:
            await pipe.reset()
        except Exception:
            pass
