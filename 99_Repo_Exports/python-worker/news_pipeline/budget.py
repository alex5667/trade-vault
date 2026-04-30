"""
news_pipeline/budget.py — Reserve-based USD budget enforcement via Redis Lua.

Strict approach: INCRBYFLOAT + immediate rollback if limit exceeded.
This prevents LLM calls that would bust the daily USD cap.

Used by: analyzer_worker.py (reasoner).
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any


# ── Lua script ─────────────────────────────────────────────────────────────────
# KEYS[1] = budget key (e.g. "news:budget:usd:20250310")
# ARGV[1] = reserve_amount (float string)
# ARGV[2] = daily_limit_usd (float string)
# Returns: {"ok": 1|0, "used": <float>}
# Atomically increments, checks, rolls back if over limit.
_LUA_RESERVE_USD = """
local key    = KEYS[1]
local amt    = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])
local used   = tonumber(redis.call('INCRBYFLOAT', key, amt))
if used <= limit then
    -- TTL: 3 days so stale keys auto-expire
    redis.call('EXPIRE', key, 259200)
    return {1, tostring(used)}
else
    -- Roll back: we went over
    local rolled = tonumber(redis.call('INCRBYFLOAT', key, -amt))
    return {0, tostring(rolled)}
end
"""


@dataclass
class BudgetResult:
    """Result of a reserve_usd() call."""
    ok: bool       # True = reserved OK, False = would exceed limit
    used: float    # Current usage (after attempted increment or rollback)
    limit: float   # Daily limit passed in


async def reserve_usd(
    r: Any
    *
    daily_limit_usd: float
    reserve_usd: float
) -> BudgetResult:
    """Atomically reserve `reserve_usd` USD against today's budget.

    If the reservation would exceed `daily_limit_usd`, it is rolled back
    and `BudgetResult.ok` is False — caller must NOT call the LLM.

    Safe to call from concurrent worker coroutines: Lua script is atomic.

    Args:
        r: async redis client (redis.asyncio.Redis)
        daily_limit_usd: total daily cap (e.g. 10.0)
        reserve_usd: per-call upper bound to reserve (e.g. 0.02)

    Returns:
        BudgetResult with ok/used/limit
    """
    # Key is date-scoped so it resets at midnight UTC automatically
    today = time.strftime("%Y%m%d", time.gmtime())
    key = f"news:budget:usd:{today}"

    result = await r.eval(
        _LUA_RESERVE_USD
        1,          # number of KEYS
        key,        # KEYS[1]
        str(reserve_usd)
        str(daily_limit_usd)
    )
    # result: [ok_int, used_str]
    ok = int(result[0]) == 1
    used = float(result[1])
    return BudgetResult(ok=ok, used=used, limit=daily_limit_usd)
