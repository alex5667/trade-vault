from __future__ import annotations

import os
import logging
import asyncio
from typing import Dict, List, Tuple, Any, Optional

import asyncpg

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("nightly_slippage_calibrator")


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return float(default)


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return int(default)


def _quantile(xs: List[float], q: float) -> float:
    """Deterministic quantile without numpy (Hyndman-Fan type 7, same as numpy default)."""
    if not xs:
        return 0.0
    q = max(0.0, min(1.0, float(q)))
    ys = sorted(float(x) for x in xs)
    n = len(ys)
    if n == 1:
        return float(ys[0])
    # h = (n-1)*q, i=floor(h), frac=h-i
    h = (n - 1) * q
    i = int(h)
    frac = h - i
    if i >= n - 1:
        return float(ys[-1])
    return float(ys[i] * (1.0 - frac) + ys[i + 1] * frac)


def _safe_f(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(d)


async def run() -> bool:
    db_url = os.getenv("DATABASE_URL", "postgresql://trading:trading@scanner-postgres:5432/scanner_analytics")
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

    if aioredis is None:
        logger.error("redis.asyncio is not available")
        return False

    logger.info("Connecting: postgres=%s redis=%s", db_url, redis_url)

    r = aioredis.Redis.from_url(redis_url, decode_responses=True)
    try:
        conn = await asyncpg.connect(db_url)
    except Exception as e:
        logger.error("DB connect failed: %s", e)
        try:
            await r.aclose()
        except Exception:
            pass
        return False

    # Calibration hyperparameters (all tunable via ENV)
    m_spread     = _env_float("CALIB_SPREAD_MULT",    "0.5")
    size_ref     = _env_float("CALIB_SIZE_REF_USD",   "10000.0")
    power        = _env_float("CALIB_SIZE_POWER",     "1.0")
    q            = _env_float("CALIB_QUANTILE",       "0.85")
    c_min        = _env_float("CALIB_C_MIN",          "1.0")
    c_max        = _env_float("CALIB_C_MAX",          "50.0")
    x_min        = _env_float("CALIB_X_MIN",          "1e-6")
    ema_alpha    = _env_float("CALIB_EMA_ALPHA",      "0.2")
    lookback_days = _env_int("CALIB_LOOKBACK_DAYS",   "14")
    min_samples  = _env_int("CALIB_MIN_SAMPLES",      "30")

    query = f"""
    SELECT
      sym,
      exec_regime_bucket,
      spread_bps,
      impact_proxy,
      size_usd,
      realized_slip_worse_bps
    FROM v_exec_slippage_eval
    WHERE ts >= now() - interval '{lookback_days} days'
    """

    logger.info("Querying v_exec_slippage_eval (lookback=%dd)", lookback_days)
    rows = await conn.fetch(query)

    groups: Dict[Tuple[str, str], List[float]] = {}
    for row in rows:
        sym    = str(row.get("sym") or "")
        bucket = str(row.get("exec_regime_bucket") or "NORMAL")
        spread = _safe_f(row.get("spread_bps"), 0.0)
        proxy  = _safe_f(row.get("impact_proxy"), 0.0)
        size   = _safe_f(row.get("size_usd"), 0.0)
        worse_slip = _safe_f(row.get("realized_slip_worse_bps"), 0.0)

        if not sym:
            continue

        # Isolate impact component by removing spread part
        spread_part = m_spread * max(0.0, spread)
        impact_part = max(0.0, worse_slip - spread_part)

        # x = |proxy| * (size/size_ref)^power
        if size <= 0:
            size = size_ref
        x = abs(proxy) * ((size / max(size_ref, 1e-9)) ** max(0.0, power))
        if x <= x_min:
            continue

        ratio = impact_part / x
        if ratio <= 0:
            continue

        groups.setdefault((sym, bucket), []).append(float(ratio))

    logger.info("Groups found: %d", len(groups))

    updated = 0
    for (sym, bucket), ratios in sorted(groups.items()):
        n = len(ratios)
        if n < min_samples:
            logger.info("skip %s %s: samples=%d < %d", sym, bucket, n, min_samples)
            continue

        c_fit = _quantile(ratios, q)
        c_fit = max(c_min, min(c_max, c_fit))

        # EMA blend with existing value (smooth, prevent sudden jumps)
        key = f"cfg:slippage_decomp_impact_coeff_bps:{sym}:{bucket}"
        old = await r.get(key)
        if old not in (None, "", "na"):
            try:
                c_old = float(old)
                c_new = (1.0 - ema_alpha) * c_old + ema_alpha * c_fit
            except Exception:
                c_new = c_fit
        else:
            c_new = c_fit

        c_new = max(c_min, min(c_max, c_new))
        await r.set(key, f"{c_new:.2f}")
        updated += 1

        logger.info("[%s|%s] n=%d q=%.2f fit=%.2f -> new=%.2f (old=%s)",
                    sym, bucket, n, q, c_fit, c_new, old)

    await conn.close()
    try:
        await r.aclose()
    except Exception:
        pass

    logger.info("done: updated=%d", updated)
    return True


if __name__ == "__main__":
    ok = asyncio.run(run())
    raise SystemExit(0 if ok else 2)
