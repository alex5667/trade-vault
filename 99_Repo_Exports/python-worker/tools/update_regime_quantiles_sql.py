from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any

import psycopg2
import redis as redis_sync

from utils.time_utils import get_ny_time_millis


def _env(k: str, d: str) -> str:
    return os.getenv(k, d)


@dataclass
class JobCfg:
    """Configuration for regime quantiles computation job."""
    dsn: str
    bars_table: str
    lookback_days: int
    symbols: list[str]
    timeframe: str
    # Column names in bars_table
    col_symbol: str = "symbol"
    col_ts: str = "ts"
    col_adx: str = "adx14"
    col_atrp: str = "atrp14"  # recommended: ratio ATR/price (e.g. 0.002)


def load_cfg() -> JobCfg:
    """Load configuration from environment variables."""
    dsn = _env("ANALYTICS_DSN", _env("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/scanner_analytics"))
    bars_table = _env("REGIME_BARS_TABLE", "bars_1m")
    lookback_days = int(_env("REGIME_Q_LOOKBACK_DAYS", "30"))
    symbols = [s.strip().upper() for s in _env("REGIME_Q_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT").split(",") if s.strip()]
    timeframe = _env("REGIME_Q_TIMEFRAME", "1m")
    col_symbol = _env("REGIME_Q_COL_SYMBOL", "symbol")
    col_ts = _env("REGIME_Q_COL_TS", "ts")
    col_adx = _env("REGIME_Q_COL_ADX", "adx14")
    col_atrp = _env("REGIME_Q_COL_ATRP", "atrp14")
    return JobCfg(
        dsn=dsn,
        bars_table=bars_table,
        lookback_days=lookback_days,
        symbols=symbols,
        timeframe=timeframe,
        col_symbol=col_symbol,
        col_ts=col_ts,
        col_adx=col_adx,
        col_atrp=col_atrp,
    )


def _redis() -> Any:
    """Create Redis connection for publishing quantiles."""
    url = _env("REDIS_URL", "redis://localhost:6379/0")
    return redis_sync.from_url(url, decode_responses=True, socket_connect_timeout=5, socket_timeout=5)


def _publish_row(r: Any, row: dict[str, Any]) -> None:
    """
    Publish quantiles to Redis for low-latency reads in tick loop.
    Key: regime:q:{symbol}:{timeframe}
    Value: JSON with quantiles + sampleSize + updatedAtMs
    TTL: 36h default (longer than update interval for safety)
    """
    sym = (row.get("symbol") or "").upper()
    tf = (row.get("timeframe") or "1m")
    if not sym:
        return
    ttl = int(_env("REGIME_Q_REDIS_TTL_SEC", "129600"))  # 36h default
    payload = dict(row)
    payload["updatedAtMs"] = get_ny_time_millis()
    key = f"regime:q:{sym}:{tf}"
    r.set(key, json.dumps(payload, ensure_ascii=False, separators=(",", ":")), ex=ttl)


UPSERT_SQL = """
INSERT INTO regime_quantiles(
  symbol,timeframe,
  adx_p40,adx_p60,adx_p75,
  atrp_p25,atrp_p50,atrp_p75,
  sample_count,computed_at
)
VALUES(
  %(symbol)s,%(timeframe)s,
  %(adx_p40)s,%(adx_p60)s,%(adx_p75)s,
  %(atrp_p25)s,%(atrp_p50)s,%(atrp_p75)s,
  %(sample_count)s, now()
)
ON CONFLICT(symbol,timeframe) DO UPDATE SET
  adx_p40=EXCLUDED.adx_p40,
  adx_p60=EXCLUDED.adx_p60,
  adx_p75=EXCLUDED.adx_p75,
  atrp_p25=EXCLUDED.atrp_p25,
  atrp_p50=EXCLUDED.atrp_p50,
  atrp_p75=EXCLUDED.atrp_p75,
  sample_count=EXCLUDED.sample_count,
  computed_at=now();
"""


def compute_for_symbol(conn, cfg: JobCfg, symbol: str) -> dict[str, Any]:
    """
    Compute quantiles for a single symbol using SQL percentile_cont.
    Requires bars_table to contain adx14 and atrp14.
    Uses percentile_cont (continuous) for stable quantiles.
    """
    q = f"""
    WITH src AS (
      SELECT
        {cfg.col_adx}  AS adx,
        {cfg.col_atrp} AS atrp
      FROM {cfg.bars_table}
      WHERE {cfg.col_symbol} = %(symbol)s
        AND {cfg.col_ts} >= now() - interval %(days)s
        AND {cfg.col_adx} IS NOT NULL
        AND {cfg.col_atrp} IS NOT NULL
    )
    SELECT
      count(*)::int AS n,
      percentile_cont(0.40) WITHIN GROUP (ORDER BY adx)  AS adx_p40,
      percentile_cont(0.60) WITHIN GROUP (ORDER BY adx)  AS adx_p60,
      percentile_cont(0.75) WITHIN GROUP (ORDER BY adx)  AS adx_p75,
      percentile_cont(0.25) WITHIN GROUP (ORDER BY atrp) AS atrp_p25,
      percentile_cont(0.50) WITHIN GROUP (ORDER BY atrp) AS atrp_p50,
      percentile_cont(0.75) WITHIN GROUP (ORDER BY atrp) AS atrp_p75
    FROM src;
    """
    with conn.cursor() as cur:
        cur.execute(q, {"symbol": symbol, "days": f"{int(cfg.lookback_days)} days"})
        row = cur.fetchone()
        if not row:
            return {"symbol": symbol, "n": 0}
        n, adx_p40, adx_p60, adx_p75, atrp_p25, atrp_p50, atrp_p75 = row
        return {
            "symbol": symbol,
            "timeframe": cfg.timeframe,
            "sample_count": int(n or 0),
            "adx_p40": float(adx_p40 or 0.0),
            "adx_p60": float(adx_p60 or 0.0),
            "adx_p75": float(adx_p75 or 0.0),
            "atrp_p25": float(atrp_p25 or 0.0),
            "atrp_p50": float(atrp_p50 or 0.0),
            "atrp_p75": float(atrp_p75 or 0.0),
        }


def main() -> int:
    """Main entry point for regime quantiles computation."""
    cfg = load_cfg()
    conn = psycopg2.connect(cfg.dsn)
    conn.autocommit = True
    out = {"ts_ms": get_ny_time_millis(), "timeframe": cfg.timeframe, "rows": []}

    # Redis publishing (optional, fail-open)
    pub_redis = bool(int(_env("REGIME_Q_PUBLISH_REDIS", "1")))
    r = None
    if pub_redis:
        try:
            r = _redis()
        except Exception:
            r = None

    try:
        for sym in cfg.symbols:
            st = compute_for_symbol(conn, cfg, sym)
            if int(st.get("sample_count", 0) or 0) <= 0:
                continue

            # Write to DB
            with conn.cursor() as cur:
                cur.execute(UPSERT_SQL, st)

            # Publish to Redis (best-effort, fail-open)
            if r is not None:
                try:
                    _publish_row(r, st)
                except Exception:
                    pass  # fail-open: continue even if Redis publish fails

            out["rows"].append(st)
    finally:
        conn.close()
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=int(os.getenv("REGIME_Q_INTERVAL_SEC", "21600")))
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    if args.once:
        sys.exit(main())
    else:
        print(f"Starting regime_quantiles loop, interval={args.interval}s")
        while True:
            try:
                main()
            except Exception as e:
                print(f"regime_quantiles error: {e}")
            time.sleep(args.interval)
