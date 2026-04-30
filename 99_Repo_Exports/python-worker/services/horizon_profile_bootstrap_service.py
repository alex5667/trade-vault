"""
services/horizon_profile_bootstrap_service.py
──────────────────────────────────────────────
Phase 1: historical calibration bootstrap for hold_target_ms / alpha_half_life_ms.

Reads trades_closed + trades_closed_p0, computes per-symbol/scenario/regime median
profiles, and publishes them to Redis under cfg:horizon:profile:* keys.

Phase 1 is observe-only. ATR_HORIZON_MODE=off means execution never touches these
values. The only effect is that meta.horizon.* in new signals is populated with
history-based numbers instead of static zeros.

Rollback: ATR_HORIZON_PROFILE_REDIS_LOOKUP=0  (horizon_contract.py stops reading)
          or set HORIZON_PROFILE_SOURCES= (disables discovery)

ENV:
  HORIZON_PROFILE_WINDOW_DAYS          (default 45)
  HORIZON_PROFILE_SAMPLE_LIMIT         (default 5000) -- per source/symbol
  HORIZON_PROFILE_MIN_N                (default 40)   -- minimum trades to publish
  HORIZON_PROFILE_STRONG_N             (default 150)  -- "strong confidence" trades
  HORIZON_PROFILE_MIN_HOLD_MS          (default 10000)
  HORIZON_PROFILE_MAX_HOLD_MS          (default 14400000) -- 4h
  HORIZON_PROFILE_MAX_SIGNAL_AGE_CAP_MS (default 300000) -- 5m hard cap
  HORIZON_PROFILE_SOURCES              (default CryptoOrderFlow, '*' = all)
  HORIZON_PROFILE_SYMBOLS              (default '*' = all)
  HORIZON_PROFILE_PG_CONNECT_TIMEOUT_SEC (default 5)
  ANALYTICS_DB_DSN / TRADES_DB_DSN     -- Postgres DSN
  REDIS_URL                            -- Redis URL
"""
from __future__ import annotations

import json
import math
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

try:
    import redis as _redis_lib
except ImportError:  # pragma: no cover
    _redis_lib = None  # type: ignore

try:
    from common.log import setup_logger
    logger = setup_logger("HorizonProfileBootstrapService")
except Exception:  # pragma: no cover
    import logging
    logger = logging.getLogger("HorizonProfileBootstrapService")

try:
    from prometheus_client import Counter, Gauge
    _M_WRITTEN = Counter(
        "trade_horizon_profile_bootstrap_written_total"
        "Horizon profile keys written to Redis per bootstrap run"
        ["source", "symbol"]
    )
    _M_ROWS = Counter(
        "trade_horizon_profile_bootstrap_rows_total"
        "Rows loaded from trades_closed_p0 per bootstrap run"
        ["source", "symbol"]
    )
    _M_LOOKUP_HIT = Counter(
        "trade_horizon_profile_lookup_hit_total"
        "Horizon profile Redis lookup hits by fallback level"
        ["level"],  # exact | scenario | default
    )
    _M_LOOKUP_MISS = Counter(
        "trade_horizon_profile_lookup_miss_total"
        "Horizon profile Redis lookup misses (no key at any level)"
    )
    _M_LOOKUP_STALE = Counter(
        "trade_horizon_profile_stale_total"
        "Horizon profile Redis keys skipped because they were stale"
    )
    _PROM_AVAILABLE = True
except Exception:  # pragma: no cover
    _PROM_AVAILABLE = False

    class _Noop:  # type: ignore
        def labels(self, **kw): return self
        def inc(self, v=1): pass
        def set(self, v): pass

    _M_WRITTEN = _M_ROWS = _Noop()
    _M_LOOKUP_HIT = _M_LOOKUP_MISS = _M_LOOKUP_STALE = _Noop()


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _parse_list(name: str, default: str = "*") -> List[str]:
    raw = str(os.getenv(name, default) or default)
    out: List[str] = []
    for p in raw.split(","):
        s = p.strip()
        if s:
            out.append(s.upper())
    return out or ["*"]


# ---------------------------------------------------------------------------
# Pure math helpers
# ---------------------------------------------------------------------------

def _percentile_disc_sorted(xs: List[int], q: float) -> int:
    """Discrete percentile on a sorted list. No numpy dependency."""
    if not xs:
        return 0
    if q <= 0:
        return xs[0]
    if q >= 1:
        return xs[-1]
    k = int(math.ceil(q * len(xs))) - 1
    k = max(0, min(k, len(xs) - 1))
    return int(xs[k])


def _bucket_by_hold_ms(hold_ms: int) -> str:
    """Map median hold duration to horizon bucket label."""
    if hold_ms <= 0:
        return "unknown"
    if hold_ms < 180_000:   # < 3 min
        return "micro"
    if hold_ms < 720_000:   # < 12 min
        return "short"
    if hold_ms < 2_700_000: # < 45 min
        return "medium"
    return "long"


def _profile_conf(sample_n: int, min_n: int, strong_n: int) -> float:
    """Smooth confidence score [0..1] based on sample size."""
    if sample_n <= 0:
        return 0.0
    if sample_n <= min_n:
        return round(max(0.05, sample_n / max(1, min_n)) * 0.5, 4)
    if sample_n >= strong_n:
        return 1.0
    span = max(1, strong_n - min_n)
    return round(0.5 + 0.5 * ((sample_n - min_n) / span), 4)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class HorizonStatRow:
    scenario: str
    regime: str
    hold_ms: int
    time_to_mfe_ms: int


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class HorizonProfileBootstrapService:
    """
    Computes and publishes horizon calibration profiles to Redis.

    Key hierarchy (fallback chain for lookup):
      cfg:horizon:profile:<source>:<symbol>:<scenario>:<regime>   # exact
      cfg:horizon:profile:<source>:<symbol>:<scenario>:na         # scenario fallback
      cfg:horizon:profile:<source>:<symbol>:default:na            # symbol default
    """

    def __init__(self, dsn: str, redis_url: str) -> None:
        self._dsn = dsn
        self._redis_url = redis_url
        self._redis: Optional[Any] = None
        self._window_days = _env_int("HORIZON_PROFILE_WINDOW_DAYS", 45)
        self._sample_limit = _env_int("HORIZON_PROFILE_SAMPLE_LIMIT", 5000)
        self._min_n = _env_int("HORIZON_PROFILE_MIN_N", 40)
        self._strong_n = _env_int("HORIZON_PROFILE_STRONG_N", 150)
        self._min_hold_ms = _env_int("HORIZON_PROFILE_MIN_HOLD_MS", 10_000)
        self._max_hold_ms = _env_int("HORIZON_PROFILE_MAX_HOLD_MS", 14_400_000)
        self._max_signal_age_cap_ms = _env_int("HORIZON_PROFILE_MAX_SIGNAL_AGE_CAP_MS", 300_000)
        self._enabled_sources = _parse_list("HORIZON_PROFILE_SOURCES", "CryptoOrderFlow")
        self._enabled_symbols = _parse_list("HORIZON_PROFILE_SYMBOLS", "*")

    # ------------------------------------------------------------------
    # Redis client (lazy)
    # ------------------------------------------------------------------

    def _get_redis(self) -> Optional[Any]:
        if self._redis is not None:
            return self._redis
        if _redis_lib is None:
            return None
        try:
            self._redis = _redis_lib.Redis.from_url(self._redis_url, decode_responses=True)
            return self._redis
        except Exception as exc:
            logger.warning("HorizonProfileBootstrapService: redis connect failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Redis key scheme
    # ------------------------------------------------------------------

    def _state_key(self, source: str, symbol: str) -> str:
        return f"horizon_profile_bootstrap:state:{source}:{symbol}"

    def _dirty_key(self, source: str, symbol: str) -> str:
        return f"horizon_profile_bootstrap:dirty:{source}:{symbol}"

    def _profile_key(self, source: str, symbol: str, scenario: str, regime: str) -> str:
        return f"cfg:horizon:profile:{source}:{symbol}:{scenario}:{regime}"

    # ------------------------------------------------------------------
    # Dirty-mark hook (called from batch_trade_writer / analytics_db)
    # ------------------------------------------------------------------

    def on_trade_closed(self, symbol: str, source: str) -> None:
        """Mark symbol+source as needing a profile refresh (best-effort)."""
        try:
            r = self._get_redis()
            if r is None:
                return
            key = self._dirty_key(source, symbol.upper())
            r.incr(key)
            r.expire(key, 86400)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def _open_conn(self):
        return psycopg2.connect(
            self._dsn
            connect_timeout=_env_int("HORIZON_PROFILE_PG_CONNECT_TIMEOUT_SEC", 5)
            application_name="horizon_profile_bootstrap_service"
        )

    def _load_rows(self, conn, source: str, symbol: str) -> List[HorizonStatRow]:
        cutoff_ms = int(time.time() * 1000) - self._window_days * 86400 * 1000
        sql = """
        SELECT
            lower(coalesce(nullif(p0.scenario, ''), 'unknown')) AS scenario
            lower(coalesce(nullif(p0.regime, ''), 'na'))       AS regime
            p0.hold_ms                                          AS hold_ms
            CASE
              WHEN coalesce(p0.time_to_mfe_ms, 0) > 0
                THEN LEAST(p0.time_to_mfe_ms, p0.hold_ms)
              ELSE 0
            END AS time_to_mfe_ms
        FROM trades_closed t
        JOIN trades_closed_p0 p0
          ON p0.order_id = t.order_id
        WHERE t.source      = %(source)s
          AND t.symbol      = %(symbol)s
          AND t.exit_ts_ms  >= %(cutoff_ms)s
          AND coalesce(p0.hold_ms, 0) BETWEEN %(min_hold_ms)s AND %(max_hold_ms)s
        ORDER BY t.exit_ts_ms DESC
        LIMIT %(limit)s
        """
        out: List[HorizonStatRow] = []
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql, {
                "source":       source
                "symbol":       symbol
                "cutoff_ms":    cutoff_ms
                "min_hold_ms":  self._min_hold_ms
                "max_hold_ms":  self._max_hold_ms
                "limit":        self._sample_limit
            })
            for r in cur.fetchall():
                out.append(HorizonStatRow(
                    scenario=str(r["scenario"] or "unknown")
                    regime=str(r["regime"] or "na")
                    hold_ms=_safe_int(r["hold_ms"], 0)
                    time_to_mfe_ms=_safe_int(r["time_to_mfe_ms"], 0)
                ))
        return out

    def _discover_pairs(self, conn) -> List[Tuple[str, str]]:
        """Return (source, symbol) pairs that have trades in the window."""
        cutoff_ms = int(time.time() * 1000) - self._window_days * 86400 * 1000
        sql = """
        SELECT source, symbol, count(*) AS n
        FROM trades_closed
        WHERE exit_ts_ms >= %(cutoff_ms)s
        GROUP BY source, symbol
        ORDER BY n DESC
        """
        out: List[Tuple[str, str]] = []
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql, {"cutoff_ms": cutoff_ms})
            for r in cur.fetchall():
                source = str(r["source"] or "")
                symbol = str(r["symbol"] or "").upper()
                if self._enabled_sources != ["*"] and source.upper() not in self._enabled_sources:
                    continue
                if self._enabled_symbols != ["*"] and symbol not in self._enabled_symbols:
                    continue
                out.append((source, symbol))
        return out

    # ------------------------------------------------------------------
    # Profile computation
    # ------------------------------------------------------------------

    def _calc_profile(self, rows: List[HorizonStatRow]) -> Optional[Dict[str, Any]]:
        """
        Compute calibration profile from a list of trade stat rows.

        hold_target_ms      = p50(hold_ms)
        alpha_half_life_ms  = p50(min(time_to_mfe_ms, hold_ms))
        max_signal_age_ms   = clamp(min(alpha_half_life_ms, 0.33 * hold_target_ms), 15s, 5m)
        risk_horizon_bucket = f(hold_target_ms)

        Returns None when sample_n < min_n.
        """
        if len(rows) < self._min_n:
            return None
        hold = sorted(int(r.hold_ms) for r in rows if r.hold_ms > 0)
        if len(hold) < self._min_n:
            return None

        mfe = sorted(int(r.time_to_mfe_ms) for r in rows if r.time_to_mfe_ms > 0)

        hold_p50 = _percentile_disc_sorted(hold, 0.50)
        hold_p75 = _percentile_disc_sorted(hold, 0.75)
        mfe_p50  = _percentile_disc_sorted(mfe, 0.50) if mfe else max(15_000, int(hold_p50 * 0.5))

        hold_target_ms     = int(hold_p50)
        alpha_half_life_ms = int(max(15_000, min(mfe_p50, hold_target_ms)))
        max_signal_age_ms  = int(max(
            15_000
            min(alpha_half_life_ms, int(hold_target_ms * 0.33), self._max_signal_age_cap_ms)
        ))
        bucket = _bucket_by_hold_ms(hold_target_ms)
        n = len(hold)
        conf = _profile_conf(n, self._min_n, self._strong_n)

        return {
            "contract_ver":         2
            "hold_target_ms":       hold_target_ms
            "alpha_half_life_ms":   alpha_half_life_ms
            "max_signal_age_ms":    max_signal_age_ms
            "risk_horizon_bucket":  bucket
            "profile_source":       "history"
            "profile_conf":         conf
            "reason_code":          "HZ_HISTORY_PROFILE"
            "sample_n":             n
            "hold_p50_ms":          hold_p50
            "hold_p75_ms":          hold_p75
            "time_to_mfe_p50_ms":   mfe_p50
            "updated_at_ms":        int(time.time() * 1000)
        }

    # ------------------------------------------------------------------
    # Redis publish
    # ------------------------------------------------------------------

    def _publish_profiles(self, source: str, symbol: str, rows: List[HorizonStatRow]) -> int:
        """
        Build and write profiles at three key granularities:
          1. exact (scenario + regime)
          2. scenario fallback (scenario + 'na')
          3. symbol default ('default' + 'na')

        Returns number of keys written.
        """
        r = self._get_redis()
        if r is None:
            logger.warning("HorizonProfileBootstrapService: redis unavailable, skipping publish")
            return 0

        # Group rows
        exact:       Dict[Tuple[str, str], List[HorizonStatRow]] = {}
        by_scenario: Dict[str, List[HorizonStatRow]] = {}
        for row in rows:
            exact.setdefault((row.scenario, row.regime), []).append(row)
            by_scenario.setdefault(row.scenario, []).append(row)

        written = 0

        # Level 1: exact (scenario, regime)
        for (scenario, regime), items in exact.items():
            prof = self._calc_profile(items)
            if prof:
                key = self._profile_key(source, symbol, scenario, regime)
                r.set(key, _dump(prof))
                written += 1

        # Level 2: scenario fallback (regime = 'na')
        for scenario, items in by_scenario.items():
            prof = self._calc_profile(items)
            if prof:
                key = self._profile_key(source, symbol, scenario, "na")
                r.set(key, _dump(prof))
                written += 1

        # Level 3: symbol default
        prof_default = self._calc_profile(rows)
        if prof_default:
            key = self._profile_key(source, symbol, "default", "na")
            r.set(key, _dump(prof_default))
            written += 1

        return written

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    def run_once(self) -> int:
        """
        Full bootstrap run: discover all symbol/source pairs, load stats
        publish profiles to Redis.

        Returns total number of Redis keys written.
        Called by: nightly runner (of_timers_worker) and __main__.
        """
        conn = self._open_conn()
        try:
            pairs = self._discover_pairs(conn)
            if not pairs:
                logger.info("HorizonProfileBootstrapService: no pairs discovered in window=%dd", self._window_days)
                return 0
            logger.info("HorizonProfileBootstrapService: discovered %d source/symbol pairs", len(pairs))

            total_written = 0
            for source, symbol in pairs:
                try:
                    rows = self._load_rows(conn, source, symbol)
                    if not rows:
                        logger.debug("HorizonProfileBootstrapService: no rows for %s/%s", source, symbol)
                        continue

                    written = self._publish_profiles(source, symbol, rows)
                    total_written += written

                    # Persist state
                    r = self._get_redis()
                    if r is not None:
                        r.set(
                            self._state_key(source, symbol)
                            _dump({"updated_at_ms": int(time.time() * 1000), "sample_n": len(rows), "keys_written": written})
                        )

                    try:
                        _M_WRITTEN.labels(source=source, symbol=symbol).inc(written)
                        _M_ROWS.labels(source=source, symbol=symbol).inc(len(rows))
                    except Exception:
                        pass

                    logger.info(
                        "HorizonProfileBootstrapService: %s/%s rows=%d keys_written=%d"
                        source, symbol, len(rows), written
                    )
                except Exception as exc:
                    logger.exception("HorizonProfileBootstrapService: failed for %s/%s: %s", source, symbol, exc)

            logger.info("HorizonProfileBootstrapService: run_once total_written=%d", total_written)
            return total_written
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_svc: Optional[HorizonProfileBootstrapService] = None
_lock = threading.Lock()


def get_horizon_profile_bootstrap_service() -> HorizonProfileBootstrapService:
    global _svc
    if _svc is None:
        with _lock:
            if _svc is None:
                dsn = (
                    os.getenv("ANALYTICS_DB_DSN")
                    or os.getenv("TRADES_DB_DSN")
                    or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
                )
                redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
                _svc = HorizonProfileBootstrapService(dsn=dsn, redis_url=redis_url)
    return _svc


# ---------------------------------------------------------------------------
# __main__ entry point (for nightly runner via run_tool)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    n = get_horizon_profile_bootstrap_service().run_once()
    logger.info("horizon profile bootstrap written=%s", n)
