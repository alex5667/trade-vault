import json
import os
import time

import psycopg2
import redis

try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None
import logging

logger = logging.getLogger("atr_policy_regime_stress_service")

def _dsn():
    return os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN") or "postgresql://postgres:12345@postgres:5432/scanner_analytics"

def _redis():
    if get_atr_redis is not None:
        return get_atr_redis()
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

def _safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default

def _get_active_symbols(r: redis.Redis) -> list[str]:
    # Very heuristic. Normally we fetch from whitelist, or we fetch from allocator states.
    # For now, let's grab all spread_ema keys
    keys = r.keys("spread_ema_half_bps:*")
    syms = set()
    for k in keys:
        s = k.split(":")[-1]
        if s:
             syms.add(s)
    return list(syms)

def _read_venue_stress(r: redis.Redis) -> bool:
    # Just an example logic: MT5 error counts from stream or keys
    rate = _safe_float(r.get("metrics:venue_errors:mt5_rate") or 0.0)
    venue_thr = _safe_float(os.getenv("ATR_POLICY_VENUE_ERROR_THR", "0.10"))
    if os.getenv("ATR_POLICY_STRESS_USE_VENUE_ERRORS", "1") == "1":
        if rate > venue_thr:
            return True
        if r.get("state:venue_stress:mt5") == "1":
             return True
    return False

def _read_news_gate(r: redis.Redis, symbol: str) -> bool:
    if os.getenv("ATR_POLICY_STRESS_USE_NEWS_GATE", "1") == "1":
        # Usually checking NewsGate lock from Redis
        if r.get(f"news:lock:{symbol}") == "1":
            return True
        # also global
        if r.get("news:lock:__all__") == "1":
             return True
    return False

def _read_dt_quality(r: redis.Redis, symbol: str) -> list[str]:
    flags = []
    # In live system, there's streams or keys
    drift = r.get(f"feature_drift:active:{symbol}")
    if drift == "1" and os.getenv("ATR_POLICY_STRESS_USE_DRIFT_GATE", "1") == "1":
         flags.append("drift_lock")

    # Stale book or tick gap
    stale = r.get(f"state:data_quality:stale_book:{symbol}")
    if stale == "1":
         flags.append("tick_gap_critical")
    return flags

def run_once() -> int:
    if os.getenv("ATR_POLICY_REGIME_STRESS_ENABLE", "1") != "1":
        logger.info("ATR_POLICY_REGIME_STRESS_ENABLE is 0. Exiting.")
        return 0

    r = _redis()
    symbols = _get_active_symbols(r)
    if not symbols:
         # fallback generic top tier
         symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    slip_shock_thr = _safe_float(os.getenv("ATR_POLICY_STRESS_SLIPPAGE_THR_BPS", "15.0"))
    spread_shock_thr = _safe_float(os.getenv("ATR_POLICY_STRESS_SPREAD_THR_BPS", "20.0"))
    depth_floor = _safe_float(os.getenv("ATR_POLICY_STRESS_DEPTH_FLOOR", "5000.0"))

    use_slip = os.getenv("ATR_POLICY_STRESS_USE_SLIPPAGE_EMA", "1") == "1"
    use_spread = os.getenv("ATR_POLICY_STRESS_USE_DEPTH_SPREAD", "1") == "1"

    venue_stress_active = _read_venue_stress(r)
    portfolio_stress_active = (r.get("state:atr_portfolio:stress_active") == "1")

    evts = []

    conn = None
    try:
        conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_regime_classifier")
    except Exception as e:
        logger.warning(f"Could not connect to DB: {e}")

    try:
        if conn:
            with conn.cursor() as cur:
                 cur.execute("UPDATE atr_policy_regime_states SET is_current=false WHERE is_current=true")

        for symbol in symbols:
            # Gather execution proxies
            spread_half = _safe_float(r.get(f"spread_ema_half_bps:{symbol}"))
            spread_bps = spread_half * 2.0
            slip_bps = _safe_float(r.get(f"slippage_ema:{symbol}") or 0.0) # depends on your actual key

            # depth approx
            depth_ask = _safe_float(r.get(f"depth_ask_5:{symbol}") or 999999.0)
            depth_bid = _safe_float(r.get(f"depth_bid_5:{symbol}") or 999999.0)
            depth_top5 = min(depth_ask, depth_bid)

            dq_flags = _read_dt_quality(r, symbol)
            news_lock = _read_news_gate(r, symbol)

            # Regime calculation (in practice from your indicator cache, defaulting to expansion/chop fallback)
            regime = "unknown"
            r_regime = r.get(f"state:atr_regime_raw:{symbol}")
            if r_regime in {"trend_up", "trend_down", "chop", "expansion"}:
                 regime = r_regime

            # Hierarchy of stress
            stress = "normal"
            reason = ""

            if "drift_lock" in dq_flags:
                 stress = "drift_lock"
                 reason = "Feature drift active"
            elif "tick_gap_critical" in dq_flags:
                 stress = "drift_lock"
                 reason = "Data quality tick gap critical"
            elif venue_stress_active:
                 stress = "venue_stress"
                 reason = "Venue error rate exceeded threshold"
            elif news_lock:
                 stress = "news_lock"
                 reason = "News gate active"
            elif portfolio_stress_active:
                 stress = "portfolio_stress"
                 reason = "Portfolio stress active"
            elif use_slip and slip_bps > slip_shock_thr:
                 stress = "slippage_shock"
                 reason = f"Slippage EMA {slip_bps:.2f}bps > {slip_shock_thr}bps"
            elif use_spread and (spread_bps > spread_shock_thr or depth_top5 < depth_floor):
                 stress = "liquidity_shock"
                 reason = f"Spread/Depth breached thresholds ({spread_bps:.2f}bps / ${depth_top5:.0f})"

            # Action deduction (just for internal tagging, the explicit config is used in the gate)
            action = "allow"
            if stress != "normal":
                 action = "clip" # generic

            # Transition detection
            prev_stress = r.get(f"state:atr_stress:{symbol}")
            if prev_stress != stress:
                 evts.append((symbol, regime, stress, action, "STATE_TRANSITION", reason))
                 r.set(f"state:atr_stress:{symbol}", stress)

            if r.get(f"state:atr_regime:{symbol}") != regime:
                 r.set(f"state:atr_regime:{symbol}", regime)

            if conn:
                with conn.cursor() as cur:
                     state_json = {"spread": spread_bps, "slippage": slip_bps, "reason": reason}
                     cur.execute("""
                        INSERT INTO atr_policy_regime_states (
                          source, symbol, regime, stress_state, confidence, state_json, is_current, created_at_ms, updated_at_ms
                        ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, true, %s, %s)
                     """, (
                        "CryptoOrderFlow", symbol, regime, stress, 1.0,
                        json.dumps(state_json), int(time.time()*1000), int(time.time()*1000)
                     ))

        if conn and evts:
             with conn.cursor() as cur:
                 for (sym, reg, st, act, rcode, rtext) in evts:
                     evt_js = {"old_state": prev_stress, "reason": rtext}
                     cur.execute("""
                        INSERT INTO atr_policy_stress_events (
                          source, symbol, regime, stress_state, action, reason_code, event_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                     """, (
                         "CryptoOrderFlow", sym, reg, st, act, rcode, json.dumps(evt_js)
                     ))

        if conn:
             conn.commit()

        return len(symbols)
    finally:
        if conn:
             conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    n = run_once()
    print(f"Processed regime/stress state for {n} symbols.")
