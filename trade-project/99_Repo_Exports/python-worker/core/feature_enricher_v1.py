"""feature_enricher_v1.py — backfill producers for v14_of dead features.

Background: audit `audit_signal_snapshot_feature_coverage_2026_05_24` found
77/359 v14_of model features missing in `signals:of:inputs.indicators`.
The 21-feature subset that bridge `_V12_BASE_OPTIONAL_KEYS` knows about
is only emitted when source has the key — sources didn't have them.
The 56-feature subset wasn't in any bridge.

This module provides per-group producers that:
  • Read existing Redis snapshots (deriv ctx, crossasset, sentiment, etc.)
  • Compute cheap microstructure features from runtime tick/book caches
  • Return a feature dict to merge into `enriched_signal["indicators"]`

Design contract:
  • Pure function. No side effects (except sync Redis GETs that are TTL-cached).
  • Per-group functions are fail-open: error → return {} (skip that group).
  • Returns ONLY keys it computed — never overrides existing values when
    merged via `setdefault`.

Architecture:
  enrich_indicators(runtime, _inds, redis_client) -> dict[str, float]
    │
    ├── _enrich_deriv_ctx        funding_rate_bps, open_interest_delta, ...
    ├── _enrich_crossasset_ctx   btc_corr_5m, alt_season_index, ...
    ├── _enrich_sentiment        crypto_fear_greed, market_breadth_score
    ├── _enrich_book_features    bid_ask_depth_ratio, book_imbalance_5lvl, ...
    ├── _enrich_microbar         microbar_body_bps, momentum_10s, ...
    ├── _enrich_vol_features     vol_fast_bps, vol_slow_bps, vol_regime_code
    └── _enrich_execution_stats  expectancy_bps, profit_factor_roll20, ...

Each group is tested independently. Wiring is in
`services/orderflow/signal_pipeline.py:_publish_of_inputs`.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

_DERIV_CTX_MAX_LAG_MS = float(os.getenv("DERIV_CTX_MAX_LAG_MS", "60000") or 60000)
_CROSSASSET_MAX_LAG_MS = float(os.getenv("CROSSASSET_CTX_MAX_LAG_MS", "120000") or 120000)
_SENTIMENT_MAX_LAG_MS = float(os.getenv("SENTIMENT_CTX_MAX_LAG_MS", "7200000") or 7200000)  # 2h — FNG updates daily, exporter polls hourly

# ── In-process snapshot cache ─────────────────────────────────────────────────
# {key: (parsed_dict, expire_wall_ns)}.
# Eliminates redundant sync Redis GETs when multiple signals burst in one
# event-loop tick. TTL << producer write interval (producers update every
# 60-120s; cache expires every 200ms), so data is never more stale than
# max_lag_ms check already guarantees.
_SNAPSHOT_CACHE_TTL_NS: int = int(
    float(os.getenv("ENRICHER_SNAPSHOT_CACHE_MS", "200")) * 1_000_000
)
_snapshot_cache: dict[str, tuple[dict, int]] = {}  # key → (data, expire_ns)

# ── Stub health tracking ──────────────────────────────────────────────────────
# Producer-backed stubs: features that SHOULD come from a running producer but
# fall back to 0.0 when the producer snapshot is missing/stale. Absence here
# may indicate a broken producer, so we warn at bounded rate (once per key per
# 5 min) and count total occurrences for alerting.
#
# Conditional stubs: legitimately absent (iceberg event optional, Roll model
# unidentified, shadow ML absent) — silence is semantically correct, no warning.
_STUBS_CONDITIONAL: frozenset[str] = frozenset((
    "iceberg_refresh",
    "roll_spread_est",
    "model_calibration_err",
))
_STUBS_PRODUCER_BACKED: frozenset[str] = frozenset((
    "adverse_drift_ms", "expectancy_bps", "fill_time_p90_ms",
    "cancel_to_fill_ratio", "depth_pull_ratio", "maker_cancel_ratio",
    "crypto_fear_greed", "market_breadth_score", "rsi_cvd",
    "amihud_x_oi_delta", "conf_ma_ratio",
    "liquidation_usd_1m", "liqmap_1h_age_ms",
    "microbar_body_bps", "microbar_range_bps", "microbar_vwap_mid_bps",
    "price_to_ema_bps", "momentum_10s", "momentum_x_vol_ratio",
    # TCA priors — tca_priors_exporter_v1 writes tca:ema:{sym}:{kind}:{session}
    "tca_eff_spread_bps_ema", "tca_perm_impact_1s_bps_ema", "tca_samples",
    # CVD detail — tick_decision_engine writes cvd:state:{sym}
    "cvd_jump_events_total", "cvd_median_abs_delta_usd",
))
_STUB_WARN_INTERVAL_S: float = float(os.getenv("ENRICHER_STUB_WARN_INTERVAL_S", "300"))
# Suppress producer-backed warnings during startup (producers need time to populate keys)
_ENRICHER_STUB_WARN_GRACE_S: float = float(os.getenv("ENRICHER_STUB_WARN_GRACE_S", "120"))
_ENRICHER_START_TIME: float = time.time()
_stub_miss_total: dict[str, int] = {}       # key → total stub-fill count
_stub_miss_last_warn: dict[str, float] = {}  # key → last warning wall-time

# ── Prometheus metrics (lazy-init, silent when prometheus_client absent) ───────
# Maps Redis key pattern → human producer label used in metric labels.
_PRODUCER_LABEL: dict[str, str] = {
    "ctx:deriv:":        "deriv_ctx",
    "crossasset:ctx:":   "crossasset_ctx",
    "cache:fear_greed":  "fear_greed",
    "sentiment:fear":    "fear_greed",
    "exec_stats:":       "execution_stats",
    "ctx:liq:":          "liquidation_ctx",
    "microstruct:ctx:":  "microstruct",
    "pressure_v2:":      "orderflow_pressure",
    "sweep_v2:":         "sweep_detector",
    "book_rates:":       "book_rates",
    "cvd:state:":        "cvd_state",
    "abs_lvl:state:":    "abs_lvl_state",
    "og:consensus:":     "og_consensus",
    "tca:ema:":          "tca_priors",
    "ctx:hawkes:":       "hawkes_vpin",
    "liqmap:snapshot:":  "liqmap_snapshot",
}

_prom_stub_miss_total = None      # Counter enricher_producer_stub_miss_total
_prom_snap_stale_total = None     # Counter enricher_snapshot_stale_total
_prom_snap_age_ms = None          # Gauge   enricher_snapshot_age_ms
_prom_initialized = False


def _init_prom() -> None:
    global _prom_stub_miss_total, _prom_snap_stale_total, _prom_snap_age_ms
    global _prom_initialized
    if _prom_initialized:
        return
    _prom_initialized = True
    try:
        from prometheus_client import Counter, Gauge
        _prom_stub_miss_total = Counter(
            "enricher_producer_stub_miss_total",
            "Times a producer-backed feature was filled with stub 0.0 "
            "(snapshot missing or stale). Sustained rate → producer down.",
            ["feature"],
        )
        _prom_snap_stale_total = Counter(
            "enricher_snapshot_stale_total",
            "Times a Redis snapshot was rejected as too old (ts_ms > max_lag_ms).",
            ["producer"],
        )
        _prom_snap_age_ms = Gauge(
            "enricher_snapshot_age_ms",
            "Age of the last successfully read snapshot in milliseconds.",
            ["producer"],
        )
    except Exception:
        pass


def _prom_inc(event: str, _label: str, key: str) -> None:
    """Increment a named counter (no-op when prometheus not available)."""
    try:
        if event == "enrich_stale" and _prom_snap_stale_total is not None:
            _prom_snap_stale_total.labels(producer=_producer_label(key)).inc()
        elif event == "enrich_miss":
            pass  # not currently tracked via counter
        elif event == "enrich_err":
            pass  # not currently tracked via counter
    except Exception:
        pass


def _producer_label(key: str) -> str:
    """Map Redis key to short producer label for Prometheus."""
    for prefix, label in _PRODUCER_LABEL.items():
        if key.startswith(prefix):
            return label
    return "unknown"


def _safe_float(x: Any, default: float = 0.0) -> float:
    """Coerce x to a finite float, returning `default` on any failure."""
    if x is None:
        return default
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except (TypeError, ValueError):
        pass
    return default


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_redis_get(r: Any, key: str) -> str | None:
    """GET with bytes→str decode and error-tolerance."""
    if r is None:
        return None
    try:
        v = r.get(key)
        if v is None:
            return None
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", "ignore")
        return str(v)
    except Exception:
        return None


def _check_ts_and_emit(data: dict, key: str, max_lag_ms: float) -> dict[str, Any]:
    """Apply ts_ms staleness guard; emit age/stale metrics. Returns {} if stale."""
    ts_ms = data.get("ts_ms") or data.get("updated_at_ms") or data.get("ts")
    if ts_ms is None:
        return data
    try:
        age = _now_ms() - int(ts_ms)
    except (TypeError, ValueError):
        return data
    if age > max_lag_ms:
        if _prom_snap_stale_total is not None:
            try:
                _prom_snap_stale_total.labels(producer=_producer_label(key)).inc()
            except Exception:
                pass
        return {}
    if _prom_snap_age_ms is not None:
        try:
            _prom_snap_age_ms.labels(producer=_producer_label(key)).set(age)
        except Exception:
            pass
    return data



def _load_hash_snapshot(r: Any, key: str, max_lag_ms: float) -> dict[str, str]:
    now_ns = time.monotonic_ns()
    cached = _snapshot_cache.get(key)
    if cached is not None:
        c_val, c_ts_ns = cached
        if now_ns - c_val.get("__cache_ts_ns", 0) < _SNAPSHOT_CACHE_TTL_NS:
            ts_ms = _safe_float(c_val.get("ts_ms"))
            now_ms = time.time() * 1000
            if (now_ms - ts_ms) > max_lag_ms and ts_ms > 0:
                _prom_inc("enrich_stale", "total", key)
                return {}
            return c_val
            
    try:
        val = r.hgetall(key)
        if not val:
            _prom_inc("enrich_miss", "total", key)
            return {}
        val["__cache_ts_ns"] = now_ns
        _snapshot_cache[key] = (val, now_ns)
        ts_ms = _safe_float(val.get("ts_ms"))
        now_ms = time.time() * 1000
        if (now_ms - ts_ms) > max_lag_ms and ts_ms > 0:
            _prom_inc("enrich_stale", "total", key)
            return {}
        return val
    except Exception as e:
        _prom_inc("enrich_err", "total", key)
        return {}


def _load_json_snapshot(r: Any, key: str, max_lag_ms: float) -> dict[str, Any]:
    """Read JSON snapshot from `key`, return {} when missing or stale.

    Results are cached in-process for ENRICHER_SNAPSHOT_CACHE_MS (default 200ms)
    to avoid repeated sync Redis GETs when multiple signals burst in one
    event-loop tick. The ts_ms staleness guard still applies to cached data.
    """
    _init_prom()
    now_ns = time.monotonic_ns()
    cached = _snapshot_cache.get(key)
    if cached is not None:
        data, expire_ns = cached
        if now_ns < expire_ns:
            return _check_ts_and_emit(data, key, max_lag_ms)

    raw = _safe_redis_get(r, key)
    if not raw:
        _snapshot_cache[key] = ({}, now_ns + _SNAPSHOT_CACHE_TTL_NS)
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        _snapshot_cache[key] = ({}, now_ns + _SNAPSHOT_CACHE_TTL_NS)
        return {}
    if not isinstance(data, dict):
        _snapshot_cache[key] = ({}, now_ns + _SNAPSHOT_CACHE_TTL_NS)
        return {}
    _snapshot_cache[key] = (data, now_ns + _SNAPSHOT_CACHE_TTL_NS)
    return _check_ts_and_emit(data, key, max_lag_ms)


def _prime_snapshot_cache(r: Any, keys: list[str]) -> None:
    """Batch-fetch keys with a single MGET and populate _snapshot_cache.

    All values are fetched at the same instant → zero time-skew between
    producer groups that read different Redis keys.  Only fetches keys
    whose cache entry has expired.
    """
    if r is None or not keys:
        return
    now_ns = time.monotonic_ns()
    expire_ns = now_ns + _SNAPSHOT_CACHE_TTL_NS
    keys_to_fetch = [k for k in keys if k not in _snapshot_cache or _snapshot_cache[k][1] <= now_ns]
    if not keys_to_fetch:
        return
    try:
        vals = r.mget(keys_to_fetch)
    except Exception:
        return
    for key, raw in zip(keys_to_fetch, vals):
        if raw is None:
            _snapshot_cache[key] = ({}, expire_ns)
            continue
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        try:
            data = json.loads(raw)
        except Exception:
            _snapshot_cache[key] = ({}, expire_ns)
            continue
        _snapshot_cache[key] = (data if isinstance(data, dict) else {}, expire_ns)


def _snapshot_keys_for_symbol(symbol: str) -> list[str]:
    """All Redis keys that enrich_indicators reads for a given symbol."""
    return [
        f"ctx:deriv:{symbol}",
        f"crossasset:ctx:{symbol}",
        f"crossasset:corr:{symbol}",
        "crossasset:ctx:_global",
        # NOTE: main-redis HASH keys (coinmarketcap, defillama, breadth) are NOT
        # listed here — MGET on worker-1 returns nil for them, which would poison
        # _snapshot_cache with {} and block the real HGETALL via redis_main.
        # ctx:deribit:global is a JSON STRING on main redis — same issue for
        # _load_json_snapshot; read directly in _enrich_external_ctx instead.
        "cache:fear_greed",
        "sentiment:fear_greed:latest",
        f"exec_stats:{symbol}",
        f"ctx:liq:{symbol}",
        f"microstruct:ctx:{symbol}",
        f"pressure_v2:{symbol}",
        f"sweep_v2:{symbol}",
        f"book_rates:{symbol}",
        # Phase 1 P1 new producer ctx keys
        f"ctx:queue_dynamics:{symbol}",
        f"ctx:cost_dynamics:{symbol}",
        f"ctx:regime_transition:{symbol}",
        f"ctx:cross_venue:{symbol}",
        f"ctx:session_vol:{symbol}",
        # P2 Group H: directional change
        f"ctx:dc:{symbol}",
        # liqmap snapshots — JSON strings on worker-1 redis, safe to MGET
        f"liqmap:snapshot:{symbol}:5m",
        f"liqmap:snapshot:{symbol}:1h",
        f"liqmap:snapshot:{symbol}:4h",
        # NOTE: cvd:state, abs_lvl:state, og:consensus, ctx:breadth:* are HASH keys
        # (HGETALL, not GET). Do NOT add them here — MGET returns WRONGTYPE for HASH
        # keys and would store nil → wasted slot. They're read via _load_hash_snapshot.
    ]


# ── Per-group producers ───────────────────────────────────────────────────────


def _enrich_deriv_ctx(symbol: str, redis_client: Any) -> dict[str, float]:
    """Funding / OI / basis features from `ctx:deriv:{symbol}` (deriv_ctx_collector).

    Schema:
      v14_of expects funding_rate_bps as bps (not raw).
      open_interest_delta is the 5-minute delta.
    """
    if not symbol:
        return {}
    data = _load_json_snapshot(
        redis_client, f"ctx:deriv:{symbol}", _DERIV_CTX_MAX_LAG_MS,
    )
    if not data:
        return {}
    out: dict[str, float] = {}
    # funding_rate is fractional (e.g. 0.0001 = 0.01%); convert to bps
    fr = data.get("funding_rate")
    if fr is not None:
        try:
            out["funding_rate_bps"] = float(fr) * 10000.0
        except (TypeError, ValueError):
            pass
    # OI delta (5-minute window) — already in absolute units
    oi_delta = data.get("delta_oi_5m") or data.get("oi_delta_5m")
    if oi_delta is not None:
        out["open_interest_delta"] = _safe_float(oi_delta)
    return out


def _enrich_crossasset_ctx(symbol: str, redis_client: Any) -> dict[str, float]:
    """BTC correlation, alt-season, cross-vol ratio from cross-context-aggregator.

    Snapshot key: `crossasset:ctx:{symbol}` (when aggregator runs).
    Falls back to `crossasset:corr:{symbol}` legacy key.
    """
    if not symbol:
        return {}
    out: dict[str, float] = {}
    for k in (f"crossasset:ctx:{symbol}", f"crossasset:corr:{symbol}"):
        data = _load_json_snapshot(redis_client, k, _CROSSASSET_MAX_LAG_MS)
        if data:
            break
    else:
        data = None
    if data:
        # btc_corr_5m: rolling 5m correlation BTC vs this symbol
        for src_k, dst_k in (
            ("btc_corr_5m", "btc_corr_5m"),
            ("corr_btc_5m", "btc_corr_5m"),
            ("alt_season_index", "alt_season_index"),
            ("alt_season", "alt_season_index"),
            ("cross_asset_vol_ratio", "cross_asset_vol_ratio"),
            ("vol_ratio_to_btc", "cross_asset_vol_ratio"),
        ):
            if src_k in data and dst_k not in out:
                out[dst_k] = _safe_float(data[src_k])

    # BTC reference: writer does not emit corr-with-self. Pad with degenerate
    # self-ref values so v15_of vector is complete for BTCUSDT.
    if symbol.upper() == "BTCUSDT":
        out.setdefault("btc_corr_5m", 1.0)
        out.setdefault("cross_asset_vol_ratio", 1.0)
        if "alt_season_index" not in out:
            glob = _load_json_snapshot(
                redis_client, "crossasset:ctx:_global", _CROSSASSET_MAX_LAG_MS,
            )
            if glob and "alt_season_index" in glob:
                out["alt_season_index"] = _safe_float(glob["alt_season_index"])

    # ctx:breadth:{symbol} HASH — per-symbol BTC-relative returns from breadth producer
    breadth = _load_hash_snapshot(redis_client, f"ctx:breadth:{symbol}", _CROSSASSET_MAX_LAG_MS)
    if breadth:
        for k in ("btc_ret_1m", "btc_ret_5m", "btc_ret_1h",
                  "symbol_rel_strength_vs_btc_1m", "cg_rel_strength_btc_1h"):
            v = breadth.get(k)
            if v is not None:
                out.setdefault(k, _safe_float(v))
        rel_1m = out.get("symbol_rel_strength_vs_btc_1m")
        if rel_1m is not None:
            out.setdefault("rel_ret_1m_vs_btc", rel_1m)
    glob_b = _load_hash_snapshot(redis_client, "ctx:breadth:global", _CROSSASSET_MAX_LAG_MS)
    if glob_b and glob_b.get("market_breadth_ret_5m") is not None:
        out.setdefault("market_breadth_ret_5m", _safe_float(glob_b["market_breadth_ret_5m"]))
    return out


def _enrich_sentiment(redis_client: Any) -> dict[str, float]:
    """Global sentiment indicators from sentiment exporter / cache.

    Sources tried in order:
      1. `cache:fear_greed` (Go sentiment exporter, JSON with `value` field)
      2. `sentiment:fear_greed:latest` (Python fallback)

    Stale window 30min — fear/greed updates only every few hours.
    Emits source health metadata: fg_data_available, fg_data_age_ms, fg_data_stale.
    """
    _FG_MAX_LAG_MS = _SENTIMENT_MAX_LAG_MS
    data: dict = {}
    for key in ("cache:fear_greed", "sentiment:fear_greed:latest"):
        d = _load_json_snapshot(redis_client, key, _FG_MAX_LAG_MS)
        if d:
            data = d
            break

    # Source health metadata — emitted regardless of value presence.
    out: dict[str, float] = {}
    if not data:
        out["fg_data_available"] = 0.0
        out["fg_data_stale"] = 1.0
        return out

    ts_raw = data.get("ts_ms") or data.get("updated_at_ms") or data.get("ts")
    if ts_raw is not None:
        age_ms = max(0.0, _now_ms() - _safe_float(ts_raw, _now_ms()))
        out["fg_data_age_ms"] = age_ms
        out["fg_data_stale"] = 1.0 if age_ms > _FG_MAX_LAG_MS else 0.0
    else:
        out["fg_data_stale"] = 0.0  # data loaded but no ts — treat as fresh
    out["fg_data_available"] = 1.0

    val = data.get("value") or data.get("fear_greed") or data.get("fg")
    if val is not None:
        # Fear&Greed index is 0..100; normalize to [0..1]. Accept both raw
        # (0-100) and already-normalized (0-1) input.
        v = _safe_float(val)
        if v > 1.0:
            out["crypto_fear_greed"] = max(0.0, min(1.0, v / 100.0))
            out["fear_greed_index"] = v  # raw 0-100 for v14/v15 ML schema
        else:
            out["crypto_fear_greed"] = max(0.0, min(1.0, v))
            out["fear_greed_index"] = v * 100.0
        cls = str(data.get("classification") or "").lower()
        out["fear_greed_regime_extreme_fear"] = 1.0 if "fear" in cls else 0.0
        out["fear_greed_regime_extreme_greed"] = 1.0 if "greed" in cls else 0.0
    breadth = data.get("market_breadth_score") or data.get("breadth")
    if breadth is not None:
        out["market_breadth_score"] = _safe_float(breadth)
    return out



def _source_age_and_health(data: dict, max_lag_ms: float) -> tuple[float, float, float]:
    """Return (available, age_ms, stale) from a snapshot dict.

    available: 1.0 if data loaded, 0.0 if empty
    age_ms:    ms since ts_ms/updated_at_ms field (0.0 if no ts)
    stale:     1.0 if age > max_lag_ms or data empty
    """
    if not data:
        return 0.0, 0.0, 1.0
    ts_raw = data.get("ts_ms") or data.get("updated_at_ms") or data.get("ts")
    if ts_raw is None:
        return 1.0, 0.0, 0.0
    try:
        age_ms = max(0.0, _now_ms() - float(ts_raw))
    except (TypeError, ValueError):
        return 1.0, 0.0, 0.0
    stale = 1.0 if age_ms > max_lag_ms else 0.0
    return 1.0, age_ms, stale


def _enrich_external_ctx(redis_client: Any) -> dict[str, float]:
    """CoinMarketCap, DefiLlama, Deribit context features + source health metadata.

    For each external source emits:
      {prefix}_data_available  1.0 if snapshot loaded, else 0.0
      {prefix}_data_age_ms     ms since snapshot ts_ms
      {prefix}_data_stale      1.0 if age > threshold or missing
    """
    out: dict[str, float] = {}
    try:
        import redis as _redis
        host = os.getenv("REDIS_HOST", "redis")
        port = int(os.getenv("REDIS_PORT", "6379"))
        redis_main = _redis.Redis(host=host, port=port, decode_responses=True,
                                  socket_connect_timeout=0.5, socket_timeout=0.5)
    except Exception:
        redis_main = redis_client

    if redis_main is None:
        for p in ("cmc", "dl", "deribit"):
            out[f"{p}_data_available"] = 0.0
            out[f"{p}_data_stale"] = 1.0
        return out

    try:
        _CMC_MAX_LAG = 300_000
        cmc = _load_hash_snapshot(redis_main, "runtime:provider:coinmarketcap:global", _CMC_MAX_LAG)
        avail, age, stale = _source_age_and_health(cmc, _CMC_MAX_LAG)
        out["cmc_data_available"] = avail
        if age > 0:
            out["cmc_data_age_ms"] = age
        out["cmc_data_stale"] = stale
        if cmc:
            if cmc.get("total_volume_24h_usd") is not None:
                out["cmc_total_volume_usd"] = _safe_float(cmc.get("total_volume_24h_usd")) / 1e9
            # field name written by cmc_provider_v1: active_cryptocurrencies
            _active = (
                cmc.get("active_cryptocurrencies")
                or cmc.get("active_cryptos")
                or cmc.get("active_currencies")
            )
            if _active is not None:
                out["cmc_active_cryptos"] = _safe_float(_active)

        _DL_MAX_LAG = 3_600_000
        dl = _load_hash_snapshot(redis_main, "runtime:provider:defillama:eth_dex", _DL_MAX_LAG)
        avail, age, stale = _source_age_and_health(dl, _DL_MAX_LAG)
        out["dl_data_available"] = avail
        if age > 0:
            out["dl_data_age_ms"] = age
        out["dl_data_stale"] = stale
        if dl and dl.get("dex_volume_spike_z") is not None:
            out["dl_dex_volume_spike_z"] = _safe_float(dl.get("dex_volume_spike_z"))

        mb = _load_hash_snapshot(redis_main, "runtime:breadth", 60_000)
        if mb and mb.get("meme_ret_1m") is not None:
            out["meme_ret_1m"] = _safe_float(mb.get("meme_ret_1m"))

        _DERIBIT_MAX_LAG = 60_000
        db = _load_json_snapshot(redis_main, "ctx:deribit:global", _DERIBIT_MAX_LAG)
        avail, age, stale = _source_age_and_health(db, _DERIBIT_MAX_LAG)
        out["deribit_data_available"] = avail
        if age > 0:
            out["deribit_data_age_ms"] = age
        out["deribit_data_stale"] = stale
        if db:
            for src_k, dst_k in (
                ("btc_deribit_iv_proxy",   "deribit_btc_iv_proxy"),
                ("btc_deribit_iv_z",       "deribit_btc_iv_z"),
                ("eth_deribit_iv_proxy",   "deribit_eth_iv_proxy"),
                ("eth_deribit_iv_z",       "deribit_eth_iv_z"),
                ("btc_deribit_funding_8h", "deribit_btc_funding_8h"),
                ("eth_deribit_funding_8h", "deribit_eth_funding_8h"),
                ("btc_iv_7d",              "deribit_btc_iv_7d"),
                ("btc_iv_30d",             "deribit_btc_iv_30d"),
                ("eth_iv_7d",              "deribit_eth_iv_7d"),
                ("eth_iv_30d",             "deribit_eth_iv_30d"),
            ):
                v = db.get(src_k)
                if v is not None:
                    out[dst_k] = _safe_float(v)
            rg_str = str(db.get("btc_eth_vol_regime") or "").lower()
            _DERIBIT_RGC = {
                "normal": 0.0, "vol_compression": 1.0,
                "vol_expansion": 2.0, "vol_stress": 3.0,
            }
            if rg_str in _DERIBIT_RGC:
                out["deribit_vol_regime_code"] = _DERIBIT_RGC[rg_str]
    finally:
        try:
            if redis_main is not None and redis_main != redis_client:
                redis_main.close()
        except Exception:
            pass

    return out


def _enrich_cg_cp_source_health(redis_client: Any) -> dict[str, float]:
    """CoinGecko and CoinPaprika `<prefix>_data_*` source-health features.

    Schema-only producers (cg/cp values themselves are populated elsewhere;
    this helper exists to close the `_data_available/_age_ms/_data_stale`
    gap flagged in the v15_of audit). Uses the canonical thresholds and
    key map from `core.source_health_v1.SOURCE_REGISTRY` so adding a new
    source only touches one place.
    """
    out: dict[str, float] = {}
    try:
        from core.source_health_v1 import (
            SNAP_KIND_HASH,
            make_source_health_features,
            get_source_spec,
        )
    except Exception:
        return out

    # Connect to the main Redis where provider snapshots live.
    try:
        import redis as _redis
        host = os.getenv("REDIS_HOST", "redis")
        port = int(os.getenv("REDIS_PORT", "6379"))
        redis_main = _redis.Redis(
            host=host, port=port, decode_responses=True,
            socket_connect_timeout=0.5, socket_timeout=0.5,
        )
    except Exception:
        redis_main = redis_client

    if redis_main is None:
        for prefix in ("cg", "cp"):
            out[f"{prefix}_data_available"] = 0.0
            out[f"{prefix}_data_stale"] = 1.0
        return out

    try:
        now = _now_ms()
        for prefix in ("cg", "cp"):
            spec = get_source_spec(prefix)
            if spec is None or spec.snap_kind != SNAP_KIND_HASH:
                continue
            snap = _load_hash_snapshot(redis_main, spec.redis_key, spec.max_lag_ms)
            out.update(make_source_health_features(spec.name, snap, now))
            # Shape determinism: always emit available/stale even if snapshot
            # path returned nothing (e.g. helper omitted age when ts missing).
            out.setdefault(f"{prefix}_data_available", 0.0)
            out.setdefault(f"{prefix}_data_stale", 1.0)
    finally:
        try:
            if redis_main is not None and redis_main != redis_client:
                redis_main.close()
        except Exception:
            pass

    return out


def _enrich_breadth_ctx(symbol: str, redis_client: Any) -> dict[str, float]:
    """BTC/ETH reference returns and relative-strength from cross_asset_breadth_producer.

    Producer: services/cross_asset_breadth_producer_v1.py
    Keys (HASH on redis-worker-1):
      ctx:breadth:{SYMBOL} — btc_ret_1m/5m/1h, sym_ret_1m/5m, btc/sym rel-strength
      ctx:breadth:global   — market_breadth_ret_5m

    Unlocks: btc_ret_1m, btc_ret_5m, btc_ret_1h, eth_ret_1m, eth_ret_5m,
             rel_ret_1m_vs_btc, rel_ret_5m_vs_btc, market_breadth_ret_5m,
             symbol_rel_strength_vs_btc_1m.
    """
    if not symbol or redis_client is None:
        return {}
    sym = symbol.upper()
    out: dict[str, float] = {}
    _BREADTH_MAX_LAG_MS = 120_000  # 2 min

    per_sym = _load_hash_snapshot(redis_client, f"ctx:breadth:{sym}", _BREADTH_MAX_LAG_MS)
    if per_sym:
        for k in ("btc_ret_1m", "btc_ret_5m", "btc_ret_1h",
                  "symbol_rel_strength_vs_btc_1m", "cg_rel_strength_btc_1h"):
            if k in per_sym:
                out[k] = _safe_float(per_sym[k])
        # Relative returns
        btc_1m = _safe_float(per_sym.get("btc_ret_1m"))
        btc_5m = _safe_float(per_sym.get("btc_ret_5m"))
        sym_1m = _safe_float(per_sym.get("sym_ret_1m"))
        sym_5m = _safe_float(per_sym.get("sym_ret_5m"))
        if "rel_ret_1m_vs_btc" not in out and "sym_ret_1m" in per_sym:
            out["rel_ret_1m_vs_btc"] = sym_1m - btc_1m
        if "rel_ret_5m_vs_btc" not in out and "sym_ret_5m" in per_sym:
            out["rel_ret_5m_vs_btc"] = sym_5m - btc_5m
        # For ETHUSDT, sym_ret_* IS eth_ret_*
        if sym == "ETHUSDT":
            if "sym_ret_1m" in per_sym:
                out["eth_ret_1m"] = sym_1m
            if "sym_ret_5m" in per_sym:
                out["eth_ret_5m"] = sym_5m
        else:
            # Pull ETH returns from ctx:breadth:ETHUSDT so any symbol gets them
            eth_sym = _load_hash_snapshot(redis_client, "ctx:breadth:ETHUSDT", _BREADTH_MAX_LAG_MS)
            if eth_sym:
                if "sym_ret_1m" in eth_sym:
                    out["eth_ret_1m"] = _safe_float(eth_sym["sym_ret_1m"])
                if "sym_ret_5m" in eth_sym:
                    out["eth_ret_5m"] = _safe_float(eth_sym["sym_ret_5m"])

    glob = _load_hash_snapshot(redis_client, "ctx:breadth:global", _BREADTH_MAX_LAG_MS)
    if glob and "market_breadth_ret_5m" in glob:
        out["market_breadth_ret_5m"] = _safe_float(glob["market_breadth_ret_5m"])

    return out


def _enrich_book_features(_inds: dict[str, Any]) -> dict[str, float]:
    """Compute book-derived ratios from existing book-side fields.

    Reuses already-published indicators when possible — no Redis read.
    """
    out: dict[str, float] = {}
    # bid_ask_depth_ratio: ratio of bid-side depth to ask-side depth at top-5
    bid_d = _safe_float(_inds.get("depth_bid_5") or _inds.get("bid_depth_5"))
    ask_d = _safe_float(_inds.get("depth_ask_5") or _inds.get("ask_depth_5"))
    if bid_d > 0 and ask_d > 0:
        out["bid_ask_depth_ratio"] = bid_d / ask_d
    # book_imbalance_5lvl: (bid - ask) / (bid + ask) for 5 levels
    if bid_d > 0 or ask_d > 0:
        out["book_imbalance_5lvl"] = (bid_d - ask_d) / (bid_d + ask_d + 1e-9)
    # depth_pull_ratio: pull rate vs add rate (uses cancel_*_rate_ema /
    # added_*_rate_ema which book sanity already exposes)
    add_bid = _safe_float(_inds.get("added_bid_rate_ema"))
    add_ask = _safe_float(_inds.get("added_ask_rate_ema"))
    cancel_bid = _safe_float(_inds.get("cancel_bid_rate_ema"))
    cancel_ask = _safe_float(_inds.get("cancel_ask_rate_ema"))
    add_total = add_bid + add_ask
    cancel_total = cancel_bid + cancel_ask
    if add_total > 0:
        out["depth_pull_ratio"] = cancel_total / add_total
    # cancel_to_fill_ratio: cancellations vs realised trades (uses trade_rate
    # placeholder when present)
    trade_rate = _safe_float(_inds.get("trade_rate_ema") or _inds.get("trades_per_sec_ema"))
    if trade_rate > 0 and cancel_total > 0:
        out["cancel_to_fill_ratio"] = cancel_total / trade_rate
    # maker_cancel_ratio: cancels initiated by maker vs total (proxy from
    # passive vs aggressive flow); use cancel_total / (add_total + 1e-9)
    if add_total > 0:
        out["maker_cancel_ratio"] = cancel_total / (add_total + 1e-9)
    # book_refresh_rate_hz: book update events per second (already published
    # as `book_update_rate_ema` by some paths)
    refresh = _safe_float(_inds.get("book_update_rate_ema"))
    if refresh > 0:
        out["book_refresh_rate_hz"] = refresh
    return out


def _enrich_microbar(
    _inds: dict[str, Any],
    symbol: str = "",
    redis_client: Any = None,
) -> dict[str, float]:
    """Microbar features: body/range/vwap-mid in bps.

    Sources (in priority order):
      1. Explicit `microbar_*_px` keys (from microbar_producer service).
      2. Standalone `microbar:{symbol}` HASH (read elsewhere, copied to indicators).
      3. Fallback: derive from `decision_*`, `last_price`, `vwap_*` aliases.
      4. Redis fallback: `high_1m`/`low_1m` from `book_rates:{symbol}` snapshot
         (published by book_rate_ema_producer from rolling tick price window).
    """
    out: dict[str, float] = {}
    # Primary path — explicit microbar_*_px
    o = _safe_float(_inds.get("microbar_open_px"))
    c = _safe_float(_inds.get("microbar_close_px"))
    h = _safe_float(_inds.get("microbar_high_px"))
    low = _safe_float(_inds.get("microbar_low_px"))
    vwap = _safe_float(_inds.get("microbar_vwap"))
    mid = _safe_float(_inds.get("microbar_mid_px") or _inds.get("decision_mid"))
    # Fallback aliases when explicit microbar keys absent.
    # Production indicator names (audit 2026-05-24):
    #   book_mid_price, micro_price, iceberg_price, atr_used_last_good.
    if c == 0:
        c = _safe_float(
            _inds.get("price") or _inds.get("last_price") or _inds.get("decision_mid")
            or _inds.get("book_mid_price") or _inds.get("micro_price")
            or _inds.get("iceberg_price")
        )
    if o == 0:
        # Use recent vwap as proxy for open; fall back to micro_price 1-step-back proxy
        o = _safe_float(_inds.get("vwap_1m") or _inds.get("microbar_vwap"))
        if o == 0 and c > 0:
            # Derive synthetic "open" from c × (1 - microprice_shift_bps / 10000)
            mp_shift = _safe_float(_inds.get("microprice_shift_bps_20"))
            o = c / (1.0 + mp_shift / 10000.0) if mp_shift != 0 else c
    if h == 0:
        h = _safe_float(_inds.get("high_1m") or _inds.get("recent_high_px"))
    if low == 0:
        low = _safe_float(_inds.get("low_1m") or _inds.get("recent_low_px"))
    # Redis fallback: rolling 1m high/low published by book_rate_ema_producer
    if (h == 0 or low == 0) and symbol and redis_client is not None:
        _br = _load_json_snapshot(redis_client, f"book_rates:{symbol}", 60_000)
        if _br:
            if h == 0:
                h = _safe_float(_br.get("high_1m"))
            if low == 0:
                low = _safe_float(_br.get("low_1m"))
    if h == 0:
        h = c
    if low == 0:
        low = c
    if vwap == 0:
        vwap = _safe_float(_inds.get("vwap_1m") or _inds.get("decision_vwap")) or c
    if mid == 0:
        mid = _safe_float(_inds.get("book_mid_price") or _inds.get("micro_price")) or c

    if c > 0 and o > 0:
        out["microbar_body_bps"] = 10000.0 * (c - o) / o
    if h > 0 and low > 0 and h > low:
        out["microbar_range_bps"] = 10000.0 * (h - low) / low
    if vwap > 0 and mid > 0:
        out["microbar_vwap_mid_bps"] = 10000.0 * (vwap - mid) / mid
    return out


def _enrich_momentum(
    _inds: dict[str, Any],
    symbol: str = "",
    redis_client: Any = None,
) -> dict[str, float]:
    """Momentum + price-to-EMA bps. Production indicator aliases (audit 2026-05-24):
      - `price_10s_ago` rarely emitted by publisher; falls back to book_rates snapshot.
      - `ema_short` falls back to `ema_px_30s` from book_rates snapshot.
    """
    out: dict[str, float] = {}
    # Redis fallback: book_rates has ema_px_30s and price_10s_ago from tick stream
    if symbol and redis_client is not None:
        _br = _load_json_snapshot(redis_client, f"book_rates:{symbol}", 60_000)
        if _br:
            if _inds.get("ema_short") is None and _br.get("ema_px_30s"):
                _inds.setdefault("ema_short", _safe_float(_br["ema_px_30s"]))
            if _inds.get("price_10s_ago") is None and _br.get("price_10s_ago"):
                _inds.setdefault("price_10s_ago", _safe_float(_br["price_10s_ago"]))
            if _inds.get("vwap_1m") is None and _br.get("vwap_1m"):
                _inds.setdefault("vwap_1m", _safe_float(_br["vwap_1m"]))
    p_now = _safe_float(
        _inds.get("price") or _inds.get("last_price")
        or _inds.get("entry") or _inds.get("decision_mid")
        or _inds.get("book_mid_price") or _inds.get("micro_price")
        or _inds.get("iceberg_price")
        or _inds.get("close_px")
    )
    # Try alternative momentum sources directly (publisher already computes some)
    direct_mom = _inds.get("momentum_5s") or _inds.get("price_momentum_short")
    if direct_mom is not None:
        out["momentum_10s"] = _safe_float(direct_mom)
    else:
        p_10s = _safe_float(_inds.get("price_10s_ago") or _inds.get("microprice_10s_ago"))
        if p_now > 0 and p_10s > 0:
            try:
                out["momentum_10s"] = math.log(p_now / p_10s)
            except (ValueError, ZeroDivisionError):
                pass
        else:
            # Derive from microprice shift (already-emitted): shift over 20 ticks ≈ 200ms-2s
            mp_shift_bps = _inds.get("microprice_shift_bps_20")
            if mp_shift_bps is not None:
                # Treat shift as proxy for short-term momentum
                out["momentum_10s"] = _safe_float(mp_shift_bps) / 10000.0

    # price_to_ema_bps: prefer explicit ema_short, else proxy
    ema = _safe_float(_inds.get("ema_short") or _inds.get("ema_50") or _inds.get("ema_20"))
    if ema == 0:
        # Proxy: use vwap as EMA-equivalent
        ema = _safe_float(_inds.get("vwap_1m") or _inds.get("decision_vwap"))
    if ema > 0 and p_now > 0:
        out["price_to_ema_bps"] = 10000.0 * (p_now - ema) / ema

    # momentum × vol-ratio interaction
    if "momentum_10s" in out:
        vr = _safe_float(_inds.get("vol_ratio") or _inds.get("vol_ratio_fast_slow"))
        if vr > 0:
            out["momentum_x_vol_ratio"] = out["momentum_10s"] * vr
    return out


def _enrich_vol_features(_inds: dict[str, Any]) -> dict[str, float]:
    """Volatility bridge for canonical and legacy source keys.

    Production indicator names (audit 2026-05-24):
      - Prefer canonical keys already present in `indicators`.
      - vol_fast aliased from `vol_compression_score` (numerical vol intensity)
        OR `atr_q` (q-scaled ATR) when present.
      - vol_slow aliased from `vol_ratio_fast_slow` denominator proxy: vol_fast / ratio.
      - vol_regime_code from direct source first, then derived from fast/slow.
      - vol_ratio_z / vol_of_vol are passed through when already present so the
        publish path does not silently lose runtime-computed volatility stats.
    """
    out: dict[str, float] = {}
    aliases = {
        "vol_fast_bps": (
            "vol_fast_bps",
            "vol_fast", "vol_fast_atr", "atr_fast_bps",
            "vol_compression_score", "atr_q",
        ),
        "vol_slow_bps": (
            "vol_slow_bps",
            "vol_slow", "vol_slow_atr", "atr_slow_bps",
            "vol_expansion_score",
        ),
        "vol_ratio_z": (
            "vol_ratio_z", "ratio_z", "sc_vol_ratio_z",
        ),
        "vol_of_vol": (
            "vol_of_vol",
        ),
        "vol_regime_code": (
            "vol_regime_code",
            "regime_class_raw_code", "regime_code",
            "deribit_vol_regime_code",
        ),
    }
    for dst, srcs in aliases.items():
        for s in srcs:
            v = _inds.get(s)
            # Skip None and zero: zero from an upstream bridge or stub is not a
            # meaningful vol estimate; keep scanning for a non-zero alias.
            if v is not None and _safe_float(v) != 0.0:
                out[dst] = _safe_float(v)
                break
    # Derive vol_slow_bps from vol_ratio_fast_slow + vol_fast when slow is missing
    if "vol_slow_bps" not in out and "vol_fast_bps" in out:
        ratio = _safe_float(_inds.get("vol_ratio_fast_slow") or _inds.get("vol_ratio"))
        if ratio > 1e-6:
            out["vol_slow_bps"] = out["vol_fast_bps"] / ratio
    if "vol_regime_code" not in out and "vol_fast_bps" in out:
        try:
            from core.v11_of_computers.regime_computers import compute_vol_regime_code
            out["vol_regime_code"] = float(compute_vol_regime_code(
                float(out["vol_fast_bps"]),
                float(out.get("vol_slow_bps", 0.0)),
            ))
        except Exception:
            pass
    return out


def _enrich_execution_stats(symbol: str, redis_client: Any) -> dict[str, float]:
    """Per-symbol rolling execution stats from `stats:execution:{symbol}` HASH.

    Producer: trade-monitor or a separate rolling-stats updater that consumes
    `trades:closed` and maintains an EWMA of {expectancy_bps,
    profit_factor_roll20, recovery_factor_roll, kelly_fraction_roll,
    slippage_realized_bps, fill_time_p90_ms}. Returns {} when key missing
    (stats updater hasn't been built yet — TODO).
    """
    if not symbol or redis_client is None:
        return {}
    try:
        raw = redis_client.hgetall(f"stats:execution:{symbol}")
    except Exception:
        return {}
    if not raw:
        return {}
    out: dict[str, float] = {}
    for key in (
        "expectancy_bps", "profit_factor_roll20", "recovery_factor_roll",
        "kelly_fraction_roll", "slippage_realized_bps", "fill_time_p90_ms",
        "adverse_drift_ms", "fill_prob_3s", "p_wait", "eta_fill_sec", "exec_cost_to_tp1_ratio", "tca_perm_impact_1s_bps_ema", "tca_perm_impact_5s_bps_ema",
    ):
        v = raw.get(key) if isinstance(raw, dict) else None
        if v is None:
            continue
        try:
            out[key] = float(v)
        except (TypeError, ValueError):
            continue
    return out


def _enrich_derived(indicators: dict[str, Any], deriv_out: dict[str, float]) -> dict[str, float]:
    """Derived features computed from already-populated indicator fields.

    Each is fail-soft: if required source absent, key not emitted (avoid fake 0).

    Outputs:
      vol_regime_code         — categorical encoding of regime string [0..6]
      amihud_x_oi_delta       — amihud_illiq × open_interest_delta interaction
      conf_ma_ratio           — confidence_v1 / EMA(confidence)
      confidence_x_of_score   — confidence_v1 × of_confirm_score
      gate_hardness_score     — fraction of "ok"/passing gates (0..1)
      rsi_cvd                 — RSI computed from cvd_series (if present)
      model_calibration_err   — |ml_shadow_conf01 - 0.5| as rough cal error proxy
    """
    out: dict[str, float] = {}

    # vol_regime_code: map regime string → integer code
    _REGIME_CODE = {
        "trending_bull": 1.0,
        "trending_bear": 2.0,
        "range": 3.0,
        "squeeze": 4.0,
        "expansion": 5.0,
        "mixed": 6.0,
        "unknown": 0.0,
        "na": 0.0,
    }
    rg = indicators.get("regime") or indicators.get("market_regime") or ""
    rg_norm = str(rg).lower().strip()
    if rg_norm in _REGIME_CODE:
        out["vol_regime_code"] = _REGIME_CODE[rg_norm]

    # amihud_x_oi_delta: needs both inputs
    amihud = indicators.get("amihud_illiq")
    if amihud is None:
        amihud = indicators.get("amihud")
    oi_delta = deriv_out.get("open_interest_delta")
    if oi_delta is None:
        oi_delta = indicators.get("open_interest_delta")
    if amihud is not None and oi_delta is not None:
        try:
            out["amihud_x_oi_delta"] = float(amihud) * float(oi_delta)
        except (TypeError, ValueError):
            pass

    # conf_ma_ratio: confidence_v1 / EMA(confidence)
    conf = indicators.get("confidence_v1") or indicators.get("confidence")
    conf_ema = indicators.get("confidence_ema") or indicators.get("confidence_ma")
    if conf is not None and conf_ema is not None:
        try:
            c = float(conf)
            ce = float(conf_ema)
            if ce > 1e-9:
                out["conf_ma_ratio"] = c / ce
        except (TypeError, ValueError):
            pass

    # confidence_x_of_score: interaction of base confidence × OF confirm score
    of_score = indicators.get("of_confirm_score") or indicators.get("of_score")
    if conf is not None and of_score is not None:
        try:
            out["confidence_x_of_score"] = float(conf) * float(of_score)
        except (TypeError, ValueError):
            pass

    # gate_hardness_score: count of "*_ok" keys that are True (or 1)
    # Captures how many gates the signal cleared cleanly.
    n_ok = 0
    n_total = 0
    for k, v in indicators.items():
        if not isinstance(k, str):
            continue
        # Look for ok/passed indicator keys
        if not (k.endswith("_ok") or k.endswith("_passed") or k.endswith("_ready")):
            continue
        # Skip non-boolean-ish numeric counters
        if isinstance(v, bool):
            n_total += 1
            if v:
                n_ok += 1
        elif isinstance(v, (int, float)) and v in (0, 1, 0.0, 1.0):
            n_total += 1
            if v:
                n_ok += 1
    if n_total >= 3:  # need at least 3 gates to be meaningful
        out["gate_hardness_score"] = n_ok / n_total

    # model_calibration_err: rough proxy from ml_shadow_conf01 distance from 0.5
    # (true calibration err needs rolling actual-vs-predicted; this is a single-shot
    # placeholder until a per-symbol HASH is built)
    ml_shadow = indicators.get("ml_shadow_conf01")
    if ml_shadow is None:
        cb = indicators.get("confidence_breakdown") or {}
        if isinstance(cb, dict):
            ml_shadow = cb.get("ml_shadow_conf01")
    if ml_shadow is not None:
        try:
            out["model_calibration_err"] = abs(float(ml_shadow) - 0.5)
        except (TypeError, ValueError):
            pass

    # Label-class features — v14_of schema expects mae_r/mfe_r but at serve
    # time these are unknown (only known post-close). Emit 0.0 as the
    # canonical "unknown" marker — matches what the trainer fed for unlabelled
    # rows and is the convention v14_of learned with.
    if "mae_r" not in indicators:
        out["mae_r"] = 0.0
    if "mfe_r" not in indicators:
        out["mfe_r"] = 0.0

    # rsi_cvd: 14-period RSI on cvd_series if present (list of recent CVD values)
    cvd_series = indicators.get("cvd_series") or indicators.get("cvd_history")
    if isinstance(cvd_series, list) and len(cvd_series) >= 14:
        try:
            vals = [float(x) for x in cvd_series[-15:]]
            gains: list[float] = []
            losses: list[float] = []
            for i in range(1, len(vals)):
                d = vals[i] - vals[i - 1]
                if d > 0:
                    gains.append(d)
                    losses.append(0.0)
                else:
                    gains.append(0.0)
                    losses.append(-d)
            if gains and losses:
                avg_gain = sum(gains) / len(gains)
                avg_loss = sum(losses) / len(losses)
                if avg_loss > 1e-12:
                    rs = avg_gain / avg_loss
                    out["rsi_cvd"] = 100.0 - (100.0 / (1.0 + rs))
                elif avg_gain > 0:
                    out["rsi_cvd"] = 100.0
                else:
                    out["rsi_cvd"] = 50.0
        except (TypeError, ValueError):
            pass

    # ── Phase 1 P1: execution-adjusted EV features ──────────────────────────────

    # ev_after_slippage_bps: edge minus expected execution cost.
    # edge_bps sources (priority order): explicit key, spread-implied, tca spread.
    edge_bps_v = (
        indicators.get("edge_bps")
        or indicators.get("raw_edge_bps")
        or indicators.get("expected_edge_bps")
        or deriv_out.get("tca_eff_spread_bps_ema")
        or indicators.get("tca_eff_spread_bps_ema")
    )
    slip_v = (
        indicators.get("expected_slippage_bps")
        or indicators.get("slippage_p95")
        or indicators.get("slippage_realized_bps")
    )
    if edge_bps_v is not None and slip_v is not None:
        try:
            out["ev_after_slippage_bps"] = float(edge_bps_v) - float(slip_v)
        except (TypeError, ValueError):
            pass

    # net_edge_to_cost_ratio: (edge - cost) / cost.
    # cost = half-spread + slippage (execution round-trip proxy).
    spread_v = indicators.get("spread_bps") or indicators.get("spread")
    if edge_bps_v is not None and slip_v is not None and spread_v is not None:
        try:
            cost_bps = max(float(spread_v) * 0.5 + float(slip_v), 1e-6)
            edge_f = float(edge_bps_v)
            out["net_edge_to_cost_ratio"] = (edge_f - cost_bps) / cost_bps
        except (TypeError, ValueError):
            pass

    # ── P2: cost decomposition features ──────────────────────────────────────────

    # ev_after_fee_bps: edge minus estimated round-trip commission.
    # tca_is_bps_ema (implementation shortfall) captures real fee+impact if available,
    # else fall back to 3.0 bps (binance maker 0.015% × 2 roundtrip).
    fee_v = (
        deriv_out.get("tca_is_bps_ema")
        or indicators.get("tca_is_bps_ema")
        or indicators.get("fee_bps")
    )
    if fee_v is None:
        fee_v = 3.0
    if edge_bps_v is not None:
        try:
            out["ev_after_fee_bps"] = float(edge_bps_v) - float(fee_v)
        except (TypeError, ValueError):
            pass

    # ev_after_spread_bps: edge minus half-spread (taker crossing cost baseline)
    if edge_bps_v is not None and spread_v is not None:
        try:
            out["ev_after_spread_bps"] = float(edge_bps_v) - float(spread_v) * 0.5
        except (TypeError, ValueError):
            pass

    # ev_after_impact_bps: edge minus permanent market impact (price impact from fill)
    perm_impact_v = (
        deriv_out.get("tca_perm_impact_1s_bps_ema")
        or indicators.get("tca_perm_impact_1s_bps_ema")
        or deriv_out.get("tca_perm_impact_5s_bps_ema")
        or indicators.get("tca_perm_impact_5s_bps_ema")
    )
    if edge_bps_v is not None and perm_impact_v is not None:
        try:
            out["ev_after_impact_bps"] = float(edge_bps_v) - float(perm_impact_v)
        except (TypeError, ValueError):
            pass

    # cost_bps_v: round-trip cost proxy used by tp1/sl net features
    sl_bps_v = (
        indicators.get("sl_dist_bps")
        or indicators.get("atr_bps")
        or indicators.get("atr_bps_th")
    )
    cost_bps_v: float | None = None
    if spread_v is not None and slip_v is not None:
        try:
            cost_bps_v = float(spread_v) * 0.5 + float(slip_v)
        except (TypeError, ValueError):
            pass

    # tp1_net_after_cost_bps: TP1 reward in bps minus round-trip entry cost
    tp1_r_v = indicators.get("tp1_target_r") or indicators.get("tp1_r")
    if tp1_r_v is not None and sl_bps_v is not None and cost_bps_v is not None:
        try:
            # tp1_target_r is the R multiple; sl_dist_bps is 1R in bps
            tp1_abs_bps = float(tp1_r_v) * float(sl_bps_v)
            out["tp1_net_after_cost_bps"] = tp1_abs_bps - cost_bps_v
        except (TypeError, ValueError):
            pass

    # sl_net_after_cost_bps: total downside exposure (SL distance + entry cost)
    if sl_bps_v is not None and cost_bps_v is not None:
        try:
            out["sl_net_after_cost_bps"] = float(sl_bps_v) + cost_bps_v
        except (TypeError, ValueError):
            pass

    # expected_hold_cost_bps: half-spread as proxy for round-trip hold cost
    tca_spread_v = (
        deriv_out.get("tca_eff_spread_bps_ema")
        or indicators.get("tca_eff_spread_bps_ema")
        or spread_v
    )
    if tca_spread_v is not None:
        try:
            out["expected_hold_cost_bps"] = float(tca_spread_v)
        except (TypeError, ValueError):
            pass

    # cost_regime_z: how current spread compares to TCA EMA (z-score proxy)
    if spread_v is not None and tca_spread_v is not None:
        try:
            s = float(spread_v)
            ema_s = float(tca_spread_v)
            tca_p95 = (
                deriv_out.get("tca_spread_p95_bps")
                or indicators.get("tca_spread_p95_bps")
            )
            if tca_p95 is not None:
                std_proxy = max((float(tca_p95) - ema_s) / 1.645, ema_s * 0.1, 0.1)
            else:
                std_proxy = max(ema_s * 0.3, 0.1)
            out["cost_regime_z"] = max(-5.0, min(5.0, (s - ema_s) / std_proxy))
        except (TypeError, ValueError):
            pass

    # conf_rsi_agree: RSI momentum alignment with signal direction.
    # Moved here from strategy.py so rsi_cvd (from _enrich_rsi_cvd / book_rates)
    # is available before the check. strategy.py's runtime.rsi_cvd warmed up only
    # after 14 microbar closes — defaulting to 50.0 made rc > 50 always False.
    rsi_price_v = indicators.get("rsi_price")
    rsi_cvd_v = out.get("rsi_cvd") or deriv_out.get("rsi_cvd") or indicators.get("rsi_cvd")
    direction_v = str(indicators.get("direction", "") or "").upper()
    if rsi_price_v is not None and rsi_cvd_v is not None and direction_v in ("LONG", "SHORT"):
        try:
            rp_c = float(rsi_price_v)
            rc_c = float(rsi_cvd_v)
            is_aligned = (
                (direction_v == "LONG" and rp_c > 50 and rc_c > 50)
                or (direction_v == "SHORT" and rp_c < 50 and rc_c < 50)
            )
            out["conf_rsi_agree"] = 1.0 if is_aligned else 0.0
        except (TypeError, ValueError):
            pass

    return out


def _enrich_atr_aliases(indicators: dict[str, Any]) -> dict[str, float]:
    """ATR pipeline aliases — v14_of expects names that differ from publisher's.

    Production indicator names (audit 2026-05-24):
      `atr_floor_picked_bps`, `atr_floor_tier`, `atr_bps`, `atr_bps_th`,
      `atr_percentile_rank_30d`, `atr_q`, `atr_local_q`, `atr_stop_pct`.

    v14_of expectations:
      `atr_bps_exec`, `atr_unified_th_bps`, `atr_candidates_n`,
      `atr_cons_ok`, `atr_consistency`, `atr_sanity_ok`,
      `atr_floor_t0_bps`, `atr_floor_t1_bps`, `atr_floor_t2_bps`,
      `atr_fees_rocket_mult`, `atr_fees_th_bps`, `atr_fees_tp1_share`.

    We map known equivalents and bridge missing-but-derivable ones.
    """
    out: dict[str, float] = {}

    # Direct aliases: v14 name ← production name
    aliases = {
        "atr_bps_exec": ("atr_bps", "atr_bps_th"),
        "atr_unified_th_bps": ("atr_bps_th", "atr_floor_picked_bps"),
        # Floor tier-derived: t0/t1/t2 = three bands of the floor — when not
        # individually exposed, mirror picked value into the matching tier slot.
    }
    for dst, srcs in aliases.items():
        for src in srcs:
            v = indicators.get(src)
            if v is not None:
                out[dst] = _safe_float(v)
                break

    # Floor tiers: synthesise t0/t1/t2 from picked floor and tier index
    floor_bps = _safe_float(indicators.get("atr_floor_picked_bps") or indicators.get("atr_bps_th"))
    tier = _safe_float(indicators.get("atr_floor_tier"))
    out["atr_floor_tier"] = tier
    out["atr_floor_ready"] = _safe_float(indicators.get("atr_floor_ready") or (1.0 if floor_bps > 0 else 0.0))
    if floor_bps > 0:
        # Approximate: t0 = floor × 0.5, t1 = floor (selected), t2 = floor × 1.5
        # When the system exposes only the picked tier, this provides a stable
        # representation matching the model's tier-aware expectations.
        out["atr_floor_t0_bps"] = floor_bps * 0.5
        out["atr_floor_t1_bps"] = floor_bps
        out["atr_floor_t2_bps"] = floor_bps * 1.5

    # Sanity flags: atr_bad → atr_sanity_ok inverse
    if "atr_bad" in indicators:
        try:
            out["atr_sanity_ok"] = 0.0 if bool(indicators["atr_bad"]) else 1.0
        except Exception:
            pass

    # atr_consistency: proxy from atr_jump_count_window (low jumps = consistent)
    jumps = indicators.get("atr_jump_count_window")
    if jumps is not None:
        try:
            j = float(jumps)
            # Smaller jumps → higher consistency; cap at 10 → 0
            out["atr_consistency"] = max(0.0, 1.0 - min(1.0, j / 10.0))
            out["atr_cons_ok"] = 1.0 if out["atr_consistency"] > 0.5 else 0.0
        except (TypeError, ValueError):
            pass

    # atr_candidates_n: number of ATR sources blended (proxy: 1 if atr_src present)
    if indicators.get("atr_src") is not None:
        out["atr_candidates_n"] = 1.0  # single canonical source — model only needs presence signal

    # Fees-related: pass-through with safe defaults so the model gets
    # consistent magnitude rather than 0.0. These are config knobs not stats.
    fees_one_side = _safe_float(indicators.get("fees_bps_one_side") or 2.0)
    if "atr_fees_th_bps" not in out:
        out["atr_fees_th_bps"] = floor_bps + 2.0 * fees_one_side if floor_bps > 0 else 0.0
    if "atr_fees_rocket_mult" not in out:
        # Conservative rocket-mode multiplier (1.5x base)
        out["atr_fees_rocket_mult"] = 1.5
    if "atr_fees_tp1_share" not in out:
        # Typical TP1 share is 50% of position
        out["atr_fees_tp1_share"] = 0.5

    return out


def _enrich_misc_aliases(indicators: dict[str, Any]) -> dict[str, float]:
    """One-off aliases for features that exist under different names in publisher.

    Closes:
      - confidence_ema ← confidence (or breakdown.base)
      - amihud_illiq ← deribit volatility-proxy or microprice variance
      - iceberg_avg_qty ← iceberg_qty (if iceberg signal)
      - liqmap_1h_age_ms ← liqmap_1h_stale_ms
      - liqmap_sl_* ← liqmap_gate_risk_bps / reward_bps as proxies
      - liq_score_x_spread ← liqmap_gate_risk_bps × spread_bps
      - book_health_veto_book_evidence / data_health_veto_book_evidence ← presence flag
    """
    out: dict[str, float] = {}

    # iceberg_avg_qty — always emit (default 0 for non-iceberg signals)
    if "iceberg_qty" in indicators:
        out["iceberg_avg_qty"] = _safe_float(indicators["iceberg_qty"])
    elif "iceberg_count_window" in indicators:
        out["iceberg_avg_qty"] = _safe_float(indicators.get("iceberg_total_qty_window")) / max(
            1.0, _safe_float(indicators.get("iceberg_count_window"))
        )
    elif "iceberg_price" in indicators:
        # iceberg signal path — proxy qty from typical bucket
        out["iceberg_avg_qty"] = 1.0
    else:
        out["iceberg_avg_qty"] = 0.0  # non-iceberg signal

    # liqmap aliases
    stale = indicators.get("liqmap_1h_stale_ms")
    if stale is not None:
        out["liqmap_1h_age_ms"] = _safe_float(stale)
    # SL recommendations — always emit (use 0.0 as known-zero when gate inactive).
    # This matches the training-time convention where unpopulated keys default
    # to 0.0 rather than absent.
    risk_bps = _safe_float(indicators.get("liqmap_gate_risk_bps"))
    out["liqmap_sl_base_bps"] = risk_bps
    out["liqmap_sl_reco_bps"] = risk_bps
    rr = _safe_float(indicators.get("liqmap_gate_rr"))
    if rr > 0:
        out["liqmap_sl_widen_needed"] = 1.0 if rr < 1.5 else 0.0
        out["liqmap_sl_widen_ratio"] = max(1.0, 1.5 / rr)
    else:
        out["liqmap_sl_widen_needed"] = 0.0
        out["liqmap_sl_widen_ratio"] = 1.0
    # liq_score_x_spread interaction
    spread_bps = _safe_float(indicators.get("spread_bps"))
    out["liq_score_x_spread"] = risk_bps * spread_bps

    # health veto flags — bool→float; default 0 (no veto evidence)
    for src, dst in (
        ("book_health_veto", "book_health_veto_book_evidence"),
        ("data_health_veto", "data_health_veto_book_evidence"),
    ):
        v = indicators.get(src)
        if v is not None:
            out[dst] = 1.0 if bool(v) else 0.0
        else:
            out[dst] = 0.0  # known absence

    # confidence_ema bridge — production keeps `confidence` (final calibrated);
    # use it as EMA proxy when no separate ema exists. The ratio computed
    # downstream in _enrich_derived will then equal 1.0 (neutral signal)
    # — better than missing.
    if "confidence_ema" not in indicators and "confidence_ma" not in indicators:
        conf = indicators.get("confidence") or indicators.get("confidence_v1")
        if conf is not None:
            out["confidence_ema"] = _safe_float(conf)

    # amihud_illiq proxy: |microprice_shift_bps_20| / max(qty_window, 1)
    if "amihud_illiq" not in indicators and "amihud" not in indicators:
        mp = abs(_safe_float(indicators.get("microprice_shift_bps_20")))
        qty = _safe_float(indicators.get("trade_qty_window") or indicators.get("vol_window")) or 1.0
        if mp > 0 and qty > 0:
            out["amihud_illiq"] = mp / qty

    
    if "meta_enforce_applied" not in indicators:
        out["meta_enforce_applied"] = 0.0
    if "news_gate_veto" not in indicators:
        out["news_gate_veto"] = 0.0
    if "data_health" not in indicators:
        out["data_health"] = 1.0
    if "signal_age_to_half_life" not in indicators:
        age = _safe_float(indicators.get("signal_age_ms"))
        hl = _safe_float(indicators.get("alpha_half_life_ms_norm") or 60000.0)
        if hl > 0:
            out["signal_age_to_half_life"] = age / hl

    return out


def _enrich_book_rates(symbol: str, redis_client: Any) -> dict[str, float]:
    """Book event rates from `book_rates:{symbol}` JSON.

    Producer: `services/book_rate_ema_producer.py`.
    Features: depth_pull_ratio, cancel_to_fill_ratio, maker_cancel_ratio,
              book_refresh_rate_hz, *_rate_ema (used by _enrich_book_features).
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"book_rates:{symbol}", 60_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for key in (
        "depth_pull_ratio", "cancel_to_fill_ratio", "maker_cancel_ratio",
        "book_refresh_rate_hz",
        "added_bid_rate_ema", "added_ask_rate_ema",
        "cancel_bid_rate_ema", "cancel_ask_rate_ema",
        "trade_rate_ema", "book_update_rate_ema",
    ):
        v = data.get(key)
        if v is not None:
            out[key] = _safe_float(v)
    return out


def _enrich_microstruct_ctx(symbol: str, redis_client: Any) -> dict[str, float]:
    """Microstructure v2 features from `microstruct:ctx:{symbol}` JSON.

    Producer: `core/microstructure_metrics_v2.py` standalone service.
    Features: kyle_lambda, taker_lambda, vpin_rolling, kyle_x_vpin,
    vpin_x_funding, tick_autocorr_lag1, roll_spread_est, hurst_exp_50,
    hurst_x_vol_regime, garman_klass_vol, parkinson_vol, yang_zhang_vol,
    vol_of_vol,
    amihud_illiquidity, pin_estimate.
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"microstruct:ctx:{symbol}", 120_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for key in (
        "kyle_lambda", "kyle_x_vpin", "taker_lambda",
        "vpin_rolling", "vpin_x_funding",
        "tick_autocorr_lag1", "roll_spread_est",
        "hurst_exp_50", "hurst_x_vol_regime",
        "garman_klass_vol", "parkinson_vol", "yang_zhang_vol", "vol_of_vol",
        "amihud_illiquidity", "pin_estimate",
        "obi_sustained", "ofi_age_ms", "fp_edge_absorb", "fp_edge_age_ms",
        "quote_stuffing_score", "trade_msg_rate_z", "vol_expansion_score",
        "cvd_buy_volume", "cvd_sell_volume",
    ):
        v = data.get(key)
        if v is not None:
            out[key] = _safe_float(v)
    return out


def _enrich_orderflow_pressure_v2(symbol: str, redis_client: Any) -> dict[str, float]:
    """Orderflow pressure v2: trade_freq, trade_size_skew, ofi_stability.

    Producer: `services/orderflow_pressure_v2.py` writes `pressure_v2:{symbol}` JSON.
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"pressure_v2:{symbol}", 60_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for key in (
        "trade_freq_per_hr", "trade_size_skew",
        "ofi", "ofi_stability_score", "ofi_stable_secs",
    ):
        v = data.get(key)
        if v is not None:
            out[key] = _safe_float(v)
    return out


def _enrich_sweep_v2(symbol: str, redis_client: Any) -> dict[str, float]:
    """Sweep v2: sweep_div_match, sweep_velocity_bps_s, signal_cluster_flag.

    Producer: `services/sweep_detector_v2.py` writes `sweep_v2:{symbol}` JSON.
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"sweep_v2:{symbol}", 60_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for key in (
        "sweep_div_match", "sweep_velocity_bps_s", "signal_cluster_flag",
        "source_jump_usd", "sweep_eql", "div_match",
        "cvd_median_abs_delta_usd", "cvd_divergence_from_price",
    ):
        v = data.get(key)
        if v is not None:
            out[key] = _safe_float(v)
    return out


def _enrich_liquidation_ctx(symbol: str, redis_client: Any) -> dict[str, float]:
    """Liquidation context per symbol: 1m aggregates + source health + cascade risk.

    Source: `ctx:liq:{symbol}` JSON (liquidation_context_worker).
    Phase 1 P1 additions: liq_source_available (#21), liq_source_age_ms (#22),
    liq_cascade_risk_score (#23).
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"ctx:liq:{symbol}", _DERIV_CTX_MAX_LAG_MS)
    if not data:
        return {}
    out: dict[str, float] = {}
    for src, dst in (
        ("liquidation_usd_1m", "liquidation_usd_1m"),
        ("liq_usd_1m", "liquidation_usd_1m"),
        ("liqmap_age_ms", "liqmap_1h_age_ms"),
    ):
        if src in data and dst not in out:
            out[dst] = _safe_float(data[src])

    # P1 #21 — liq_source_available: 1 if OK, 0 if stale/absent
    quality = str(data.get("quality_status") or "")
    out["liq_source_available"] = 1.0 if quality == "OK" else 0.0

    # P1 #22 — liq_source_age_ms: ms since last liquidation event
    ts_snap = _safe_float(data.get("ts_ms"), -1.0)
    if ts_snap > 0:
        out["liq_source_age_ms"] = max(0.0, float(_now_ms()) - ts_snap)

    # P1 #23 — liq_cascade_risk_score: composite [0..1] stress indicator.
    # Combines |imbalance_z| / 10 + stress_flag + largest_notional_ratio.
    imb_z = _safe_float(data.get("liq_imbalance_z"), 0.0)
    stress = _safe_float(data.get("liq_stress_flag"), 0.0)
    largest = _safe_float(data.get("largest_liq_notional_1m"), 0.0)
    total_notional = (
        _safe_float(data.get("liq_buy_notional_1m"), 0.0)
        + _safe_float(data.get("liq_sell_notional_1m"), 0.0)
    )
    z_component = min(abs(imb_z) / 10.0, 1.0)
    # size_component: fraction largest_single / total_window; 0 if no trades
    if total_notional > 1.0:
        size_component = min(largest / total_notional, 1.0)
    else:
        size_component = 0.0
    out["liq_cascade_risk_score"] = min(
        1.0,
        z_component * 0.5 + stress * 0.3 + size_component * 0.2,
    )

    return out


def _enrich_bybit_health(symbol: str, redis_client: Any) -> dict[str, float]:
    """Phase 1 P1 #18 — bybit source health from runtime:bybit:{symbol} HASH.

    Emits bybit_data_age_ms, bybit_data_available, bybit_data_stale.
    Go bybit_features_collector writes this key every ~15s.
    """
    _BYBIT_MAX_LAG = 120_000
    if not symbol or redis_client is None:
        return {"bybit_data_available": 0.0, "bybit_data_stale": 1.0}
    data = _load_hash_snapshot(redis_client, f"runtime:bybit:{symbol}", _BYBIT_MAX_LAG)
    avail, age_ms, stale = _source_age_and_health(data, _BYBIT_MAX_LAG)
    out: dict[str, float] = {
        "bybit_data_available": avail,
        "bybit_data_stale": stale,
    }
    if age_ms > 0:
        out["bybit_data_age_ms"] = age_ms
    return out


def _enrich_p1_queue_dynamics(symbol: str, redis_client: Any) -> dict[str, float]:
    """P1 #6-10 queue dynamics from `ctx:queue_dynamics:{symbol}`.

    Producer: services/queue_dynamics_producer.py
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"ctx:queue_dynamics:{symbol}", 60_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for k in (
        "queue_depletion_rate_l1", "queue_refill_rate_l1",
        "adverse_selection_1s_bps", "post_fill_reversion_prob",
        "limit_vs_market_entry_edge_bps",
        # P2 Group B
        "queue_depletion_rate_l5", "queue_refill_rate_l5",
        "queue_position_risk_score", "adverse_selection_3s_bps",
        "fill_or_kill_edge_bps",
    ):
        v = data.get(k)
        if v is not None:
            out[k] = _safe_float(v)
    return out


def _enrich_p1_cost_dynamics(symbol: str, redis_client: Any) -> dict[str, float]:
    """P1 #13 + P2 cost decomposition from `ctx:cost_dynamics:{symbol}`.

    Producer: services/cost_dynamics_producer.py (writes the full
    p1_d_cost_dynamics + p2_c_cost_decomposition feature set).
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"ctx:cost_dynamics:{symbol}", 60_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for k in (
        "cost_widening_5s_bps",            # P1 #13
        # P2 Group C — cost decomposition
        "ev_after_fee_bps",
        "ev_after_spread_bps",
        "ev_after_impact_bps",
        "tp1_net_after_cost_bps",
        "sl_net_after_cost_bps",
        "expected_hold_cost_bps",
        "cost_regime_z",
    ):
        v = data.get(k)
        if v is not None:
            out[k] = _safe_float(v)
    return out


def _enrich_p1_regime_transition(symbol: str, redis_client: Any) -> dict[str, float]:
    """P1 #14-15 regime transition features from `ctx:regime_transition:{symbol}`.

    Producer: services/regime_transition_producer.py
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"ctx:regime_transition:{symbol}", 120_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for k in (
        "regime_transition_code", "failed_breakout_count_30m",
        # P2 Group D
        "regime_transition_age_ms", "trend_to_chop_prob", "chop_to_expansion_prob",
        "expansion_exhaustion_score", "range_break_attempt_count_30m",
        "vol_ofi_regime_agree", "vol_price_divergence_score",
    ):
        v = data.get(k)
        if v is not None:
            out[k] = _safe_float(v)
    return out


def _enrich_p1_cross_venue(symbol: str, redis_client: Any) -> dict[str, float]:
    """P1 #19-20 cross-venue health from `ctx:cross_venue:{symbol}`.

    Producer: services/cross_venue_health_producer.py
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"ctx:cross_venue:{symbol}", 60_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for k in (
        "cross_venue_lead_lag_ms", "venue_consensus_persistence_3s",
        # P2 Group E
        "bybit_book_age_ms", "bybit_trade_rate_hz", "cross_venue_latency_diff_ms",
        "binance_leads_bybit_score", "bybit_leads_binance_score",
        "venue_consensus_flip_count_10s", "cross_venue_spread_diff_bps",
    ):
        v = data.get(k)
        if v is not None:
            out[k] = _safe_float(v)
    return out


def _enrich_p1_pit_priors(symbol: str, redis_client: Any) -> dict[str, float]:
    """P1 #24-25 timeout/tp1-before-timeout rates from pit_priors rolling HASHes.

    Reads pit_priors:rolling:7d:{symbol}:default:all for the new
    timeout_rate and tp1_before_timeout_rate fields added in Phase 1.
    Uses 'default:all' as the broadest available slice.
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_hash_snapshot(
        redis_client, f"pit_priors:rolling:7d:{symbol}:default:all", 86_400_000
    )
    if not data:
        return {}
    out: dict[str, float] = {}
    tr = data.get("timeout_rate")
    if tr is not None:
        out["prior_timeout_rate_symbol_kind_session"] = _safe_float(tr)
    tp1t = data.get("tp1_before_timeout_rate")
    if tp1t is not None:
        out["prior_tp1_before_timeout_rate"] = _safe_float(tp1t)
    return out


def _enrich_p1_session_vol(symbol: str, redis_client: Any) -> dict[str, float]:
    """P1 #16 — session_liquidity_z from ctx:session_vol:{symbol}.

    Producer: services/session_volume_aggregator.py
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"ctx:session_vol:{symbol}", 7_200_000)
    if not data:
        return {}
    raw = data.get("session_liquidity_z")
    if raw is None:
        return {}
    return {"session_liquidity_z": max(-5.0, min(5.0, _safe_float(raw)))}


def _enrich_p1_session_priors(symbol: str, redis_client: Any) -> dict[str, float]:
    """P1 #17 — session_signal_quality_prior from pit_priors:rolling:7d:{sym}:default:{session}.

    No new producer required: pit_priors_rolling_v1 already writes per-session HASHes.
    Session is derived from current wall-clock UTC hour.
    Quality composite = winrate*0.5 + (ev_r>0)*0.3 + tp1_hit_rate*0.2.
    """
    if not symbol or redis_client is None:
        return {}
    ts_now = _now_ms()
    h = (ts_now // 3_600_000) % 24
    if 13 <= h < 22:
        session = "us"
    elif 7 <= h < 16:
        session = "europe"
    else:
        session = "asia"
    key = f"pit_priors:rolling:7d:{symbol}:default:{session}"
    data = _load_hash_snapshot(redis_client, key, 86_400_000)
    if not data:
        return {}
    winrate = _safe_float(data.get("winrate"), 0.5)
    ev_r = _safe_float(data.get("ev_r"), 0.0)
    tp1_rate = _safe_float(data.get("tp1_hit_rate"), 0.0)
    quality = winrate * 0.5 + (1.0 if ev_r > 0 else 0.0) * 0.3 + tp1_rate * 0.2
    return {"session_signal_quality_prior": max(0.0, min(1.0, quality))}


def _enrich_p2_pit_priors(symbol: str, redis_client: Any) -> dict[str, float]:
    """P2 Group G — extended pit_priors features.

    Reads (sym, default, session) for winrate/ev_r/timeout_loss_rate and
    (sym, default, all) for trailing_success_rate, be_stopout_rate, hold_time.
    """
    if not symbol or redis_client is None:
        return {}
    out: dict[str, float] = {}
    ts_now = _now_ms()
    h = (ts_now // 3_600_000) % 24
    if 13 <= h < 22:
        session = "us"
    elif 7 <= h < 16:
        session = "europe"
    else:
        session = "asia"

    # Session-specific priors for winrate/ev_r/timeout_loss_rate
    sess_data = _load_hash_snapshot(
        redis_client, f"pit_priors:rolling:7d:{symbol}:default:{session}", 86_400_000
    )
    if sess_data:
        wr = sess_data.get("winrate")
        if wr is not None:
            out["prior_winrate_symbol_kind_regime_session"] = _safe_float(wr)
        ev = sess_data.get("ev_r")
        if ev is not None:
            out["prior_ev_r_symbol_kind_regime_session"] = _safe_float(ev)
        tr = sess_data.get("timeout_rate")
        if tr is not None:
            out["prior_timeout_loss_rate_session"] = _safe_float(tr)

    # All-session priors for trailing/be/hold_time
    all_data = _load_hash_snapshot(
        redis_client, f"pit_priors:rolling:7d:{symbol}:default:all", 86_400_000
    )
    if all_data:
        for src, dst in (
            ("trailing_success_rate", "prior_trailing_success_rate"),
            ("be_stopout_rate", "prior_be_stopout_rate"),
            ("hold_time_p50_ms", "prior_hold_time_p50_ms"),
            ("hold_time_p90_ms", "prior_hold_time_p90_ms"),
        ):
            v = all_data.get(src)
            if v is not None:
                out[dst] = _safe_float(v)

        # prior_mae_before_mfe_ratio: median_mae_r_winners / max(median_mfe_r, 0.01)
        mae_r = all_data.get("median_mae_r_winners")
        mfe_r = all_data.get("median_mfe_r")
        if mae_r is not None and mfe_r is not None:
            mfe_f = max(_safe_float(mfe_r), 0.01)
            out["prior_mae_before_mfe_ratio"] = _safe_float(mae_r) / mfe_f

        # prior_best_exit_policy_code: winrate-based best policy code
        # 1=TP1, 2=trail, 3=TP2, 0=unknown — use trailing_success_rate to discriminate
        trail_rate = all_data.get("trailing_success_rate")
        tp1_rate = all_data.get("tp1_hit_rate")
        if trail_rate is not None and tp1_rate is not None:
            trail_f = _safe_float(trail_rate)
            tp1_f = _safe_float(tp1_rate)
            if trail_f > tp1_f and trail_f > 0.3:
                out["prior_best_exit_policy_code"] = 2.0  # trailing wins
            elif tp1_f > 0.5:
                out["prior_best_exit_policy_code"] = 1.0  # TP1 wins
            else:
                out["prior_best_exit_policy_code"] = 0.0  # unclear

    return out


def _enrich_p2_directional_change(symbol: str, redis_client: Any, indicators: dict | None = None) -> dict[str, float]:
    """P2 Group H — directional change features from `ctx:dc:{symbol}`.

    Producer: services/directional_change_producer.py

    Extended keys (new): dc_trend_duration_ms, dc_last_confirmation_bps,
    dc_noise_ratio, dc_overshoot_to_atr_ratio.
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"ctx:dc:{symbol}", 120_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for k in (
        "dc_event_dir", "dc_event_age_ms", "dc_overshoot_bps", "dc_reversal_count_15m",
        "dc_trend_duration_ms", "dc_last_confirmation_bps", "dc_noise_ratio",
    ):
        v = data.get(k)
        if v is not None:
            out[k] = _safe_float(v)

    # dc_overshoot_to_atr_ratio — computed here because ATR is available in indicators
    dc_overshoot_bps = out.get("dc_overshoot_bps", 0.0)
    ind = indicators or {}
    atr_bps = _safe_float(ind.get("atr_bps") or data.get("atr_bps"), 0.0)
    out["dc_overshoot_to_atr_ratio"] = (dc_overshoot_bps / atr_bps) if atr_bps > 0 else 0.0

    return out


def _enrich_liq_cluster_v2(
    symbol: str,
    indicators: dict,
    redis_client: Any,
    now_ms: int | None = None,
) -> dict[str, float]:
    """Liq cluster v2: sweep-distance and absorption score.

    liq_sweep_to_cluster_dist_bps — distance from most recent sweep event to nearest
      liqmap cluster (min of liq_cluster_dist_above_bps / liq_cluster_dist_below_bps
      from indicators). 0.0 if sweep older than 60s.

    liq_absorption_after_sweep_score — composite score [0, 1] measuring how much a
      recent sweep's velocity has been "absorbed" near the cluster. Decays linearly
      over 60s.
    """
    if not symbol or redis_client is None:
        return {}
    _now = now_ms if now_ms is not None else _now_ms()
    sweep_data = _load_json_snapshot(redis_client, f"sweep_v2:{symbol}", 60_000)

    # liq_sweep_to_cluster_dist_bps
    sweep_dist = 0.0
    absorption_score = 0.0

    if sweep_data:
        sweep_ts = _safe_float(sweep_data.get("ts_ms"), 0.0)
        age_ms = max(0.0, float(_now) - sweep_ts) if sweep_ts > 0 else 61_000.0
        if age_ms <= 60_000.0:
            dist_above = _safe_float(indicators.get("liq_cluster_dist_above_bps"), 0.0)
            dist_below = _safe_float(indicators.get("liq_cluster_dist_below_bps"), 0.0)
            # nearest cluster: pick smallest non-zero, else sum/2
            if dist_above > 0 and dist_below > 0:
                sweep_dist = min(dist_above, dist_below)
            elif dist_above > 0:
                sweep_dist = dist_above
            elif dist_below > 0:
                sweep_dist = dist_below

            # absorption score: recency × velocity saturation
            velocity = abs(_safe_float(sweep_data.get("sweep_velocity_bps_s"), 0.0))
            recency_factor = max(0.0, 1.0 - age_ms / 60_000.0)
            absorption_score = recency_factor * min(1.0, velocity / 50.0)

    return {
        "liq_sweep_to_cluster_dist_bps": sweep_dist,
        "liq_absorption_after_sweep_score": absorption_score,
    }


def _enrich_p2_microstruct(symbol: str, redis_client: Any) -> dict[str, float]:
    """P2 Group A — extended microstructure features from `microstruct:ctx:{symbol}`.

    Reads the P2 Group A keys written by microstructure_metrics_v2 service.
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"microstruct:ctx:{symbol}", 120_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for k in (
        "mlofi_1_3_5_slope", "mlofi_l1_l5_divergence", "mlofi_accel_500ms",
        "mlofi_exhaustion_score", "microprice_ret_250ms", "midprice_impact_per_1k_usd",
    ):
        v = data.get(k)
        if v is not None:
            out[k] = _safe_float(v)
    return out


def _enrich_rsi_cvd(symbol: str, redis_client: Any) -> dict[str, float]:
    """RSI(14) of cumulative volume delta series from book_rates:{symbol}.

    Producer: book_rate_ema_producer appends a CVD snapshot each publish
    interval (every 10s by default). With CVD_SERIES_SIZE=25 that gives
    ~250s of history, enough for a 14-period RSI.
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_json_snapshot(redis_client, f"book_rates:{symbol}", 60_000)
    if not data:
        return {}
    cvd_raw = data.get("cvd_series")
    if not isinstance(cvd_raw, list) or len(cvd_raw) < 14:
        return {}
    try:
        vals = [float(x) for x in cvd_raw[-15:]]
        gains: list[float] = []
        losses: list[float] = []
        for i in range(1, len(vals)):
            d = vals[i] - vals[i - 1]
            if d > 0:
                gains.append(d)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(-d)
        if not gains:
            return {}
        avg_gain = sum(gains) / len(gains)
        avg_loss = sum(losses) / len(losses)
        if avg_loss > 1e-12:
            rsi = 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
        elif avg_gain > 0:
            rsi = 100.0
        else:
            rsi = 50.0
        return {"rsi_cvd": rsi}
    except Exception:
        return {}


def _enrich_cvd_ctx(symbol: str, redis_client: Any) -> dict[str, float]:
    """CVD state snapshot from `cvd:state:{symbol}` HASH.

    Producer: tick_decision_engine publishes after runtime.cvd_state.indicators_light().
    Unlocks: cvd_jump_events_total, cvd_median_abs_delta_usd, cvd_tick, cvd_ema, cvd_slope
    for veto-path and cross-service signals that didn't have runtime.cvd_state available.
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_hash_snapshot(redis_client, f"cvd:state:{symbol}", 120_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for k in (
        "cvd_jump_events_total", "cvd_median_abs_delta_usd",
        "cvd_tick", "cvd_ema", "cvd_slope",
    ):
        v = data.get(k)
        if v is not None:
            out[k] = _safe_float(v)
    return out


def _enrich_abs_lvl_ctx(symbol: str, redis_client: Any) -> dict[str, float]:
    """abs_lvl calibration diagnostics from `abs_lvl:state:{symbol}` HASH.

    Producer: tick_decision_engine publishes from cfg2 (dynamic config).
    Unlocks: abs_lvl_eff_quote_th, abs_lvl_min_quote_delta, abs_lvl_calib_n
    for veto-path and cross-service signals.
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_hash_snapshot(redis_client, f"abs_lvl:state:{symbol}", 300_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for k in ("abs_lvl_eff_quote_th", "abs_lvl_min_quote_delta", "abs_lvl_calib_n"):
        v = data.get(k)
        if v is not None:
            out[k] = _safe_float(v)
    return out


def _enrich_og_consensus(symbol: str, redis_client: Any) -> dict[str, float]:
    """OG gate consensus features from `og:consensus:{symbol}` HASH.

    Producer: of_confirm_engine.build() after build_og_payload().
    Fallback for signals that don't go through of_confirm_engine directly
    (e.g. cross-service replay, signal fan-out paths).
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_hash_snapshot(redis_client, f"og:consensus:{symbol}", 120_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for k in (
        "og_have", "og_need", "og_have_minus_need", "og_ok",
        "og_score_minus_threshold", "og_contrib_z", "og_contrib_wp",
        "og_contrib_reclaim", "og_contrib_obi", "og_contrib_iceberg",
        "og_contrib_absorption", "og_gate_bits_count",
        "og_strong_need_rev", "og_strong_need_cont", "og_weak_progress_any",
    ):
        v = data.get(k)
        if v is not None:
            out[k] = _safe_float(v)
    return out


def _enrich_tca_priors(symbol: str, redis_client: Any) -> dict[str, float]:
    """TCA (transaction cost analysis) EMA priors from `tca:ema:{symbol}:{kind}:{session}`.

    Producer: orderflow_services/tca_priors_exporter_v1.py — EMA of eff_spread,
    realized/permanent impact, implementation shortfall per (symbol, kind, session).

    Keys mapped:
      eff_spread  → tca_eff_spread_bps_ema
      realized_1s → tca_realized_spread_1s_bps_ema
      perm_1s     → tca_perm_impact_1s_bps_ema
      perm_5s     → tca_perm_impact_5s_bps_ema
      is_bps      → tca_is_bps_ema
      samples     → tca_samples
    """
    if not symbol or redis_client is None:
        return {}
    sym = symbol.upper()
    # Session bucket: same logic as tca_priors_exporter_v1._session_bucket
    now_ms = _now_ms()
    h = (now_ms // 3_600_000) % 24
    if 13 <= h < 22:
        session = "us"
    elif 7 <= h < 16:
        session = "europe"
    else:
        session = "asia"

    _TCA_MAX_LAG_MS = 3600_000  # 1h — TCA EMAs are slow-moving
    data: dict[str, str] = {}
    # Try "default" kind first, then fallback to no-kind key
    for kind in ("default", "iceberg", "delta_spike"):
        d = _load_hash_snapshot(redis_client, f"tca:ema:{sym}:{kind}:{session}", _TCA_MAX_LAG_MS)
        if d:
            data = d
            break
    if not data:
        return {}
    out: dict[str, float] = {}
    field_map = (
        ("eff_spread",   "tca_eff_spread_bps_ema"),
        ("realized_1s",  "tca_realized_spread_1s_bps_ema"),
        ("perm_1s",      "tca_perm_impact_1s_bps_ema"),
        ("perm_5s",      "tca_perm_impact_5s_bps_ema"),
        ("is_bps",       "tca_is_bps_ema"),
        ("samples",      "tca_samples"),
        ("spread_p95_bps", "tca_spread_p95_bps"),
    )
    for src, dst in field_map:
        v = data.get(src)
        if v is not None:
            out[dst] = _safe_float(v)
    return out


def _enrich_hawkes_vpin(symbol: str, redis_client: Any) -> dict[str, float]:
    """Hawkes-process intensities + VPIN toxicity from `ctx:hawkes:{symbol}` HASH.

    Producer: orderflow_services/of_hawkes_vpin_v1.py — writes HSET every ~10s.
    TTL on the key is 30s. Feature set mirrors the output dict in of_hawkes_vpin_v1.py.
    """
    if not symbol or redis_client is None:
        return {}
    data = _load_hash_snapshot(redis_client, f"ctx:hawkes:{symbol}", 60_000)
    if not data:
        return {}
    out: dict[str, float] = {}
    for k in (
        "hawkes_dt_s",
        "hawkes_taker_buy_lam", "hawkes_taker_sell_lam",
        "hawkes_cancel_bid_lam", "hawkes_cancel_ask_lam",
        "hawkes_limit_add_lam",
        "hawkes_taker_lam", "hawkes_cancel_lam", "hawkes_churn_lam",
        "hawkes_limit_add_bid_lam", "hawkes_limit_add_ask_lam",
        "hawkes_limit_add_imbalance",
        "added_bid_rate_ema", "added_ask_rate_ema", "added_total_rate_ema",
        "vpin_tox_ema", "vpin_tox_z",
        "vpin_tox_1m", "vpin_tox_5m", "vpin_tox_slope",
        "hawkes_buy_sell_lam_ratio", "hawkes_cancel_imbalance",
        "hawkes_S_taker_buy", "hawkes_S_taker_sell",
        "hawkes_S_cancel_bid", "hawkes_S_cancel_ask",
        "hawkes_S_limit_add",
    ):
        v = data.get(k)
        if v is not None:
            out[k] = _safe_float(v)
    return out


def _enrich_liqmap_snapshot(
    symbol: str,
    redis_client: Any,
    indicators: dict[str, Any] | None = None,
) -> dict[str, float]:
    """Liqmap window features from `liqmap:snapshot:{symbol}:{window}` JSON strings.

    Parses the raw `levels` array via core.liqmap_features_v1 to produce the
    canonical `liqmap_{w}_near_short_usd`, `liqmap_{w}_near_long_usd`,
    `liqmap_{w}_dist_up_bps`, `liqmap_{w}_dist_dn_bps` keys that
    of_confirm_engine Phase 4.7 reads.

    Requires current price — read from indicators["entry"] (preferred) or
    indicators["price"]. Falls back to flat-field pass-through when price unavailable.

    Keys are pre-primed via _prime_snapshot_cache (MGET), so no individual r.get here.
    """
    if not symbol or redis_client is None:
        return {}
    _inds = indicators or {}
    price = _safe_float(
        _inds.get("entry") or _inds.get("price") or _inds.get("mid_price")
    )
    _window_lag = {"5m": 900_000, "1h": 5_400_000, "4h": 5_400_000}
    now_ms = _now_ms()
    out: dict[str, float] = {}

    for window in ("5m", "1h", "4h"):
        key = f"liqmap:snapshot:{symbol}:{window}"
        raw_snap = _load_json_snapshot(redis_client, key, _window_lag[window])
        if not raw_snap:
            continue

        # Parse + compute via liqmap_features_v1 when price is available.
        # This produces near_short_usd, near_long_usd, dist_up_bps, dist_dn_bps.
        if price > 0.0:
            try:
                from core.liqmap_features_v1 import (
                    compute_liqmap_features,
                    parse_liqmap_snapshot_v1,
                )
                raw_json = json.dumps(raw_snap) if isinstance(raw_snap, dict) else str(raw_snap)
                snap_obj = parse_liqmap_snapshot_v1(raw_json, expected_symbol=symbol, expected_window=window)
                feats = compute_liqmap_features(
                    snap_obj,
                    price=price,
                    windows=(window,),
                    near_band_bps=20.0,
                    peak_min_share=0.05,
                    now_ms=now_ms,
                )
                out.update(feats)
            except Exception:
                pass

        # Extras that liqmap_features_v1 doesn't produce
        for extra_k in ("calib_n", "notional_thr", "geom_monitor_hit"):
            v = raw_snap.get(extra_k)
            if v is not None:
                out.setdefault(f"liq_{extra_k}", _safe_float(v))
        break  # first non-empty window is canonical
    return out


# ── Top-level enricher ────────────────────────────────────────────────────────


def enrich_indicators(
    *,
    indicators: dict[str, Any],
    symbol: str,
    redis_client: Any | None = None,
) -> dict[str, float]:
    """Run all per-group producers and merge their output.

    Caller should `setdefault` each returned key into the live indicators
    dict to avoid overriding existing populated values.

    Returns:
        dict[str, float] — keys to merge.  Empty dict on total failure.
    """
    out: dict[str, float] = {}

    # Batch-fetch all Redis snapshot keys for this symbol in one MGET so
    # every producer group reads data from the same point in time (zero
    # time-skew between groups). Individual _load_json_snapshot calls
    # will then be served from the just-primed in-process cache.
    _prime_snapshot_cache(redis_client, _snapshot_keys_for_symbol(symbol))

    # Each group is wrapped in try/except so one failure doesn't cascade.
    for fn, args in (
        (_enrich_deriv_ctx, (symbol, redis_client)),
        (_enrich_crossasset_ctx, (symbol, redis_client)),
        (_enrich_breadth_ctx, (symbol, redis_client)),
        (_enrich_sentiment, (redis_client,)),
        (_enrich_external_ctx, (redis_client,)),
        (_enrich_cg_cp_source_health, (redis_client,)),
        (_enrich_book_features, (indicators,)),
        (_enrich_microbar, (indicators, symbol, redis_client)),
        (_enrich_momentum, (indicators, symbol, redis_client)),
        (_enrich_vol_features, (indicators,)),
        (_enrich_execution_stats, (symbol, redis_client)),
        (_enrich_liquidation_ctx, (symbol, redis_client)),
        (_enrich_microstruct_ctx, (symbol, redis_client)),
        (_enrich_orderflow_pressure_v2, (symbol, redis_client)),
        (_enrich_sweep_v2, (symbol, redis_client)),
        (_enrich_book_rates, (symbol, redis_client)),
        (_enrich_bybit_health, (symbol, redis_client)),
        (_enrich_p1_queue_dynamics, (symbol, redis_client)),
        (_enrich_p1_cost_dynamics, (symbol, redis_client)),
        (_enrich_p1_regime_transition, (symbol, redis_client)),
        (_enrich_p1_cross_venue, (symbol, redis_client)),
        (_enrich_p1_pit_priors, (symbol, redis_client)),
        (_enrich_p1_session_vol, (symbol, redis_client)),
        (_enrich_p1_session_priors, (symbol, redis_client)),
        (_enrich_p2_pit_priors, (symbol, redis_client)),
        (_enrich_p2_directional_change, (symbol, redis_client, indicators)),
        (_enrich_p2_microstruct, (symbol, redis_client)),
        (_enrich_liq_cluster_v2, (symbol, indicators, redis_client)),
        (_enrich_rsi_cvd, (symbol, redis_client)),
        (_enrich_cvd_ctx, (symbol, redis_client)),
        (_enrich_abs_lvl_ctx, (symbol, redis_client)),
        (_enrich_og_consensus, (symbol, redis_client)),
        (_enrich_tca_priors, (symbol, redis_client)),
        (_enrich_hawkes_vpin, (symbol, redis_client)),
        (_enrich_liqmap_snapshot, (symbol, redis_client, indicators)),
        (_enrich_atr_aliases, (indicators,)),
        (_enrich_misc_aliases, (indicators,)),
    ):
        try:
            chunk = fn(*args)  # type: ignore[call-arg]
            if chunk:
                # Merge but do not override values already produced in this cycle
                for k, v in chunk.items():
                    out.setdefault(k, v)
        except Exception as exc:
            logger.debug("feature_enricher: %s failed (fail-open): %s",
                         getattr(fn, "__name__", "?"), exc)

    # Derived features run LAST so they can use cross-group outputs
    # (e.g. amihud_x_oi_delta needs open_interest_delta from _enrich_deriv_ctx).
    try:
        derived = _enrich_derived(indicators, out)
        for k, v in derived.items():
            out.setdefault(k, v)
    except Exception as exc:
        logger.debug("feature_enricher: derived failed (fail-open): %s", exc)

    # Re-run derived AFTER misc_aliases so that `confidence_ema` (from misc)
    # is available for `conf_ma_ratio` calculation.
    try:
        derived2 = _enrich_derived({**indicators, **out}, out)
        for k, v in derived2.items():
            out.setdefault(k, v)
    except Exception as exc:
        logger.debug("feature_enricher: derived2 pass failed (fail-open): %s", exc)

    # ── Stub-fill ─────────────────────────────────────────────────────────────
    # Train-time convention: missing features were padded with 0.0. Serve must
    # match. Two categories with different health semantics:
    #
    #   _STUBS_CONDITIONAL   — absence is semantically correct (no iceberg event,
    #                          Roll model unidentified, shadow ML absent). Silent.
    #   _STUBS_PRODUCER_BACKED — should come from a running producer. Absence MAY
    #                          indicate a broken producer → rate-limited warning.
    now_wall = time.time()
    for _sf in _STUBS_CONDITIONAL | _STUBS_PRODUCER_BACKED:
        if _sf in indicators or _sf in out:
            continue
        out[_sf] = 0.0
        if _sf in _STUBS_PRODUCER_BACKED:
            _stub_miss_total[_sf] = _stub_miss_total.get(_sf, 0) + 1
            if _prom_stub_miss_total is not None:
                try:
                    _prom_stub_miss_total.labels(feature=_sf).inc()
                except Exception:
                    pass
            # Suppress during startup grace window (producers take time to warm up)
            if now_wall - _ENRICHER_START_TIME < _ENRICHER_STUB_WARN_GRACE_S:
                continue
            last = _stub_miss_last_warn.get(_sf, 0.0)
            if now_wall - last >= _STUB_WARN_INTERVAL_S:
                _stub_miss_last_warn[_sf] = now_wall
                logger.warning(
                    "feature_enricher: producer-backed stub filled with 0.0 "
                    "for '%s' (symbol=%s total_misses=%d) — "
                    "check producer service health",
                    _sf, symbol, _stub_miss_total[_sf],
                )

    return out


__all__ = [
    "enrich_indicators",
    "_prime_snapshot_cache",
    "_snapshot_keys_for_symbol",
    "_STUBS_CONDITIONAL",
    "_STUBS_PRODUCER_BACKED",
    "_stub_miss_total",
]
