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
        "cache:fear_greed",
        "sentiment:fear_greed:latest",
        f"exec_stats:{symbol}",
        f"ctx:liq:{symbol}",
        f"microstruct:ctx:{symbol}",
        f"pressure_v2:{symbol}",
        f"sweep_v2:{symbol}",
        f"book_rates:{symbol}",
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
    return out


def _enrich_sentiment(redis_client: Any) -> dict[str, float]:
    """Global sentiment indicators from sentiment exporter / cache.

    Sources tried in order:
      1. `cache:fear_greed` (Go sentiment exporter, JSON with `value` field)
      2. `sentiment:fear_greed:latest` (Python fallback)

    Stale window 30min — fear/greed updates only every few hours.
    """
    for key in ("cache:fear_greed", "sentiment:fear_greed:latest"):
        data = _load_json_snapshot(redis_client, key, _SENTIMENT_MAX_LAG_MS)
        if data:
            break
    else:
        return {}
    out: dict[str, float] = {}
    val = data.get("value") or data.get("fear_greed") or data.get("fg")
    if val is not None:
        # Fear&Greed index is 0..100; normalize to [0..1]. Accept both raw
        # (0-100) and already-normalized (0-1) input.
        v = _safe_float(val)
        if v > 1.0:
            out["crypto_fear_greed"] = max(0.0, min(1.0, v / 100.0))
        else:
            out["crypto_fear_greed"] = max(0.0, min(1.0, v))
    breadth = data.get("market_breadth_score") or data.get("breadth")
    if breadth is not None:
        out["market_breadth_score"] = _safe_float(breadth)
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
        "adverse_drift_ms",
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
        "source_jump_usd",
    ):
        v = data.get(key)
        if v is not None:
            out[key] = _safe_float(v)
    return out


def _enrich_liquidation_ctx(symbol: str, redis_client: Any) -> dict[str, float]:
    """Liquidation context per symbol: liquidation_usd_1m + liqmap_1h_age_ms.

    Source: `ctx:liq:{symbol}` HASH/JSON. Producer = liquidation_map_service.
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
        (_enrich_sentiment, (redis_client,)),
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
        (_enrich_rsi_cvd, (symbol, redis_client)),
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
