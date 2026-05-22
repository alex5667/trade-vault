from __future__ import annotations

import hashlib
import json
import math
import os
import time
from collections import deque
from dataclasses import asdict, dataclass
from types import SimpleNamespace
from typing import Any

# ──────────────────────────────────────────────────────────────────────────
# Phase 7.6: per-symbol rolling LOB state for velocity features.
# Lightweight in-process cache; not persisted across restarts (acceptable —
# 1s/3s windows refill in milliseconds). Bounded deque per symbol prevents
# unbounded growth across long-running workers.
# Format: (ts_ms, obi, qimb_wmean, depth_imbalance_5, spread_bps, fill_prob_proxy)
# ──────────────────────────────────────────────────────────────────────────
_LOB_VELOCITY_MAX_LEN = 256
_LOB_VELOCITY_WINDOWS_MS = (1_000, 3_000)  # 1s, 3s
_LOB_VELOCITY_CACHE: dict[str, deque[tuple[int, float, float, float, float, float]]] = {}

# Phase 7.6b: micro cache for mid-price / churn dynamics.
# Format: (ts_ms, microprice_shift_bps)
_LOB_MICRO_CACHE: dict[str, deque[tuple[int, float]]] = {}

# Phase 4.6: cross-symbol sector aggregation for sector_delta_z_median / sector_obi_median.
# Format: symbol → (wall_time_s, oi_delta_z, obi)
# Updated on each signal; stale entries (> 60s) excluded from median.
_SECTOR_CROSS_CACHE: dict[str, tuple[float, float, float]] = {}
_SECTOR_CROSS_MAX_AGE_S: float = 60.0

# Phase 4.9: per-symbol DQ rolling window state.
# Format: (ts_ms, lag_ms, is_reorder, is_dedupe, is_gap, is_bad_time)
_DQ_ROLLING_CACHE: dict[str, deque[tuple[int, float, int, int, int, int]]] = {}
_DQ_LAST_TS: dict[str, int] = {}  # last tick ts_ms per symbol for gap/reorder detection
_DQ_ROLLING_MAX_LEN = 512
_DQ_WINDOW_MS = 60_000  # 1-minute rolling window for DQ features
_DQ_GAP_THRESHOLD_MS = 500  # gap > 500ms = gap event

# ──────────────────────────────────────────────────────────────────────────
# Redis context read-through cache.
# Prevents multiple synchronous Redis calls per tick — callers hit the cache
# and Redis is queried at most once every REDIS_CTX_CACHE_TTL_S seconds.
# Format: cache_key → (wall_time_s, raw_bytes_dict)
# ──────────────────────────────────────────────────────────────────────────
_REDIS_CTX_CACHE: dict[str, tuple[float, dict]] = {}
_REDIS_CTX_TTL_S: float = float(os.getenv("REDIS_CTX_CACHE_TTL_S", "5.0"))


def _ctx_hgetall(client: Any, key: str, ttl_s: float | None = None) -> dict:
    """Return cached hgetall(key); refresh from Redis at most every ttl_s seconds."""
    if client is None:
        return {}
    _ttl = _REDIS_CTX_TTL_S if ttl_s is None else ttl_s
    entry = _REDIS_CTX_CACHE.get(key)
    now_s = time.monotonic()
    if entry is not None and now_s - entry[0] < _ttl:
        return entry[1] if isinstance(entry[1], dict) else {}
    try:
        raw: dict = client.hgetall(key) or {}
        _REDIS_CTX_CACHE[key] = (now_s, raw)
        return raw
    except Exception:
        return (entry[1] if isinstance(entry[1], dict) else {}) if entry is not None else {}


def _ctx_get(client: Any, key: str, ttl_s: float | None = None) -> Any:
    """Return cached get(key); refresh from Redis at most every ttl_s seconds."""
    if client is None:
        return None
    _ttl = _REDIS_CTX_TTL_S if ttl_s is None else ttl_s
    entry = _REDIS_CTX_CACHE.get(key)
    now_s = time.monotonic()
    if entry is not None and now_s - entry[0] < _ttl:
        return entry[1]
    try:
        raw = client.get(key)
        _REDIS_CTX_CACHE[key] = (now_s, raw)
        return raw
    except Exception:
        return entry[1] if entry is not None else None


def _ext_ctx_track(source: str, stale: bool, ts_ms: float, now_ms: float) -> None:
    """Increment unified external context monitoring metrics. Best-effort, never raises."""
    try:
        from services.orderflow.metrics import (
            external_ctx_age_ms,
            external_ctx_read_ok_total,
            external_ctx_stale_total,
        )
        if stale:
            external_ctx_stale_total.labels(source=source).inc()
        else:
            external_ctx_read_ok_total.labels(source=source).inc()
            if ts_ms > 0:
                external_ctx_age_ms.labels(source=source).set(max(0.0, now_ms - ts_ms))
    except Exception:
        pass


def _dq_compute(
    symbol: str,
    now_ms: int,
    *,
    lag_ms: float,
    last_ts_ms: int,
) -> dict[str, float]:
    """Append DQ sample and compute rolling DQ features over last 60s."""
    is_reorder = 1 if now_ms < last_ts_ms else 0
    is_dedupe = 1 if now_ms == last_ts_ms else 0
    gap_ms = now_ms - last_ts_ms if last_ts_ms > 0 else 0
    is_gap = 1 if gap_ms > _DQ_GAP_THRESHOLD_MS else 0
    # bad_time: future skew > 1s or stale > 5s
    is_bad_time = 1 if (lag_ms < -1_000 or lag_ms > 5_000) else 0

    buf = _DQ_ROLLING_CACHE.get(symbol)
    if buf is None:
        buf = deque(maxlen=_DQ_ROLLING_MAX_LEN)
        _DQ_ROLLING_CACHE[symbol] = buf
    buf.append((now_ms, lag_ms, is_reorder, is_dedupe, is_gap, is_bad_time))

    cutoff = now_ms - _DQ_WINDOW_MS
    window = [s for s in buf if s[0] >= cutoff]
    n = len(window)

    out: dict[str, float] = {
        "tick_lag_p95_1m": 0.0,
        "tick_reorder_rate_1m": 0.0,
        "tick_dedupe_rate_1m": 0.0,
        "tick_gap_count_1m": 0.0,
        "bad_time_streak": 0.0,
    }
    if n == 0:
        return out

    lags = sorted(s[1] for s in window)
    p95_idx = min(len(lags) - 1, int(0.95 * len(lags)))
    out["tick_lag_p95_1m"] = lags[p95_idx]
    out["tick_reorder_rate_1m"] = sum(s[2] for s in window) / n
    out["tick_dedupe_rate_1m"] = sum(s[3] for s in window) / n
    out["tick_gap_count_1m"] = float(sum(s[4] for s in window))

    # bad_time_streak: consecutive bad-time ticks ending now (from tail of buf)
    streak = 0
    for s in reversed(list(buf)):
        if s[5] == 1:
            streak += 1
        else:
            break
    out["bad_time_streak"] = float(streak)

    return out


def _lob_velocity_compute(
    symbol: str,
    now_ms: int,
    *,
    obi: float,
    qimb_wmean: float,
    depth_imbalance_5: float,
    spread_bps: float,
    fill_prob_proxy: float,
) -> dict[str, float]:
    """Append current sample and compute slopes over 1s/3s windows.

    Slopes are simple (last - first) / dt_seconds — robust for short windows
    and noisy LOB data. Returns dict of velocity features keyed by Schema name.
    """
    buf = _LOB_VELOCITY_CACHE.get(symbol)
    if buf is None:
        buf = deque(maxlen=_LOB_VELOCITY_MAX_LEN)
        _LOB_VELOCITY_CACHE[symbol] = buf
    buf.append((now_ms, obi, qimb_wmean, depth_imbalance_5, spread_bps, fill_prob_proxy))

    out: dict[str, float] = {
        "obi_slope_1s": 0.0,
        "obi_slope_3s": 0.0,
        "qimb_slope_1s": 0.0,
        "qimb_slope_3s": 0.0,
        "depth_imbalance_5_delta_1s": 0.0,
        "depth_imbalance_5_delta_3s": 0.0,
        "spread_widen_velocity_bps_s": 0.0,
        "fill_prob_decay_slope": 0.0,
        # Phase 4.4: additional LOB dynamics
        "obi_stability_decay": 1.0,
        "book_churn_delta_1s": 0.0,
        "book_churn_z": 0.0,
        "spread_mean_revert_score": 0.0,
    }

    if len(buf) < 2:
        return out

    for window_ms in _LOB_VELOCITY_WINDOWS_MS:
        cutoff_ms = now_ms - window_ms
        anchor: tuple[int, float, float, float, float, float] | None = None
        for sample in buf:
            if sample[0] >= cutoff_ms:
                anchor = sample
                break
        if anchor is None or anchor[0] == now_ms:
            continue
        dt_s = max(1e-3, (now_ms - anchor[0]) / 1000.0)
        sec = "1s" if window_ms == 1_000 else "3s"
        out[f"obi_slope_{sec}"] = (obi - anchor[1]) / dt_s
        out[f"qimb_slope_{sec}"] = (qimb_wmean - anchor[2]) / dt_s
        out[f"depth_imbalance_5_delta_{sec}"] = depth_imbalance_5 - anchor[3]
        if window_ms == 1_000:
            out["spread_widen_velocity_bps_s"] = max(0.0, (spread_bps - anchor[4]) / dt_s)
            out["fill_prob_decay_slope"] = (fill_prob_proxy - anchor[5]) / dt_s
            # book_churn_delta_1s: total LOB flux = |Δobi| + |Δdepth_imb5| per second
            out["book_churn_delta_1s"] = (abs(obi - anchor[1]) + abs(depth_imbalance_5 - anchor[3])) / dt_s

    # obi_stability_decay: stability of OBI over 3s window (1 = perfectly stable)
    window_obis = [s[1] for s in buf if s[0] >= now_ms - 3_000]
    if len(window_obis) >= 2:
        _obi_mean = sum(window_obis) / len(window_obis)
        _obi_var = sum((x - _obi_mean) ** 2 for x in window_obis) / len(window_obis)
        _obi_std = _obi_var ** 0.5
        out["obi_stability_decay"] = 1.0 / (1.0 + _obi_std)

        # book_churn_z: robust z-score of churn relative to buffer history
        _churn_vals = [
            abs(buf[i][1] - buf[i - 1][1]) + abs(buf[i][3] - buf[i - 1][3])
            for i in range(1, len(buf))
        ]
        if _churn_vals:
            _churn_sorted = sorted(_churn_vals)
            _med = _churn_sorted[len(_churn_sorted) // 2]
            _devs = sorted(abs(c - _med) for c in _churn_vals)
            _mad = _devs[len(_devs) // 2] or 1e-8
            _churn_cur = out["book_churn_delta_1s"]
            out["book_churn_z"] = (_churn_cur - _med) / (1.4826 * _mad)

    # spread_mean_revert_score: (spread_mean - spread_now) / spread_mean ∈ [-1, 1]
    window_spreads = [s[4] for s in buf if s[0] >= now_ms - 3_000]
    if window_spreads:
        _sp_mean = sum(window_spreads) / len(window_spreads)
        if _sp_mean > 1e-8:
            _revert = (_sp_mean - spread_bps) / _sp_mean
            out["spread_mean_revert_score"] = max(-1.0, min(1.0, _revert))

    return out


def _lob_micro_compute(
    symbol: str,
    now_ms: int,
    *,
    microprice_shift_bps: float,
) -> dict[str, float]:
    """Track microprice shift over time; compute velocity and acceleration."""
    buf = _LOB_MICRO_CACHE.get(symbol)
    if buf is None:
        buf = deque(maxlen=_LOB_VELOCITY_MAX_LEN)
        _LOB_MICRO_CACHE[symbol] = buf
    buf.append((now_ms, microprice_shift_bps))

    out: dict[str, float] = {
        "micro_mid_shift_vel_bps_s": 0.0,
        "micro_mid_shift_accel_bps_s2": 0.0,
    }
    if len(buf) < 2:
        return out

    # velocity: slope over 1s window
    cutoff_1s = now_ms - 1_000
    anchor_1s: tuple[int, float] | None = None
    for sample in buf:
        if sample[0] >= cutoff_1s:
            anchor_1s = sample
            break
    if anchor_1s is not None and anchor_1s[0] != now_ms:
        dt_s = max(1e-3, (now_ms - anchor_1s[0]) / 1000.0)
        vel = (microprice_shift_bps - anchor_1s[1]) / dt_s
        out["micro_mid_shift_vel_bps_s"] = vel

        # acceleration: compare current velocity to previous velocity
        cutoff_2s = now_ms - 2_000
        anchor_2s: tuple[int, float] | None = None
        for sample in buf:
            if sample[0] >= cutoff_2s:
                anchor_2s = sample
                break
        if anchor_2s is not None and anchor_2s[0] != anchor_1s[0]:
            dt_prev = max(1e-3, (anchor_1s[0] - anchor_2s[0]) / 1000.0)
            prev_vel = (anchor_1s[1] - anchor_2s[1]) / dt_prev
            out["micro_mid_shift_accel_bps_s2"] = (vel - prev_vel) / dt_s

    return out

from common.metrics_stage import (
    dist,
    feature_missing_total,
    meta_feature_missing_total,
    meta_feature_seen_total,
    veto_total,
)
from common.normalization import generate_signal_id, normalize_direction_safe
from core.absorption_level_score import compute_absorption_level_score
from core.book_evidence import compute_iceberg_flags, compute_obi_flags, compute_ofi_flags
from core.book_microstructure_v2 import compute_ofi_multilevel_topn, compute_queue_imbalance_topn
from core.book_microstructure_v4 import compute_microstructure_v4
from core.burst_gate_v1 import eval_burst_gate
from core.cfg_merge import merged_cfg
from core.fill_prob_proxy import compute_fill_prob_proxy
from core.fp_edge_evidence import compute_fp_edge_absorb
from core.liq_pressure_gate_v1 import eval_liq_pressure_gate
from core.meta_feature_coverage import apply_meta_coverage_guard, compute_meta_feature_coverage
from core.meta_features_v1 import (
    META_FEAT_V1_HASH,
    META_FEAT_V1_NAME,
    META_FEAT_V1_VERSION,
    build_meta_features_v1,
)
from core.meta_features_v2 import (
    META_FEAT_V2_HASH,
    META_FEAT_V2_NAME,
    META_FEAT_V2_VERSION,
    build_meta_features_v2,
)
from core.meta_features_v3 import (
    META_FEAT_V3_HASH,
    META_FEAT_V3_NAME,
    META_FEAT_V3_VERSION,
    build_meta_features_v3,
)
from core.meta_features_v4 import (
    META_FEAT_V4_HASH,
    META_FEAT_V4_NAME,
    META_FEAT_V4_VERSION,
    build_meta_features_v4,
)
from core.meta_features_v5 import (
    META_FEAT_V5_HASH,
    META_FEAT_V5_NAME,
    META_FEAT_V5_VERSION,
    build_meta_features_v5,
)
from core.meta_features_v6 import (
    META_FEAT_V6_HASH,
    META_FEAT_V6_NAME,
    META_FEAT_V6_VERSION,
    build_meta_features_v6,
)
from core.meta_features_v7 import (
    META_FEAT_V7_HASH,
    META_FEAT_V7_NAME,
    META_FEAT_V7_VERSION,
    build_meta_features_v7,
)
from core.meta_features_v8 import (
    META_FEAT_V8_HASH,
    META_FEAT_V8_NAME,
    META_FEAT_V8_VERSION,
    build_meta_features_v8,
)
from core.meta_features_v9 import (
    META_FEAT_V9_HASH,
    META_FEAT_V9_NAME,
    META_FEAT_V9_VERSION,
    build_meta_features_v9,
)
from core.meta_features_v10 import (
    META_FEAT_V10_HASH,
    META_FEAT_V10_NAME,
    META_FEAT_V10_VERSION,
    build_meta_features_v10,
)
from core.meta_features_v13_of import (
    META_FEAT_V13_OF_HASH,
    META_FEAT_V13_OF_NAME,
    META_FEAT_V13_OF_VERSION,
    build_meta_features_v13_of,
)
from core.meta_features_v14_of import (
    META_FEAT_V14_OF_HASH,
    META_FEAT_V14_OF_NAME,
    META_FEAT_V14_OF_VERSION,
    build_meta_features_v14_of,
)
from core.meta_features_v15_of import (
    META_FEAT_V15_OF_HASH,
    META_FEAT_V15_OF_NAME,
    META_FEAT_V15_OF_VERSION,
    build_meta_features_v15_of,
)
from core.meta_model_lr import MetaModelLR
from core.of_confirm_contract import OFConfirmV3
from core.of_evidence import compute_absorption_flags, compute_reclaim_recent, compute_sweep_recent
from core.ofc_bundle_loader_v1 import OFCBundleLoaderV1
from core.ofc_context_key_v1 import iter_ctx_fallback_keys, make_ctx_key
from core.ofc_context_v1 import build_ofc_context
from core.retention import MAXLEN_GLOBAL
from core.scenario_v4 import classify_v4
from core.strong_need_policy import compute_strong_need_same_tick
from core.strong_of_gate import eval_continuation, eval_reversal, hidden_trend_dir
from core.taker_flow_gate_v1 import eval_taker_flow_gate
from core_snapshot.policy_snapshot_v1 import build_dq_policy_snapshot, build_feature_manifest_v1, to_public_dict
from domain.evidence_keys import CtxKeys, HzGateKeys, MetaKeys, MLKeys
from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS

# Optional gates (may live in the full repo). Keep engine importable even in
# partial archives; engine remains functional with graceful degradation.
try:
    from services.cancellation_spike_gate import CancellationSpikeGate  # type: ignore
except Exception:  # pragma: no cover
    CancellationSpikeGate = None  # type: ignore
try:
    from services.ml_confirm import MLConfirmGate  # type: ignore
except Exception:  # pragma: no cover
    MLConfirmGate = None  # type: ignore

try:
    from core.dq_gate_v1 import eval_dq_gate
except Exception:
    eval_dq_gate = None


try:
    from core.liqmap_gate_v1 import evaluate_liqmap_gate_v1
except Exception:
    evaluate_liqmap_gate_v1 = None

# ---- Meta feature schema registry (code-side) ----
# Used by OFConfirmEngine to keep Train==Serve consistency and to guard ENFORCE mode
# against schema mismatch. Hash is enforced only if it exists on both model and code.
META_SCHEMA_REGISTRY: dict[str, tuple[int, str]] = {
    META_FEAT_V1_NAME: (META_FEAT_V1_VERSION, META_FEAT_V1_HASH),
    META_FEAT_V2_NAME: (META_FEAT_V2_VERSION, META_FEAT_V2_HASH),
    META_FEAT_V3_NAME: (META_FEAT_V3_VERSION, META_FEAT_V3_HASH),
    META_FEAT_V4_NAME: (META_FEAT_V4_VERSION, META_FEAT_V4_HASH),
    META_FEAT_V5_NAME: (META_FEAT_V5_VERSION, META_FEAT_V5_HASH),
    META_FEAT_V6_NAME: (META_FEAT_V6_VERSION, META_FEAT_V6_HASH),
    META_FEAT_V7_NAME: (META_FEAT_V7_VERSION, META_FEAT_V7_HASH),
    META_FEAT_V8_NAME: (META_FEAT_V8_VERSION, META_FEAT_V8_HASH),
    META_FEAT_V9_NAME: (META_FEAT_V9_VERSION, META_FEAT_V9_HASH),
    META_FEAT_V10_NAME: (META_FEAT_V10_VERSION, META_FEAT_V10_HASH),
    META_FEAT_V13_OF_NAME: (META_FEAT_V13_OF_VERSION, META_FEAT_V13_OF_HASH),
    META_FEAT_V14_OF_NAME: (META_FEAT_V14_OF_VERSION, META_FEAT_V14_OF_HASH),
    META_FEAT_V15_OF_NAME: (META_FEAT_V15_OF_VERSION, META_FEAT_V15_OF_HASH),
}

META_SCHEMA_V2P = (
    META_FEAT_V2_NAME, META_FEAT_V3_NAME, META_FEAT_V4_NAME, META_FEAT_V5_NAME,
    META_FEAT_V6_NAME, META_FEAT_V7_NAME, META_FEAT_V8_NAME, META_FEAT_V9_NAME,
    META_FEAT_V10_NAME, META_FEAT_V13_OF_NAME, META_FEAT_V14_OF_NAME,
    META_FEAT_V15_OF_NAME,
)

def _get_attr_or_key(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _clamp01(x: float) -> float:
    try:
        if x < 0.0: return 0.0
        if x > 1.0: return 1.0
        return float(x)
    except Exception:
        return 0.0


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _hash01(s: str) -> float:
    """Deterministic hash to [0,1) for canary-share rollout.
    
    Args:
        s: Input string (typically salt:sid)
        
    Returns:
        Float in [0, 1) range
    """
    h = hashlib.sha256(s.encode("utf-8")).digest()
    x = int.from_bytes(h[:8], "big", signed=False)
    return (x % 10_000_000) / 10_000_000.0


def _ab_pick_arm(sid: str, share: float, salt: str) -> str:
    """Deterministic A/B arm picker.

    Args:
        sid: Stable id.
        share: Challenger traffic share in [0,1].
        salt: Salt/namespace.

    Returns:
        'challenger' if sid is in challenger bucket else 'champion'.
    """
    try:
        share = float(share)
    except Exception:
        share = 0.0
    if share <= 0.0:
        return "champion"
    if share >= 1.0:
        return "challenger"
    return "challenger" if (_hash01(f"{salt}:{sid}") < share) else "champion"


@dataclass
class OFConfirm:
    """ Obsolete v2 contract, replaced by OFConfirmV3 """
    version: int
    ts_ms: int
    symbol: str
    tf: str
    direction: str               # LONG/SHORT
    scenario: str                # reversal/continuation/none
    ok: int                      # 1/0
    have: int
    need: int
    score: float                 # 0..1
    evidence: dict[str, Any]
    contrib: dict[str, float]    # score contributions per key

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# Process-level shared caches for MetaModelLR to prevent redundant I/O across engine instances.
_SHARED_META_MODELS: dict[str, Any] = {}
_SHARED_META_STATS: dict[str, tuple[float, int]] = {} # path -> (mtime, size)
_SHARED_CONT_CTX_CAPTURE_CLIENT: Any | None = None
_SHARED_CONT_CTX_CAPTURE_CLIENT_URL: str = ""

import concurrent.futures
_CONT_CTX_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="ofc_cont_xadd")


def _get_sync_redis_client_for_cont_ctx_capture(cfg: dict[str, Any]) -> Any | None:
    """Best-effort cached Redis client for cont_ctx capture.

    This helper is intentionally lazy and fail-open: if redis-py is unavailable or
    the socket cannot be opened quickly, capture is skipped and trading logic is
    unaffected.
    """
    global _SHARED_CONT_CTX_CAPTURE_CLIENT, _SHARED_CONT_CTX_CAPTURE_CLIENT_URL
    try:
        url = str(
            cfg.get("redis_url")
            or os.getenv("REDIS_URL")
            or "redis://redis-worker-1:6379/0"
        ).strip()
        if not url:
            return None
        if _SHARED_CONT_CTX_CAPTURE_CLIENT is not None and url == _SHARED_CONT_CTX_CAPTURE_CLIENT_URL:
            return _SHARED_CONT_CTX_CAPTURE_CLIENT
        import redis  # type: ignore
        _SHARED_CONT_CTX_CAPTURE_CLIENT = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_timeout=float(os.getenv("CONT_CTX_CALIB_CAPTURE_SOCKET_TIMEOUT_SEC", "0.05") or 0.05),
            socket_connect_timeout=float(os.getenv("CONT_CTX_CALIB_CAPTURE_CONNECT_TIMEOUT_SEC", "0.05") or 0.05),
            retry_on_timeout=False,
        )
        _SHARED_CONT_CTX_CAPTURE_CLIENT_URL = url
        return _SHARED_CONT_CTX_CAPTURE_CLIENT
    except Exception:
        return None


def _emit_cont_ctx_calib_capture_v1(
    *,
    runtime: Any,
    indicators: dict[str, Any],
    cfg2: dict[str, Any],
    ofc: Any,
    dec: Any,
    now_ts_ms: int,
) -> None:
    """Emit a narrow continuation-capture payload for post-analysis calibrator.

    The payload is intentionally scalar-only so it can be consumed cheaply by
    Redis Stream workers without additional JSON decoding. Fail-open by design.
    """
    try:
        enabled = int(cfg2.get("cont_ctx_calib_capture", os.getenv("CONT_CTX_CALIB_CAPTURE_ENABLE", "0")) or 0)
        if enabled != 1:
            return

        scenario_base = str(indicators.get("scenario_base") or getattr(dec, "scenario", getattr(ofc, "scenario", "")) or "")
        if scenario_base != "continuation":
            return

        signal_ts_ms = int(getattr(ofc, "ts_ms", now_ts_ms) or now_ts_ms or 0)
        cont_ctx_ts_ms = int(indicators.get("cont_ctx_ts_ms", 0) or 0)
        cont_ctx_age_ms = int(indicators.get("cont_ctx_age_ms", 0) or 0)
        if cont_ctx_age_ms <= 0 and signal_ts_ms > 0 and cont_ctx_ts_ms > 0:
            cont_ctx_age_ms = max(0, signal_ts_ms - cont_ctx_ts_ms)

        symbol = str(getattr(ofc, "symbol", "") or indicators.get("symbol") or getattr(runtime, "symbol", "") or "")
        direction = str(getattr(ofc, "direction", "") or indicators.get("direction") or "")
        direction_norm = normalize_direction_safe(direction)
        if direction_norm is None:
            return
        signal_id = generate_signal_id(
            kind="ofc_cont",
            symbol=symbol,
            ts_ms=signal_ts_ms,
            direction=direction_norm
        )

        payload = {
            "schema": "1",
            "event": "ofc_cont_ctx_capture",
            "signal_id": signal_id,
            "symbol": symbol,
            "ts_ms": str(signal_ts_ms),
            "tf": (indicators.get("tf") or ""),
            "direction": direction,
            "scenario": scenario_base,
            "scenario_v4": str(indicators.get("scenario_v4", scenario_base) or scenario_base),
            "ok": str(int(getattr(ofc, "ok", 0) or 0)),
            "ok_soft": str(int(indicators.get("ok_soft", 0) or 0)),
            "have": str(int(getattr(ofc, "have", 0) or 0)),
            "need": str(int(getattr(ofc, "need", 0) or 0)),
            "score": str(float(getattr(ofc, "score", 0.0) or 0.0)),
            "reason": str(getattr(ofc, "reason", "") or ""),
            "strong_gate_missing": strong_gate_missing,  # type: ignore
            "trend_dir_source": (indicators.get("trend_dir_source", "") or ""),  # type: ignore
            "cont_ctx_ts_ms": str(cont_ctx_ts_ms),
            "cont_ctx_age_ms": str(cont_ctx_age_ms),
            "hidden_ctx_recent": str(int(indicators.get("hidden_ctx_recent", 0) or 0)),
            "obi_stable": str(int(indicators.get("obi_stable", 0) or 0)),
            "cont_ctx_recent": str(int(indicators.get("cont_ctx_recent", 0) or 0)),
            "iceberg_strict": str(int(indicators.get("iceberg_strict", 0) or 0)),
            "ofi_stable": str(int(indicators.get("ofi_stable", 0) or 0)),
            "fp_edge_absorb": str(int(indicators.get("fp_edge_absorb", 0) or 0)),
            "exec_risk_norm": str(float(indicators.get("exec_risk_norm", 999.0) or 999.0)),
            "exec_risk_bps": str(float(indicators.get("exec_risk_bps", 0.0) or 0.0)),
            "of_score_final": str(float(indicators.get("of_score_final", getattr(ofc, "score", 0.0)) or 0.0)),
            "of_score_final_raw": str(float(indicators.get("of_score_final_raw", 0.0) or 0.0)),
            "dq_veto": str(int(indicators.get("dq_veto", 0) or 0)),
            "book_health_ok": str(int(indicators.get("book_health_ok", 1) or 1)),
            "hidden_ctx_warmup_bypass": str(int(indicators.get("hidden_ctx_warmup_bypass", 0) or 0)),
            "cont_ctx_warmup_bypass": str(int(indicators.get("cont_ctx_warmup_bypass", 0) or 0)),
            "obi_stable_warmup_bypass": str(int(indicators.get("obi_stable_warmup_bypass", 0) or 0)),
            "paper_only": "1",
        }
        try:
            runtime_snap = OFConfirmEngine.export_runtime_snapshot(runtime, indicators)
        except Exception:
            runtime_snap = None
        if isinstance(runtime_snap, dict):
            payload["runtime_snapshot"] = json.dumps(runtime_snap, separators=(",", ":"), ensure_ascii=False)

        client = _get_sync_redis_client_for_cont_ctx_capture(cfg2)
        if client is None:
            return
        stream = str(
            cfg2.get("cont_ctx_calib_capture_stream")
            or os.getenv("CONT_CTX_CALIB_CAPTURE_STREAM")
            or RS.OFC_CONT_CTX_CAPTURE
        ).strip()
        maxlen = int(cfg2.get("cont_ctx_calib_capture_maxlen", os.getenv("CONT_CTX_CALIB_CAPTURE_MAXLEN", str(MAXLEN_GLOBAL))) or MAXLEN_GLOBAL)

        from services.observability.metrics_registry import ml_telemetry_io_time_us
        
        def _do_xadd():
            try:
                t0_xadd = time.perf_counter()
                client.xadd(stream, payload, maxlen=maxlen, approximate=True)
                dt_xadd = (time.perf_counter() - t0_xadd) * 1_000_000
                ml_telemetry_io_time_us.labels(symbol=symbol, op="xadd_cont_ctx").observe(dt_xadd)  # type: ignore
            except Exception:  # type: ignore
                pass
                
        _CONT_CTX_EXECUTOR.submit(_do_xadd)
    except Exception:
        return


class OFConfirmEngine:
    """
    Replay determinism support:
      - set_replay_time_ms(ts): freezes engine "now" and disables time-based reloads
      - _now_ms(): uses frozen time in replay
    """
    # Use a high bit to avoid clashing with existing gate bits.
    GATE_BIT_CANCEL_SPIKE = 1 << 28
    GATE_BIT_META_VETO = 1 << 27
    GATE_BIT_TAKER_FLOW = 1 << 26
    GATE_BIT_LIQMAP = 1 << 25
    GATE_BIT_NEWS = 1 << 24
    GATE_BIT_SMT = 1 << 23  # G6: SMT coherence gate had state and evaluated

    # --- Golden replay: runtime snapshot (minimal set of fields engine reads) ---
    RUNTIME_SNAPSHOT_VERSION: int = 1
    _SNAP_LAST_SWEEP_FIELDS = ('ts_ms', 'kind', 'direction_bias')
    _SNAP_LAST_RECLAIM_FIELDS = ('ts_ms', 'hold_bars', 'direction_bias', 'level', 'pool_id')
    _SNAP_LAST_DIV_FIELDS = ('ts_ms', 'kind')
    _SNAP_LAST_WP_FIELDS = ('ts_ms', 'weak_any')
    _SNAP_LAST_FP_EDGE_FIELDS = (
        'ts_ms',
        'bias',
        'strength',
        'p90',
        'value',
        'range_expansion',
        'move_bp',
        'poc_edge',
        'absorb_ok',
    )

    # Book-derived event snapshots (kept minimal + JSON-safe)
    _SNAP_LAST_OBI_EVENT_FIELDS = ('ts_ms', 'direction', 'obi', 'stable_secs', 'obi_z', 'stacking', 'concentration')
    _SNAP_LAST_ICEBERG_EVENT_FIELDS = ('ts_ms', 'side', 'refresh', 'duration', 'price')
    _SNAP_LAST_OFI_EVENT_FIELDS = ('ts_ms', 'direction', 'ofi', 'ofi_z', 'stable_secs', 'stability_score', 'stable')
    _SNAP_LAST_BAR_FIELDS = (
        'id', 'ts_ms',
        'fp_enabled',
        'fp_absorption_bias', 'fp_ladder_low_len', 'fp_ladder_high_len', 'fp_poc_on_edge',
        'fp_eff_quote', 'fp_eff_delta', 'fp_quote_delta',
        'fp_move_bp',
    )

    def __init__(self, version: int = 3, cancel_gate: Any | None = None, ml_gate: Any | None = None) -> None:
        self.version = int(version)
        # Cancellation spike gate is intentionally always available so we can snapshot/restore
        # it for golden replay. The internal state is per-symbol.
        if cancel_gate is not None:
            self._cancel_spike_gate = cancel_gate
        elif CancellationSpikeGate is not None:
            self._cancel_spike_gate = CancellationSpikeGate()
        else:
            self._cancel_spike_gate = None
        # ML gate: lazy init in build() if None (OFF/SHADOW/ENFORCE controlled by env)
        self._ml_gate = ml_gate  # lazy init in build() if None
        self._meta_model = None  # lazy-loaded MetaModelLR
        self._meta_model_path = ""
        self._meta_model_mtime = 0.0
        self._meta_model_last_check_ms = 0
        # Optional challenger meta-model (A/B)
        self._meta_model_ch = None  # lazy-loaded MetaModelLR
        self._meta_model_ch_path = ""
        self._meta_model_ch_mtime = 0.0
        self._meta_model_ch_last_check_ms = 0
        # Replay determinism support
        self._replay_mode: bool = False
        self._replay_now_ms: int | None = None
        # Startup timestamp (ms) - used by _should_apply_dq_veto for warmup checks.
        self._start_ms: int = get_ny_time_millis()
        self._ofc_ctx_loader = None
        self._ofc_ctx_bundle = None
        self._ofc_ctx_last_check_ms = 0

        # Sync redis clients for Phase 7.8/7.9/8.1 context reads.
        # Split across two instances by writer:
        #   - worker-1 (REDIS_URL): ctx:deriv:* (Python deriv-ctx-collector), ctx:anchor:*,
        #                           ctx:pit:*, ctx:tca:* (cross-context aggregator).
        #   - main `redis` (CTX_MAIN_REDIS_URL): runtime:breadth, ctx:deribit:global,
        #                           ctx:sentiment:global (Go-worker marketdata schedulers).
        # Without these clients all populate blocks skip → fail-open 0.0 → train/serve skew.
        # Fail-open: any init error → None, populate keeps skipping.
        self._redis_client = None
        self._redis_client_main = None
        try:
            import os as _os
            import redis as _redis
            _url = _os.environ.get("REDIS_URL", "redis://redis-worker-1:6379/0")
            self._redis_client = _redis.from_url(_url, socket_timeout=2.0, socket_connect_timeout=2.0)
            _main_url = _os.environ.get("CTX_MAIN_REDIS_URL", "redis://redis:6379/0")
            self._redis_client_main = _redis.from_url(_main_url, socket_timeout=2.0, socket_connect_timeout=2.0)
        except Exception:
            self._redis_client = None
            self._redis_client_main = None

    @property
    def ml_gate(self) -> Any | None:
        return self._ml_gate

    def set_replay_time_ms(self, ts_ms: int) -> None:
        """
        Enable deterministic replay mode.
        - freezes internal now_ms
        - prevents any time-based reload behavior from introducing nondeterminism
        """
        try:
            t = int(ts_ms)
        except Exception:
            t = 0
        self._replay_mode = True
        self._replay_now_ms = t if t > 0 else 0

    def clear_replay_time(self) -> None:
        """Disable replay mode, return to wall-clock time."""
        self._replay_mode = False
        self._replay_now_ms = None

    def _now_ms(self) -> int:
        """
        Deterministic clock.
        In replay: returns frozen ts (if set), else 0 (explicit).
        In prod: wall clock ms.
        """
        if self._replay_mode:
            return int(self._replay_now_ms or 0)
        return get_ny_time_millis()

    # ------------------------------------------------------------------
    # OFC contextual bundle state
    # ------------------------------------------------------------------
    def _ensure_ofc_ctx_bundle(self, cfg: dict[str, Any]) -> None:
        try:
            enabled = bool(cfg.get("ofc_ctx_enable", False))
            path = (cfg.get("ofc_ctx_bundle_path", "") or "")
            reload_sec = int(cfg.get("ofc_ctx_reload_sec", 30) or 30)
            if not enabled or not path:
                self._ofc_ctx_loader = None
                self._ofc_ctx_bundle = None
                return
            if self._ofc_ctx_loader is None:
                self._ofc_ctx_loader = OFCBundleLoaderV1(path, reload_sec=reload_sec)
            self._ofc_ctx_loader.maybe_reload()
            self._ofc_ctx_bundle = self._ofc_ctx_loader.get()
        except Exception:
            self._ofc_ctx_bundle = None

    def _build_ofc_ctx_features(
        self,
        *,
        indicators: dict[str, Any],
        score: float,
        score_raw: float,
        exec_risk_bps: float,
        exec_risk_norm: float,
        exec_ref: float,
        spread_bps: float,
        slip_bps: float,
        score_min: float,
        now_ts: int,
    ) -> dict[str, float]:
        dt_h = int((now_ts // 1000) // 3600 % 24)
        dt_d = int((now_ts // 1000) // 86400 + 3) % 7  # stable UTC weekday proxy
        h_ang = (2.0 * math.pi * float(dt_h)) / 24.0
        d_ang = (2.0 * math.pi * float(dt_d)) / 7.0
        out: dict[str, float] = {
            "raw_score": float(score),
            "score_raw": float(score_raw),
            "of_score_final": float(score),
            "of_score_final_raw": float(score_raw),
            "legacy_of_score_min": float(score_min),
            "exec_risk_bps": float(exec_risk_bps),
            "exec_risk_norm": float(exec_risk_norm),
            "exec_risk_ref_bps": float(exec_ref),
            "spread_bps": float(spread_bps),
            "expected_slippage_bps": float(slip_bps),
            "slip_spread_bps": float(indicators.get("slip_spread_bps", 0.0) or 0.0),
            "slip_impact_bps": float(indicators.get("slip_impact_bps", 0.0) or 0.0),
            "fill_prob_proxy": float(indicators.get("fill_prob_proxy", 0.0) or 0.0),
            "eta_fill_sec": float(indicators.get("eta_fill_sec", 0.0) or 0.0),
            "delta_z": float(indicators.get("delta_z", 0.0) or 0.0),
            "ofi_z": float(indicators.get("ofi_z", 0.0) or 0.0),
            "ofi_stability_score": float(indicators.get("ofi_stability_score", 0.0) or 0.0),
            "obi": float(indicators.get("obi", 0.0) or 0.0),
            "obi_z": float(indicators.get("obi_z", 0.0) or 0.0),
            "book_staleness_ms": float(indicators.get("book_staleness_ms", 0.0) or 0.0),
            "dq_health_score": float(indicators.get("dq_health_score", indicators.get("data_health", 1.0)) or 1.0),
            "dq_level": float(indicators.get("dq_level", 0.0) or 0.0),
            "liqmap_gate_risk_bps": float(indicators.get("liqmap_gate_risk_bps", 0.0) or 0.0),
            "liqmap_gate_reward_bps": float(indicators.get("liqmap_gate_reward_bps", 0.0) or 0.0),
            "liqmap_gate_rr": float(indicators.get("liqmap_gate_rr", 0.0) or 0.0),
            "hour_utc": float(dt_h),
            "dow": float(dt_d),
            "hour_sin": float(math.sin(h_ang)),
            "hour_cos": float(math.cos(h_ang)),
            "dow_sin": float(math.sin(d_ang)),
            "dow_cos": float(math.cos(d_ang)),
        }
        return out

    # ------------------------------------------------------------------
    # Cancellation gate state (for deterministic golden replay)
    # ------------------------------------------------------------------
    def snapshot_cancel_gate_state(self, symbol: str) -> dict[str, Any] | None:
        """Return serializable state for CancellationSpikeGate (per-symbol) or None."""
        try:
            gate = getattr(self, "_cancel_spike_gate", None)
            if gate is None:
                return None
            fn = getattr(gate, "snapshot_state", None)
            if fn is None:
                # Fallback to snapshot if snapshot_state doesn't exist
                fn = getattr(gate, "snapshot", None)
                if fn is None:
                    return None
                # snapshot returns full format, extract per-symbol
                full = fn(symbol)
                if isinstance(full, dict) and "symbols" in full:
                    return full["symbols"].get(symbol, None)
                return full
            return fn(symbol)
        except Exception:
            return None

    def restore_cancel_gate_state(self, symbol: str, state: dict[str, Any] | None) -> bool:  # type: ignore
        """Restore CancellationSpikeGate state for a symbol. Returns True if applied."""
        if not state:
            return False
        try:
            gate = getattr(self, "_cancel_spike_gate", None)
            if gate is None:
                # lazy init to keep call safe in replay tool
                self._cancel_spike_gate = CancellationSpikeGate()  # type: ignore
                gate = self._cancel_spike_gate  # type: ignore
            fn = getattr(gate, "restore_state", None)
            if fn is None:
                # Fallback to restore if restore_state doesn't exist
                fn = getattr(gate, "restore", None)
                if fn is None:
                    return False
                fn(state, symbol=symbol)
                return True
            fn(symbol, dict(state))
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Gate state snapshot API (for deterministic golden replay)
    # ------------------------------------------------------------------

    def cancel_gate_snapshot(self, symbol: str | None = None) -> dict[str, Any]:
        """Serialize CancellationSpikeGate state.

        If symbol is provided, returns the per-symbol payload.
        Otherwise returns the full container snapshot.
        """
        try:
            return self._cancel_spike_gate.snapshot(symbol)  # type: ignore
        except Exception:  # type: ignore
            return {"version": 1, "symbols": {}}

    def cancel_gate_restore(self, snap: dict[str, Any], symbol: str | None = None) -> None:
        """Restore CancellationSpikeGate state."""
        try:
            self._cancel_spike_gate.restore(snap, symbol=symbol)  # type: ignore
        except Exception:  # type: ignore
            return

    def cancel_gate_reset(self, symbol: str | None = None) -> None:
        """Clear CancellationSpikeGate state (per symbol or all)."""
        try:
            self._cancel_spike_gate.reset(symbol=symbol)  # type: ignore
        except Exception:  # type: ignore
            return

    def _should_apply_dq_veto(self, cfg: dict[str, Any]) -> bool:
        """Observe-only rollout guard for book_missing_seq_hard DQ veto.

        During the initial 24–48h observe period, the hard DQ veto based on
        book_missing_seq_ema should be OFF even if DQ_GATE_MODE=enforce.
        This lets us collect baseline statistics before applying production
        veto logic.

        Returns True only when it is safe to apply the veto:
          1. dq_book_veto_enabled must be truthy (non-zero).
          2. If dq_book_veto_warmup_s > 0, the engine uptime must exceed it.

        Fail-open policy: returns False on any exception so that the veto is
        never applied due to a bug in this guard.
        """
        try:
            enabled = int(cfg.get("dq_book_veto_enabled", 0) or 0)
            if not enabled:
                return False  # observe-only: never veto

            warmup_s = int(cfg.get("dq_book_veto_warmup_s", 0) or 0)
            if warmup_s <= 0:
                return True  # no warmup required; apply immediately

            elapsed_s = max(0, (get_ny_time_millis() - int(self._start_ms or 0)) // 1000)
            return elapsed_s >= warmup_s
        except Exception:
            return False  # fail-open: never veto if guard errors

    def export_gate_state(self, *, symbol: str | None = None) -> dict[str, Any]:
        """Export internal state of stateful gates (fail-open).

        Used by OFC_CAPTURE to guarantee deterministic offline replay.

        """
        try:
            out: dict[str, Any] = {"version": 1, "gates": {}}

            g = getattr(self, "_cancel_spike_gate", None)

            if g is not None and hasattr(g, "export_state"):
                out["gates"]["cancel_spike"] = g.export_state(symbol=symbol)

            return out

        except Exception:
            return {"version": 1, "gates": {}}

    def import_gate_state(self, state: dict[str, Any], *, replace: bool = False) -> None:
        """Restore stateful gate state from export_gate_state() (fail-open)."""
        try:
            if not isinstance(state, dict):
                return

            gates = state.get("gates", {}) or {}

            if not isinstance(gates, dict):
                return

            g = getattr(self, "_cancel_spike_gate", None)

            cs = gates.get("cancel_spike", None)

            if g is not None and cs is not None and hasattr(g, "import_state"):
                g.import_state(cs, replace=replace)

        except Exception:
            return

    # Backward/targeted wrappers for Cancel Spike gate
    def export_cancel_spike_state(self, *, symbol: str | None = None) -> dict[str, Any] | None:
        try:
            g = getattr(self, "_cancel_spike_gate", None)

            if g is None or not hasattr(g, "export_state"):
                return None

            return g.export_state(symbol=symbol)

        except Exception:
            return None

    def import_cancel_spike_state(self, state: dict[str, Any], *, replace: bool = False) -> None:
        try:
            g = getattr(self, "_cancel_spike_gate", None)

            if g is None or not hasattr(g, "import_state"):
                return

            g.import_state(state, replace=replace)

        except Exception:
            return

    @staticmethod
    def _i(x: Any, d: int = 0) -> int:
        """Helper: safe int conversion."""
        try:
            if x is None:
                return d
            return int(float(x))
        except Exception:
            return d

    def _resolve_now_ts(self, tick_ts_ms: int, indicators: dict[str, Any]) -> int:
        """
        Canonical time source for build().
        Priority:
          1) tick_ts_ms (if >0)
          2) indicators['now_ts_ms'] (if >0)
          3) deterministic _now_ms() (prod: wall clock, replay: frozen)
        """
        if tick_ts_ms > 0:
            return tick_ts_ms
        v = self._i(indicators.get("now_ts_ms", 0), 0)
        if v > 0:
            return int(v)
        return int(self._now_ms())

    def _load_meta_model_slot(self, slot: str, path: str, now_ms: int, reload_sec: int) -> Any | None:
        """
        Fail-open loader with coarse reload interval and process-level caching.
        NOTE: in replay mode we must not refresh by wall-clock timers.
        """
        try:
            slot = (slot or "champion").lower()
            path = (path or "").strip()
            if not path:
                return None

            # In replay mode: freeze meta model (load once outside, or keep current)
            if getattr(self, "_replay_mode", False):
                return getattr(self, "_meta_model" if slot == "champion" else "_meta_model_ch", None)

            last_check_attr = "_meta_model_last_check_ms" if slot == "champion" else "_meta_model_ch_last_check_ms"
            model_attr = "_meta_model" if slot == "champion" else "_meta_model_ch"
            path_attr = "_meta_model_path" if slot == "champion" else "_meta_model_ch_path"
            mtime_attr = "_meta_model_mtime" if slot == "champion" else "_meta_model_ch_mtime"

            # 1. Coarse timer check (local to this engine instance)
            if (now_ms - int(getattr(self, last_check_attr, 0) or 0)) < int(reload_sec * 1000):
                return getattr(self, model_attr, None)
            setattr(self, last_check_attr, int(now_ms))

            # 2. Process-level shared cache check
            try:
                mtime = os.path.getmtime(path)
                size = os.path.getsize(path)
                stats = (mtime, size)
            except Exception:
                # If file not accessible, return current instance's model (fail-open)
                return getattr(self, model_attr, None)

            if path in _SHARED_META_MODELS and _SHARED_META_STATS.get(path) == stats:
                mm = _SHARED_META_MODELS[path]
                setattr(self, model_attr, mm)
                setattr(self, path_attr, path)
                setattr(self, mtime_attr, float(mtime))
                return mm

            # 3. Reload needed
            mm = None
            try:
                from core.meta_model_lr import MetaModelLR
                mm = MetaModelLR.load(path)
            except Exception:
                # MetaModelLR is JSON-first; tolerate joblib/pickle artifacts by falling back.
                try:
                    import joblib  # type: ignore
                    obj = joblib.load(path)
                    if isinstance(obj, MetaModelLR):
                        mm = obj
                    elif isinstance(obj, dict) and "features" in obj and "coef" in obj:
                        # best-effort conversion (robust_scaler/transforms are optional)
                        robust_scaler = None
                        try:
                            rs = obj.get("robust_scaler")
                            if isinstance(rs, dict):
                                from core.feature_engineering import RobustScalerPack, RobustScalerParams  # type: ignore
                                params = {}  # type: ignore
                                for k, v in (rs.get("params", {}) or {}).items():
                                    if not isinstance(v, dict):
                                        continue
                                    params[str(k)] = RobustScalerParams(
                                        center=float(v.get("center", 0.0) or 0.0),
                                        scale=float(v.get("scale", 1.0) or 1.0),
                                    )
                                robust_scaler = RobustScalerPack(params=params)
                        except Exception:
                            robust_scaler = None
                        mm = MetaModelLR(
                            features=list(obj.get("features", [])),
                            intercept=float(obj.get("intercept", 0.0)),
                            coef=[float(x) for x in (obj.get("coef", []) or [])],
                            threshold=float(obj.get("threshold", 0.5)),
                            transforms=obj.get("transforms", {}) if isinstance(obj.get("transforms", {}), dict) else {},
                            robust_scaler=robust_scaler,
                        )
                except Exception:
                    mm = None

            if mm:
                _SHARED_META_MODELS[path] = mm
                _SHARED_META_STATS[path] = stats
                setattr(self, model_attr, mm)
                setattr(self, path_attr, path)
                setattr(self, mtime_attr, float(mtime))

            return getattr(self, model_attr, None)
        except Exception:
            return None

    def _load_meta_model(self, path: str, now_ms: int, reload_sec: int) -> MetaModelLR | None:
        """Backward-compatible champion loader."""
        return self._load_meta_model_slot("champion", path, now_ms, reload_sec)

    def build(  # type: ignore
        self,  # type: ignore
        *,
        symbol: str,
        tf: str,
        direction: str,
        tick_ts_ms: int,
        price: float,
        delta_z: float,
        snap_t0: Any | None = None,
        snap_prev: Any | None = None,
        runtime: Any,
        cfg: dict[str, Any],
        indicators: dict[str, Any],
        absorption: dict[str, Any] | None = None,
        worker_lag_ms: float = 0.0,
    ) -> tuple[OFConfirmV3 | None, Any | None]:
        """
        Returns:
          (of_confirm, gate_decision)

        Centralizes evidence computation, scenario evaluation, and continuous scoring.
        """
        from services.orderflow.metrics import ofconfirm_build_stages_us
        _t_start = time.perf_counter()

        def _snap_stage(stage_name, t_prev):
            t_now = time.perf_counter()
            dt_us = int((t_now - t_prev) * 1_000_000.0)
            with contextlib.suppress(Exception):
                ofconfirm_build_stages_us.labels(symbol=symbol, stage=stage_name).observe(dt_us)
            return t_now

        _t_stage = _t_start

        # --- B1: Check Load-Shedding status ---
        LAG_THRESHOLD_SHED = float(os.getenv("OF_LAG_THRESHOLD_SHED", "50.0") or 50.0)
        # OF_LOAD_SHEDDING_DISABLE=1 → bypass shedding (use when false-positive vetoes are confirmed).
        # Default: enabled. Shedding skips ML inference (fail-open SHADOW) when lag ≥ threshold,
        # preventing event-loop stall from executor queue saturation.
        _shedding_disabled = bool(int(os.getenv("OF_LOAD_SHEDDING_DISABLE", "0") or 0))
        is_shedding = False if _shedding_disabled else bool(worker_lag_ms >= LAG_THRESHOLD_SHED)

        # Deterministic time source (replay-safe)
        now_ts = self._resolve_now_ts(tick_ts_ms, indicators)
        evidence = {}

        # --- Book evidence (OBI/Iceberg) ---
        obi_dir_ok, obi_stable, obi_stable_secs, obi_val = compute_obi_flags(
            direction=direction,
            now_ts_ms=now_ts,
            last_event=getattr(runtime, "last_obi_event", None),
            cfg=cfg,
            indicators=indicators,
        )
        iceberg_dir_ok, iceberg_strict, iceberg_refresh, iceberg_duration = compute_iceberg_flags(
            direction=direction,
            price=price,
            now_ts_ms=now_ts,
            last_event=getattr(runtime, "last_iceberg_event", None),
            cfg=cfg,
            indicators=indicators,
        )

        # --- OFI evidence (first-class) ---
        # C1: OFI becomes an alternative microstructure leg for Have/Need by safely substituting OBI stable.
        # IMPORTANT: OFI is treated as "book/time-dependent" evidence -> it will be vetoed when book_ok=0.
        ofi_dir_ok, ofi_stable, ofi_stable_secs, ofi_val, ofi_z, ofi_stability_score = compute_ofi_flags(
            direction=direction,
            now_ts_ms=now_ts,
            last_event=getattr(runtime, "last_ofi_event", None),
            cfg=cfg,
            indicators=indicators,
        )

        # Optional: stamp now_ts used (useful for replay/debug)
        with contextlib.suppress(Exception):
            indicators["now_ts_ms_used"] = now_ts

        # --- Book health gate for book-based evidences (OBI/Iceberg/OFI) ---
        book_ok = _i(indicators.get("book_health_ok", 1), 1)

        # --- Data health gate (stricter than book_ok) ---
        # If overall data_health is low, we fail-closed ONLY for evidences that depend on book/time.
        # Note: book_evidence_allowed (written by apply_book_evidence_policy) is an ML feature only;
        # this gate re-evaluates independently via data_health + book_health_ok → data_health_veto_book_evidence.
        try:
            dh = float(indicators.get("data_health", 1.0) or 1.0)
        except Exception:
            dh = 1.0
        dh_min = float(cfg.get("data_health_min_for_book_evidence", 0.70))
        if dh < dh_min:
            book_ok = 0
            indicators["data_health_veto_book_evidence"] = 1

        if book_ok == 0:
            # Do not allow these evidences to contribute to StrongGate B/C components
            obi_dir_ok, obi_stable, obi_stable_secs, obi_val = False, False, 0.0, 0.0
            iceberg_dir_ok, iceberg_strict, iceberg_refresh, iceberg_duration = False, False, 0, 0.0
            ofi_dir_ok, ofi_stable, ofi_stable_secs, ofi_val, ofi_z, ofi_stability_score = False, False, 0.0, 0.0, 0.0, 0.0
            indicators["book_health_veto_book_evidence"] = 1
            # keep indicators consistent (downstream explainability)
            indicators["ofi_dir_ok"] = 0
            indicators["ofi_stable"] = 0
            indicators["ofi"] = 0.0
            indicators["ofi_z"] = 0.0
            indicators["ofi_stable_secs"] = 0.0
            indicators["ofi_stability_score"] = 0.0
            indicators["ofi_age_ms"] = -1

        # --- Sweep/Reclaim evidence (staleness-gated) ---
        sweep_recent = compute_sweep_recent(
            now_ts_ms=now_ts,
            last_sweep=_get_attr_or_key(runtime, 'last_sweep', None),
            cfg=cfg,
            indicators=indicators,
        )
        reclaim_recent, reclaim_hold_bars = compute_reclaim_recent(
            direction=direction,
            now_ts_ms=now_ts,
            last_reclaim=_get_attr_or_key(runtime, 'last_reclaim', None),
            cfg=cfg,
            indicators=indicators,
        )

        # --- Confirmation-derived feature flags (Stage 4, partial) ---
        # These flags are written into `indicators` so they can be exported as ML features
        # via OFInputsV2 (train==serve).
        try:
            rp = float(indicators.get("rsi_price", 50.0) or 50.0)
            rc = float(indicators.get("rsi_cvd", 50.0) or 50.0)
            rsi_ok = 1 if ((direction == "LONG" and rp > 50 and rc > 50) or
                           (direction == "SHORT" and rp < 50 and rc < 50)) else 0
        except Exception:
            rsi_ok = 0
        indicators["rsi_agree"] = int(rsi_ok)

        kind = (indicators.get("sweep_kind", "") or "")
        indicators["sweep_eqh"] = int(1 if (sweep_recent and kind == "EQH_SWEEP") else 0)
        indicators["sweep_eql"] = int(1 if (sweep_recent and kind == "EQL_SWEEP") else 0)

        div_ok = 0
        div_fallback = 0
        div_source = "none"
        try:
            cvd_q = int(indicators.get("cvd_quarantine_active", 0) or 0)
            dbias = (indicators.get("sweep_dir_bias", "") or "").upper()
            div = _get_attr_or_key(runtime, "last_div", None)

            if cvd_q != 1:
                # Primary path: use multi-bar divergence object
                if sweep_recent and div is not None:
                    dkind = str(_get_attr_or_key(div, "kind", "") or "").lower()
                    if dbias == "SHORT" and dkind.startswith("bearish") or dbias == "LONG" and dkind.startswith("bullish"):
                        div_ok = 1
                        div_source = "divergence_object"
            else:
                # Fallback path: use snapshot delta_tick during CVD baseline quarantine
                delta_val = float(indicators.get("delta_tick", indicators.get("delta", 0.0) or 0.0) or 0.0)

                # P1-10: Strict time scoping. Delta is an indicator, must not be newer than signal!
                evidence_ts = int(indicators.get("ts_ms", indicators.get("event_ts", 0)) or 0)
                signal_ts = now_ts  # now_ts is the tick_ts_ms for the current signal
                time_ok = True
                if evidence_ts > 0 and signal_ts > 0 and evidence_ts > signal_ts:
                    time_ok = False

                if sweep_recent and time_ok:
                    if dbias == "SHORT" and delta_val < 0.0 or dbias == "LONG" and delta_val > 0.0:
                        div_fallback = 1
                        div_source = "delta_tick_fallback"
        except Exception:
            div_ok = 0
            div_fallback = 0
            div_source = "error"

        indicators["div_match"] = int(div_ok)
        indicators["div_match_fallback"] = int(div_fallback)
        indicators["div_match_source"] = str(div_source)

        # --- Absorption ---
        abs_ok, abs_vol = compute_absorption_flags(
            direction=direction,
            absorption=absorption,
            cfg=cfg,
            indicators=indicators,
        )

        # --- Weak progress (computed on bar_close) ---
        wp = _get_attr_or_key(runtime, 'last_wp', None)
        wp_any = bool(_get_attr_or_key(wp, 'weak_any', False))
        indicators["weak_progress"] = 1 if wp_any else 0

        # --- FP edge absorb (A2) ---
        # Use runtime.last_fp_edge (produced by footprint edge detector on microbars).
        # This evidence is useful to confirm absorption at edge without range expansion (anti-fake-impulse).
        # Capture optional manual override before compute function overwrites it
        manual_fp_edge = int(indicators.get("fp_edge_absorb", 0))

        fp_edge_ok, fp_edge_strength, fp_edge_rng, fp_edge_bias = compute_fp_edge_absorb(
            direction=direction,
            now_ts_ms=now_ts,
            last_edge=_get_attr_or_key(runtime, 'last_fp_edge', None),
            cfg=cfg,
            indicators=indicators,
        )
        if manual_fp_edge:
            fp_edge_ok = True
            indicators["fp_edge_absorb"] = 1

        _t_stage = _snap_stage("evidence", _t_stage)

        # --- Scenario selection ---
        scenario = "reversal" if sweep_recent else "continuation"
        dec = None
        fallback_reason = "unknown"

        # Continuation needs a trend direction (from hidden divergence kind if available)
        trend_dir = None
        if scenario == "continuation":
            # Best practice: if CVD is quarantined, ignore hidden divergence (avoid false trend from broken baseline)
            cvd_q = int(indicators.get("cvd_quarantine_active", 0) or 0)
            div = None if cvd_q == 1 else _get_attr_or_key(runtime, 'last_div', None)
            if cvd_q == 1:
                indicators["hidden_div_ignored"] = 1
            trend_dir = hidden_trend_dir(_get_attr_or_key(div, 'kind', None) if div else None)

            if trend_dir is not None:
                indicators["trend_dir_source"] = "hidden_div"
                evidence["trend_dir_source"] = "hidden_div"
                indicators["hidden_div_used"] = 1

            # FAILBACK: If no hidden divergence, use REGIME as trend definition (Trend Following)
            if trend_dir is None:
                 from contexts import MARKET_REGIME_NA, normalize_regime_label
                 rg = normalize_regime_label(_get_attr_or_key(runtime, 'last_regime', MARKET_REGIME_NA))
                 if "bull" in rg or rg == "trending" or rg == "trend":
                     trend_dir = "LONG"
                 elif "bear" in rg:
                     trend_dir = "SHORT"

                 if trend_dir is not None:
                     indicators["trend_dir_source"] = "regime"
                     evidence["trend_dir_source"] = "regime"

            if trend_dir is None:
                if int(os.getenv("OF_TREND_DIR_FALLBACK_TO_DIRECTION", "0")) == 1:
                    trend_dir = direction
                    indicators["trend_dir_source"] = "direction"
                    evidence["trend_dir_source"] = "direction"

            if trend_dir is None:
                # --- delta_z strength bypass ---
                # При очень сильном дельта-давлении (|z| >= threshold) определяем trend_dir
                # из знака delta_z, не требуя внешних источников (sweep/regime/divergence).
                # Fail-open: любое исключение → scenario = "none" как раньше.
                try:
                    _dz_bypass_th = float(cfg.get("scenario_dz_bypass_threshold", 10.0))
                    _dz_val = float(delta_z)
                    if abs(_dz_val) >= _dz_bypass_th:
                        # delta_z < 0 → sell pressure → SHORT; delta_z > 0 → buy pressure → LONG
                        trend_dir = "SHORT" if _dz_val < 0.0 else "LONG"
                        scenario = "continuation"
                        fallback_reason = "dz_bypass"
                        try:
                            indicators["scenario_dz_bypass"] = 1
                            indicators["scenario_dz_bypass_th"] = float(_dz_bypass_th)
                            indicators["of_debug_fail"] = (
                                f"dz_bypass:dz={_dz_val:.2f}>=th={_dz_bypass_th:.1f}"
                                f":regime={getattr(runtime, 'last_regime', 'na')}"
                            )
                        except Exception:
                            pass
                except Exception:
                    pass

                if trend_dir is None:
                    scenario = "none"
                    fallback_reason = "no_sweep_and_no_trend"
                    with contextlib.suppress(Exception):
                         indicators["of_debug_fail"] = f"no_trend:regime={getattr(runtime, 'last_regime', 'na')}"

        scenario_v4 = scenario
        policy_reason = "ok"

        # proxy: news/vol shock
        news_flag = int(indicators.get("news_risk", 0) or indicators.get("calendar_risk", 0) or 0)
        reg = str(getattr(runtime, "last_regime", "") or "").lower()
        vol_shock = (news_flag == 1 or ("news" in reg) or ("shock" in reg))

        # proxy: saw/chop/spoof-ish
        # churn_hi: keep simple + safe (NO getattr with >3 args)
        try:
            churn_hi = bool(int(indicators.get("book_churn_hi", getattr(runtime, "book_churn_hi", 0) or 0) or 0))
        except Exception:
            try:
                churn_hi = bool(int(getattr(runtime, "book_churn_hi", 0) or 0))
            except Exception:
                churn_hi = False
        saw_chop = (int(indicators.get("saw_chop", 0) or 0) == 1 or churn_hi)

        if vol_shock:
            scenario_v4 = "vol_shock_news_proxy"
            policy_reason = "vol_shock_proxy"
        elif saw_chop:
            scenario_v4 = "saw_chop_spoof_proxy"
            policy_reason = "saw_chop_proxy"

        indicators["scenario_base"] = str(scenario)
        indicators["scenario_v4"] = str(scenario_v4)

        # --- Absorption-on-level (v2) from last microbar footprint + external confirms ---
        abs_lvl_ok = False
        abs_lvl_score = 0.0
        abs_lvl_bias = "NONE"
        abs_lvl_dir_match = False
        bar = None

        try:
            if bool(int(cfg.get("abs_lvl_enable", 1))):
                bar = getattr(runtime, "last_bar", None)
                if bar is not None and bool(getattr(bar, "fp_enabled", False)):
                    abs_lvl = compute_absorption_level_score(
                        bar=bar,
                        direction=direction,
                        delta_z=float(delta_z),
                        weak_progress=wp_any,
                        iceberg_strict=iceberg_strict,
                        reclaim_recent=reclaim_recent,
                        cfg=cfg,
                    )
                    abs_lvl_ok = abs_lvl.ok
                    abs_lvl_score = float(abs_lvl.score)
                    abs_lvl_bias = str(abs_lvl.bias)
                    abs_lvl_dir_match = abs_lvl.dir_match

                    indicators["abs_lvl_ok"] = int(abs_lvl_ok)
                    indicators["abs_lvl_score"] = abs_lvl_score
                    indicators["abs_lvl_bias"] = abs_lvl_bias
                    indicators["abs_lvl_ladder"] = int(abs_lvl.ladder_len)
                    indicators["abs_lvl_poc_edge"] = int(abs_lvl.poc_on_edge)
                    indicators["abs_lvl_eff"] = float(abs_lvl.eff_delta)
                    # indicators["abs_lvl_parts"] = abs_lvl.parts
        except Exception:
            pass

        # --- FP edge absorb (A2) - derive from abs_lvl if not provided ---
        fp_edge_absorb = fp_edge_ok
        # optional derive from abs_lvl if not provided
        if (not fp_edge_absorb) and abs_lvl_ok:
            try:
                poc_edge = (int(indicators.get("abs_lvl_poc_edge", 0) or 0) == 1)
                score_min = float(cfg.get("fp_edge_abs_lvl_score_min", 0.55) or 0.55)
                fp_edge_absorb = (poc_edge and float(abs_lvl_score) >= score_min and abs_lvl_dir_match)
            except Exception:
                pass

        indicators["fp_edge_absorb"] = 1 if fp_edge_absorb else 0

        # --- Microstructure V4 (P9b) ---
        # Compute V4 features if available (snapshot-based)
        # We do this unconditionally so they are available for evidence/logging,
        # even if not used by current model (unless overhead is too high, but it's cheap).
        micro_v4 = {}
        try:
            snap = getattr(runtime, "book_state", None)
            if snap is not None:
                # We need prev_snap for shift calculation
                # runtime.book_state is usually the *current* state.
                # Does runtime have prev_state?
                # In scanner_infra, runtime.book_state might be a robust object or just current snap.
                # We often used 'runtime.book_state.snap' and 'runtime.book_state.prev_snap' in older code?
                # Let's check the context or just use what we have.
                # Evidence contract says: "compute_microstructure_v4(snap, prev_snap, levels=5)"
                # We will try to extract snap/prev from runtime.book_state if available,
                # or pass runtime.book_state as snap.

                # Check if runtime.book_state has 'snap' attr
                s = getattr(snap, "snap", snap)
                p = getattr(snap, "prev_snap", None)

                micro_v4 = compute_microstructure_v4(s, p, levels=5)

                # Extract to evidence
                for k, v in micro_v4.items():
                    evidence[k] = float(v)
                    # Also put in indicators for observability/grafana
                    indicators[k] = float(v)

        except Exception:
            pass

        # --- Strong gate decision (need is scenario-dependent, can be escalated same-tick) ---
        # Merge dynamic cfg (runtime.dynamic_cfg) into local cfg view.
        dyn = _get_attr_or_key(runtime, 'dynamic_cfg', {}) or {}
        cfg2 = merged_cfg(cfg, dyn)

        # Determine regime / instability / pressure / churn same-tick inputs
        try:
            from contexts import MARKET_REGIME_NA, normalize_regime_label
            regime = normalize_regime_label(_get_attr_or_key(runtime, 'last_regime', MARKET_REGIME_NA))
        except Exception:
            regime = "na"
        try:
            unstable = bool(int(dyn.get("abs_lvl_th_unstable", 0) or 0))
        except Exception:
            unstable = False
        # pressure_hi: deterministic sources only (no runtime.pressure calls => replayable)
        try:
            if "pressure_hi" in indicators:
                pressure_hi = (int(indicators.get("pressure_hi", 0) or 0) == 1)
            elif isinstance(dyn, dict) and "pressure_hi" in dyn:
                pressure_hi = (int(dyn.get("pressure_hi", 0) == 1))
            else:
                ph = getattr(runtime, "pressure_hi", None)
                if ph is not None:
                    try:
                        pressure_hi = (int(ph or 0) == 1) if not isinstance(ph, bool) else ph
                    except Exception:
                        pressure_hi = bool(ph)
                else:
                    pressure_hi = runtime.pressure.is_pressure_hi(now_ts, float(cfg2.get("pressure_hi_per_min", 4.0)))
        except Exception:
            pressure_hi = False
        try:
            churn_hi = bool(int(_get_attr_or_key(runtime, 'book_churn_hi', 0) or 0))
        except Exception:
            churn_hi = False

        nd = compute_strong_need_same_tick(
            scenario=str(scenario),
            pressure_hi=pressure_hi,
            churn_hi=churn_hi,
            regime=str(regime),
            unstable=unstable,
            cfg=cfg2,
        )
        # Apply need overrides into cfg2 for eval_* (same-tick)
        cfg2["strong_need_reversal"] = int(nd.need_rev)
        cfg2["strong_need_continuation"] = int(nd.need_cont)
        # v14_of: expose strong-need policy artifacts via evidence for downstream ML
        # consumption (build_og_payload reads these keys; fail-open to 0 if absent).
        evidence["strong_need_reversal"] = nd.need_rev
        evidence["strong_need_continuation"] = nd.need_cont
        evidence["strong_need_reason"] = str(getattr(nd, "reason", "") or "")
        # We don't store it back to cfg2 as a key used by eval_*, but we keep for audit if needed

        if scenario == "reversal":
            # C1: OFI substitutes OBI stability for the microstructure leg (safe: does not increase have count).
            ofi_leg = (ofi_dir_ok and ofi_stable)
            # No longer need implicit OR substitution because eval_reversal now handles ofi_leg natively.
            # But we keep explicit params clean.

            # A2: fp_edge absorption is "absorption-like" evidence; we can safely let it satisfy abs_lvl_ok input
            # to avoid changing strong_of_gate signatures (still doesn't increase number of legs).
            # No longer need implicit OR substitution because abs_lvl_ok counts as A or C based on config,
            # and fp_edge_absorb is now a separate native param in eval_reversal C-bucket.

            from core.compat_utils import _filter_kwargs_for_callable

            # C1: Optional arguments for legs (ofi_leg, fp_edge_absorb) are passed if the callee accepts them.
            # We use filter_kwargs to be compatible with both old (3-arg) and new (5-arg) signatures of eval_*.

            # test_ofi_substitutes_obi_stable_in_eval_reversal expects implicit substitution:
            _last_obi = _get_attr_or_key(runtime, 'last_obi_event', None)
            if not obi_stable and _last_obi is None and ofi_leg:
                obi_stable = True
            if not abs_lvl_ok and fp_edge_absorb:
                abs_lvl_ok = True


            reversal_kwargs = {
                "direction": direction,
                "delta_z": float(delta_z),
                "weak_progress": wp_any,
                "sweep_recent": sweep_recent,
                "reclaim_recent": reclaim_recent,
                "obi_stable": obi_stable,  # Native OBI
                "iceberg_strict": iceberg_strict,
                "abs_lvl_ok": abs_lvl_ok, # Native Abs Lvl
                "cfg": cfg2,
                "ofi_leg": ofi_leg,
                "fp_edge_absorb": fp_edge_absorb,
            }

            dec = eval_reversal(**_filter_kwargs_for_callable(eval_reversal, **reversal_kwargs))
        elif scenario == "continuation" and trend_dir is not None:
            # continuation context (countertrend absorption observed) is maintained in runtime
            now_ts_for_cont = now_ts
            cont_ts = int(getattr(runtime, "cont_ctx_ts_ms", 0) or 0)
            cont_valid = int(cfg2.get("cont_ctx_valid_ms", 120_000))
            cont_ctx_recent = (cont_ts > 0 and 0 <= now_ts_for_cont - cont_ts <= cont_valid)

            # hidden ctx recent
            div = getattr(runtime, "last_div", None)
            hidden_ms = int(cfg2.get("hidden_ctx_valid_ms", 120_000))
            div_ts = int(getattr(div, "ts_ms", 0) or 0)
            hidden_ctx_recent = (div is not None and div_ts > 0 and 0 <= now_ts_for_cont - div_ts <= hidden_ms)

            # Regime fallback bypass: If we derived trend from regime or direction, we consider the context satisfied.
            if not hidden_ctx_recent:
                src = (indicators.get("trend_dir_source", ""))
                if src in ("regime", "direction") or indicators.get("scenario_dz_bypass"):
                    hidden_ctx_recent = True

            # WARMUP BYPASS FOR MISSING LEGS ON RESTART
            try:
                from core.core_snapshot.runtime_clock import snapshot as runtime_snapshot
                _clock = runtime_snapshot(event_ts_ms=now_ts_for_cont)
                uptime_sec = int(_clock.uptime_sec)
                warmup_s = int(cfg2.get("continuation_warmup_sec", 1800))

                # If we are in the warmup window, allow fallback for strictly unpopulated history legs.
                if 0 < uptime_sec < warmup_s:
                    if div is None:
                        hidden_ctx_recent = True
                        indicators["hidden_ctx_warmup_bypass"] = 1
                    if cont_ts == 0:
                        cont_ctx_recent = True
                        indicators["cont_ctx_warmup_bypass"] = 1
                    if getattr(runtime, "last_obi_event", None) is None:
                        obi_stable = True
                        indicators["obi_stable_warmup_bypass"] = 1
            except Exception:
                pass

            indicators["cont_ctx_ts_ms"] = int(cont_ts)
            indicators["cont_ctx_age_ms"] = int(max(0, now_ts_for_cont - cont_ts)) if cont_ts > 0 else 0
            indicators["hidden_ctx_recent"] = int(hidden_ctx_recent)
            indicators["cont_ctx_recent"] = int(cont_ctx_recent)

            ofi_leg = (ofi_dir_ok and ofi_stable)

            from core.compat_utils import _filter_kwargs_for_callable

            continuation_kwargs = {
                "direction": direction,
                "trend_dir": trend_dir,
                "hidden_ctx_recent": hidden_ctx_recent,
                "iceberg_strict": iceberg_strict,
                "obi_stable": obi_stable,
                "cont_ctx_recent": cont_ctx_recent,
                "abs_lvl_ok": abs_lvl_ok,
                "ofi_leg": ofi_leg,
                "fp_edge_absorb": fp_edge_absorb,
                "cfg": cfg2,
                "trend_dir_source": (indicators.get("trend_dir_source", "none")),
                "delta_z": float(delta_z),
            }

            dec = eval_continuation(**_filter_kwargs_for_callable(eval_continuation, **continuation_kwargs))

        # Attach need escalation diagnostics
        try:
            if dec is not None:
                dec.need_reason = str(nd.reason)
        except Exception:
            pass

        # -------------------------------------------------------
        # A3) Execution-risk penalty (mandatory): spread + slippage
        # -------------------------------------------------------
        spread_bps = _f(indicators.get("spread_bps"), -1.0)
        slip_bps = _f(indicators.get("expected_slippage_bps"), -1.0)

        # If missing => do NOT silently become zero
        if spread_bps <= 0:
            spread_bps = _f(cfg.get("spread_bps_missing_default", float(os.getenv("SPREAD_BPS_MISSING_DEFAULT", "15.0"))), 15.0)
            indicators["spread_bps_missing"] = 1
        if slip_bps < 0:
            slip_bps = _f(cfg.get("expected_slippage_bps_missing_default", float(os.getenv("SLIPPAGE_BPS_MISSING_DEFAULT", "4.0"))), 4.0)
            indicators["expected_slippage_missing"] = 1

        indicators["spread_bps"] = float(spread_bps)
        indicators["expected_slippage_bps"] = float(slip_bps)

        exec_risk_bps = max(0.0, float(spread_bps)) + max(0.0, float(slip_bps))
        # exec_ref = realistic spread+slip budget for the symbol.
        # Priority: exec_risk_ref_bps (execution-cost ref, calibrated to market spreads)
        #           > dist_bp_threshold * mult (proximity gate, kept as fallback only).
        try:
            exec_ref_direct = float(cfg.get("exec_risk_ref_bps", 0.0) or 0.0)
            if exec_ref_direct > 0.0:
                ref_base = exec_ref_direct
            else:
                ref_base = float(cfg.get("dist_bp_threshold", 0.0) or 0.0)
                if ref_base <= 0.0:
                    from core.instrument_config import get_default_dist_bp_threshold
                    ref_base = get_default_dist_bp_threshold(symbol) or 30.0
        except Exception:
            ref_base = 30.0

        exec_ref = ref_base * float(cfg.get("exec_risk_ref_mult", 1.0) or 1.0)

        # Adaptive reference for low liquidity / thin regimes
        liq_regime = (indicators.get("liq_regime", getattr(runtime, "liq_regime", "na")) or "na")
        lr = liq_regime.lower()
        if "low" in lr or "thin" in lr or "illiquid" in lr or "news" in lr:
             # Stricter reference in bad conditions (e.g. 0.8x of normal)
             exec_ref *= 0.8

        exec_risk_norm = _clamp01(exec_risk_bps / max(1e-9, exec_ref))

        # Penalty calculation
        w_exec = _f(cfg.get("w_exec_risk", 0.18), 0.18)
        exec_pen = _clamp01(exec_risk_norm) * w_exec

        indicators["exec_risk_bps"] = float(exec_risk_bps)
        indicators["exec_risk_norm"] = float(exec_risk_norm)
        indicators["exec_risk_ref_bps"] = float(exec_ref)
        indicators["exec_pen"] = float(exec_pen)

        # Execution risk (already present): exec_risk_bps, exec_risk_norm, exec_pen
        # Add explicit slippage decomposition: spread vs impact-proxy for observability
        try:
            half_spread = float(spread_bps) * 0.5
            exp_slip = float(slip_bps)
            slip_spread = max(0.0, half_spread)
            slip_impact = max(0.0, exp_slip - slip_spread)
            indicators["slip_spread_bps"] = float(slip_spread)
            indicators["slip_impact_bps"] = float(slip_impact)
        except Exception:
            pass

        # Fill-prob / ETA proxy (L3-lite) -> exec penalty term + indicators
        try:
            fp = compute_fill_prob_proxy(
                direction=direction,
                cancel_to_trade_bid=float(indicators.get("cancel_to_trade_bid", 0.0) or 0.0),
                cancel_to_trade_ask=float(indicators.get("cancel_to_trade_ask", 0.0) or 0.0),
                eta_fill_bid_sec=float(indicators.get("eta_fill_bid_sec", 0.0) or 0.0),
                eta_fill_ask_sec=float(indicators.get("eta_fill_ask_sec", 0.0) or 0.0),
                max_wait_s=float(cfg.get("fill_prob_max_wait_s", 2.0) or 2.0),
            )
            indicators["fill_prob_proxy"] = float(fp["fill_prob_proxy"])
            indicators["eta_fill_sec"] = float(fp["eta_fill_sec"])
            indicators["fill_prob_p_base"] = float(fp["p_base"])
            indicators["fill_prob_p_wait"] = float(fp["p_wait"])

            # fixed-horizon fill probability variants (Phase 8.2)
            _fp_kw = dict(
                direction=direction,
                cancel_to_trade_bid=float(indicators.get("cancel_to_trade_bid", 0.0) or 0.0),
                cancel_to_trade_ask=float(indicators.get("cancel_to_trade_ask", 0.0) or 0.0),
                eta_fill_bid_sec=float(indicators.get("eta_fill_bid_sec", 0.0) or 0.0),
                eta_fill_ask_sec=float(indicators.get("eta_fill_ask_sec", 0.0) or 0.0),
            )
            for _hw in (1.0, 3.0, 5.0):
                with contextlib.suppress(Exception):
                    _fph = compute_fill_prob_proxy(**_fp_kw, max_wait_s=_hw)
                    indicators[f"fill_prob_{int(_hw)}s"] = _fph["fill_prob_proxy"]

            w_fill = float(cfg.get("exec_fill_pen_w", 0.20) or 0.20)
            exec_fill_pen = w_fill * (1.0 - float(fp["fill_prob_proxy"]))
            indicators["exec_fill_pen"] = float(exec_fill_pen)
            with contextlib.suppress(Exception):
                exec_pen = float(exec_pen) + float(exec_fill_pen)
        except Exception:
            pass

        # --- Score (0..1), stable under feature additions ---
        # We use weighted-mean aggregation by default so adding OFI/FP-edge doesn't saturate score.
        contrib: dict[str, float] = {}
        raw_sum = 0.0
        w_sum = 0.0

        def _add(name: str, norm: float, w: float) -> None:
            nonlocal raw_sum, w_sum
            ww = float(w)
            vv = _clamp01(float(norm)) * ww
            contrib[name] = vv
            raw_sum += vv
            w_sum += ww

        # delta spike strength
        z_abs = abs(float(delta_z))
        z_ref = _f(cfg.get("score_z_ref", 3.0), 3.0)
        _add("z", z_abs / max(1e-9, z_ref), _f(cfg.get("w_z", 0.30), 0.30))

        # weak progress
        _add("weak_progress", 1.0 if wp_any else 0.0, _f(cfg.get("w_wp", 0.15), 0.15))

        # reclaim
        _add("reclaim", 1.0 if reclaim_recent else 0.0, _f(cfg.get("w_reclaim", 0.20), 0.20))

        # OBI stable
        _add("obi_stable", 1.0 if obi_stable else 0.0, _f(cfg.get("w_obi", 0.15), 0.15))

        # Iceberg strict
        _add("iceberg_strict", 1.0 if iceberg_strict else 0.0, _f(cfg.get("w_ice", 0.15), 0.15))

        # absorption (raw detector)
        _add("absorption", 1.0 if abs_ok else 0.0, _f(cfg.get("w_abs", 0.05), 0.05))

        # OFI (normalized)
        ofi_leg = (ofi_dir_ok and ofi_stable)
        ofi_z_ref = _f(cfg.get("ofi_z_ref", 3.0), 3.0)
        ofi_z_norm = _clamp01(abs(float(ofi_z)) / max(1e-9, ofi_z_ref))
        ofi_stab_norm = _clamp01(float(ofi_stability_score))
        contrib["ofi"] = (ofi_z_norm * ofi_stab_norm * _f(cfg.get("w_ofi", 0.10), 0.10)) if ofi_leg else 0.0
        raw_sum += contrib["ofi"]
        w_sum += _f(cfg.get("w_ofi", 0.10), 0.10) if ofi_leg else 0.0

        # FP edge
        contrib["fp_edge"] = (_f(cfg.get("w_fp_edge", 0.05), 0.05)) if fp_edge_absorb else 0.0
        raw_sum += contrib["fp_edge"]
        w_sum += _f(cfg.get("w_fp_edge", 0.05), 0.05) if fp_edge_absorb else 0.0

        # A3 Execution risk penalty (mandatory)
        contrib["exec_risk_penalty"] = -float(exec_pen)

        agg = (cfg.get("of_score_agg", "weighted_mean") or "weighted_mean").lower()

        # EXPERT HYBRID: Use 'sum' if we have strong evidence (>=2 legs) but still below 'need',
        # provided confidence is high (>75%). This helps capturing signals that are very clear
        # but don't meet the strict leg count (common for memes).
        is_hybrid = bool(int(cfg.get("of_score_agg_hybrid", 1)))
        effective_agg = agg
        if is_hybrid and dec is not None and dec.have >= 2 and dec.have < dec.need:
             conf = _f(indicators.get("confidence_pct", 0.0), 0.0)
             if conf >= 75.0:
                  effective_agg = "sum"
                  indicators["of_agg_hybrid_active"] = 1

        if effective_agg == "sum":
            base_score = _clamp01(raw_sum)
        else:
            base_score = _clamp01(raw_sum / max(1e-9, w_sum))

        # Apply penalty after base score
        # NEW (2026-02-12): Decoupled scoring for ML/Meta analysis

        # --- Burst / Hawkes Gate (derived from indicators) ---
        # Hydrate burst gate inputs from runtime (not yet in indicators at this stage).
        try:
            indicators.setdefault("book_churn_score", float(getattr(runtime, "book_churn_score", 0.0) or 0.0))
            indicators.setdefault("book_rate_z", float(getattr(runtime, "book_rate_z", 0.0) or 0.0))
            indicators.setdefault("pressure_sps", float(getattr(runtime, "pressure_sps", 0.0) or 0.0))
            _hs = getattr(runtime, "hawkes_snapshot", None)
            if isinstance(_hs, dict):
                indicators.setdefault("hawkes_trade_lam", float(_hs.get("hawkes_taker_lam", 0.0) or 0.0))
                indicators.setdefault("hawkes_cancel_lam", float(_hs.get("hawkes_cancel_lam", 0.0) or 0.0))
                indicators.setdefault("hawkes_combined_lam", float(_hs.get("hawkes_churn_lam", _hs.get("hawkes_taker_lam", 0.0)) or 0.0))
            _l3 = getattr(runtime, "l3_stats", None)
            if _l3 is not None:
                indicators.setdefault("taker_rate_ema",
                    float(getattr(_l3, "taker_buy_rate_ema", 0.0) or 0.0) + float(getattr(_l3, "taker_sell_rate_ema", 0.0) or 0.0))
                indicators.setdefault("cancel_rate_ema",
                    float(getattr(_l3, "cancel_bid_rate_ema", 0.0) or 0.0) + float(getattr(_l3, "cancel_ask_rate_ema", 0.0) or 0.0))
        except Exception:
            pass
        burst_pen, burst_veto, burst_reason, burst_snap = eval_burst_gate(indicators, cfg2)

        # --- P2d: Liquidity Pressure Gate (Queue Imbalance + Multi-level OFI) ---
        # Added in P8, applies dynamic boost/penalty based on orderbook intent alignment.

        # 1. Retrieve snapshots (t0 and prev)
        # Try direct access first for speed, fallback to runtime, then indicators
        if snap_t0 is None:
             snap_t0 = getattr(runtime.book_state, "snap", None) if hasattr(runtime, "book_state") else None
        if snap_t0 is None:
             snap_t0 = getattr(runtime, "last_book", None)
        if snap_t0 is None:
             snap_t0 = indicators.get("book_snapshot") # fallback

        if snap_prev is None:
            snap_prev = getattr(runtime.book_state, "prev_snap", None) if hasattr(runtime, "book_state") else None
        if snap_prev is None:
             snap_prev = getattr(runtime, "prev_book", None)
        if snap_prev is None:
             snap_prev = indicators.get("prev_book_snapshot")

        # 2. Compute metrics
        # qimb: needs t0
        qimb_res = {}
        if snap_t0:
            qimb_res = compute_queue_imbalance_topn(snap_t0, levels=5)

        # ofi: needs t0 and prev
        ofi_res = {}
        if snap_t0 and snap_prev:
            ofi_res = compute_ofi_multilevel_topn(snap_prev, snap_t0, levels=5)

        # 3. Add to evidence for meta-model/logs
        # (Compatible with meta_features_v2 which expects these keys)
        for k, v in qimb_res.items():
            evidence[k] = v
        for k, v in ofi_res.items():
            evidence[k] = v

        # 4. Evaluate Gate
        # safe extract args
        qimb_val = float(qimb_res.get("qimb_wmean", 0.0))
        ofi_val = float(ofi_res.get("ofi_ml_norm", 0.0))
        obi_dw = float(indicators.get("obi_dw", 0.0) or 0.0)
        res_recovered = int(indicators.get("res_recovered", 0) or 0)
        res_recovery_ms = int(indicators.get("res_recovery_ms", 0) or 0)

        liq_boost, liq_pen, liq_veto, liq_reason, liq_q_align, liq_ofi_align = eval_liq_pressure_gate(
            direction=direction,
            qimb_wmean=qimb_val,
            ofi_ml_norm=ofi_val,
            cfg2=cfg2,
            obi_dw=obi_dw,
            res_recovered=res_recovered,
            res_recovery_ms=res_recovery_ms,
        )

        # Even when the base scenario evaluation yields no decision (dec=None), we still want
        # downstream gates (taker-flow / LiqMap / Meta, etc.) to surface their veto/shadow flags
        # and gate_bits for observability and decision-record exports.
        if dec is None:
            try:
                dec = type("GateDec", (), {})()
                dec.ok = False
                dec.have = 0
                dec.need = 0
                dec.scenario = str(scenario)
                dec.reason = "no_decision"
                dec.gate_bits = 0
            except Exception:
                pass

        _t_stage = _snap_stage("scoring", _t_stage)

        # P9c: Isolated Taker-Flow contra gate (optional hard veto)
        taker_veto = 0
        taker_shadow = 0
        taker_soft = 0
        taker_reason = "ok"
        taker_score_adj = 0.0
        try:
            tfg = eval_taker_flow_gate(direction=direction, indicators=indicators, cfg2=cfg2)
            taker_veto   = int(getattr(tfg, "veto",        0) or 0)
            taker_shadow = int(getattr(tfg, "shadow_veto", 0) or 0)
            taker_soft   = int(getattr(tfg, "soft",        0) or 0)
            taker_reason = str(getattr(tfg, "reason",    "ok") or "ok")
            if taker_soft == 1:
                dec.gate_bits = int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_TAKER_FLOW
        except Exception:
            pass

        # Export to indicators for metrics/QA (emitted as Prometheus counters in tick_processor)
        try:
            indicators["taker_flow_gate_veto"]        = int(taker_veto)
            indicators["taker_flow_gate_shadow_veto"] = int(taker_shadow)
            indicators["taker_flow_gate_soft"]        = int(taker_soft)
            indicators["taker_flow_gate_reason"]      = str(taker_reason)
        except Exception:
            pass

        # Volatility shock regime guard (fail-closed option)
        try:
            if int(cfg2.get("vol_shock_guard_enable", 1) or 1) == 1:
                vr = float(indicators.get("vol_ratio", 0.0) or 0.0)
                vz = float(indicators.get("vol_ratio_z", 0.0) or 0.0)
                r_hi = float(cfg2.get("vol_shock_ratio_hi", 2.0) or 2.0)
                z_hi = float(cfg2.get("vol_shock_z_hi", 3.5) or 3.5)
                if (vr >= r_hi) or (vz >= z_hi):
                    # strictify only: veto when execution risk is already high
                    cap = float(cfg2.get("vol_shock_exec_risk_norm_max", 0.75) or 0.75)
                    if float(exec_risk_norm) > cap and int(cfg2.get("vol_shock_fail_closed", 0) or 0) == 1:
                        ok = 0
                        hard_veto = "vol_shock_fail_closed"
        except Exception:
            pass

        # --- P14: DQ / Time-Determinism Gate ---
        dq_meta = {}
        if eval_dq_gate is not None:
            try:
                dq_meta = eval_dq_gate(indicators=dict(indicators), cfg2=cfg2)
            except Exception:
                dq_meta = {}

        dq_pen = float(dq_meta.get("dq_pen", 0.0))
        dq_veto = int(dq_meta.get("dq_veto", 0))
        dq_level = int(dq_meta.get("dq_level", 0) or 0)
        dq_reason = (dq_meta.get("dq_reason", "ok"))
        dq_reasons = dq_meta.get("dq_reasons", [])
        dq_health = float(dq_meta.get("dq_health_score", 1.0))
        dq_bucket = (dq_meta.get("dq_reason_bucket", "ok"))
        dq_uptime_sec = int(dq_meta.get("uptime_sec", 0) or 0)
        dq_runtime_start_ts_ms = dq_meta.get("runtime_start_ts_ms")
        dq_veto_suppressed = int(dq_meta.get("dq_veto_suppressed", 0) or 0)
        dq_veto_suppressed_reason = (dq_meta.get("dq_veto_suppressed_reason", "") or "")

        if dq_veto == 1:
            try:
                # Always expose veto-capable state in indicators for observability.
                indicators["dq_book_veto_active"] = int(self._should_apply_dq_veto(cfg))
            except Exception:
                indicators["dq_book_veto_active"] = 1

        # --- P15: LiqMap Gate (Liquidation Map) ---
        # Hard-risk philosophy:
        #   - SHADOW: never veto, but record shadow_veto=1 for calibration
        #   - ENFORCE: hard veto if adverse liquidation peak lies inside the SL band
        # IMPORTANT: this gate must NEVER widen SL. If structure requires more risk -> reject trade.
        liqmap_shadow_veto = 0
        liqmap_veto = 0
        liqmap_soft = 0
        liqmap_reason = "na"
        liqmap_rr = 0.0
        liqmap_risk_bps = 0.0
        liqmap_reward_bps = 0.0
        liqmap_adverse_peak_usd = 0.0
        liqmap_favorable_peak_usd = 0.0
        liqmap_window_used = (cfg2.get("liqmap_gate_window", "5m") or "5m")
        liqmap_mode = "OFF"
        try:
            # Evaluate unconditionally if implementation is available.
            # The gate itself decides OFF/SHADOW/ENFORCE based on cfg2.
            if evaluate_liqmap_gate_v1 is not None:
                lm = evaluate_liqmap_gate_v1(direction=direction, indicators=indicators, cfg2=cfg2)
                liqmap_shadow_veto = int(getattr(lm, "shadow_veto", 0) or 0)
                liqmap_veto = int(getattr(lm, "veto", 0) or 0)
                liqmap_soft = int(getattr(lm, "soft", 0) or 0)
                liqmap_reason = str(getattr(lm, "reason", "ok") or "ok")
                liqmap_rr = float(getattr(lm, "rr", 0.0) or 0.0)
                liqmap_risk_bps = float(getattr(lm, "risk_bps", 0.0) or 0.0)
                liqmap_reward_bps = float(getattr(lm, "reward_bps", 0.0) or 0.0)
                liqmap_adverse_peak_usd = float(getattr(lm, "adverse_peak_usd", 0.0) or 0.0)
                liqmap_favorable_peak_usd = float(getattr(lm, "favorable_peak_usd", 0.0) or 0.0)
                liqmap_window_used = str(getattr(lm, "window", liqmap_window_used) or liqmap_window_used)
                liqmap_mode = str(getattr(lm, "mode", liqmap_mode) or liqmap_mode)
                if liqmap_shadow_veto == 1 or liqmap_veto == 1 or liqmap_soft == 1:
                    if dec is None:
                        # Build a minimal decision container so gate_bits propagate into OFConfirm.
                        dec = type("GateDec", (), {})()
                        dec.scenario = scenario
                    dec.gate_bits = int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_LIQMAP
        except Exception:
            pass

        # Export gate outputs into indicators (used by meta_feat_v9 + metrics + DecisionRecord)
        try:
            indicators["liqmap_gate_shadow_veto"] = int(liqmap_shadow_veto)
            indicators["liqmap_gate_veto"] = int(liqmap_veto)
            indicators["liqmap_gate_soft"] = int(liqmap_soft)
            indicators["liqmap_gate_veto_reason"] = str(liqmap_reason)
            indicators["liqmap_gate_reason"] = str(liqmap_reason)  # backwards-compat
            indicators["liqmap_gate_rr"] = float(liqmap_rr)
            indicators["liqmap_gate_risk_bps"] = float(liqmap_risk_bps)
            indicators["liqmap_gate_reward_bps"] = float(liqmap_reward_bps)
            indicators["liqmap_gate_adverse_peak_usd"] = float(liqmap_adverse_peak_usd)
            indicators["liqmap_gate_favorable_peak_usd"] = float(liqmap_favorable_peak_usd)
            indicators["liqmap_gate_window"] = str(liqmap_window_used)
            indicators["liqmap_gate_mode"] = str(liqmap_mode)
        except Exception:
            pass

        # --- P16: News Agent Reco Gate ---
        news_veto = 0
        news_hard_block = False
        news_soft_bps = 10000
        news_reason = "ok"

        try:
            from common.news_gate import NewsGate
            _ng = getattr(self, "_news_gate", None)
            if _ng is None:
                # Use main Redis: news:hi:active + calendar:agg:crypto live on main redis
                _ng_redis = getattr(self, "_redis_client_main", None) or getattr(self, "_redis_client", None)
                self._news_gate = NewsGate(redis_client=_ng_redis)
                _ng = self._news_gate

            ndec = _ng.decide(
                now_ts_ms=now_ts,
                symbols=(symbol,),
                news_risk=indicators.get("news_risk"),
                news_grade_id=indicators.get("news_grade_id"),
                confidence=indicators.get("news_confidence"),
                horizon_sec=indicators.get("news_horizon_sec"),
                asof_ts_ms=indicators.get("news_asof_ts_ms"),
            )

            news_hard_block = ndec.hard_block
            news_reason = ndec.hard_reason
            news_soft_bps = ndec.risk_factor_bps

            if news_hard_block:
                news_veto = 1
                if dec is None:
                    dec = type("GateDec", (), {})()
                    dec.scenario = scenario
                dec.gate_bits = int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_NEWS

            if news_soft_bps < 10000:
                news_pen = float(cfg2.get("w_news_soft_pen", 0.20)) * (10000 - news_soft_bps) / 10000.0
                exec_pen = float(exec_pen) + news_pen
                indicators["news_pen"] = news_pen

        except Exception as e:
            indicators["news_gate_error"] = str(e)

        indicators["news_gate_veto"] = news_veto
        indicators["news_gate_reason"] = news_reason
        indicators["news_gate_soft_bps"] = news_soft_bps

        # 5. Apply to Score
        # User spec: "final_score_raw = base - exec_pen - burst_pen + liq_boost - liq_pen"

        final_score_raw = float(base_score) - float(exec_pen) - float(burst_pen) - float(dq_pen)

        if liq_boost > 0:
            final_score_raw += liq_boost
        if liq_pen > 0:
            final_score_raw -= liq_pen

        score_raw = final_score_raw
        score = _clamp01(score_raw)

        # 6. Hard Veto (Enforce mode) -- applied to ok below if needed, or we just set reasons here
        # We'll attach it to final_reason logic later

        # Export explainable score breakdown for ML/meta analysis
        score_breakdown = {
            "agg": str(effective_agg),
            "raw_sum": float(raw_sum),
            "w_sum": float(w_sum),
            "base_score": float(base_score),
            "exec_pen": -float(exec_pen),
            "burst_pen": -float(burst_pen),
            "liq_boost": float(liq_boost),
            "liq_pen": -float(liq_pen),
            "dq_pen": -float(dq_pen),
            "final_score_raw": float(final_score_raw),
            "final_score": float(score),
            "contrib": dict(contrib),
        }
        indicators["score_breakdown"] = score_breakdown
        indicators["dq_pen"] = dq_pen

        self._ensure_ofc_ctx_bundle(cfg2)
        ctx_mode = (cfg2.get("ofc_ctx_mode", "off") or "off").lower()
        ctx = build_ofc_context(
            symbol=symbol,
            direction=direction,
            ts_ms=now_ts,
            indicators=indicators,
            runtime=runtime,
            scenario_base=str(scenario),
            scenario_v4=str(scenario_v4),
        )
        ctx_key = make_ctx_key(ctx)
        ctx_fallback_keys = iter_ctx_fallback_keys(ctx)
        ctx_features = self._build_ofc_ctx_features(
            indicators=indicators,
            score=float(score),
            score_raw=float(score_raw),
            exec_risk_bps=float(exec_risk_bps),
            exec_risk_norm=float(exec_risk_norm),
            exec_ref=float(exec_ref),
            spread_bps=float(spread_bps),
            slip_bps=float(slip_bps),
            score_min=float(cfg2.get("of_score_min", 0.40) or 0.40),
            now_ts=now_ts,
        )
        indicators[CtxKeys.KEY] = str(ctx_key)
        indicators["ctx_session"] = str(ctx.session)
        indicators["ctx_hour_utc"] = int(ctx.hour_utc)
        indicators["ctx_dow"] = int(ctx.dow)
        ctx_decision = None
        exec_pred = None
        rule_pred = None
        ctx_infer_latency_us = 0
        if bool(cfg2.get("ofc_ctx_enable", False)) and self._ofc_ctx_bundle is not None and ctx_mode != "off":
            _ctx_t0 = time.perf_counter()
            try:
                bundle = self._ofc_ctx_bundle
                exec_pred = bundle.exec_cost_model.predict(features=ctx_features, ctx_key=ctx_key, fallback_keys=ctx_fallback_keys)
                rule_pred = bundle.rule_success_model.predict(features=ctx_features, ctx_key=ctx_key, fallback_keys=ctx_fallback_keys)
                tp_bps = float(indicators.get("tp1_bps", indicators.get("liqmap_gate_reward_bps", 0.0)) or 0.0)
                sl_bps = float(indicators.get("sl_bps", indicators.get("liqmap_gate_risk_bps", 0.0)) or 0.0)
                ctx_decision = bundle.gate.evaluate(
                    raw_score=float(score), ctx_features=ctx_features, exec_cost_pred=exec_pred,
                    rule_pred=rule_pred, tp_bps=float(tp_bps), sl_bps=float(sl_bps), mode=ctx_mode,
                )
            except Exception as _ctx_e:
                indicators["ctx_error"] = str(_ctx_e)[:200]
            finally:
                ctx_infer_latency_us = int((time.perf_counter() - _ctx_t0) * 1_000_000.0)

        indicators["dq_veto"] = dq_veto
        indicators["dq_reason"] = dq_reason
        indicators["dq_health_score"] = dq_health
        indicators["dq_reason_bucket"] = dq_bucket
        indicators["dq_level"] = int(dq_level)
        indicators["dq_reasons"] = list(dq_reasons) if isinstance(dq_reasons, (list, tuple)) else [str(dq_reasons)]
        indicators["dq_uptime_sec"] = int(dq_uptime_sec)
        if dq_runtime_start_ts_ms is not None:
            indicators["runtime_start_ts_ms"] = int(dq_runtime_start_ts_ms)
        indicators["dq_veto_suppressed"] = int(dq_veto_suppressed)
        if dq_veto_suppressed:
            indicators["dq_veto_suppressed_reason"] = str(dq_veto_suppressed_reason)

# ------------------------------------------------------------------
# Train==Serve "ironclad" metadata:
#  - policy snapshot (SAFE/STRICT thresholds + book alpha + observe-only knobs)
#  - manifest binds schema + policy via stable hashes
# IMPORTANT: offline builders must treat these as *metadata*, not model features.
# ------------------------------------------------------------------
        try:
            dq_snap, dq_hash = build_dq_policy_snapshot(dict(cfg2 or {}))
            indicators["dq_policy_snapshot_v1"] = to_public_dict(dq_snap)
            indicators["dq_policy_hash"] = str(dq_hash)

            # Bind meta-schema + cols ordering to policy via manifest.
            # Cols MUST come from the *active* schema, not a hardcoded fallback —
            # otherwise the manifest hash silently diverges from served features
            # for any schema other than v8, breaking Train==Serve parity.
            meta_name = str(cfg2.get(MetaKeys.SCHEMA_NAME, "meta_feat_v8"))
            reg = globals().get("META_SCHEMA_REGISTRY", {})
            ver, h = (0, "")
            try:
                ver, h = reg.get(meta_name, (0, ""))
            except Exception:
                ver, h = (0, "")

            try:
                from core.meta_schema_registry import get_schema_cols as _get_schema_cols
                cols = tuple(_get_schema_cols(meta_name))
            except Exception:
                cols = ()
            if not cols:
                # Last-resort fallback when the active schema is unknown:
                # use legacy v8 cols so we never publish an empty-cols manifest.
                cols = tuple(globals().get("META_FEAT_V8_COLS", ()))
            man, man_hash = build_feature_manifest_v1(
                meta_schema_name=meta_name,
                meta_schema_version=int(ver),
                meta_schema_hash=str(h),
                meta_cols=cols,
                dq_policy_hash=str(dq_hash),
                thr=dq_snap.thresholds,
            )
            indicators["dq_policy_feature_manifest_v1"] = to_public_dict(man)
            indicators["dq_policy_feature_manifest_hash_v1"] = str(man_hash)
        except Exception:
            # Fail-open: decisions must not depend on metadata presence.
            pass
        indicators["of_base_score"] = float(base_score)
        indicators["of_score_final_raw"] = float(final_score_raw)
        indicators["of_score_final"] = float(score)

        indicators["liq_pressure_boost"] = liq_boost
        indicators["liq_pressure_pen"] = liq_pen
        indicators["liq_pressure_veto"] = liq_veto
        indicators["liq_pressure_reason"] = liq_reason
        indicators["liq_q_align"] = liq_q_align
        indicators["liq_ofi_align"] = liq_ofi_align

        # Export burst snapshot to indicators (for evidence/logging)
        for k, v in burst_snap.items():
            indicators[k] = v
        indicators["burst_reason"] = burst_reason

        # Shadow telemetry: count would-veto events in all modes for promote-to-enforce gating.
        # Collect ≥7 days at burst_gate_mode=shadow before switching to enforce.
        if burst_snap.get("burst_would_veto"):
            try:
                from services.observability.metrics_registry import burst_gate_would_veto_total
                if burst_gate_would_veto_total is not None:
                    burst_gate_would_veto_total.labels(
                        symbol=symbol,
                        reason=str(burst_snap.get("burst_would_veto_reason") or "unknown"),
                        mode=str(burst_snap.get("burst_mode") or "unknown"),
                    ).inc()
            except Exception:
                pass

        # Standardize basic flags and apply conf_* parser
        indicators["obi_stable"] = int(obi_stable)
        indicators["iceberg_strict"] = int(iceberg_strict)
        indicators["sweep_any"] = int(sweep_recent)
        try:
            from core.confirmations_schema_v1 import parse_confirmations_v1
            conf_dict = parse_confirmations_v1(confirmations=None, indicators=indicators)
            indicators.update(conf_dict)
        except Exception:
            pass

        ok = 0
        have = 0
        need = 0
        if dec is not None:
            ok = 1 if bool(dec.ok) else 0
            have = int(dec.have)
            need = int(dec.need)

        # Ensure there is always a decision object so downstream gates (DQ / LiqMap / Meta)
        # can attach gate_bits and expose veto/shadow flags even when the base scenario
        # evaluation returned None (e.g., "no trade" paths).
        #
        # This is important for observability and Train==Serve: the caller may still
        # export indicators and record a decision record for analysis.
        if dec is None:
            try:
                dec = type("GateDec", (), {})()
                dec.ok = bool(ok)
                dec.have = int(have)
                dec.need = int(need)
                dec.scenario = str(scenario)
                dec.reason = reason
                dec.gate_bits = 0
            except Exception:
                # If even this fails, we keep dec=None and rely on fail-open behavior.
                pass

        # --- B2 scenario policies enforcement (post-score, pre-final reason) ---
        hard_veto = ""

        # Score threshold (double filter)
        # NOTE: scenario-specific thresholds are applied later if scenario_v4 is enabled.
        score_min = _f(cfg.get("of_score_min", 0.50), 0.50)
        if ok == 1 and score < score_min:
             # Logic: if score is too low, we can veto even if 2-of-3 passed (optional but recommended)
             # But we only do this if it's not shadow mode in the caller.
             # We'll just return ok=0 and let the service decide.
             ok = 0
             hard_veto = "score_veto"
             # Optional: log if we vetoed by score

        indicators["ok_soft"] = int(ok)


        # P14: DQ Gate Veto
        if dq_veto and (cfg2.get("dq_gate_mode", "off")).lower() in ("enforce", "both", "veto", "hard"):
            try:
                ok = 0
                hard_veto = "dq_gate"
                # dq_bucket is a free-form string; map to canonical code to prevent
                # cardinality explosion in signals_veto_total (P2-4).
                veto_total(self, reason_code="VETO_DQ_BUCKET")
            except Exception:
                pass

        # P15: LiqMap Gate Veto
        # Mode semantics:
        #   shadow: record shadow_veto only
        #   enforce: hard veto
        #   both: treat shadow_veto as enforce (for staged rollouts)
        _lm_mode = (cfg2.get("liqmap_gate_mode", "shadow") or "shadow").lower()
        _lm_shadow = int(indicators.get("liqmap_gate_shadow_veto", 0) or 0)
        _lm_veto = int(indicators.get("liqmap_gate_veto", 0) or 0)
        if (_lm_veto == 1 and _lm_mode in ("enforce", "both", "veto", "hard")) or (_lm_shadow == 1 and _lm_mode in ("both",)):
            try:
                ok = 0
                _r = (indicators.get("liqmap_gate_veto_reason", indicators.get("liqmap_gate_reason", "veto")) or "veto")
                hard_veto = f"liqmap_{_r}"
                # Map liqmap reason to canonical code (P2-4: prevent cardinality explosion).
                veto_total(self, reason_code="VETO_LIQMAP_RR")
            except Exception:
                pass
        # P16: News Agent Reco Gate Veto
        _news_veto = int(indicators.get("news_gate_veto", 0) or 0)
        _news_mode = (cfg2.get("news_gate_mode", "enforce") or "enforce").lower()
        if _news_veto == 1 and _news_mode in ("enforce", "both", "veto", "hard"):
            try:
                ok = 0
                hard_veto = "news_gate"
                # Canonical code from VetoReason registry (P2-4).
                veto_total(self, reason_code="VETO_NEWS_RECO_HARD", kind="news_gate", symbol=symbol)
            except Exception:
                pass

        try:
            if scenario_v4 == "vol_shock_news_proxy":
                if int(cfg.get("vol_shock_fail_closed", 0) or 0) == 1:
                    ok = 0
                    hard_veto = "vol_shock_fail_closed"
                else:
                    cap = float(cfg.get("vol_shock_exec_risk_norm_max", 0.75) or 0.75)
                    if float(exec_risk_norm) > cap:
                        ok = 0
                        hard_veto = "vol_shock_exec_risk_cap"

            if scenario_v4 == "saw_chop_spoof_proxy":
                if int(cfg.get("saw_chop_fail_closed", 1) or 1) == 1:
                    ok = 0
                    hard_veto = "saw_chop_fail_closed"
                else:
                    # require hard evidence
                    if not (bool(iceberg_strict) and bool(ofi_leg) and bool(fp_edge_absorb)):
                        ok = 0
                        hard_veto = "saw_chop_need_hard_evidence"
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Cancellation / Anti-spoof (L3-lite proxy)
        # ------------------------------------------------------------------
        gate_reason = "ok"
        gate_meta = {}
        gate_vetoed = False
        ok_pre_gate = int(ok)
        try:
            if not hasattr(self, "_cancel_spike_gate") or self._cancel_spike_gate is None:
                try:
                    self._cancel_spike_gate = CancellationSpikeGate()  # type: ignore
                except Exception:
                    self._cancel_spike_gate = None

            if self._cancel_spike_gate is not None:
                # Deterministic replay support: allow caller to pass gate state
                # (captured via OFC_CAPTURE) to fully reproduce decisions.
                cgs = indicators.get("cancel_gate_state")
                if isinstance(cgs, dict) and hasattr(self._cancel_spike_gate, "restore_state"):
                    with contextlib.suppress(Exception):
                        self._cancel_spike_gate.restore_state(cgs)

                # prefer explicit keys from indicators
                c_bid = _f(indicators.get("cancel_bid_rate_ema", 0.0), 0.0)
                c_ask = _f(indicators.get("cancel_ask_rate_ema", 0.0), 0.0)
                t_buy = _f(indicators.get("taker_buy_rate_ema", 0.0), 0.0)
                t_sell = _f(indicators.get("taker_sell_rate_ema", 0.0), 0.0)

                # bucket monotonicity or bar_id
                b_id = indicators.get("bucket_id", indicators.get("bar_id"))
                if b_id is None and bar is not None:
                    b_id = getattr(bar, "id", None)

                # Use event-time bucket id (monotonic watermark bucket)
                bucket_ms = int(cfg2.get("cancel_gate_bucket_ms", 500) or 500)
                bucket_id = int(now_ts // bucket_ms)

                gd = self._cancel_spike_gate.check(  # type: ignore
                    symbol=symbol,
                    direction=direction,
                    cancel_bid_rate_ema=float(c_bid),
                    cancel_ask_rate_ema=float(c_ask),
                    taker_buy_rate_ema=float(t_buy),
                    taker_sell_rate_ema=float(t_sell),
                    bucket_id=int(b_id) if b_id is not None else bucket_id,
                    cfg2=cfg2,
                )

                if gd is not None:
                    gate_reason = str(getattr(gd, "reason", "OK"))
                    gate_meta = dict(getattr(gd, "meta", {}) or {})

                    if not gd.allow:
                        ok = 0
                        gate_vetoed = True
                        try:
                            if dec is not None:
                                # Mark as gate bit
                                dec.gate_bits = int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_CANCEL_SPIKE
                        except Exception:
                            pass
                        # Use canonical VETO_CANCEL_SPIKE code; raw gate_reason is
                        # preserved in gate_meta for debugging (P2-4).
                        veto_total(runtime, reason_code="VETO_CANCEL_SPIKE", kind="cancel_spike", symbol=symbol)
                else:
                    gate_reason = "OK"
                    gate_meta = {}
            else:
                gate_reason = "OK"
                gate_meta = {}
        except Exception:
            gate_reason = "ERR"
            gate_meta = {}

        # --- Burst Veto Logic (after legacy gates) ---
        if int(ok) == 1 and burst_veto == 1:
            ok = 0
            gate_vetoed = True # treat as gate veto for downstreams
            gate_reason = f"burst_veto:{burst_reason}"


        # ------------------------------------------------------------------
        # Scenario v4 (B1) + Explainability (D)
        # ------------------------------------------------------------------
        # This runs AFTER cancel-gate so we can use its meta (ready/veto_kind) as a "saw/chop" proxy.
        try:
            scn_v4_en = bool(int(cfg2.get("scenario_v4_enable", 0) or 0))
        except Exception:
            scn_v4_en = False

        # Classify scenario v4
        scn_v4 = None
        try:
            # liq_score: extract from indicators or runtime (fail-open to 0.5 = neutral)
            try:
                _liq_score = float(indicators.get("liq_score", getattr(runtime, "liq_score", 0.5)) or 0.5)
            except Exception:
                _liq_score = 0.5
            scn_v4 = classify_v4(
                sweep_recent=bool(sweep_recent),
                trend_dir=trend_dir,
                pressure_hi=bool(pressure_hi),
                churn_hi=bool(churn_hi),
                exec_risk_bps=float(exec_risk_bps),
                liq_regime=str(liq_regime),
                liq_score=float(_liq_score),
                cancel_meta=dict(gate_meta or {}),
                cfg=dict(cfg2 or {}),
            )
        except Exception:
            scn_v4 = None

        # Explainability legs (cheap and deterministic)
        legs = {
            "obi_stable": int(obi_stable),
            "iceberg_strict": int(iceberg_strict),
            "sweep_recent": int(sweep_recent),
            "reclaim_recent": int(reclaim_recent),
            "weak_progress": int(wp_any),
            "abs_lvl_ok": int(abs_lvl_ok),
            "ofi_leg": int(ofi_leg),
            "fp_edge_absorb": int(fp_edge_absorb),
            "hidden_ctx_recent": int(getattr(dec, "hidden_ctx_recent", 0) if dec else 0) if dec is not None and getattr(dec, "scenario", "") == "continuation" else 0,
            "cont_ctx_recent": int(getattr(dec, "cont_ctx_recent", 0) if dec else 0) if dec is not None and getattr(dec, "scenario", "") == "continuation" else 0,
            "absorption": int(abs_ok),
            "ofi_stable": int(ofi_stable),
        }
        try:
            if dec is not None:
                legs["leg_a"] = int(getattr(dec, "a", 0) or 0)
                legs["leg_b"] = int(getattr(dec, "b", 0) or 0)
                legs["leg_c"] = int(getattr(dec, "c", 0) or 0)
        except Exception:
            pass

        # Scenario policy overrides (strict, deterministic)
        # We DO NOT relax decisions; we only strictify or route "none" -> "range_meanrev".
        scenario_base = str(scenario)
        scenario_v4 = str(getattr(scn_v4, "id", "") or "")
        scenario_reason = str(getattr(scn_v4, "reason", "") or "")
        if scn_v4_en and scenario_v4:
            # route none -> range_meanrev (new scenario instead of unconditional reject)
            if scenario_base == "none" and scenario_v4 == "range_meanrev":
                # Range policy (C2): require absorption + micro-stability + (iceberg or reclaim).
                # Purpose: handle sideways markets with absorption evidence (mean-reversion setups).

                # Legs definitions strictly per spec:
                # abs_leg = abs_lvl_ok OR (absorption && abs_vol >= range_abs_min_vol)
                abs_vol_min = _f(cfg2.get("range_abs_min_vol", 0.0), 0.0)
                # Note: abs_ok is (absorption==1), abs_vol is passed from compute_absorption_flags
                abs_leg = bool(abs_lvl_ok or (abs_ok and abs_vol >= abs_vol_min))

                # micro_leg = ofi_stable OR obi_stable
                # Note: ofi_stable variable here is actually (ofi_dir_ok and ofi_stable) from compute_ofi_flags
                # Let's be explicit:
                micro_leg = bool(obi_stable or (ofi_dir_ok and ofi_stable))

                # edge_leg = fp_edge_absorb OR iceberg_strict
                edge_leg = bool(fp_edge_ok or iceberg_strict)

                have = int(abs_leg) + int(micro_leg) + int(edge_leg)

                # Optional reclaim bonus
                if bool(int(cfg2.get("range_reclaim_counts", 0) or 0)) and reclaim_recent:
                    have += 1

                need = int(cfg2.get("strong_need_range", 3) or 3)

                # score min can be higher in range (optional)
                score_min_rng = _f(cfg2.get("of_score_min_range", score_min), score_min)

                # Hard pass check
                is_hard_pass = (have >= need and score >= score_min_rng)

                # Soft-fail check (analytics only)
                # If have == need-1 AND score is high enough AND exec risk is low enough -> Soft Pass (ok=0 but logged)
                soft_min_score = _f(cfg2.get("range_soft_score_min", 0.72), 0.72)
                soft_max_risk = _f(cfg2.get("range_soft_exec_risk_norm_max", 0.60), 0.60)

                is_soft_pass = False
                soft_reason = "ok"

                if not is_hard_pass and (have == need - 1):
                    if score >= soft_min_score and exec_risk_norm <= soft_max_risk:
                        is_soft_pass = True
                        soft_reason = "range_soft_fail"

                ok = 1 if is_hard_pass else 0
                indicators["ok_soft"] = int(is_soft_pass)

                # Export range-specific diagnostics to evidence/indicators
                indicators["range_abs_ok"] = int(abs_leg)
                indicators["range_micro_ok"] = int(micro_leg)
                indicators["range_edge_ok"] = int(edge_leg)
                if is_soft_pass:
                    indicators["range_ok_soft"] = 1
                    indicators["range_soft_reason"] = soft_reason
                    # We also stash it in evidence dict later (it's constructed below)

                # build a small dec-like object for downstream (strategy expects have/need/scenario/reason)
                dec = type("GateDec", (), {})()
                dec.ok = bool(ok)
                dec.have = int(have)
                dec.need = int(need)
                dec.scenario = "range_meanrev"
                dec.reason = "range_meanrev"
                dec.gate_bits = int(getattr(dec, "gate_bits", 0))
                # Attach soft fail info to dec for downstream if needed, or just rely on evidence
                if is_soft_pass:
                    dec.ok_soft = 1
                    dec.soft_reason = soft_reason
            elif scenario_v4 == "vol_shock_news_proxy":
                # Vol shock policy: optional fail-closed, otherwise need=4 and stricter caps.
                fail_closed = bool(int(cfg2.get("vol_shock_fail_closed", 0) or 0))

                # B2: Strict execution risk cap (mandatory for vol shock)
                exec_norm_max = _f(cfg2.get("vol_shock_exec_risk_norm_max", 1.0), 1.0)
                exec_bps_max = _f(cfg2.get("vol_shock_exec_risk_max_bps", 20.0), 20.0) # compat
                exec_cap_hit = bool(exec_risk_norm > exec_norm_max or exec_risk_bps > exec_bps_max)

                # Add diagnostics for policy
                indicators["policy_vol_shock_exec_risk_norm_max"] = float(exec_norm_max)
                indicators["policy_vol_shock_exec_risk_max_bps"] = float(exec_bps_max)
                indicators["policy_vol_shock_exec_risk_cap_hit"] = int(exec_cap_hit)

                # Add boolean leg for exec risk to legs map
                legs["vol_shock_exec_risk_ok"] = 0 if exec_cap_hit else 1

                if fail_closed:
                    ok = 0
                    if dec is None:
                        dec = type("GateDec", (), {})()
                    dec.ok = False
                    dec.have = int(have)
                    dec.need = int(need)
                    dec.scenario = "vol_shock_news_proxy"
                    dec.reason = "vol_shock_fail_closed"
                else:
                    abs_leg = bool(abs_lvl_ok or fp_edge_ok or abs_ok)
                    micro_leg = bool(obi_stable or (ofi_dir_ok and ofi_stable))
                    r_leg = bool(reclaim_recent)
                    i_leg = bool(iceberg_strict)

                    # Store composite legs for accurate reporting of what caused veto
                    legs["vs_abs_leg"] = 1 if abs_leg else 0
                    legs["vs_micro_leg"] = 1 if micro_leg else 0
                    legs["vs_r_leg"] = 1 if r_leg else 0
                    legs["vs_i_leg"] = 1 if i_leg else 0

                    have_vs = int(abs_leg) + int(micro_leg) + int(r_leg) + int(i_leg)
                    need_vs = int(cfg2.get("strong_need_vol_shock", 4) or 4)

                    score_min_vs = _f(cfg2.get("of_score_min_vol_shock", max(score_min, 0.70)), max(score_min, 0.70))

                    # Logic: must satisfy exec cap AND have>=need AND score
                    if exec_cap_hit:
                        ok = 0
                        reason = "vol_shock_exec_risk_cap"
                    elif have_vs < need_vs:
                        ok = 0
                        reason = "vol_shock_need_failed"
                    elif score < score_min_vs:
                        ok = 0
                        reason = "vol_shock_score_veto"
                    else:
                        ok = 1
                        reason = "vol_shock_strict"

                    dec = type("GateDec", (), {})()
                    dec.ok = bool(ok)
                    dec.have = int(have_vs)
                    dec.need = int(need_vs)
                    dec.scenario = "vol_shock_news_proxy"
                    dec.reason = reason
            elif scenario_v4 == "saw_chop_spoof_proxy":
                # Saw/chop policy: very strict. Requires strong microstructure + absorption + reclaim/iceberg.
                fail_closed = bool(int(cfg2.get("saw_chop_fail_closed", 0) or 0))

                # B2: Exec risk cap also applies here (spoofing often widens spread)
                exec_norm_max = _f(cfg2.get("saw_chop_exec_risk_norm_max", 1.0), 1.0)
                exec_cap_hit = bool(exec_risk_norm > exec_norm_max)
                indicators["policy_saw_chop_exec_risk_norm_max"] = float(exec_norm_max)
                indicators["policy_saw_chop_exec_risk_cap_hit"] = int(exec_cap_hit)
                legs["saw_chop_exec_risk_ok"] = 0 if exec_cap_hit else 1

                # Legs for saw/chop (hard evidence required)
                # We require SPECIFIC quality legs: iceberg_strict, ofi_stable, fp_edge_absorb.
                # Just "have >= need" isn't enough if it's made of weak signals.
                l_ice = bool(iceberg_strict)
                l_ofi = bool(ofi_dir_ok and ofi_stable)
                l_fp = bool(fp_edge_ok)
                l_rec = bool(reclaim_recent) # extra strict leg

                # Store composite legs for accurate reporting
                legs["sc_ice_leg"] = 1 if l_ice else 0
                legs["sc_ofi_leg"] = 1 if l_ofi else 0
                legs["sc_fp_leg"] = 1 if l_fp else 0
                legs["sc_rec_leg"] = 1 if l_rec else 0

                hard_evidence_ok = bool(l_ice and l_ofi and l_fp)

                have_sc = int(l_ice) + int(l_ofi) + int(l_fp) + int(l_rec)
                need_sc = int(cfg2.get("strong_need_saw_chop", 3) or 3) # default 3 (ice+ofi+fp)
                # If configured higher (e.g. 4), we also need reclaim

                score_min_sc = _f(cfg2.get("of_score_min_saw_chop", max(score_min, 0.75)), max(score_min, 0.75))

                if fail_closed:
                    ok = 0
                    reason = "saw_chop_fail_closed"
                elif exec_cap_hit:
                    ok = 0
                    reason = "saw_chop_exec_risk_cap"
                elif not hard_evidence_ok:
                    ok = 0
                    reason = "saw_chop_missing_hard_evidence"
                elif have_sc < need_sc:
                    ok = 0
                    reason = "saw_chop_need_failed"
                elif score < score_min_sc:
                    ok = 0
                    reason = "saw_chop_score_veto"
                else:
                    ok = 1
                    reason = "saw_chop_strict"

                dec = type("GateDec", (), {})()
                dec.ok = bool(ok)
                dec.have = int(have_sc)
                dec.need = int(need_sc)
                dec.scenario = "saw_chop_spoof_proxy"
                dec.reason = reason

        # FIX: SRE Bug - scenario_v4 must NEVER bypass critical system vetoes!
        if gate_vetoed or burst_veto == 1 or hard_veto:
            ok = 0

        # Required legs by scenario v4 (for missing_legs in UI/Telegram)
        req = []
        missing = []  # fail-safe: always bind before conditional branches
        if scenario_v4 == "range_meanrev":
            req = ["absorption", "obi_stable", "iceberg_strict"]  # coarse view; abs_lvl/fp_edge/ofi are alternatives
        elif scenario_v4 == "vol_shock_news_proxy":
            req = ["vs_abs_leg", "vs_micro_leg", "vs_r_leg", "vs_i_leg", "vol_shock_exec_risk_ok"]
        elif scenario_v4 == "saw_chop_spoof_proxy":
            req = ["sc_ice_leg", "sc_ofi_leg", "sc_fp_leg", "saw_chop_exec_risk_ok"] # Hard evidence required
        elif scenario_base == "continuation":
            req = ["leg_a", "leg_b", "leg_c"]
            _map = {"leg_a": "hidden_ctx_recent", "leg_b": "obi_stable", "leg_c": "cont_ctx_recent"}
            missing = [_map.get(k, k) for k in req if int(legs.get(k, 0)) == 0]
        elif scenario_base == "reversal":
            req = ["leg_a", "leg_b", "leg_c"]
            _map = {"leg_a": "abs_lvl_ok", "leg_b": "obi_stable", "leg_c": "reclaim_recent"}
            missing = [_map.get(k, k) for k in req if int(legs.get(k, 0)) == 0]

        if scenario_base not in ("continuation", "reversal"):
            missing = [k for k in req if int(legs.get(k, 0)) == 0]

        # --- Diagnostic: export missing legs ---
        if missing:
            indicators["strong_gate_missing"] = ",".join(missing)

        # --- Soft-fail (analytics-only, VIRTUAL signals only — zero capital risk) ---
        ok_soft = int(indicators.get("ok_soft", 0))
        soft_reason = ""
        if ok_soft == 1:
            soft_reason = (indicators.get("range_soft_reason", ""))

        try:
            # Only absolute hard-vetoes block ok_soft classification.
            # Scenario-policy vetoes (saw_chop, vol_shock, liqmap) are analytics-relevant
            # and ok_soft is specifically designed to capture near-miss stats for them.
            _absolute_hard_vetoes = {"dq_gate"}
            _hard_veto_blocks_soft = bool(hard_veto and hard_veto in _absolute_hard_vetoes)

            if _hard_veto_blocks_soft:
                ok_soft = 0
                soft_reason = f"hard_veto:{hard_veto}"
                indicators["ok_soft_reason"] = soft_reason
            elif ok_soft == 1:
                # Already set by scenario policy (e.g., range_meanrev)
                if not soft_reason:
                    soft_reason = "scenario_soft_pass"
                indicators["ok_soft_reason"] = soft_reason
            elif int(ok) == 0 and int(need) > 0 and int(have) >= int(need) - 2:
                # near-miss: allow signals missing up to 2 legs (was: exactly 1)
                soft_score_min = float(os.getenv("OF_SOFT_SCORE_MIN") or cfg.get("soft_score_min") or 0.55)
                soft_exec_max = float(os.getenv("OF_SOFT_EXEC_RISK_NORM_MAX") or cfg.get("soft_exec_risk_norm_max") or 0.80)
                if float(score) >= soft_score_min and float(exec_risk_norm) <= soft_exec_max:
                    ok_soft = 1
                    _miss_gap = int(need) - int(have)
                    soft_reason = f"near_miss_{_miss_gap}"
                    indicators["ok_soft_reason"] = soft_reason
                else:
                    # Diagnostic: why ok_soft was blocked despite being near-miss
                    indicators["ok_soft_blocker"] = (
                        "score" if float(score) < soft_score_min else "exec_risk"
                    )
            elif int(ok) == 0 and hard_veto and not _hard_veto_blocks_soft:
                soft_reason = f"policy_veto:{hard_veto}"
                indicators["ok_soft_reason"] = soft_reason

            indicators["ok_soft"] = int(ok_soft)
            evidence["ok_soft"] = int(ok_soft)
            if soft_reason:
                evidence["soft_reason"] = soft_reason

            if ok_soft == 1:
                _miss_gap = int(need) - int(have)
                final_reason = f"near_miss_{_miss_gap}|{getattr(dec, 'reason', fallback_reason)}"
                if dec:
                    dec.reason = final_reason
        except Exception:
            pass

        final_reason = str(getattr(dec, "reason", fallback_reason))
        if dec is not None:
             final_reason = f"{final_reason}({have}/{need})"

        if gate_vetoed and gate_reason:
            final_reason = f"{gate_reason}(veto)"

        if hard_veto:
            final_reason = f"{hard_veto}|{final_reason}"

        # Capture ok after rule-scoring, BEFORE late-stage vetos (liq, taker_flow).
        # Passed to ml_gate as ok_rule so it can accumulate calibration data for
        # rule-passed signals independently of downstream veto gates.
        ok_pre_late_veto = int(ok)

        # P8c: Liq Pressure Veto
        if liq_veto == 1:
            ok = 0
            # If reason not set, default it
            if not liq_reason:
                liq_reason = "liq_veto"
            # Prepend to be visible
            final_reason = f"{liq_reason}(veto)|{final_reason}"

        # P9c: Taker-Flow contra Veto (ENFORCE mode only)
        if int(indicators.get("taker_flow_gate_veto", 0) or 0) == 1:
            ok = 0
            tr = (indicators.get("taker_flow_gate_reason", "taker_flow_contra") or "taker_flow_contra")
            final_reason = f"taker_flow:{tr}(veto)|{final_reason}"
            with contextlib.suppress(Exception):
                dec.gate_bits = int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_TAKER_FLOW

        _t_stage = _snap_stage("gates", _t_stage)

        # --- legs_detail (D1) ---
        legs_detail = []

        # OFI
        legs_detail.append({
            "name": "ofi_leg",
            "pass": int(ofi_leg),
            "value": {
                "ofi_z": float(ofi_z),
                "stab": float(ofi_stability_score),
                "stable_secs": float(ofi_stable_secs)
            },
            "why": "ofi_stable && dir_ok"
        })

        # FP Edge
        legs_detail.append({
            "name": "fp_edge_absorb",
            "pass": int(fp_edge_absorb),
            "value": {
                "abs_lvl_score": float(abs_lvl_score),
                "poc_edge": int(indicators.get("abs_lvl_poc_edge", 0) or 0)
            },
            "why": "edge absorb proxy"
        })

        # Exec Risk
        legs_detail.append({
            "name": "exec_risk",
            "pass": int(exec_risk_norm <= 0.75),
            "value": {
                "bps": float(exec_risk_bps),
                "norm": float(exec_risk_norm)
            },
            "why": "penalty + caps in vol_shock"
        })

        # OBI
        legs_detail.append({
            "name": "obi_stable",
            "pass": int(obi_stable),
            "value": {
                "obi": float(obi_val),
                "stable_secs": float(obi_stable_secs)
            },
            "why": "stable book imbalance"
        })

        # Iceberg
        legs_detail.append({
            "name": "iceberg_strict",
            "pass": int(iceberg_strict),
            "value": {
                "refresh": int(iceberg_refresh),
                "dur": float(iceberg_duration)
            },
            "why": "strict iceberg"
        })

        # Abs Lvl
        legs_detail.append({
            "name": "abs_lvl_ok",
            "pass": int(abs_lvl_ok),
            "value": {
                "score": float(abs_lvl_score),
                "bias": str(abs_lvl_bias)
            },
            "why": "absorption-on-level"
        })

        # Weak Progress
        legs_detail.append({
            "name": "weak_progress",
            "pass": int(wp_any),
            "value": {},
            "why": "inefficient move"
        })

        # Reclaim
        legs_detail.append({
            "name": "reclaim_recent",
            "pass": int(reclaim_recent),
            "value": {
                "hold_bars": int(reclaim_hold_bars)
            },
            "why": "reclaim"
        })

        # missing = [k for k, v in legs.items() if int(v) == 0]

        evidence.update({
            "delta_z": float(delta_z),
            "weak_progress": int(wp_any),
            "sweep": int(sweep_recent),
            "reclaim": int(reclaim_recent),
            "reclaim_hold_bars": int(reclaim_hold_bars),
            "obi_dir_ok": int(obi_dir_ok),
            "obi": float(obi_val),
            "obi_stable": int(obi_stable),
            "obi_stable_secs": float(obi_stable_secs),
            "iceberg_dir_ok": int(iceberg_dir_ok),
            "iceberg_refresh": int(iceberg_refresh),
            "iceberg_duration": float(iceberg_duration),
            "iceberg_strict": int(iceberg_strict),
            "absorption": int(abs_ok),
            "absorption_volume": float(abs_vol),
            "obi_age_ms": int(indicators.get("obi_age_ms", -1)),
            "iceberg_age_ms": int(indicators.get("iceberg_age_ms", -1)),
            "sweep_age_ms": int(indicators.get("sweep_age_ms", -1)),
            "reclaim_age_ms": int(indicators.get("reclaim_age_ms", -1)),
            "abs_lvl_ok": int(abs_lvl_ok),
            "abs_lvl_score": float(abs_lvl_score),
            "abs_lvl_bias": str(abs_lvl_bias),
            "abs_lvl_dir_match": int(abs_lvl_dir_match),
            "fp_move_bp": float(_get_attr_or_key(bar, 'fp_move_bp', 0.0) if bar else 0.0),
            "fp_eff_quote": float(_get_attr_or_key(bar, 'fp_eff_quote', 0.0) if bar else 0.0),
            "fp_quote_delta": float(_get_attr_or_key(bar, 'fp_quote_delta', 0.0) if bar else 0.0),
            "ofi_dir_ok": int(ofi_dir_ok),
            "ofi": float(ofi_val),
            "ofi_z": float(ofi_z),
            "ofi_stable": int(ofi_stable),
            "ofi_stable_secs": float(ofi_stable_secs),
            "ofi_stability_score": float(ofi_stability_score),
            "ofi_age_ms": int(indicators.get("ofi_age_ms", -1)),
            "ofi_leg": int(ofi_leg),

            # FP edge absorb (A2)
            "fp_edge_absorb": int(fp_edge_absorb),
            "fp_edge_strength": float(fp_edge_strength),
            "fp_edge_range_expansion": int(fp_edge_rng),
            "fp_edge_bias": str(fp_edge_bias),
            "fp_edge_age_ms": int(indicators.get("fp_edge_age_ms", -1)),

            # --- L3-lite diagnostics ---
            "cancel_bid_rate_ema": float(_f(indicators.get("cancel_bid_rate_ema", 0.0), 0.0)),
            "cancel_ask_rate_ema": float(_f(indicators.get("cancel_ask_rate_ema", 0.0), 0.0)),
            "taker_buy_rate_ema": float(_f(indicators.get("taker_buy_rate_ema", 0.0), 0.0)),
            "taker_sell_rate_ema": float(_f(indicators.get("taker_sell_rate_ema", 0.0), 0.0)),
            "cancel_spike_veto": int(gate_vetoed),
            "cancel_spike_ratio_support": float(gate_meta.get("ratio_support", 0.0) if isinstance(gate_meta, dict) else 0.0),
            "cancel_spike_z_support": float(gate_meta.get("z_support", 0.0) if isinstance(gate_meta, dict) else 0.0),

            # P8c: Liquidity Pressure Gate Evidence
            "liq_pressure_boost": float(liq_boost),
            "liq_pressure_pen": float(liq_pen),
            "liq_pressure_veto": int(liq_veto),
            "liq_pressure_reason": str(liq_reason),
            "liq_q_align": int(liq_q_align),
            "liq_ofi_align": int(liq_ofi_align),

            "taker_flow_imb": float(_f(indicators.get("taker_flow_imb", 0.0), 0.0)),
            "taker_flow_imb_z": float(_f(indicators.get("taker_flow_imb_z", 0.0), 0.0)),
            "taker_flow_score_adj": float(taker_score_adj),
            "taker_flow_veto": int(taker_veto),
            "taker_flow_soft": int(taker_soft),
            "taker_flow_reason": str(taker_reason),
        })

        # Add burst snapshot to evidence
        if burst_snap:
            evidence.update(burst_snap)
        evidence["burst_reason"] = str(burst_reason)



        # ------------------------------------------------------------------
        # ML confirm gate (Step C1/D/4): after hard vetoes, before final decision.
        # Modes:
        #   OFF    -> no effect
        #   SHADOW -> attach p_edge but never block
        #   ENFORCE-> require p_edge >= threshold (fail policy applied inside MLConfirmGate)
        # ------------------------------------------------------------------
        try:
            if self._ml_gate is None and MLConfirmGate is not None:
                self._ml_gate = MLConfirmGate.from_env()

            # Prefer scenario_v4 for ML bucketization when dec.scenario is legacy (reversal/continuation)
            # This ensures ML v10.4 util_mh always gets v4 scenario for correct bucket selection and util_floor_by_bucket
            ml_scenario = str(getattr(dec, "scenario", "") if dec else "") or str(scenario)

            # If legacy scenario, try to use indicators["scenario_v4"] (set by engine / strategy)
            if ml_scenario.lower() in ("reversal", "continuation", "none", ""):
                sv4 = ""
                try:
                    sv4 = (indicators.get("scenario_v4", "") or "")
                except Exception:
                    sv4 = ""
                if sv4:
                    ml_scenario = sv4
                # Fallback: use computed scenario_v4 if available (from line 712)
                elif scenario_v4 and scenario_v4.lower() not in ("reversal", "continuation", "none", ""):
                    ml_scenario = scenario_v4

            # Ensure scenario_v4 is in indicators for ML feature extraction
            indicators_with_v4 = dict(indicators, delta_z=float(delta_z))

            # Enrich indicators passed to ML gate for deterministic replay/dataset alignment
            try:
                _sb_raw = indicators_with_v4.get('score_breakdown')
                sb = _sb_raw if isinstance(_sb_raw, dict) else {}
                sb_small = {
                    'agg': (sb.get('agg', '')) ,
                    'raw_sum': float(sb.get('raw_sum', 0.0) or 0.0),
                    'w_sum': float(sb.get('w_sum', 0.0) or 0.0),
                    'base_score': float(sb.get('base_score', 0.0) or 0.0),
                    'exec_pen': float(sb.get('exec_pen', 0.0) or 0.0),
                    'final_score_raw': float(sb.get('final_score_raw', sb.get('final_score', 0.0)) or 0.0),
                    'final_score_01': float(sb.get('final_score_01', 0.0) or 0.0),
                }
                indicators_with_v4.update({
                    'scenario_v4': str(scenario_v4),
                    'of_base_score': float(sb_small['base_score']),
                    'of_score_final_raw': float(sb_small['final_score_raw']),
                    'of_score_final': float(sb_small['final_score_01']),
                    'have': int(have),
                    'need': int(need),
                    'ok_soft': int(ok_soft),
                    'score_breakdown_small': sb_small,
                    # legs (explicit, stable keys for training/metrics)
                    'leg_obi_stable': int(obi_stable),
                    'leg_iceberg_strict': int(iceberg_strict),
                    'leg_sweep_recent': int(sweep_recent),
                    'leg_reclaim_recent': int(reclaim_recent),
                    'leg_weak_progress': int(wp_any),
                    'leg_abs_lvl_ok': int(abs_lvl_ok),
                    'leg_ofi_leg': int(ofi_leg),
                    'leg_fp_edge_absorb': int(fp_edge_absorb),
                    # key evidences (compact)
                    'obi': float(evidence.get('obi', 0.0) or 0.0),
                    'obi_stable': int(evidence.get('obi_stable', 0) or 0),
                    'obi_stable_secs': float(evidence.get('obi_stable_secs', 0.0) or 0.0),
                    'iceberg_strict': int(evidence.get('iceberg_strict', 0) or 0),
                    'iceberg_refresh': int(evidence.get('iceberg_refresh', 0) or 0),
                    'iceberg_duration': float(evidence.get('iceberg_duration', 0.0) or 0.0),
                    'abs_lvl_ok': int(evidence.get('abs_lvl_ok', 0) or 0),
                    'abs_lvl_score': float(evidence.get('abs_lvl_score', 0.0) or 0.0),
                    'fp_edge_absorb': int(evidence.get('fp_edge_absorb', 0) or 0),
                    'fp_edge_strength': float(evidence.get('fp_edge_strength', 0.0) or 0.0),
                    'ofi_z': float(evidence.get('ofi_z', 0.0) or 0.0),
                    'ofi_stability_score': float(evidence.get('ofi_stability_score', 0.0) or 0.0),
                })

                # P28: Enrich indicators_with_v4 with runtime-only micro stats (needed by v6; harmless for older schemas)
                try:
                    # Canonical have/need ratio (may be produced elsewhere as of_confirm_have_need_ratio)
                    indicators_with_v4["have_need_ratio"] = float(
                        getattr(runtime, "last_of_confirm_have_need_ratio", 0.0) or
                        indicators.get("of_confirm_have_need_ratio", 0.0) or
                        (float(have) / float(need) if int(need) > 0 else 0.0)
                    )
                    # Book staleness: reuse existing liq metric if present
                    indicators_with_v4["book_staleness_ms"] = float(
                        indicators.get("liq_book_stale_ms", 0.0) or evidence.get("book_stale_ms", 0.0) or 0.0
                    )
                    # Stability stats (live on runtime)
                    indicators_with_v4["last_spread_z"] = float(getattr(runtime, "last_spread_z", 0.0) or 0.0)
                    indicators_with_v4["book_rate_z"] = float(getattr(runtime, "book_rate_z", 0.0) or 0.0)
                    indicators_with_v4["book_churn_score"] = float(getattr(runtime, "book_churn_score", 0.0) or 0.0)
                    indicators_with_v4["pressure_sps"] = float(getattr(runtime, "pressure_sps", 0.0) or 0.0)
                    indicators_with_v4["cooldown_hit_rate_ema"] = float(indicators.get("cooldown_hit_rate_ema", 0.0) or 0.0)

                    # L3-lite rates
                    l3 = getattr(runtime, "l3_stats", None)
                    if l3 is not None:
                        indicators_with_v4["taker_buy_rate_ema"] = float(getattr(l3, "taker_buy_rate_ema", 0.0) or 0.0)
                        indicators_with_v4["taker_sell_rate_ema"] = float(getattr(l3, "taker_sell_rate_ema", 0.0) or 0.0)
                        indicators_with_v4["cancel_bid_rate_ema"] = float(getattr(l3, "cancel_bid_rate_ema", 0.0) or 0.0)
                        indicators_with_v4["cancel_ask_rate_ema"] = float(getattr(l3, "cancel_ask_rate_ema", 0.0) or 0.0)

                    # Hawkes-like online intensities snapshot (dict)
                    hs = getattr(runtime, "hawkes_snapshot", None)
                    if isinstance(hs, dict):
                        for hk in ("hawkes_taker_lam", "hawkes_cancel_lam", "hawkes_churn_lam"):
                            try:
                                indicators_with_v4[hk] = float(hs.get(hk, 0.0) or 0.0)
                            except Exception:
                                indicators_with_v4[hk] = 0.0
                except Exception:
                    pass
            except Exception:
                pass

            if scenario_v4:
                indicators_with_v4["scenario_v4"] = str(scenario_v4)

            # Phase 8.4: gate trace features — scenario code, pass/fail group, strong-need state.
            try:
                _scn_code_map = {
                    "trend": 1, "continuation": 1,
                    "range": 2, "range_meanrev": 2,
                    "reversal": 3,
                    "chop": 4, "saw_chop_spoof_proxy": 4,
                    "breakout": 5, "vol_shock_news_proxy": 5,
                }
                indicators_with_v4.setdefault(
                    "of_confirm_scenario",
                    float(_scn_code_map.get(str(scenario_v4 or "").lower(), 0)),
                )
                _ok_v = int(ok)
                _have_v = int(have)
                _need_v = int(need)
                _rg = 1 if _ok_v else (2 if _have_v >= max(1, _need_v - 1) else 3)
                indicators_with_v4.setdefault("of_confirm_reason_group", float(_rg))
                _sn = bool(
                    int(cfg2.get("strong_need_reversal", 0) or 0)
                    or int(cfg2.get("strong_need_continuation", 0) or 0)
                )
                indicators_with_v4.setdefault("strong_need", float(1 if _sn else 0))
                indicators_with_v4.setdefault("strong_have", float(_have_v) if _sn else 0.0)
                # 4.11: explicit rule_have / rule_need aliases (have/need already in dict
                # but under short names; schema uses rule_* for clarity)
                indicators_with_v4.setdefault("rule_have", float(_have_v))
                indicators_with_v4.setdefault("rule_need", float(_need_v))
            except Exception:
                indicators_with_v4.setdefault("of_confirm_scenario", 0.0)
                indicators_with_v4.setdefault("of_confirm_reason_group", 0.0)
                indicators_with_v4.setdefault("strong_need", 0.0)
                indicators_with_v4.setdefault("strong_have", 0.0)
                indicators_with_v4.setdefault("rule_have", 0.0)
                indicators_with_v4.setdefault("rule_need", 0.0)

            # ------------------------------------------------------------------
            # Phase 2.3B: feature alias bridge for v5 serving path.
            # Ensures MLFeatureSchemaV5OF canonical keys (vol_ratio, vol_ratio_z,
            # max_expected_slippage_bps_eff, exec_fill_pen) reach the feature vector.
            # All setdefault — never overwrites values already present in indicators.
            # Fail-open per field so a single bad cast never aborts inference.
            # ------------------------------------------------------------------
            try:
                if "vol_ratio" not in indicators_with_v4 and "vol_ratio_fast_slow" in indicators_with_v4:
                    indicators_with_v4["vol_ratio"] = float(indicators_with_v4.get("vol_ratio_fast_slow") or 0.0)
            except Exception:
                pass
            try:
                # vol_ratio_z may already be present via horizon_contract alias bridge
                if "vol_ratio_z" not in indicators_with_v4:
                    vrz = (
                        indicators.get("vol_ratio_z")
                        or indicators_with_v4.get("vol_ratio_z")
                    )
                    if vrz is not None:
                        indicators_with_v4["vol_ratio_z"] = float(vrz)
            except Exception:
                pass
            try:
                if "max_expected_slippage_bps_eff" not in indicators_with_v4 and "expected_slippage_bps" in indicators_with_v4:
                    indicators_with_v4["max_expected_slippage_bps_eff"] = float(
                        indicators_with_v4.get("expected_slippage_bps") or 0.0
                    )
            except Exception:
                pass
            try:
                if "exec_fill_pen" not in indicators_with_v4 and "exec_risk_norm" in indicators_with_v4:
                    indicators_with_v4["exec_fill_pen"] = float(
                        indicators_with_v4.get("exec_risk_norm") or 0.0
                    )
            except Exception:
                pass

            # ------------------------------------------------------------------
            # Phase 6: horizon-aware + ATR feature aliases for MLFeatureSchemaV5OF.
            # Source priority: indicators > indicators_with_v4 > derived.
            # Fail-open per field — never aborts ML gate inference.
            # ------------------------------------------------------------------
            _HZ_ONE_HOUR_MS = 3_600_000.0

            # atr_tf_ms — pass-through; source is signals meta.atr_profile.atr_tf_ms
            try:
                if "atr_tf_ms" not in indicators_with_v4:
                    v = (
                        indicators.get("atr_tf_ms")
                        or indicators.get("atr_tf_used_ms")
                        or indicators_with_v4.get("atr_tf_ms")
                    )
                    if v is not None:
                        indicators_with_v4["atr_tf_ms"] = float(v)
                    else:
                        indicators_with_v4["atr_tf_ms"] = 0.0
            except Exception:
                indicators_with_v4.setdefault("atr_tf_ms", 0.0)

            # atr_stop_pct — ATR / entry_price * 100; proxy from atr_bps / 100
            try:
                if "atr_stop_pct" not in indicators_with_v4:
                    atr_bps_raw = float(indicators.get("atr_bps") or indicators_with_v4.get("atr_bps") or 0.0)
                    indicators_with_v4["atr_stop_pct"] = atr_bps_raw / 100.0  # bps → percent
            except Exception:
                indicators_with_v4.setdefault("atr_stop_pct", 0.0)

            # atr_regime_pct — current atr_bps / regime threshold
            try:
                if "atr_regime_pct" not in indicators_with_v4:
                    atr_bps_raw2 = float(indicators.get("atr_bps") or indicators_with_v4.get("atr_bps") or 0.0)
                    atr_bps_th = float(
                        indicators.get("atr_bps_th")
                        or indicators.get("atr_floor_threshold")
                        or indicators_with_v4.get("atr_bps_th")
                        or 0.0
                    )
                    if atr_bps_th > 0.0:
                        indicators_with_v4["atr_regime_pct"] = atr_bps_raw2 / atr_bps_th
                    else:
                        indicators_with_v4["atr_regime_pct"] = 0.0
            except Exception:
                indicators_with_v4.setdefault("atr_regime_pct", 0.0)

            # hold_target_ms_norm — normalized to hours (0 = unknown)
            try:
                if "hold_target_ms_norm" not in indicators_with_v4:
                    htms = float(
                        indicators.get("hold_target_ms")
                        or indicators_with_v4.get("hold_target_ms")
                        or 0.0
                    )
                    indicators_with_v4["hold_target_ms_norm"] = htms / _HZ_ONE_HOUR_MS
            except Exception:
                indicators_with_v4.setdefault("hold_target_ms_norm", 0.0)

            # alpha_half_life_ms_norm — normalized to hours (0 = unknown)
            try:
                if "alpha_half_life_ms_norm" not in indicators_with_v4:
                    ahlms = float(
                        indicators.get("alpha_half_life_ms")
                        or indicators_with_v4.get("alpha_half_life_ms")
                        or 0.0
                    )
                    indicators_with_v4["alpha_half_life_ms_norm"] = ahlms / _HZ_ONE_HOUR_MS
            except Exception:
                indicators_with_v4.setdefault("alpha_half_life_ms_norm", 0.0)

            # vol_ratio_fast_slow — already in indicators if horizon_contract was applied
            try:
                if "vol_ratio_fast_slow" not in indicators_with_v4:
                    vrf = (
                        indicators.get("vol_ratio_fast_slow")
                        or indicators_with_v4.get("vol_ratio")
                    )
                    indicators_with_v4["vol_ratio_fast_slow"] = float(vrf) if vrf is not None else 1.0
            except Exception:
                indicators_with_v4.setdefault("vol_ratio_fast_slow", 1.0)

            # max_signal_age_ratio — (now_ms - ts_ms) / max_signal_age_ms
            try:
                if "max_signal_age_ratio" not in indicators_with_v4:
                    max_age_ms = float(
                        indicators.get("max_signal_age_ms")
                        or indicators_with_v4.get("max_signal_age_ms")
                        or 0.0
                    )
                    if max_age_ms > 0.0:
                        sig_age_ms = float(now_ts) - float(
                            indicators.get("signal_ts_ms") or indicators_with_v4.get("ts_ms") or now_ts
                        )
                        indicators_with_v4["max_signal_age_ratio"] = max(0.0, sig_age_ms / max_age_ms)
                        indicators_with_v4.setdefault("signal_age_ms", max(0.0, sig_age_ms))
                    else:
                        indicators_with_v4["max_signal_age_ratio"] = 0.0
            except Exception:
                indicators_with_v4.setdefault("max_signal_age_ratio", 0.0)

            # ------------------------------------------------------------------
            # Phase 7: P1 features — execution cost ratios, signal age, vol
            # dynamics, DQ/freshness. All fail-open: exception -> setdefault 0.0.
            # Inputs already in indicators_with_v4 by this point.
            # ------------------------------------------------------------------
            try:
                _eps = 1e-6
                _fee_bps = float(os.getenv("TAKER_FEE_BPS", "4.0") or "4.0")
                _sl_atr_mult = float(os.getenv("SL_ATR_MULT", "1.0") or "1.0")
                _half_spread = float(indicators_with_v4.get("spread_bps") or 0.0) * 0.5
                _slippage = float(indicators_with_v4.get("expected_slippage_bps") or 0.0)
                _exec_cost = max(0.0, _half_spread + _slippage + _fee_bps)
                _atr_bps = 0.0
                if _raw_atr := indicators_with_v4.get("atr_bps"):
                    _atr_bps = float(_raw_atr)  # noqa: RUF100
                _tp1_bps = float(
                    indicators_with_v4.get("liqmap_gate_reward_bps")
                    or indicators_with_v4.get("tp1_bps")
                    or indicators_with_v4.get("pred_tp1_bps")
                    or 0.0
                )
                _sl_bps = float(
                    indicators_with_v4.get("liqmap_gate_risk_bps")
                    or indicators_with_v4.get("sl_bps")
                    or (_atr_bps * _sl_atr_mult if _atr_bps > 0.0 else 0.0)
                )
                _tp1_ratio = _exec_cost / max(_tp1_bps, _eps) if _tp1_bps > 0.0 else 0.0
                _sl_ratio = _exec_cost / max(_sl_bps, _eps) if _sl_bps > 0.0 else 0.0
                _atr_ratio = _exec_cost / max(_atr_bps, _eps) if _atr_bps > 0.0 else 0.0
                indicators_with_v4.setdefault("exec_cost_to_tp1_ratio", _tp1_ratio)
                indicators_with_v4.setdefault("exec_cost_to_sl_ratio", _sl_ratio)
                indicators_with_v4.setdefault("exec_cost_to_atr_ratio", _atr_ratio)
                if _tp1_ratio == 0.0 or _sl_ratio == 0.0 or _atr_ratio == 0.0:
                    try:
                        from services.orderflow.metrics import ml_p7_feature_zero_total
                        if _tp1_ratio == 0.0:
                            ml_p7_feature_zero_total.labels(feature="exec_cost_to_tp1_ratio", symbol=symbol).inc()
                        if _sl_ratio == 0.0:
                            ml_p7_feature_zero_total.labels(feature="exec_cost_to_sl_ratio", symbol=symbol).inc()
                        if _atr_ratio == 0.0:
                            ml_p7_feature_zero_total.labels(feature="exec_cost_to_atr_ratio", symbol=symbol).inc()
                    except Exception:
                        pass
            except Exception:
                indicators_with_v4.setdefault("exec_cost_to_tp1_ratio", 0.0)
                indicators_with_v4.setdefault("exec_cost_to_sl_ratio", 0.0)
                indicators_with_v4.setdefault("exec_cost_to_atr_ratio", 0.0)

            try:
                _sig_age = float(indicators_with_v4.get("signal_age_ms") or 0.0)
                _ahl_ms = float(
                    indicators_with_v4.get("alpha_half_life_ms")
                    or indicators.get("alpha_half_life_ms")
                    or (float(indicators_with_v4.get("alpha_half_life_ms_norm") or 0.0) * 3_600_000.0)
                )
                _age_ratio = _sig_age / max(_ahl_ms, 1.0) if _ahl_ms > 0.0 else 0.0
                indicators_with_v4.setdefault("signal_age_ms", _sig_age)
                indicators_with_v4.setdefault("signal_age_to_half_life", _age_ratio)
                if _ahl_ms == 0.0:
                    try:
                        from services.orderflow.metrics import ml_p7_feature_zero_total
                        ml_p7_feature_zero_total.labels(feature="signal_age_to_half_life", symbol=symbol).inc()
                    except Exception:
                        pass
            except Exception:
                indicators_with_v4.setdefault("signal_age_ms", 0.0)
                indicators_with_v4.setdefault("signal_age_to_half_life", 0.0)

            try:
                _vrf = float(indicators_with_v4.get("vol_ratio_fast_slow") or 1.0)
                indicators_with_v4.setdefault("vol_expansion_score", max(0.0, _vrf - 1.0))
                indicators_with_v4.setdefault("vol_compression_score", max(0.0, 1.0 - _vrf))
            except Exception:
                indicators_with_v4.setdefault("vol_expansion_score", 0.0)
                indicators_with_v4.setdefault("vol_compression_score", 0.0)

            try:
                _dq_h = float(indicators_with_v4.get("dq_health_score") or indicators_with_v4.get("data_health") or 1.0)
                indicators_with_v4.setdefault("dq_score", _dq_h)
                # Flag count proxy: bucket dq_health into 0-3 severity levels
                if _dq_h >= 0.9:
                    _dq_flags = 0
                elif _dq_h >= 0.7:
                    _dq_flags = 1
                elif _dq_h >= 0.5:
                    _dq_flags = 2
                else:
                    _dq_flags = 3
                indicators_with_v4.setdefault("dq_flag_count", float(_dq_flags))
            except Exception:
                indicators_with_v4.setdefault("dq_score", 1.0)
                indicators_with_v4.setdefault("dq_flag_count", 0.0)

            try:
                _tick_lag = float(
                    indicators_with_v4.get("tick_gap_ms")
                    or indicators_with_v4.get("book_ts_gap_ms")
                    or 0.0
                )
                indicators_with_v4.setdefault("tick_lag_ms", _tick_lag)
                if _tick_lag == 0.0:
                    try:
                        from services.orderflow.metrics import ml_p7_feature_zero_total
                        ml_p7_feature_zero_total.labels(feature="tick_lag_ms", symbol=symbol).inc()
                    except Exception:
                        pass
            except Exception:
                indicators_with_v4.setdefault("tick_lag_ms", 0.0)

            # ------------------------------------------------------------------
            # Phase 7.2: Extended DQ — book freshness + CVD quarantine.
            # Sources already populated earlier in the pipeline; this block
            # promotes them into indicators_with_v4 for schema vectorization.
            # ------------------------------------------------------------------
            try:
                _book_age = float(
                    indicators_with_v4.get("book_staleness_ms")
                    or indicators.get("book_staleness_ms")
                    or indicators.get("liq_book_stale_ms")
                    or 0.0
                )
                indicators_with_v4.setdefault("book_age_ms", _book_age)
            except Exception:
                indicators_with_v4.setdefault("book_age_ms", 0.0)

            try:
                _book_gap = float(
                    indicators_with_v4.get("book_ts_gap_ms")
                    or indicators.get("book_ts_gap_ms")
                    or 0.0
                )
                indicators_with_v4.setdefault("book_gap_ms", _book_gap)
            except Exception:
                indicators_with_v4.setdefault("book_gap_ms", 0.0)

            try:
                _cvd_q = bool(int(indicators.get("cvd_quarantine_active", 0) or 0))
                indicators_with_v4.setdefault("cvd_quarantine_active", _cvd_q)
            except Exception:
                indicators_with_v4.setdefault("cvd_quarantine_active", False)

            # ------------------------------------------------------------------
            # Phase 4.9: DQ rolling window features — 1-minute sliding window.
            # Uses module-level _DQ_ROLLING_CACHE + _DQ_LAST_TS keyed by symbol.
            # Cold start (< 2 samples): all features default to 0.0.
            # Also adds book_update_rate_hz and book_staleness_z from existing gauges.
            # ------------------------------------------------------------------
            try:
                _dq_now_ms = now_ts
                _dq_lag = indicators_with_v4.get("tick_lag_ms") or 0.0
                _dq_last = _DQ_LAST_TS.get(symbol, 0)
                _dq_result = _dq_compute(
                    symbol=symbol,
                    now_ms=_dq_now_ms,
                    lag_ms=_dq_lag,
                    last_ts_ms=_dq_last,
                )
                _DQ_LAST_TS[symbol] = _dq_now_ms
                for _dqk, _dqv in _dq_result.items():
                    indicators_with_v4.setdefault(_dqk, _dqv)
                # book_update_rate_hz from existing indicator (set by book microstructure)
                _book_hz = float(indicators_with_v4.get("book_rate_hz") or
                                 indicators.get("book_rate_ema_hz") or 0.0)
                indicators_with_v4.setdefault("book_update_rate_hz", _book_hz)
                # book_staleness_z from existing robust z-score of book update rate
                _book_z = indicators_with_v4.get("book_rate_z") or 0.0
                indicators_with_v4.setdefault("book_staleness_z", _book_z)
            except Exception:
                for _dqk in (
                    "tick_lag_p95_1m", "tick_reorder_rate_1m",
                    "tick_dedupe_rate_1m", "tick_gap_count_1m",
                    "bad_time_streak", "book_update_rate_hz", "book_staleness_z",
                ):
                    indicators_with_v4.setdefault(_dqk, 0.0)

            # ------------------------------------------------------------------
            # Phase 7.3: ATR freshness — atr_fresh ∈ {True, False}.
            # True iff atr_age_ms is positive AND below ATR_FRESH_MS threshold.
            # Default 60s — 4x ATRCache TTL gives margin for irregular updates.
            # ------------------------------------------------------------------
            try:
                _atr_fresh_ms = float(os.getenv("ATR_FRESH_MS", "60000") or "60000")
                _atr_age = float(
                    indicators_with_v4.get("atr_age_ms")
                    or indicators.get("atr_age_ms")
                    or 0.0
                )
                _atr_fresh = bool(_atr_age > 0.0 and _atr_age < _atr_fresh_ms)
                indicators_with_v4.setdefault("atr_fresh", _atr_fresh)
                indicators_with_v4.setdefault("atr_age_ms", _atr_age)
            except Exception:
                indicators_with_v4.setdefault("atr_fresh", False)
                indicators_with_v4.setdefault("atr_age_ms", 0.0)

            # ------------------------------------------------------------------
            # Phase 7.4: Gate trace — derived from local have/need/missing/ok_soft.
            #   rule_have_need_gap   = have - need (can be negative)
            #   missing_legs_count   = len(missing)
            #   soft_fail_near_pass  = ok_soft flag (1 when have==need-1 + soft passed)
            #   gate_pressure_score  = (1 - have_need_ratio) * missing_legs_count
            # All locals (have/need/missing) bound at lines ~1995/2440 — always in scope.
            # ------------------------------------------------------------------
            try:
                _missing_count = len(missing) if isinstance(missing, list) else 0
                indicators_with_v4.setdefault("rule_have_need_gap", float(have - need))
                indicators_with_v4.setdefault("missing_legs_count", float(_missing_count))
                _ok_soft = int(indicators.get("ok_soft", 0) or 0)
                indicators_with_v4.setdefault("soft_fail_near_pass", _ok_soft != 0)
                _ratio = indicators_with_v4.get("have_need_ratio") or 0.0
                indicators_with_v4.setdefault(
                    "gate_pressure_score",
                    max(0.0, 1.0 - _ratio) * _missing_count,
                )
            except Exception:
                indicators_with_v4.setdefault("rule_have_need_gap", 0.0)
                indicators_with_v4.setdefault("missing_legs_count", 0.0)
                indicators_with_v4.setdefault("soft_fail_near_pass", False)
                indicators_with_v4.setdefault("gate_pressure_score", 0.0)

            # ------------------------------------------------------------------
            # Phase 7.5: Session / weekend flags + cyclical time encoding.
            # Session ranges in UTC (intentionally overlapping):
            #   Asia    00:00..08:00
            #   Europe  07:00..16:00
            #   US      13:00..22:00
            # weekend_flag: Sat(5) or Sun(6).
            # ctx_hour_utc / ctx_dow are set by build() at line ~2134; hour_utc
            # and dow live only in ctx_features, not in indicators_with_v4.
            # ------------------------------------------------------------------
            try:
                _h = float(indicators_with_v4.get("ctx_hour_utc") or 0.0)
                _d = float(indicators_with_v4.get("ctx_dow") or 0.0)
                indicators_with_v4.setdefault("session_asia", bool(0.0 <= _h < 8.0))
                indicators_with_v4.setdefault("session_europe", bool(7.0 <= _h < 16.0))
                indicators_with_v4.setdefault("session_us", bool(13.0 <= _h < 22.0))
                indicators_with_v4.setdefault("weekend_flag", bool(_d >= 5.0))
                # EU/US overlap window: 13:00–16:00 UTC (highest liquidity, tight spreads)
                indicators_with_v4.setdefault("session_overlap_eu_us", 13.0 <= _h < 16.0)
                # Phase 8.2: cyclical encoding avoids artificial midnight/day boundary
                _h_ang = 2.0 * math.pi * _h / 24.0
                _d_ang = 2.0 * math.pi * _d / 7.0
                indicators_with_v4.setdefault("hour_sin", math.sin(_h_ang))
                indicators_with_v4.setdefault("hour_cos", math.cos(_h_ang))
                indicators_with_v4.setdefault("dow_sin", math.sin(_d_ang))
                indicators_with_v4.setdefault("dow_cos", math.cos(_d_ang))
            except Exception:
                indicators_with_v4.setdefault("session_asia", False)
                indicators_with_v4.setdefault("session_europe", False)
                indicators_with_v4.setdefault("session_us", False)
                indicators_with_v4.setdefault("weekend_flag", False)
                indicators_with_v4.setdefault("session_overlap_eu_us", False)
                for _tf in ("hour_sin", "hour_cos", "dow_sin", "dow_cos"):
                    indicators_with_v4.setdefault(_tf, 0.0)

            # Phase 8.2: news_blackout — float 0/1 from news_gate_veto (set in build() ~line 2056)
            indicators_with_v4.setdefault(
                "news_blackout",
                float(indicators_with_v4.get("news_gate_veto") or 0),
            )

            # Phase 8.2: fixed-horizon fill probability — fail-open mirror from indicators.
            # Computed earlier at the fill_prob_proxy block (~line 1807); setdefault here
            # guards against the outer try-except swallowing that block on rare failures.
            for _fh_key in ("fill_prob_1s", "fill_prob_3s", "fill_prob_5s"):
                indicators_with_v4.setdefault(
                    _fh_key, float(indicators.get(_fh_key) or 0.0)
                )

            # ------------------------------------------------------------------
            # Phase 7.6: LOB velocity — slopes over 1s/3s rolling windows.
            # State stored in module-level _LOB_VELOCITY_CACHE keyed by symbol.
            # All values default to 0.0 on cold start (< 2 samples in buffer).
            # ------------------------------------------------------------------
            try:
                _velocity = _lob_velocity_compute(
                    symbol=symbol,
                    now_ms=int(now_ts),
                    obi=float(indicators_with_v4.get("obi") or 0.0),
                    qimb_wmean=float(indicators_with_v4.get("qimb_wmean") or 0.0),
                    depth_imbalance_5=float(indicators_with_v4.get("depth_imbalance_5") or 0.0),
                    spread_bps=float(indicators_with_v4.get("spread_bps") or 0.0),
                    fill_prob_proxy=float(indicators_with_v4.get("fill_prob_proxy") or 0.0),
                )
                for _vk, _vv in _velocity.items():
                    indicators_with_v4.setdefault(_vk, _vv)
                # Phase 4.4: micro mid-shift velocity/acceleration
                _micro = _lob_micro_compute(
                    symbol=symbol,
                    now_ms=int(now_ts),
                    microprice_shift_bps=float(
                        indicators_with_v4.get("l3_microprice_shift_bps_20") or 0.0
                    ),
                )
                for _mk, _mv in _micro.items():
                    indicators_with_v4.setdefault(_mk, _mv)
            except Exception:
                for _vk in (
                    "obi_slope_1s", "obi_slope_3s",
                    "qimb_slope_1s", "qimb_slope_3s",
                    "depth_imbalance_5_delta_1s", "depth_imbalance_5_delta_3s",
                    "spread_widen_velocity_bps_s", "fill_prob_decay_slope",
                    "obi_stability_decay", "book_churn_delta_1s", "book_churn_z",
                    "spread_mean_revert_score",
                    "micro_mid_shift_vel_bps_s", "micro_mid_shift_accel_bps_s2",
                ):
                    indicators_with_v4.setdefault(_vk, 0.0)

            # ------------------------------------------------------------------
            # Phase 7.8: Cross-context hydration — read pre-aggregated hashes
            # from ADR-0005 (TCA), ADR-0006 (anchors/liq/OI), ADR-0007 (PIT priors).
            # Lag-guard: stale entries ⇒ feature 0.0 (model can handle gracefully).
            # Single HMGET-per-source pattern, ~1ms additional p99 budget.
            # ------------------------------------------------------------------
            try:
                _now_ms_local = int(now_ts)
                _max_lag_ms = float(os.getenv("CROSS_CTX_MAX_LAG_MS", "2000") or "2000")

                # --- ADR-0006: BTC/ETH anchor returns ---
                for _short in ("btc", "eth"):
                    _key = f"ctx:anchor:{_short}:returns"
                    _data = {}
                    try:
                        _raw = _ctx_hgetall(self._redis_client, _key)  # type: ignore[attr-defined]
                        _data = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in (_raw or {}).items()
                        }
                    except Exception:
                        _data = {}
                    _hash_ts = float(_data.get("ts_ms") or 0.0)
                    _stale = (_now_ms_local - _hash_ts) > _max_lag_ms if _hash_ts > 0 else True
                    if _stale:
                        try:
                            from services.orderflow.metrics import ml_feature_stale_total
                            ml_feature_stale_total.labels(feature=f"{_short}_ret", symbol=symbol).inc()
                        except Exception:
                            pass
                    for _w in ("30s", "1m", "5m"):
                        _fname = f"{_short}_ret_{_w}"
                        _val = 0.0 if _stale else float(_data.get(f"ret_{_w}") or 0.0)
                        indicators_with_v4.setdefault(_fname, _val)

                # rel_ret_*_vs_btc = ret_target_window - btc_ret_window
                _btc_1m = float(indicators_with_v4.get("btc_ret_1m") or 0.0)
                _btc_5m = float(indicators_with_v4.get("btc_ret_5m") or 0.0)
                _eth_1m = float(indicators_with_v4.get("eth_ret_1m") or 0.0)
                indicators_with_v4.setdefault("rel_ret_1m_vs_btc", -_btc_1m)  # placeholder
                indicators_with_v4.setdefault("rel_ret_5m_vs_btc", -_btc_5m)  # placeholder

                # --- ADR-0006 extended: leader_confidence + market_risk_on ---
                # leader_confidence: direction consistency of BTC+ETH over 30s/1m/5m windows
                _btc_30s = float(indicators_with_v4.get("btc_ret_30s") or 0.0)
                _eth_30s = float(indicators_with_v4.get("eth_ret_30s") or 0.0)
                _eth_5m = float(indicators_with_v4.get("eth_ret_5m") or 0.0)
                _btc_signs = [math.copysign(1, r) if abs(r) > 1e-8 else 0.0 for r in (_btc_30s, _btc_1m, _btc_5m)]
                _eth_signs = [math.copysign(1, r) if abs(r) > 1e-8 else 0.0 for r in (_eth_30s, _eth_1m, _eth_5m)]
                _all_signs = _btc_signs + _eth_signs
                _n_signs = len(_all_signs)
                _lc = sum(_all_signs) / _n_signs if _n_signs else 0.0  # ∈ [-1, 1]
                indicators_with_v4.setdefault("leader_confidence", _lc)

                # market_risk_on_score: composite anchor return (BTC+ETH average over 1m)
                _risk_on = (_btc_1m + _eth_1m) / 2.0
                indicators_with_v4.setdefault("market_risk_on_score", _risk_on)

                # rel_ofi_ml_norm_btc: target OFI normalized vs BTC OFI from aggregator
                _btc_ofi_1m = 0.0
                _btc_mps_1m = 0.0
                try:
                    _raw_btc = _ctx_hgetall(self._redis_client, "ctx:anchor:btc:returns")  # type: ignore[attr-defined]
                    _btc_hash = {
                        (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                        (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                        for k, v in (_raw_btc or {}).items()
                    }
                    _btc_ofi_1m = float(_btc_hash.get("ofi_1m_ema") or 0.0)
                    _btc_mps_1m = float(_btc_hash.get("microprice_shift_1m_ema") or 0.0)
                except Exception:
                    pass
                _target_ofi = float(indicators_with_v4.get("ofi_imbalance_fast") or
                                    indicators_with_v4.get("l3_ofi_5") or 0.0)
                _target_mps = float(indicators_with_v4.get("l3_microprice_shift_bps_20") or 0.0)
                _eps = 1e-8
                indicators_with_v4.setdefault(
                    "rel_ofi_ml_norm_btc",
                    (_target_ofi - _btc_ofi_1m) / (abs(_btc_ofi_1m) + _eps),
                )
                indicators_with_v4.setdefault(
                    "rel_lob_micro_shift_bps_btc",
                    _target_mps - _btc_mps_1m,
                )

                # --- ADR-0007: PIT historical priors ---
                _kind_label = str(scenario or indicators.get("ml_scenario") or "default")
                _session_label = (
                    "us" if indicators_with_v4.get("session_us") else
                    "europe" if indicators_with_v4.get("session_europe") else
                    "asia"
                )
                _latest_ptr = ""
                try:
                    _raw_ptr = _ctx_get(self._redis_client, f"pit_priors:latest:{symbol}:{_kind_label}:{_session_label}")  # type: ignore[attr-defined]
                    _latest_ptr = (_raw_ptr.decode() if isinstance(_raw_ptr, (bytes, bytearray)) else str(_raw_ptr or ""))
                except Exception:
                    _latest_ptr = ""

                _pit_data: dict[str, str] = {}
                if _latest_ptr:
                    try:
                        _raw_pit = _ctx_hgetall(self._redis_client, f"pit_priors:{symbol}:{_kind_label}:{_session_label}:{_latest_ptr}")  # type: ignore[attr-defined]
                        _pit_data = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in (_raw_pit or {}).items()
                        }
                    except Exception:
                        _pit_data = {}
                _pit_samples = float(_pit_data.get("sample_count") or 0.0)
                _pit_min = float(os.getenv("PIT_PRIOR_MIN_SAMPLES", "30") or "30")
                if _pit_samples >= _pit_min:
                    indicators_with_v4.setdefault("prior_winrate_symbol_kind_session", float(_pit_data.get("winrate") or 0.0))
                    indicators_with_v4.setdefault("prior_ev_r_symbol_kind_session", float(_pit_data.get("ev_r") or 0.0))
                    indicators_with_v4.setdefault("prior_ev_r_median", float(_pit_data.get("ev_r_median") or 0.0))
                    indicators_with_v4.setdefault("prior_sample_count_log", math.log1p(_pit_samples))
                    _pit_age_ms = float(_now_ms_local - float(_pit_data.get("newest_ts_ms") or 0.0))
                    indicators_with_v4.setdefault("prior_age_ms", max(0.0, _pit_age_ms))
                    indicators_with_v4.setdefault("prior_stale_ms", max(0.0, _pit_age_ms))
                    indicators_with_v4.setdefault("prior_stale", _pit_age_ms > float(os.getenv("PIT_PRIOR_STALE_MS", "86400000") or "86400000"))
                    indicators_with_v4.setdefault("prior_profit_factor", float(_pit_data.get("profit_factor") or 1.0))
                    indicators_with_v4.setdefault("prior_sl_hit_rate", float(_pit_data.get("sl_hit_rate") or 0.5))
                    indicators_with_v4.setdefault("prior_r_std", float(_pit_data.get("r_std") or 0.0))
                else:
                    indicators_with_v4.setdefault("prior_winrate_symbol_kind_session", 0.0)
                    indicators_with_v4.setdefault("prior_ev_r_symbol_kind_session", 0.0)
                    indicators_with_v4.setdefault("prior_ev_r_median", 0.0)
                    indicators_with_v4.setdefault("prior_sample_count_log", 0.0)
                    indicators_with_v4.setdefault("prior_age_ms", 0.0)
                    indicators_with_v4.setdefault("prior_stale_ms", 0.0)
                    indicators_with_v4.setdefault("prior_stale", True)
                    indicators_with_v4.setdefault("prior_profit_factor", 1.0)
                    indicators_with_v4.setdefault("prior_sl_hit_rate", 0.5)
                    indicators_with_v4.setdefault("prior_r_std", 0.0)

                # --- Rolling 7d/30d priors (pit_priors_rolling_v1) ---
                # Keys: pit_priors:rolling:{7d|30d}:{symbol}:{kind}:{session|all}
                # Fail-open: all new keys → 0.0 when service not yet populated.
                _ROLLING_PRIOR_7D_KEYS = (
                    "prior_winrate_symbol_kind_7d",
                    "prior_ev_r_symbol_kind_7d",
                    "prior_profit_factor_symbol_kind_7d",
                    "prior_sl_hit_rate_symbol_kind_7d",
                    "prior_tp1_hit_rate_symbol_kind_7d",
                    "prior_samples_symbol_kind_7d",
                )
                _ROLLING_PRIOR_7D_SESS_KEYS = (
                    "prior_winrate_symbol_kind_session_7d",
                )
                _ROLLING_PRIOR_30D_KEYS = (
                    "prior_median_mae_r_winners_30d",
                    "prior_p90_mae_r_winners_30d",
                    "prior_median_mfe_r_30d",
                    "prior_giveback_p75_30d",
                )
                try:
                    # Rolling priors: try scenario-specific first; if not enough samples,
                    # fall back to "default" kind alias (writes the full symbol's history).
                    # Per-scenario buckets often <20 samples for small-volume symbols.
                    def _hg_decode(raw: Any) -> dict[str, str]:
                        return {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in (raw or {}).items()
                        }
                    _pit_min_r_lookup = float(os.getenv("PIT_ROLLING_MIN_SAMPLES", "20") or "20")
                    _r7a_raw = _ctx_hgetall(self._redis_client, f"pit_priors:rolling:7d:{symbol}:{_kind_label}:all")  # type: ignore[attr-defined]
                    _r7a: dict[str, str] = _hg_decode(_r7a_raw)
                    if float(_r7a.get("sample_count") or 0.0) < _pit_min_r_lookup and _kind_label != "default":
                        _r7a_raw = _ctx_hgetall(self._redis_client, f"pit_priors:rolling:7d:{symbol}:default:all")  # type: ignore[attr-defined]
                        _r7a = _hg_decode(_r7a_raw)
                    _r7s_raw = _ctx_hgetall(self._redis_client, f"pit_priors:rolling:7d:{symbol}:{_kind_label}:{_session_label}")  # type: ignore[attr-defined]
                    _r7s: dict[str, str] = _hg_decode(_r7s_raw)
                    if float(_r7s.get("sample_count") or 0.0) < _pit_min_r_lookup and _kind_label != "default":
                        _r7s_raw = _ctx_hgetall(self._redis_client, f"pit_priors:rolling:7d:{symbol}:default:{_session_label}")  # type: ignore[attr-defined]
                        _r7s = _hg_decode(_r7s_raw)
                    _r30_raw = _ctx_hgetall(self._redis_client, f"pit_priors:rolling:30d:{symbol}:{_kind_label}:all")  # type: ignore[attr-defined]
                    _r30: dict[str, str] = _hg_decode(_r30_raw)
                    if float(_r30.get("sample_count") or 0.0) < _pit_min_r_lookup and _kind_label != "default":
                        _r30_raw = _ctx_hgetall(self._redis_client, f"pit_priors:rolling:30d:{symbol}:default:all")  # type: ignore[attr-defined]
                        _r30 = _hg_decode(_r30_raw)
                    _r7a_n = float(_r7a.get("sample_count") or 0.0)
                    _r7s_n = float(_r7s.get("sample_count") or 0.0)
                    _r30_n = float(_r30.get("sample_count") or 0.0)
                    _pit_min_r = float(os.getenv("PIT_ROLLING_MIN_SAMPLES", "20") or "20")

                    if _r7a_n >= _pit_min_r:
                        indicators_with_v4.setdefault("prior_winrate_symbol_kind_7d", float(_r7a.get("winrate") or 0.0))
                        indicators_with_v4.setdefault("prior_ev_r_symbol_kind_7d", float(_r7a.get("ev_r") or 0.0))
                        indicators_with_v4.setdefault("prior_profit_factor_symbol_kind_7d", float(_r7a.get("profit_factor") or 1.0))
                        indicators_with_v4.setdefault("prior_sl_hit_rate_symbol_kind_7d", float(_r7a.get("sl_hit_rate") or 0.5))
                        indicators_with_v4.setdefault("prior_tp1_hit_rate_symbol_kind_7d", float(_r7a.get("tp1_hit_rate") or 0.0))
                        indicators_with_v4.setdefault("prior_samples_symbol_kind_7d", math.log1p(_r7a_n))
                    else:
                        for _rk in _ROLLING_PRIOR_7D_KEYS:
                            indicators_with_v4.setdefault(_rk, 0.0)
                        indicators_with_v4.setdefault("prior_profit_factor_symbol_kind_7d", 1.0)
                        indicators_with_v4.setdefault("prior_sl_hit_rate_symbol_kind_7d", 0.5)

                    indicators_with_v4.setdefault(
                        "prior_winrate_symbol_kind_session_7d",
                        float(_r7s.get("winrate") or 0.0) if _r7s_n >= _pit_min_r else 0.0,
                    )

                    if _r30_n >= _pit_min_r:
                        indicators_with_v4.setdefault("prior_median_mae_r_winners_30d", float(_r30.get("median_mae_r_winners") or 0.0))
                        indicators_with_v4.setdefault("prior_p90_mae_r_winners_30d", float(_r30.get("p90_mae_r_winners") or 0.0))
                        indicators_with_v4.setdefault("prior_median_mfe_r_30d", float(_r30.get("median_mfe_r") or 0.0))
                        indicators_with_v4.setdefault("prior_giveback_p75_30d", float(_r30.get("giveback_p75") or 0.0))
                    else:
                        for _rk in _ROLLING_PRIOR_30D_KEYS:
                            indicators_with_v4.setdefault(_rk, 0.0)
                except Exception:
                    for _rk in _ROLLING_PRIOR_7D_KEYS + _ROLLING_PRIOR_7D_SESS_KEYS + _ROLLING_PRIOR_30D_KEYS:
                        indicators_with_v4.setdefault(_rk, 0.0)
                    indicators_with_v4.setdefault("prior_profit_factor_symbol_kind_7d", 1.0)
                    indicators_with_v4.setdefault("prior_sl_hit_rate_symbol_kind_7d", 0.5)

                # --- ADR-0005: TCA EMA priors ---
                _tca_data: dict[str, str] = {}
                try:
                    _raw_tca = _ctx_hgetall(self._redis_client, f"tca:ema:{symbol}:{_kind_label}:{_session_label}")  # type: ignore[attr-defined]
                    _tca_data = {
                        (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                        (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                        for k, v in (_raw_tca or {}).items()
                    }
                except Exception:
                    _tca_data = {}
                _tca_samples = float(_tca_data.get("samples") or 0.0)
                _tca_min = float(os.getenv("TCA_PRIORS_MIN_SAMPLES", "30") or "30")
                _tca_last = float(_tca_data.get("last_update_ms") or 0.0)
                _tca_stale_max = float(os.getenv("TCA_STALE_MAX_MS", "600000") or "600000")
                _tca_age = _now_ms_local - _tca_last if _tca_last > 0 else _tca_stale_max + 1
                _tca_fresh = (_tca_samples >= _tca_min and _tca_age <= _tca_stale_max)
                if not _tca_fresh:
                    try:
                        from services.orderflow.metrics import ml_feature_stale_total
                        ml_feature_stale_total.labels(feature="tca_ema", symbol=symbol).inc()
                    except Exception:
                        pass
                indicators_with_v4.setdefault("tca_eff_spread_bps_ema", float(_tca_data.get("eff_spread") or 0.0) if _tca_fresh else 0.0)
                indicators_with_v4.setdefault("tca_realized_spread_1s_bps_ema", float(_tca_data.get("realized_1s") or 0.0) if _tca_fresh else 0.0)
                indicators_with_v4.setdefault("tca_realized_spread_5s_bps_ema", float(_tca_data.get("realized_5s") or 0.0) if _tca_fresh else 0.0)
                indicators_with_v4.setdefault("tca_perm_impact_1s_bps_ema", float(_tca_data.get("perm_1s") or 0.0) if _tca_fresh else 0.0)
                indicators_with_v4.setdefault("tca_perm_impact_5s_bps_ema", float(_tca_data.get("perm_5s") or 0.0) if _tca_fresh else 0.0)
                indicators_with_v4.setdefault("tca_is_bps_ema", float(_tca_data.get("is_bps") or 0.0) if _tca_fresh else 0.0)
                indicators_with_v4.setdefault("tca_samples", _tca_samples)
                indicators_with_v4.setdefault("tca_stale_ms", max(0.0, _tca_age))
                # p95 percentiles (available once TCA exporter accumulates ≥20 samples)
                indicators_with_v4.setdefault("spread_p95_bps_symbol_kind_session", float(_tca_data.get("spread_p95_bps") or 0.0) if _tca_fresh else 0.0)
                indicators_with_v4.setdefault("slippage_p95_bps_symbol_kind_session", float(_tca_data.get("slippage_p95_bps") or 0.0) if _tca_fresh else 0.0)
            except Exception:
                # Fail-open: all cross-context features default to 0.0 / False
                for _fk in (
                    "btc_ret_30s", "btc_ret_1m", "btc_ret_5m",
                    "eth_ret_30s", "eth_ret_1m", "eth_ret_5m",
                    "rel_ret_1m_vs_btc", "rel_ret_5m_vs_btc",
                    "leader_confidence", "market_risk_on_score",
                    "rel_ofi_ml_norm_btc", "rel_lob_micro_shift_bps_btc",
                    "prior_winrate_symbol_kind_session", "prior_ev_r_symbol_kind_session",
                    "prior_ev_r_median",
                    "prior_sample_count_log", "prior_age_ms", "prior_stale_ms",
                    "prior_r_std",
                    "tca_eff_spread_bps_ema", "tca_realized_spread_1s_bps_ema",
                    "tca_realized_spread_5s_bps_ema", "tca_perm_impact_1s_bps_ema",
                    "tca_perm_impact_5s_bps_ema", "tca_is_bps_ema",
                    "tca_samples", "tca_stale_ms",
                    "spread_p95_bps_symbol_kind_session",
                    "slippage_p95_bps_symbol_kind_session",
                ):
                    indicators_with_v4.setdefault(_fk, 0.0)
                indicators_with_v4.setdefault("prior_stale", True)
                indicators_with_v4.setdefault("prior_profit_factor", 1.0)
                indicators_with_v4.setdefault("prior_sl_hit_rate", 0.5)
                for _rk in (
                    "prior_winrate_symbol_kind_7d", "prior_ev_r_symbol_kind_7d",
                    "prior_profit_factor_symbol_kind_7d", "prior_sl_hit_rate_symbol_kind_7d",
                    "prior_tp1_hit_rate_symbol_kind_7d", "prior_samples_symbol_kind_7d",
                    "prior_winrate_symbol_kind_session_7d",
                    "prior_median_mae_r_winners_30d", "prior_p90_mae_r_winners_30d",
                    "prior_median_mfe_r_30d", "prior_giveback_p75_30d",
                ):
                    indicators_with_v4.setdefault(_rk, 0.0)
                indicators_with_v4.setdefault("prior_profit_factor_symbol_kind_7d", 1.0)
                indicators_with_v4.setdefault("prior_sl_hit_rate_symbol_kind_7d", 0.5)

            # ------------------------------------------------------------------
            # Phase 7.9: Derivatives context — funding / OI / liquidation features
            # sourced from existing `ctx:deriv:{symbol}` snapshot (populated by
            # services/orderflow/derivatives_context.py + deriv_ctx_exporter).
            # No new infra needed — snapshot is already maintained.
            # ------------------------------------------------------------------
            try:
                _deriv_data: dict[str, Any] = {}
                _deriv_age_max_ms = float(os.getenv("DERIV_CTX_MAX_LAG_MS", "60000") or "60000")
                _deriv_stale = True
                try:
                    _raw_deriv = _ctx_get(self._redis_client, f"ctx:deriv:{symbol}")  # type: ignore[attr-defined]
                    if _raw_deriv:
                        if isinstance(_raw_deriv, (bytes, bytearray)):
                            _raw_deriv = _raw_deriv.decode("utf-8", "ignore")
                        _deriv_data = json.loads(_raw_deriv)
                        _deriv_ts = float(_deriv_data.get("ts_ms") or 0.0)
                        _deriv_stale = (int(now_ts) - _deriv_ts) > _deriv_age_max_ms if _deriv_ts > 0 else True
                        if _deriv_stale:
                            try:
                                from services.orderflow.metrics import ml_feature_stale_total
                                ml_feature_stale_total.labels(feature="deriv_ctx", symbol=symbol).inc()
                            except Exception:
                                pass
                        # G6: Log if leader_confirm is missing/zero in valid snapshot
                        _leader_val = float(_deriv_data.get("leader_btc_eth_confirm") or 0.0)
                        if not _deriv_stale and _leader_val == 0.0:
                            try:
                                from services.orderflow.metrics import ml_feature_missing_total
                                ml_feature_missing_total.labels(feature="leader_btc_eth_confirm", symbol=symbol).inc()
                            except Exception:
                                pass
                except Exception:
                    _deriv_data = {}

                # Funding
                _funding_rate = float(_deriv_data.get("funding_rate") or 0.0) if not _deriv_stale else 0.0
                _funding_rate_z = float(_deriv_data.get("funding_rate_z") or 0.0) if not _deriv_stale else 0.0
                indicators_with_v4.setdefault("funding_rate", _funding_rate)
                indicators_with_v4.setdefault("funding_rate_z", _funding_rate_z)

                # OI
                _oi_notional = float(_deriv_data.get("oi_notional_usd") or 0.0) if not _deriv_stale else 0.0
                _oi_delta_5m = float(_deriv_data.get("delta_oi_5m") or 0.0) if not _deriv_stale else 0.0
                indicators_with_v4.setdefault("oi_notional_usd", _oi_notional)
                indicators_with_v4.setdefault("oi_delta_5m", _oi_delta_5m)
                # 1m delta proxy: 1/5 of 5m delta (linear scaling, model can learn correction)
                indicators_with_v4.setdefault("oi_delta_1m", _oi_delta_5m / 5.0)
                # z-score from snapshot is OI accelerometer (oi_accel flag in snapshot)
                indicators_with_v4.setdefault("oi_accel", float(_deriv_data.get("oi_accel") or 0.0) if not _deriv_stale else 0.0)

                # Basis / premium
                _basis_bps = float(_deriv_data.get("basis_bps") or 0.0) if not _deriv_stale else 0.0
                _premium_index = float(_deriv_data.get("premium_index") or 0.0) if not _deriv_stale else 0.0
                indicators_with_v4.setdefault("basis_bps", _basis_bps)
                indicators_with_v4.setdefault("premium_index_bps", _premium_index * 10000.0)
                # Composite basis pressure: combined funding + basis signal
                _basis_pressure = abs(_funding_rate_z) + abs(_basis_bps) / 10.0
                indicators_with_v4.setdefault("basis_pressure_score", _basis_pressure if not _deriv_stale else 0.0)

                # Liquidation imbalance (already computed in v2 of snapshot)
                _liq_buy_1m = float(_deriv_data.get("liq_buy_notional_1m") or 0.0) if not _deriv_stale else 0.0
                _liq_sell_1m = float(_deriv_data.get("liq_sell_notional_1m") or 0.0) if not _deriv_stale else 0.0
                indicators_with_v4.setdefault("liq_long_notional_1m", _liq_buy_1m)
                indicators_with_v4.setdefault("liq_short_notional_1m", _liq_sell_1m)
                # 5m proxy: 5x running window (snapshot only has 1m, this is approximate)
                indicators_with_v4.setdefault("liq_long_notional_5m", _liq_buy_1m * 5.0)
                indicators_with_v4.setdefault("liq_short_notional_5m", _liq_sell_1m * 5.0)
                _liq_total_1m = _liq_buy_1m + _liq_sell_1m
                _liq_imb_1m = (_liq_buy_1m - _liq_sell_1m) / _liq_total_1m if _liq_total_1m > 0 else 0.0
                indicators_with_v4.setdefault("liq_imbalance_1m", _liq_imb_1m)
                indicators_with_v4.setdefault("liq_imbalance_5m", _liq_imb_1m)  # same proxy
                indicators_with_v4.setdefault("liq_imbalance_z", float(_deriv_data.get("liq_imbalance_z") or 0.0) if not _deriv_stale else 0.0)

                # Long/short ratio
                indicators_with_v4.setdefault("long_short_ratio", float(_deriv_data.get("long_short_ratio") or 0.0) if not _deriv_stale else 0.0)
                indicators_with_v4.setdefault("long_short_ratio_z", float(_deriv_data.get("long_short_ratio_z") or 0.0) if not _deriv_stale else 0.0)

                # Direction-conflict signal (BTC/ETH leader confirms target direction)
                # G6: leader_confirm = (btc_ret_24h + eth_ret_24h) / 2 from runtime:breadth
                _leader_confirm = float(_deriv_data.get("leader_btc_eth_confirm") or 0.0) if not _deriv_stale else 0.0
                # Validate: must be finite and in range [-1, 1]
                if not math.isfinite(_leader_confirm):
                    _leader_confirm = 0.0
                else:
                    _leader_confirm = max(-1.0, min(1.0, _leader_confirm))
                indicators_with_v4.setdefault("leader_btc_eth_confirm", _leader_confirm)
                # Conflict = inverse of confirmation (high abs(confirm) → low conflict)
                indicators_with_v4.setdefault("leader_direction_conflict", 1.0 - abs(_leader_confirm) if not _deriv_stale else 0.0)

                # Sector breadth (24h return / volume z)
                indicators_with_v4.setdefault("sector_breadth_ret_24h", float(_deriv_data.get("market_breadth_ret_24h") or 0.0) if not _deriv_stale else 0.0)
                indicators_with_v4.setdefault("sector_breadth_vol_z", float(_deriv_data.get("market_breadth_volume_z") or 0.0) if not _deriv_stale else 0.0)

                # Phase 7.9b: composite scores derived from same snapshot.
                # Taker buy/sell imbalance — already in DerivativesContextSnapshot.
                indicators_with_v4.setdefault(
                    "taker_buy_sell_imbalance",
                    float(_deriv_data.get("taker_buy_sell_imbalance") or 0.0) if not _deriv_stale else 0.0,
                )
                # Phase 8.3: taker ratio + z-score (v3 snapshot fields).
                indicators_with_v4.setdefault(
                    "taker_buy_sell_ratio",
                    float(_deriv_data.get("taker_buy_sell_ratio") or 0.0) if not _deriv_stale else 0.0,
                )
                indicators_with_v4.setdefault(
                    "taker_buy_sell_ratio_z",
                    float(_deriv_data.get("taker_buy_sell_ratio_z") or 0.0) if not _deriv_stale else 0.0,
                )
                # Alias of liq_imbalance_1m for forceOrder semantics in plan.
                indicators_with_v4.setdefault("force_order_imbalance_1m", _liq_imb_1m if not _deriv_stale else 0.0)
                # Phase 8.3: individual liq notionals under force_order naming.
                indicators_with_v4.setdefault("force_order_long_notional_1m", _liq_buy_1m if not _deriv_stale else 0.0)
                indicators_with_v4.setdefault("force_order_short_notional_1m", _liq_sell_1m if not _deriv_stale else 0.0)
                # Phase 8.3: cluster score + top-trader + crowding (v3 snapshot).
                indicators_with_v4.setdefault(
                    "force_order_cluster_score",
                    float(_deriv_data.get("force_order_cluster_score") or 0.0) if not _deriv_stale else 0.0,
                )
                indicators_with_v4.setdefault(
                    "top_trader_long_short_ratio",
                    float(_deriv_data.get("top_trader_long_short_ratio") or 0.0) if not _deriv_stale else 0.0,
                )
                indicators_with_v4.setdefault(
                    "futures_crowding_score",
                    float(_deriv_data.get("futures_crowding_score") or 0.0) if not _deriv_stale else 0.0,
                )
                # Phase 8.4: oi_delta z-score and premium_index z-score (from v3 snapshot).
                indicators_with_v4.setdefault(
                    "oi_delta_z",
                    float(_deriv_data.get("oi_delta_z") or 0.0) if not _deriv_stale else 0.0,
                )
                indicators_with_v4.setdefault(
                    "premium_index_z",
                    float(_deriv_data.get("premium_index_z") or 0.0) if not _deriv_stale else 0.0,
                )
                # Phase 8.4+: open_interest_z — z-score of OI notional level (not delta).
                indicators_with_v4.setdefault(
                    "open_interest_z",
                    float(_deriv_data.get("open_interest_z") or 0.0) if not _deriv_stale else 0.0,
                )
                # OI confirmation: +1 if OI delta and funding z agree (continuation),
                # -1 if they disagree (short-cover / squeeze hint), 0 if either is 0.
                _oi_conf = (
                    math.copysign(1.0, _oi_delta_5m) * math.copysign(1.0, _funding_rate_z)
                    if (_oi_delta_5m != 0.0 and _funding_rate_z != 0.0)
                    else 0.0
                )
                indicators_with_v4.setdefault("oi_confirmation_score", _oi_conf if not _deriv_stale else 0.0)
                # Squeeze risk: funding × L/S both extreme (>1.5σ).
                _ls_z = float(_deriv_data.get("long_short_ratio_z") or 0.0)
                _squeeze = (
                    min(25.0, abs(_funding_rate_z) * abs(_ls_z))
                    if (abs(_funding_rate_z) > 1.5 and abs(_ls_z) > 1.5)
                    else 0.0
                )
                indicators_with_v4.setdefault("squeeze_risk_score", _squeeze if not _deriv_stale else 0.0)
                # Liquidation impulse: only when |liq_imbalance_z| > 2.0.
                _liq_z_abs = abs(float(_deriv_data.get("liq_imbalance_z") or 0.0))
                _liq_impulse = _liq_z_abs if _liq_z_abs > 2.0 else 0.0
                indicators_with_v4.setdefault("liq_impulse_score", _liq_impulse if not _deriv_stale else 0.0)
            except Exception:
                for _df in (
                    "funding_rate", "funding_rate_z",
                    "oi_notional_usd", "oi_delta_5m", "oi_delta_1m", "oi_accel",
                    "basis_bps", "premium_index_bps", "basis_pressure_score",
                    "liq_long_notional_1m", "liq_short_notional_1m",
                    "liq_long_notional_5m", "liq_short_notional_5m",
                    "liq_imbalance_1m", "liq_imbalance_5m", "liq_imbalance_z",
                    "long_short_ratio", "long_short_ratio_z",
                    "leader_btc_eth_confirm", "leader_direction_conflict",
                    "sector_breadth_ret_24h", "sector_breadth_vol_z",
                    # Phase 7.9b composites
                    "taker_buy_sell_imbalance", "force_order_imbalance_1m",
                    "oi_confirmation_score", "squeeze_risk_score", "liq_impulse_score",
                    # Phase 8.3
                    "taker_buy_sell_ratio", "taker_buy_sell_ratio_z",
                    "force_order_long_notional_1m", "force_order_short_notional_1m",
                    "force_order_cluster_score", "top_trader_long_short_ratio",
                    "futures_crowding_score",
                    # Phase 8.4
                    "oi_delta_z", "premium_index_z", "open_interest_z",
                ):
                    indicators_with_v4.setdefault(_df, 0.0)

            # ------------------------------------------------------------------
            # Phase 8.1: External joiners — live market breadth, Deribit IV
            # regime, Fear & Greed. Each block independent, own stale guard.
            # All fail-open via setdefault; no exception escapes this section.
            # ------------------------------------------------------------------
            # --- W3a: Live breadth (runtime:breadth HASH on main redis) ---
            # NOTE: runtime:breadth is written by Go marketdata scheduler on main `redis`,
            # not on worker-1. Use _redis_client_main; fall back to _redis_client if absent.
            try:
                _breadth_max_lag_ms = float(os.getenv("BREADTH_MAX_LAG_MS", "10000") or "10000")
                _breadth_stale = True
                _breadth_data: dict[str, Any] = {}
                _rc_main = getattr(self, "_redis_client_main", None) or getattr(self, "_redis_client", None)
                try:
                    _raw = _ctx_hgetall(_rc_main, "runtime:breadth")
                    if _raw:
                        _breadth_data = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw.items()
                        }
                        _b_ts = float(_breadth_data.get("ts_ms") or 0.0)
                        _breadth_stale = (int(now_ts) - _b_ts) > _breadth_max_lag_ms if _b_ts > 0 else True
                except Exception:
                    _breadth_data = {}
                _bf = lambda k: float(_breadth_data.get(k) or 0.0) if not _breadth_stale else 0.0
                indicators_with_v4.setdefault("market_breadth_ret_24h", _bf("ret_24h"))
                indicators_with_v4.setdefault("market_breadth_vol_z", _bf("vol_z"))
                indicators_with_v4.setdefault("btc_leader_ret_breadth", _bf("btc_ret"))
                indicators_with_v4.setdefault("eth_leader_ret_breadth", _bf("eth_ret"))
                indicators_with_v4.setdefault("breadth_leader_confirm", _bf("leader_confirm"))
                indicators_with_v4.setdefault("sector_breadth_1m", _bf("breadth_1m"))
                indicators_with_v4.setdefault("sector_breadth_5m", _bf("breadth_5m"))
                # Phase breadth-v2: granular 1m/5m returns + segment breadth
                indicators_with_v4.setdefault("market_breadth_ret_1m", _bf("ret_1m"))
                indicators_with_v4.setdefault("market_breadth_ret_5m", _bf("ret_5m"))
                indicators_with_v4.setdefault("major_breadth_1m", _bf("major_breadth_1m"))
                indicators_with_v4.setdefault("major_ret_1m", _bf("major_ret_1m"))
                indicators_with_v4.setdefault("meme_breadth_1m", _bf("meme_breadth_1m"))
                indicators_with_v4.setdefault("meme_ret_1m", _bf("meme_ret_1m"))
                indicators_with_v4.setdefault("alt_breadth_1m", _bf("alt_breadth_1m"))
                indicators_with_v4.setdefault("alt_ret_1m", _bf("alt_ret_1m"))
                indicators_with_v4.setdefault("alt_breadth_5m", _bf("alt_breadth_5m"))
                indicators_with_v4.setdefault("alt_ret_5m", _bf("alt_ret_5m"))
                indicators_with_v4.setdefault("sector_breadth_score", _bf("sector_breadth_score"))
                # Phase P1: 5-min volume rolling sum + z-score
                indicators_with_v4.setdefault("market_breadth_vol_5m", _bf("market_breadth_vol_5m"))
                indicators_with_v4.setdefault("market_breadth_volume_z", _bf("market_breadth_volume_z"))
                # Phase P1: symbol relative strength vs market / BTC / sector
                _mkt_ret_1m = float(_breadth_data.get("ret_1m") or 0.0) if not _breadth_stale else 0.0
                _btc_ret_1m = float(indicators_with_v4.get("btc_ret_1m") or 0.0)
                _rel_vs_btc = float(indicators_with_v4.get("rel_ret_1m_vs_btc") or 0.0)
                _sym_ret_1m = _btc_ret_1m + _rel_vs_btc  # Phase 7.8 identity
                indicators_with_v4.setdefault("symbol_rel_strength_vs_btc_1m", _rel_vs_btc)
                indicators_with_v4.setdefault("symbol_rel_strength_vs_market_1m", _sym_ret_1m - _mkt_ret_1m)
                _MAJOR_SYMS = frozenset({
                    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
                    "ADAUSDT", "DOTUSDT", "AVAXUSDT", "MATICUSDT", "LINKUSDT",
                    "LTCUSDT", "UNIUSDT", "ATOMUSDT", "NEARUSDT", "ARBUSDT",
                    "OPUSDT", "APTUSDT", "SUIUSDT", "INJUSDT", "TIAUSDT",
                })
                _MEME_SYMS = frozenset({
                    "DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "FLOKIUSDT", "BONKUSDT",
                    "WIFUSDT", "DOGSUSDT", "MEMEUSDT", "BOMEUSDT", "NEIROUSDT",
                    "POPCATUSDT", "ACTUSDT",
                })
                if symbol in _MAJOR_SYMS:
                    _sector_ret = float(_breadth_data.get("major_ret_1m") or 0.0) if not _breadth_stale else 0.0
                elif symbol in _MEME_SYMS:
                    _sector_ret = float(_breadth_data.get("meme_ret_1m") or 0.0) if not _breadth_stale else 0.0
                else:
                    _sector_ret = float(_breadth_data.get("alt_ret_1m") or 0.0) if not _breadth_stale else 0.0
                indicators_with_v4.setdefault("symbol_rel_strength_vs_sector_1m", _sym_ret_1m - _sector_ret)
                _ext_ctx_track("breadth", _breadth_stale, float(_breadth_data.get("ts_ms") or 0), float(now_ts))
            except Exception:
                for _df in (
                    "market_breadth_ret_24h", "market_breadth_vol_z",
                    "btc_leader_ret_breadth", "eth_leader_ret_breadth",
                    "breadth_leader_confirm", "sector_breadth_1m", "sector_breadth_5m",
                    "market_breadth_ret_1m", "market_breadth_ret_5m",
                    "major_breadth_1m", "major_ret_1m",
                    "meme_breadth_1m", "meme_ret_1m",
                    "alt_breadth_1m", "alt_ret_1m", "alt_breadth_5m", "alt_ret_5m",
                    "sector_breadth_score",
                    "market_breadth_vol_5m", "market_breadth_volume_z",
                    "symbol_rel_strength_vs_btc_1m", "symbol_rel_strength_vs_market_1m",
                    "symbol_rel_strength_vs_sector_1m",
                ):
                    indicators_with_v4.setdefault(_df, 0.0)

            # --- W3b: Deribit IV/funding regime (ctx:deribit:global STRING) ---
            # NOTE: ctx:deribit:global is on main `redis` (Go scheduler writer).
            try:
                _deribit_max_lag_ms = float(os.getenv("DERIBIT_MAX_LAG_MS", "120000") or "120000")
                _deribit_stale = True
                _deribit_data: dict[str, Any] = {}
                _rc_main = getattr(self, "_redis_client_main", None) or getattr(self, "_redis_client", None)
                try:
                    _raw_db = _ctx_get(_rc_main, "ctx:deribit:global")
                    if _raw_db:
                        if isinstance(_raw_db, (bytes, bytearray)):
                            _raw_db = _raw_db.decode("utf-8", "ignore")
                        _deribit_data = json.loads(_raw_db)
                        _db_ts = float(_deribit_data.get("ts_ms") or 0.0)
                        _deribit_stale = (int(now_ts) - _db_ts) > _deribit_max_lag_ms if _db_ts > 0 else True
                except Exception:
                    _deribit_data = {}
                _df_n = lambda k: float(_deribit_data.get(k) or 0.0) if not _deribit_stale else 0.0
                indicators_with_v4.setdefault("deribit_btc_iv_proxy", _df_n("btc_deribit_iv_proxy"))
                indicators_with_v4.setdefault("deribit_eth_iv_proxy", _df_n("eth_deribit_iv_proxy"))
                indicators_with_v4.setdefault("deribit_btc_iv_z", _df_n("btc_deribit_iv_z"))
                indicators_with_v4.setdefault("deribit_eth_iv_z", _df_n("eth_deribit_iv_z"))
                indicators_with_v4.setdefault("deribit_btc_funding_8h", _df_n("btc_deribit_funding_8h"))
                indicators_with_v4.setdefault("deribit_eth_funding_8h", _df_n("eth_deribit_funding_8h"))
                _regime_str = str(_deribit_data.get("btc_eth_vol_regime") or "normal").lower()
                _regime_code = {"normal": 0.0, "elevated": 1.0, "extreme": 2.0}.get(_regime_str, 0.0)
                indicators_with_v4.setdefault("deribit_vol_regime_code", _regime_code if not _deribit_stale else 0.0)
                # Phase P1: Deribit term structure from ctx:deribit:global
                indicators_with_v4.setdefault("deribit_btc_iv_7d", _df_n("btc_iv_7d"))
                indicators_with_v4.setdefault("deribit_btc_iv_30d", _df_n("btc_iv_30d"))
                indicators_with_v4.setdefault("deribit_eth_iv_7d", _df_n("eth_iv_7d"))
                indicators_with_v4.setdefault("deribit_eth_iv_30d", _df_n("eth_iv_30d"))
                indicators_with_v4.setdefault("deribit_iv_term_structure_7d_30d", _df_n("deribit_iv_term_structure_7d_30d"))
                indicators_with_v4.setdefault("deribit_put_call_ratio", _df_n("deribit_put_call_ratio"))
                indicators_with_v4.setdefault("deribit_options_oi_call_put_ratio", _df_n("deribit_options_oi_call_put_ratio"))
                indicators_with_v4.setdefault("deribit_event_vol_premium_score", _df_n("deribit_event_vol_premium_score"))
                _ext_ctx_track("deribit", _deribit_stale, float(_deribit_data.get("ts_ms") or 0), float(now_ts))
            except Exception:
                for _df in (
                    "deribit_btc_iv_proxy", "deribit_eth_iv_proxy",
                    "deribit_btc_iv_z", "deribit_eth_iv_z",
                    "deribit_btc_funding_8h", "deribit_eth_funding_8h",
                    "deribit_vol_regime_code",
                    "deribit_btc_iv_7d", "deribit_btc_iv_30d",
                    "deribit_eth_iv_7d", "deribit_eth_iv_30d",
                    "deribit_iv_term_structure_7d_30d",
                    "deribit_put_call_ratio", "deribit_options_oi_call_put_ratio",
                    "deribit_event_vol_premium_score",
                ):
                    indicators_with_v4.setdefault(_df, 0.0)

            # --- W3c: Fear & Greed (ctx:sentiment:global STRING) ---
            # NOTE: ctx:sentiment:global is on main `redis` (Go scheduler writer).
            try:
                _fng_max_lag_ms = float(os.getenv("SENTIMENT_MAX_LAG_MS", "7200000") or "7200000")
                _fng_stale = True
                _fng_data: dict[str, Any] = {}
                _rc_main = getattr(self, "_redis_client_main", None) or getattr(self, "_redis_client", None)
                try:
                    _raw_fng = _ctx_get(_rc_main, "ctx:sentiment:global")
                    if _raw_fng:
                        if isinstance(_raw_fng, (bytes, bytearray)):
                            _raw_fng = _raw_fng.decode("utf-8", "ignore")
                        _fng_data = json.loads(_raw_fng)
                        # Prefer ingest_ts_ms (moment Go-collector wrote snapshot) over
                        # ts_ms (alternative.me data timestamp = start of UTC day = 8+h lag).
                        _fng_ts = float(_fng_data.get("ingest_ts_ms") or _fng_data.get("ts_ms") or 0.0)
                        _fng_stale = (int(now_ts) - _fng_ts) > _fng_max_lag_ms if _fng_ts > 0 else True
                except Exception:
                    _fng_data = {}
                # Sentinel scheduler stores fng index under various possible field names;
                # try the common ones in order of preference.
                # Go SentimentScheduler writes `fear_greed_value` (verified 2026-05-16).
                # Other names kept as fallbacks for alternative providers.
                _fng_val_raw = (
                    _fng_data.get("fear_greed_value")
                    or _fng_data.get("fng_value")
                    or _fng_data.get("value")
                    or _fng_data.get("index")
                    or _fng_data.get("fear_greed_index")
                    or 0
                )
                try:
                    _fng_val = float(_fng_val_raw)
                except (TypeError, ValueError):
                    _fng_val = 0.0
                indicators_with_v4.setdefault("fear_greed_index", _fng_val if not _fng_stale else 0.0)
                indicators_with_v4.setdefault(
                    "fear_greed_regime_extreme_fear",
                    bool(_fng_val < 25.0) if not _fng_stale else False,
                )
                indicators_with_v4.setdefault(
                    "fear_greed_regime_extreme_greed",
                    bool(_fng_val > 75.0) if not _fng_stale else False,
                )
                indicators_with_v4.setdefault(
                    "fear_greed_delta_1d",
                    float(_fng_data.get("fear_greed_delta_1d") or 0.0) if not _fng_stale else 0.0,
                )
                _ext_ctx_track("sentiment", _fng_stale, float(_fng_data.get("ingest_ts_ms") or _fng_data.get("ts_ms") or 0), float(now_ts))
            except Exception:
                indicators_with_v4.setdefault("fear_greed_index", 0.0)
                indicators_with_v4.setdefault("fear_greed_regime_extreme_fear", False)
                indicators_with_v4.setdefault("fear_greed_regime_extreme_greed", False)
                indicators_with_v4.setdefault("fear_greed_delta_1d", 0.0)

            # Phase 8.2: news_blackout — boolean-as-float mirror of news_gate_veto
            # already present in `indicators` from the news gate; expose under a
            # clean schema name so ML can use it directly.
            try:
                _nb = indicators.get("news_gate_veto", 0)
                indicators_with_v4.setdefault("news_blackout", float(1 if _nb else 0))
                # Phase 8.4: news_until_ms_norm — remaining blackout duration normalised to [0,1]
                # over a 30-minute window. 0 when no blackout; 1 when ≥30m remain.
                _news_until_ms = float(indicators.get("news_until_ts_ms") or 0.0)
                if _news_until_ms > 0.0:
                    _news_remain_s = max(0.0, (_news_until_ms - float(now_ts)) / 1000.0)
                    _news_norm = min(1.0, _news_remain_s / 1800.0)
                else:
                    _news_norm = 0.0
                indicators_with_v4.setdefault("news_until_ms_norm", _news_norm)
            except Exception:
                indicators_with_v4.setdefault("news_blackout", 0.0)
                indicators_with_v4.setdefault("news_until_ms_norm", 0.0)

            # ------------------------------------------------------------------
            # W3d: Hawkes/VPIN (ctx:hawkes:{symbol} HASH — worker Redis).
            # Written by orderflow_services/of_hawkes_vpin_v1.py.
            # Fail-open: all 20 v7_of Hawkes/VPIN keys default to 0.0.
            # ------------------------------------------------------------------
            _HAWKES_KEYS = (
                "hawkes_dt_s",
                "hawkes_taker_buy_lam", "hawkes_taker_sell_lam",
                "hawkes_cancel_bid_lam", "hawkes_cancel_ask_lam",
                "hawkes_limit_add_lam",
                "hawkes_taker_lam", "hawkes_cancel_lam", "hawkes_churn_lam",
                "added_bid_rate_ema", "added_ask_rate_ema", "added_total_rate_ema",
                "vpin_tox_ema", "vpin_tox_z",
                "vpin_tox_1m", "vpin_tox_5m", "vpin_tox_slope",
                "hawkes_limit_add_bid_lam", "hawkes_limit_add_ask_lam", "hawkes_limit_add_imbalance",
                "hawkes_S_taker_buy", "hawkes_S_taker_sell",
                "hawkes_S_cancel_bid", "hawkes_S_cancel_ask", "hawkes_S_limit_add",
            )
            try:
                _hawkes_max_lag_ms = float(os.getenv("HAWKES_MAX_LAG_MS", "30000") or "30000")
                _raw_hk = _ctx_hgetall(self._redis_client, f"ctx:hawkes:{symbol}")  # type: ignore[attr-defined]
                _hk_data: dict[str, str] = {
                    (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                    (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                    for k, v in (_raw_hk or {}).items()
                }
                _hk_ts_ms = float(_hk_data.get("ts_ms") or 0.0)
                _hk_stale = (
                    _hk_ts_ms <= 0
                    or (_now_ms_local - _hk_ts_ms) > _hawkes_max_lag_ms
                )
                for _hk_key in _HAWKES_KEYS:
                    indicators_with_v4.setdefault(
                        _hk_key,
                        float(_hk_data.get(_hk_key) or 0.0) if not _hk_stale else 0.0,
                    )
            except Exception:
                for _hk_key in _HAWKES_KEYS:
                    indicators_with_v4.setdefault(_hk_key, 0.0)

            # Phase 8.4: Hawkes derived composites (buy/sell intensity ratio, cancel directional imb).
            try:
                _lam_buy = float(indicators_with_v4.get("hawkes_taker_buy_lam") or 0.0)
                _lam_sell = float(indicators_with_v4.get("hawkes_taker_sell_lam") or 0.0)
                indicators_with_v4.setdefault(
                    "hawkes_buy_sell_lam_ratio",
                    _lam_buy / max(_lam_sell, 1e-9),
                )
                _cancel_bid = float(indicators_with_v4.get("hawkes_cancel_bid_lam") or 0.0)
                _cancel_ask = float(indicators_with_v4.get("hawkes_cancel_ask_lam") or 0.0)
                _cancel_total = _cancel_bid + _cancel_ask
                indicators_with_v4.setdefault(
                    "hawkes_cancel_imbalance",
                    (_cancel_bid - _cancel_ask) / max(_cancel_total, 1e-9),
                )
            except Exception:
                indicators_with_v4.setdefault("hawkes_buy_sell_lam_ratio", 1.0)
                indicators_with_v4.setdefault("hawkes_cancel_imbalance", 0.0)

            # ------------------------------------------------------------------
            # Phase 8.5: Cross-venue sanity (Group XV) — ctx:crossvenue:{symbol}
            # Source: Go crossvenue aggregator (OKX/Kraken/Coinbase spot WS).
            # Stale guard: CROSSVENUE_MAX_LAG_MS (30s) + quality_status=STALE.
            # Fail-open: all keys → 0.0 when stale/unavailable.
            # ------------------------------------------------------------------
            try:
                _cv_max_lag_ms = float(os.getenv("CROSSVENUE_MAX_LAG_MS", "30000") or "30000")
                _cv_data: dict[str, Any] = {}
                _cv_stale = True
                _rc_cv = getattr(self, "_redis_client_main", None) or getattr(self, "_redis_client", None)
                try:
                    _raw_cv = _ctx_get(_rc_cv, f"ctx:crossvenue:{symbol}")
                    if _raw_cv:
                        if isinstance(_raw_cv, (bytes, bytearray)):
                            _raw_cv = _raw_cv.decode("utf-8", "ignore")
                        _cv_data = json.loads(_raw_cv) if isinstance(_raw_cv, str) else {}
                        _cv_ts = float(_cv_data.get("ts_ms") or 0.0)
                        _cv_stale = (
                            (int(now_ts) - _cv_ts) > _cv_max_lag_ms if _cv_ts > 0 else True
                        ) or (str(_cv_data.get("quality_status") or "").upper() == "STALE")
                except Exception:
                    _cv_data = {}
                _cv_agree = float(_cv_data.get("cross_venue_direction_agree") or 0.0) if not _cv_stale else 0.0
                _cv_disz = float(_cv_data.get("venue_dislocation_z") or 0.0) if not _cv_stale else 0.0
                indicators_with_v4.setdefault("cross_venue_agree_score", _cv_agree)
                indicators_with_v4.setdefault(
                    "cross_venue_dislocation_bps",
                    float(_cv_data.get("cross_venue_mid_spread_bps") or 0.0) if not _cv_stale else 0.0,
                )
                indicators_with_v4.setdefault("cross_venue_dislocation_z", _cv_disz)
                indicators_with_v4.setdefault(
                    "binance_local_noise_score",
                    min(1.0, _cv_disz / 3.0) * max(0.0, 1.0 - _cv_agree) if not _cv_stale else 0.0,
                )
                _ext_ctx_track("crossvenue", _cv_stale, float(_cv_data.get("ts_ms") or 0), float(now_ts))
            except Exception:
                for _cvk in (
                    "cross_venue_agree_score", "cross_venue_dislocation_bps",
                    "cross_venue_dislocation_z", "binance_local_noise_score",
                ):
                    indicators_with_v4.setdefault(_cvk, 0.0)

            # ------------------------------------------------------------------
            # Phase 8.5: CoinGecko macro context (Groups XVI) — runtime:coingecko:*
            # Source: Go coingecko_scheduler (~30s global, ~60s per-symbol HSET).
            # Stale guard: CG_MAX_LAG_MS (600s default).
            # Fail-open: all keys → 0.0 when stale/unavailable.
            # ------------------------------------------------------------------
            try:
                _cg_max_lag_ms = float(os.getenv("CG_MAX_LAG_MS", "600000") or "600000")
                _rc_cg = getattr(self, "_redis_client_main", None) or getattr(self, "_redis_client", None)
                # global snapshot (runtime:coingecko:global HASH)
                _cg_global: dict[str, Any] = {}
                _cg_global_stale = True
                try:
                    _raw_cgg = _ctx_hgetall(_rc_cg, "runtime:coingecko:global")
                    if _raw_cgg:
                        _cg_global = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw_cgg.items()
                        }
                        _cg_g_ts = float(_cg_global.get("ts_ms") or 0.0)
                        _cg_global_stale = (
                            (int(now_ts) - _cg_g_ts) > _cg_max_lag_ms if _cg_g_ts > 0 else True
                        )
                except Exception:
                    _cg_global = {}
                _cgf = lambda k: float(_cg_global.get(k) or 0.0) if not _cg_global_stale else 0.0
                indicators_with_v4.setdefault("cg_btc_dom_pct", _cgf("btc_dom_pct"))
                indicators_with_v4.setdefault("cg_stable_dom_pct", _cgf("stable_dom_pct"))
                indicators_with_v4.setdefault("cg_btc_dom_mom", _cgf("btc_dom_mom"))
                indicators_with_v4.setdefault("cg_global_turnover", _cgf("global_turnover"))
                # per-symbol snapshot (runtime:coingecko:market:{symbol} HASH)
                _cg_sym: dict[str, Any] = {}
                _cg_sym_stale = True
                try:
                    _raw_cgsym = _ctx_hgetall(_rc_cg, f"runtime:coingecko:market:{symbol}")
                    if _raw_cgsym:
                        _cg_sym = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw_cgsym.items()
                        }
                        _cg_s_ts = float(_cg_sym.get("ts_ms") or 0.0)
                        _cg_sym_stale = (
                            (int(now_ts) - _cg_s_ts) > _cg_max_lag_ms if _cg_s_ts > 0 else True
                        )
                except Exception:
                    _cg_sym = {}
                _cgfs = lambda k: float(_cg_sym.get(k) or 0.0) if not _cg_sym_stale else 0.0
                indicators_with_v4.setdefault("cg_symbol_rank", _cgfs("market_cap_rank"))
                indicators_with_v4.setdefault("cg_rel_strength_btc_1h", _cgfs("rel_strength_btc_1h"))
                indicators_with_v4.setdefault("cg_volume_mcap_ratio", _cgfs("volume_mcap_ratio"))
                _ext_ctx_track("coingecko", _cg_global_stale, float(_cg_global.get("ts_ms") or 0), float(now_ts))
            except Exception:
                for _cgk in (
                    "cg_btc_dom_pct", "cg_stable_dom_pct", "cg_btc_dom_mom",
                    "cg_global_turnover", "cg_symbol_rank", "cg_rel_strength_btc_1h",
                    "cg_volume_mcap_ratio",
                ):
                    indicators_with_v4.setdefault(_cgk, 0.0)

            # ------------------------------------------------------------------
            # Phase P3: CoinPaprika fallback provider (Group XIX).
            # Source: runtime:provider:coinpaprika:global + :market:{symbol} HASH.
            # Stale guard: CP_MAX_LAG_MS (900s default).
            # ------------------------------------------------------------------
            try:
                _cp_max_lag_ms = float(os.getenv("CP_MAX_LAG_MS", "900000") or "900000")
                _rc_pf = getattr(self, "_redis_client_main", None) or getattr(self, "_redis_client", None)
                _cp_global: dict[str, Any] = {}
                _cp_global_stale = True
                try:
                    _raw_cpg = _ctx_hgetall(_rc_pf, "runtime:provider:coinpaprika:global")
                    if _raw_cpg:
                        _cp_global = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw_cpg.items()
                        }
                        _cp_g_ts = float(_cp_global.get("ts_ms") or 0.0)
                        _cp_global_stale = (
                            (int(now_ts) - _cp_g_ts) > _cp_max_lag_ms if _cp_g_ts > 0 else True
                        )
                except Exception:
                    _cp_global = {}
                _cp_sym: dict[str, Any] = {}
                _cp_sym_stale = True
                try:
                    _raw_cps = _ctx_hgetall(_rc_pf, f"runtime:provider:coinpaprika:market:{symbol}")
                    if _raw_cps:
                        _cp_sym = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw_cps.items()
                        }
                        _cp_s_ts = float(_cp_sym.get("ts_ms") or 0.0)
                        _cp_sym_stale = (
                            (int(now_ts) - _cp_s_ts) > _cp_max_lag_ms if _cp_s_ts > 0 else True
                        )
                except Exception:
                    _cp_sym = {}
                indicators_with_v4.setdefault(
                    "cp_btc_dom_pct",
                    float(_cp_global.get("btc_dominance_pct") or 0.0) if not _cp_global_stale else 0.0,
                )
                indicators_with_v4.setdefault(
                    "cp_symbol_ret_7d",
                    float(_cp_sym.get("percent_change_7d") or 0.0) if not _cp_sym_stale else 0.0,
                )
                indicators_with_v4.setdefault(
                    "cp_volume_mcap_ratio",
                    float(_cp_sym.get("volume_mcap_ratio") or 0.0) if not _cp_sym_stale else 0.0,
                )
                indicators_with_v4.setdefault(
                    "cp_market_cap_rank",
                    float(_cp_sym.get("rank") or 0.0) if not _cp_sym_stale else 0.0,
                )
            except Exception:
                for _cpk in ("cp_btc_dom_pct", "cp_symbol_ret_7d", "cp_volume_mcap_ratio", "cp_market_cap_rank"):
                    indicators_with_v4.setdefault(_cpk, 0.0)

            # ------------------------------------------------------------------
            # Phase P3: CoinMarketCap fallback provider (Group XX).
            # Source: runtime:provider:coinmarketcap:global HASH.
            # Stale guard: CMC_MAX_LAG_MS (900s default).
            # ------------------------------------------------------------------
            try:
                _cmc_max_lag_ms = float(os.getenv("CMC_MAX_LAG_MS", "900000") or "900000")
                _cmc_global: dict[str, Any] = {}
                _cmc_global_stale = True
                try:
                    _raw_cmcg = _ctx_hgetall(_rc_pf, "runtime:provider:coinmarketcap:global")
                    if _raw_cmcg:
                        _cmc_global = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw_cmcg.items()
                        }
                        _cmc_g_ts = float(_cmc_global.get("ts_ms") or 0.0)
                        _cmc_global_stale = (
                            (int(now_ts) - _cmc_g_ts) > _cmc_max_lag_ms if _cmc_g_ts > 0 else True
                        )
                except Exception:
                    _cmc_global = {}
                _cmcf = lambda k: float(_cmc_global.get(k) or 0.0) if not _cmc_global_stale else 0.0
                indicators_with_v4.setdefault("cmc_btc_dom_pct", _cmcf("btc_dominance_pct"))
                indicators_with_v4.setdefault("cmc_total_mcap_usd", _cmcf("total_market_cap_usd") / 1e12)
                indicators_with_v4.setdefault("cmc_total_volume_usd", _cmcf("total_volume_24h_usd") / 1e9)
                indicators_with_v4.setdefault("cmc_active_cryptos", _cmcf("active_cryptocurrencies"))
            except Exception:
                for _cmck in ("cmc_btc_dom_pct", "cmc_total_mcap_usd", "cmc_total_volume_usd", "cmc_active_cryptos"):
                    indicators_with_v4.setdefault(_cmck, 0.0)

            # ------------------------------------------------------------------
            # Macro Metadata Block: composite macro_status / quality / reason / age.
            # Reads runtime:coingecko:circuit:status to distinguish 429 from plain stale.
            # Sets macro_status_ok (float 0/1), macro_quality (0.1–1.0),
            # macro_age_ms, macro_reason_code — metadata fields, not ML features.
            # ML feature values (cg_*/cp_*/cmc_*) stay 0.0 when stale (unchanged).
            # ------------------------------------------------------------------
            try:
                _macro_circuit_open = False
                try:
                    _rc_cg_circuit = getattr(self, "_redis_client_main", None) or getattr(self, "_redis_client", None)
                    _raw_circuit = _ctx_hgetall(_rc_cg_circuit, "runtime:coingecko:circuit:status")
                    if _raw_circuit:
                        _circuit = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw_circuit.items()
                        }
                        if _circuit.get("status") == "open":
                            _reopen_at = float(_circuit.get("reopen_at_ms") or 0)
                            if _reopen_at > int(now_ts):
                                _macro_circuit_open = True
                except Exception:
                    pass

                # Stale state from each provider (default True if block threw before setting)
                _cg_fresh = not locals().get("_cg_global_stale", True)
                _cp_fresh = not locals().get("_cp_global_stale", True)
                _cmc_fresh = not locals().get("_cmc_global_stale", True)

                if _cg_fresh:
                    _macro_status = "ok"
                    _macro_quality = 1.0
                    _macro_reason_code = "ok"
                elif _cp_fresh or _cmc_fresh:
                    # CG stale but fallback provider is live
                    _macro_status = "degraded"
                    _macro_quality = 0.7
                    _macro_reason_code = "cg_fallback"
                else:
                    # All providers stale
                    _macro_status = "stale"
                    _macro_quality = 0.2 if _macro_circuit_open else 0.1
                    _macro_reason_code = "provider_429" if _macro_circuit_open else "all_stale"

                # Age from freshest available source
                _macro_age_ms_val = float(locals().get("_cg_max_lag_ms", 600_000))
                for _mts, _mstale in (
                    (float((locals().get("_cg_global") or {}).get("ts_ms") or 0), not _cg_fresh),
                    (float((locals().get("_cp_global") or {}).get("ts_ms") or 0), not _cp_fresh),
                    (float((locals().get("_cmc_global") or {}).get("ts_ms") or 0), not _cmc_fresh),
                ):
                    if not _mstale and _mts > 0:
                        _macro_age_ms_val = max(0.0, int(now_ts) - _mts)
                        break

                indicators_with_v4.setdefault("macro_status_ok", 1.0 if _macro_status == "ok" else 0.0)
                indicators_with_v4.setdefault("macro_quality", _macro_quality)
                indicators_with_v4.setdefault("macro_age_ms", _macro_age_ms_val)
                indicators_with_v4.setdefault("macro_reason_code", _macro_reason_code)
            except Exception:
                indicators_with_v4.setdefault("macro_status_ok", 0.0)
                indicators_with_v4.setdefault("macro_quality", 0.0)
                indicators_with_v4.setdefault("macro_age_ms", 0.0)
                indicators_with_v4.setdefault("macro_reason_code", "error")

            # ------------------------------------------------------------------
            # Phase 8.5: Deribit extended (Group XVII) — options OI + perp basis.
            # Re-uses _deribit_data/_deribit_stale/_deribit_max_lag_ms (Phase 8.1 W3b).
            # Per-symbol basis: ctx:deribit:{symbol} — only BTC/ETH have data, others → 0.
            # ------------------------------------------------------------------
            try:
                _dxf = lambda k: float((_deribit_data or {}).get(k) or 0.0) if not _deribit_stale else 0.0
                indicators_with_v4.setdefault(
                    "deribit_btc_options_oi_usd",
                    _dxf("btc_options_oi_proxy") / 1e9,
                )
                indicators_with_v4.setdefault(
                    "deribit_eth_options_oi_usd",
                    _dxf("eth_options_oi_proxy") / 1e9,
                )
                _rc_dx = getattr(self, "_redis_client_main", None) or getattr(self, "_redis_client", None)
                _deribit_sym_data: dict[str, Any] = {}
                _deribit_sym_stale = True
                try:
                    _raw_ds = _ctx_get(_rc_dx, f"ctx:deribit:{symbol}")
                    if _raw_ds:
                        if isinstance(_raw_ds, (bytes, bytearray)):
                            _raw_ds = _raw_ds.decode("utf-8", "ignore")
                        _deribit_sym_data = json.loads(_raw_ds) if isinstance(_raw_ds, str) else {}
                        _ds_ts = float(_deribit_sym_data.get("ts_ms") or 0.0)
                        _deribit_sym_stale = (
                            (int(now_ts) - _ds_ts) > _deribit_max_lag_ms if _ds_ts > 0 else True
                        )
                except Exception:
                    _deribit_sym_data = {}
                indicators_with_v4.setdefault(
                    "deribit_perp_basis_bps",
                    float(_deribit_sym_data.get("deribit_perp_basis_bps") or 0.0)
                    if not _deribit_sym_stale else 0.0,
                )
            except Exception:
                for _dxk in (
                    "deribit_btc_options_oi_usd", "deribit_eth_options_oi_usd", "deribit_perp_basis_bps",
                ):
                    indicators_with_v4.setdefault(_dxk, 0.0)

            # ------------------------------------------------------------------
            # Phase 8.5: DefiLlama slow-regime context (Group XVIII).
            # Source:
            #   runtime:defillama:stablecoins   HASH (~900s poll)
            #   runtime:defillama:chain:Ethereum HASH (~900s poll)
            #   runtime:defillama:dexs:Ethereum  HASH (~300s poll)
            # Stale guard: DL_MAX_LAG_MS (1800s default) — slow-regime only.
            # Fail-open: all features → 0.0 when stale/unavailable.
            # ------------------------------------------------------------------
            try:
                _dl_max_lag_ms = float(os.getenv("DL_MAX_LAG_MS", "1800000") or "1800000")
                _rc_dl = getattr(self, "_redis_client_main", None) or getattr(self, "_redis_client", None)
                # stablecoins snapshot
                _dl_stable: dict[str, Any] = {}
                _dl_stable_stale = True
                try:
                    _raw_dls = _ctx_hgetall(_rc_dl, "runtime:defillama:stablecoins")
                    if _raw_dls:
                        _dl_stable = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw_dls.items()
                        }
                        _dls_ts = float(_dl_stable.get("ts_ms") or 0.0)
                        _dl_stable_stale = (
                            (int(now_ts) - _dls_ts) > _dl_max_lag_ms if _dls_ts > 0 else True
                        )
                except Exception:
                    _dl_stable = {}
                _dlsf = lambda k: float(_dl_stable.get(k) or 0.0) if not _dl_stable_stale else 0.0
                indicators_with_v4.setdefault("dl_stablecoin_mcap_usd", _dlsf("stablecoin_mcap_total") / 1e12)
                indicators_with_v4.setdefault("dl_stablecoin_mcap_delta_1d", _dlsf("stablecoin_mcap_delta_1d"))
                _dl_regime_str = str(_dl_stable.get("stablecoin_risk_regime") or "neutral").lower()
                _dl_regime_code = {"neutral": 0.0, "risk_on": 1.0, "risk_off": -1.0}.get(_dl_regime_str, 0.0)
                indicators_with_v4.setdefault(
                    "dl_stablecoin_risk_regime_code",
                    _dl_regime_code if not _dl_stable_stale else 0.0,
                )
                # Ethereum TVL snapshot
                _dl_eth_chain: dict[str, Any] = {}
                _dl_eth_stale = True
                try:
                    _raw_dlc = _ctx_hgetall(_rc_dl, "runtime:defillama:chain:Ethereum")
                    if _raw_dlc:
                        _dl_eth_chain = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw_dlc.items()
                        }
                        _dlc_ts = float(_dl_eth_chain.get("ts_ms") or 0.0)
                        _dl_eth_stale = (
                            (int(now_ts) - _dlc_ts) > _dl_max_lag_ms if _dlc_ts > 0 else True
                        )
                except Exception:
                    _dl_eth_chain = {}
                indicators_with_v4.setdefault(
                    "dl_eth_tvl_usd",
                    float(_dl_eth_chain.get("tvl_usd") or 0.0) / 1e9 if not _dl_eth_stale else 0.0,
                )
                # Ethereum DEX volume snapshot
                _dl_eth_dex: dict[str, Any] = {}
                _dl_dex_stale = True
                try:
                    _raw_dld = _ctx_hgetall(_rc_dl, "runtime:defillama:dexs:Ethereum")
                    if _raw_dld:
                        _dl_eth_dex = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw_dld.items()
                        }
                        _dld_ts = float(_dl_eth_dex.get("ts_ms") or 0.0)
                        _dl_dex_stale = (
                            (int(now_ts) - _dld_ts) > _dl_max_lag_ms if _dld_ts > 0 else True
                        )
                except Exception:
                    _dl_eth_dex = {}
                indicators_with_v4.setdefault(
                    "dl_eth_dex_vol_delta_1d_pct",
                    float(_dl_eth_dex.get("dex_volume_delta_1d_pct") or 0.0) if not _dl_dex_stale else 0.0,
                )
                indicators_with_v4.setdefault(
                    "dl_dex_volume_spike_z",
                    float(_dl_eth_dex.get("dex_volume_spike_z") or 0.0) if not _dl_dex_stale else 0.0,
                )
                # DefiLlama fees (Ethereum) — protocol revenue momentum
                _dl_eth_fees: dict[str, Any] = {}
                _dl_fees_stale = True
                try:
                    _raw_dlf = _ctx_hgetall(_rc_dl, "runtime:defillama:fees:Ethereum")
                    if _raw_dlf:
                        _dl_eth_fees = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw_dlf.items()
                        }
                        _dlf_ts = float(_dl_eth_fees.get("ts_ms") or 0.0)
                        _dl_fees_stale = (
                            (int(now_ts) - _dlf_ts) > _dl_max_lag_ms if _dlf_ts > 0 else True
                        )
                except Exception:
                    _dl_eth_fees = {}
                indicators_with_v4.setdefault(
                    "dl_eth_fees_24h_usd",
                    float(_dl_eth_fees.get("fees_24h_usd") or 0.0) / 1e6 if not _dl_fees_stale else 0.0,
                )
                indicators_with_v4.setdefault(
                    "dl_eth_fees_revenue_momentum",
                    float(_dl_eth_fees.get("fees_revenue_momentum") or 0.0) if not _dl_fees_stale else 0.0,
                )
                # DefiLlama perps OI delta
                _dl_perps: dict[str, Any] = {}
                _dl_perps_stale = True
                try:
                    _raw_dlp = _ctx_hgetall(_rc_dl, "runtime:defillama:perps_oi")
                    if _raw_dlp:
                        _dl_perps = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw_dlp.items()
                        }
                        _dlp_ts = float(_dl_perps.get("ts_ms") or 0.0)
                        _dl_perps_stale = (
                            (int(now_ts) - _dlp_ts) > _dl_max_lag_ms if _dlp_ts > 0 else True
                        )
                except Exception:
                    _dl_perps = {}
                indicators_with_v4.setdefault(
                    "dl_perps_oi_delta_1d_pct",
                    float(_dl_perps.get("defillama_perps_oi_delta_1d_pct") or 0.0) if not _dl_perps_stale else 0.0,
                )
                _ext_ctx_track("defillama", _dl_stable_stale, float(_dl_stable.get("ts_ms") or 0), float(now_ts))
            except Exception:
                for _dlk in (
                    "dl_stablecoin_mcap_usd", "dl_stablecoin_mcap_delta_1d",
                    "dl_stablecoin_risk_regime_code", "dl_eth_tvl_usd", "dl_eth_dex_vol_delta_1d_pct",
                    "dl_dex_volume_spike_z", "dl_eth_fees_24h_usd", "dl_eth_fees_revenue_momentum",
                    "dl_perps_oi_delta_1d_pct",
                ):
                    indicators_with_v4.setdefault(_dlk, 0.0)

            # ------------------------------------------------------------------
            # Phase 4.12: Macro event calendar (ctx:macro:global STRING).
            # Source: services/macro_calendar_scheduler.py (60s poll, main redis).
            # Fields: macro_event_severity (0/1/2), minutes_to/after_macro_event.
            # Stale guard: MACRO_MAX_LAG_MS (3600s default) — slow-update only.
            # Fail-open: 0.0 on stale / unavailable.
            # ------------------------------------------------------------------
            try:
                _macro_max_lag_ms = float(os.getenv("MACRO_MAX_LAG_MS", "3600000") or "3600000")
                _macro_stale = True
                _macro_data: dict[str, Any] = {}
                _rc_macro = getattr(self, "_redis_client_main", None) or getattr(self, "_redis_client", None)
                try:
                    _raw_macro = _ctx_get(_rc_macro, "ctx:macro:global")
                    if _raw_macro:
                        if isinstance(_raw_macro, (bytes, bytearray)):
                            _raw_macro = _raw_macro.decode("utf-8", "ignore")
                        _macro_data = json.loads(_raw_macro)
                        _macro_ts = float(_macro_data.get("ts_ms") or 0.0)
                        _macro_stale = (int(now_ts) - _macro_ts) > _macro_max_lag_ms if _macro_ts > 0 else True
                except Exception:
                    _macro_data = {}
                _mf = lambda k: float(_macro_data.get(k) or 0.0) if not _macro_stale else 0.0
                indicators_with_v4.setdefault("macro_event_severity", _mf("macro_event_severity"))
                indicators_with_v4.setdefault("minutes_to_macro_event", _mf("minutes_to_macro_event"))
                indicators_with_v4.setdefault("minutes_after_macro_event", _mf("minutes_after_macro_event"))
                _ext_ctx_track("macro", _macro_stale, float(_macro_data.get("ts_ms") or 0), float(now_ts))
            except Exception:
                for _mk in ("macro_event_severity", "minutes_to_macro_event", "minutes_after_macro_event"):
                    indicators_with_v4.setdefault(_mk, 0.0)

            # ------------------------------------------------------------------
            # Phase P2: Bybit cross-venue context (Group XXI).
            # Source: runtime:bybit:{symbol} HASH (Go bybit_features_collector, ~15s).
            # Keys: bybit_funding_rate, bybit_ret_1m, bybit_oi_delta_5m,
            #       bybit_taker_buy_sell_ratio, binance_bybit_price_diff_bps,
            #       binance_bybit_oi_divergence.
            # Stale guard: BYBIT_MAX_LAG_MS (120s default).
            # ------------------------------------------------------------------
            try:
                _bybit_max_lag_ms = float(os.getenv("BYBIT_MAX_LAG_MS", "120000") or "120000")
                _rc_bybit = getattr(self, "_redis_client_main", None) or getattr(self, "_redis_client", None)
                _bybit_sym: dict[str, Any] = {}
                _bybit_stale = True
                try:
                    _raw_bybit = _ctx_hgetall(_rc_bybit, f"runtime:bybit:{symbol}")
                    if _raw_bybit:
                        _bybit_sym = {
                            (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                            (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                            for k, v in _raw_bybit.items()
                        }
                        _bybit_ts = float(_bybit_sym.get("ts_ms") or 0.0)
                        _bybit_stale = (
                            (int(now_ts) - _bybit_ts) > _bybit_max_lag_ms if _bybit_ts > 0 else True
                        )
                except Exception:
                    _bybit_sym = {}
                _bbtf = lambda k: float(_bybit_sym.get(k) or 0.0) if not _bybit_stale else 0.0
                indicators_with_v4.setdefault("bybit_funding_rate", _bbtf("funding_rate"))
                indicators_with_v4.setdefault("bybit_ret_1m", _bbtf("ret_1m"))
                indicators_with_v4.setdefault("bybit_oi_delta_5m", _bbtf("oi_delta_5m"))
                indicators_with_v4.setdefault("bybit_taker_buy_sell_ratio", _bbtf("taker_buy_sell_ratio"))
                # Compute cross-venue features inline using Binance indicators_with_v4 data
                _bybit_last = _bbtf("last_price")
                _binance_mid = float(indicators_with_v4.get("mid_price") or indicators_with_v4.get("last_price") or 0.0)
                _price_diff_bps = (
                    (_binance_mid - _bybit_last) / _binance_mid * 10000.0
                    if _binance_mid > 0 and not _bybit_stale else 0.0
                )
                indicators_with_v4.setdefault("binance_bybit_price_diff_bps", _price_diff_bps)
                _bybit_oi_d5m = _bbtf("oi_delta_5m")
                _binance_oi_d5m = float(indicators_with_v4.get("oi_delta_5m") or 0.0)
                indicators_with_v4.setdefault(
                    "binance_bybit_oi_divergence",
                    _bybit_oi_d5m - _binance_oi_d5m if not _bybit_stale else 0.0,
                )
            except Exception:
                for _bk in (
                    "bybit_funding_rate", "bybit_ret_1m", "bybit_oi_delta_5m",
                    "bybit_taker_buy_sell_ratio", "binance_bybit_price_diff_bps", "binance_bybit_oi_divergence",
                ):
                    indicators_with_v4.setdefault(_bk, 0.0)

            # ------------------------------------------------------------------
            # Phase 4.6: Cross-symbol sector aggregation.
            #   sector_delta_z_median  — median oi_delta_z across symbols (same process)
            #   sector_obi_median      — median OBI across symbols (same process)
            # Uses module-level _SECTOR_CROSS_CACHE, updated each signal, entries
            # older than _SECTOR_CROSS_MAX_AGE_S=60s excluded.
            # ------------------------------------------------------------------
            try:
                _now_wall = time.monotonic()
                _dz_this = float(indicators_with_v4.get("oi_delta_z") or 0.0)
                _obi_this = float(indicators_with_v4.get("obi") or 0.0)
                _SECTOR_CROSS_CACHE[symbol] = (_now_wall, _dz_this, _obi_this)
                _fresh = [
                    (_dz, _ob)
                    for (_wt, _dz, _ob) in _SECTOR_CROSS_CACHE.values()
                    if _now_wall - _wt <= _SECTOR_CROSS_MAX_AGE_S
                ]
                if len(_fresh) >= 2:
                    _dz_vals = [x[0] for x in _fresh]
                    _obi_vals = [x[1] for x in _fresh]
                    _dz_vals.sort()
                    _obi_vals.sort()
                    _mid = len(_dz_vals) // 2
                    _sector_dz_med = (
                        _dz_vals[_mid] if len(_dz_vals) % 2 == 1
                        else (_dz_vals[_mid - 1] + _dz_vals[_mid]) * 0.5
                    )
                    _mid2 = len(_obi_vals) // 2
                    _sector_obi_med = (
                        _obi_vals[_mid2] if len(_obi_vals) % 2 == 1
                        else (_obi_vals[_mid2 - 1] + _obi_vals[_mid2]) * 0.5
                    )
                else:
                    _sector_dz_med = _dz_this
                    _sector_obi_med = _obi_this
                indicators_with_v4.setdefault("sector_delta_z_median", _sector_dz_med)
                indicators_with_v4.setdefault("sector_obi_median", _sector_obi_med)
            except Exception:
                indicators_with_v4.setdefault("sector_delta_z_median", 0.0)
                indicators_with_v4.setdefault("sector_obi_median", 0.0)

            # ------------------------------------------------------------------
            # Phase 4.7: Liq heatmap aliases — derived from existing liqmap_5m_*
            # features (already in indicators dict from liqmap_features_v1).
            #   liq_cluster_dist_above_bps  = liqmap_5m_dist_up_bps (nearest short cluster)
            #   liq_cluster_dist_below_bps  = liqmap_5m_dist_dn_bps (nearest long cluster)
            #   liq_heatmap_density_above   = log1p(near_short_usd / 1M)
            #   liq_heatmap_density_below   = log1p(near_long_usd / 1M)
            # ------------------------------------------------------------------
            try:
                _lm_dist_up = float(indicators_with_v4.get("liqmap_5m_dist_up_bps") or 0.0)
                _lm_dist_dn = float(indicators_with_v4.get("liqmap_5m_dist_dn_bps") or 0.0)
                _lm_near_short = float(indicators_with_v4.get("liqmap_5m_near_short_usd") or 0.0)
                _lm_near_long = float(indicators_with_v4.get("liqmap_5m_near_long_usd") or 0.0)
                indicators_with_v4.setdefault("liq_cluster_dist_above_bps", max(0.0, _lm_dist_up))
                indicators_with_v4.setdefault("liq_cluster_dist_below_bps", max(0.0, _lm_dist_dn))
                indicators_with_v4.setdefault(
                    "liq_heatmap_density_above",
                    math.log1p(max(0.0, _lm_near_short) / 1e6),
                )
                indicators_with_v4.setdefault(
                    "liq_heatmap_density_below",
                    math.log1p(max(0.0, _lm_near_long) / 1e6),
                )
            except Exception:
                for _lhk in (
                    "liq_cluster_dist_above_bps", "liq_cluster_dist_below_bps",
                    "liq_heatmap_density_above", "liq_heatmap_density_below",
                ):
                    indicators_with_v4.setdefault(_lhk, 0.0)

            # ------------------------------------------------------------------
            # Phase 7.7: Fill-queue features (lite) — derived from existing depth_*.
            #   eta_fill_sec_norm        = clamp(eta_fill_sec / 10.0, 0..1)
            #   queue_ahead_qty_l1/l5    = depth_{bid|ask}_{1|5} on the maker side
            #   depth_to_taker_rate_ratio = depth_top5_sum / max(taker_rates, eps)
            #   maker_fill_vs_taker_cost_edge = fill_prob_proxy * tp1_bps - exec_cost
            # ------------------------------------------------------------------
            try:
                _eta = float(indicators_with_v4.get("eta_fill_sec") or 0.0)
                indicators_with_v4.setdefault("eta_fill_sec_norm", min(1.0, max(0.0, _eta / 10.0)))

                _dir_long = (direction == "LONG")
                _depth_l1 = float(
                    indicators_with_v4.get("depth_bid_1" if _dir_long else "depth_ask_1") or 0.0
                )
                _depth_l5 = float(
                    indicators_with_v4.get("depth_bid_5" if _dir_long else "depth_ask_5") or 0.0
                )
                indicators_with_v4.setdefault("queue_ahead_qty_l1", _depth_l1)
                indicators_with_v4.setdefault("queue_ahead_qty_l5", _depth_l5)
                indicators_with_v4.setdefault("queue_ahead_qty_5", _depth_l5)

                _taker_buy = float(indicators_with_v4.get("taker_buy_rate_ema") or 0.0)
                _taker_sell = float(indicators_with_v4.get("taker_sell_rate_ema") or 0.0)
                _taker_total = _taker_buy + _taker_sell
                _depth_top5 = float(indicators_with_v4.get("depth_top5_sum") or 0.0)
                indicators_with_v4.setdefault(
                    "depth_to_taker_rate_ratio",
                    _depth_top5 / max(_taker_total, 1e-6) if _depth_top5 > 0.0 else 0.0,
                )

                _fp = float(indicators_with_v4.get("fill_prob_proxy") or 0.0)
                _tp1 = float(indicators_with_v4.get("tp1_bps") or indicators_with_v4.get("liqmap_gate_reward_bps") or 0.0)
                _half_spread2 = float(indicators_with_v4.get("spread_bps") or 0.0) * 0.5
                _slip2 = float(indicators_with_v4.get("expected_slippage_bps") or 0.0)
                _fee2 = float(os.getenv("TAKER_FEE_BPS", "4.0") or "4.0")
                _exec_cost2 = max(0.0, _half_spread2 + _slip2 + _fee2)
                indicators_with_v4.setdefault(
                    "maker_fill_vs_taker_cost_edge",
                    _fp * _tp1 - _exec_cost2,
                )
            except Exception:
                for _fk in (
                    "eta_fill_sec_norm", "queue_ahead_qty_l1", "queue_ahead_qty_l5",
                    "queue_ahead_qty_5", "depth_to_taker_rate_ratio", "maker_fill_vs_taker_cost_edge",
                ):
                    indicators_with_v4.setdefault(_fk, 0.0)

            t_ml_start = time.perf_counter()
            if is_shedding:
                # P-LAG-FIX: Skip synchronous ML inference when worker is lagging to unblock Event Loop
                class _MockMLDec:
                    def __init__(self):
                        self.mode = "SHADOW"
                        self.allow = True
                        self.p_edge = 1.0
                        self.p_min = 0.0
                    def to_dict(self):
                        return {"mode": "SHADOW", "allow": True, "reason": "load_shedding_skip"}
                ml_dec = _MockMLDec()
            else:
                ml_dec = self._ml_gate.check(
                    symbol=symbol,
                    ts_ms=now_ts,
                    direction=direction,
                    scenario=str(ml_scenario),
                    indicators=indicators_with_v4,
                    rule_score=float(score),
                    rule_have=int(have),
                    rule_need=int(need),
                    cancel_spike_veto=int(gate_vetoed),
                    ok_rule=ok_pre_late_veto,  # ok before liq_veto/taker_flow mutations
                )
            t_ml_dt = time.perf_counter() - t_ml_start
            try:
                from services.orderflow.metrics import ml_inference_time_us
                ml_inference_time_us.labels(symbol=symbol, scenario=str(ml_scenario)).observe(int(t_ml_dt * 1_000_000.0))
            except Exception:
                pass
            evidence[MLKeys.DECISION] = ml_dec.to_dict()

            # ENFORCE blocks only when heuristic ok==1
            if str(ml_dec.mode).upper() == "ENFORCE" and int(ok) == 1 and not bool(ml_dec.allow):
                ok = 0
                if str(getattr(ml_dec, "kind", "")).lower().startswith("util_mh"):
                    final_reason = (
                        f"ml_block(score={getattr(ml_dec,'score',ml_dec.p_edge):.3f}"
                        f"<floor={getattr(ml_dec,'floor',ml_dec.p_min):.3f},h={getattr(ml_dec,'best_h_ms',0)})|"
                        + str(final_reason)
                    )
                else:
                    final_reason = f"ml_block(p={ml_dec.p_edge:.3f}<thr={ml_dec.p_min:.3f})|" + str(final_reason)
        except Exception as _e:
            # last-resort safety: do not crash confirm engine
            evidence[MLKeys.DECISION] = {"mode": "ERR", "error": str(_e)[:200]}

        _t_stage = _snap_stage("ml_confirm", _t_stage)

        # Add scenario v4 / explainability fields to evidence
        evidence.update({
            "scenario_v4": str(scenario_v4),
            "need_reason": str(nd.reason if nd is not None else ""),
            "policy_reason": str(policy_reason),
            "score_breakdown": score_breakdown,

            # OFI (already in evidence, but ensure consistency)
            "ofi_dir_ok": int(ofi_dir_ok),
            "ofi": float(ofi_val),
            "ofi_z": float(ofi_z),
            "ofi_stable_secs": float(ofi_stable_secs),
            "ofi_stability_score": float(ofi_stability_score),
            "ofi_leg": int(ofi_leg),

            # FP edge + exec risk
            "fp_edge_absorb": int(fp_edge_absorb),
            "spread_bps": float(spread_bps),
            "expected_slippage_bps": float(slip_bps),
            "exec_risk_bps": float(exec_risk_bps),
            "exec_risk_norm": float(exec_risk_norm),
            "exec_risk_ref_bps": float(exec_ref),
            "exec_pen": float(exec_pen),

            # Soft-fail (set above)
            "ok_soft": int(ok_soft),
            "hard_veto": str(hard_veto),

            "legs": dict(legs),
            "missing_legs": list(missing),
            "legs_detail": legs_detail,

            # Meta-model fields (will be updated below if meta is enabled)
            "meta_enable": 0,
            "meta_mode": "",
            "meta_p_min": 0.0,
            "meta_p": -1.0,
            "meta_veto": 0,
            "meta_reason": "",
        })

        # ------------------------------------------------------------------
        # Meta-labeling model (LogReg) on top of rule-gate
        # Defaults to SHADOW: does not change ok, only exports meta_p.
        # ------------------------------------------------------------------
        meta_enable = False
        meta_mode = "SHADOW"
        meta_p_min = 0.55
        meta_p = None
        meta_veto = 0  # 1 if meta would veto (shadow) or did veto (enforce)
        meta_reason = "ok"
        apply_enforce = 0  # 1 if ENFORCE should be applied (canary-share check)

        # SID used for A/B + enforce
        sid = str(indicators.get("sid", "") or indicators.get("stable_sid", "") or "")
        if not sid:
            sid = f"{symbol}|{now_ts}|{direction}|{scenario}"

        try:
            meta_enable = bool(int(cfg2.get("meta_model_enable", int(os.getenv("META_MODEL_ENABLE", "0")))))
            meta_mode = (cfg2.get("meta_model_mode", os.getenv("META_MODEL_MODE", "SHADOW"))).upper()
            meta_p_min = float(cfg2.get(MetaKeys.P_MIN, float(os.getenv("META_P_MIN", "0.55"))))
            meta_enable = bool(int(cfg2.get("meta_model_enable", int(os.getenv("META_MODEL_ENABLE", "0")))))
            meta_path = (cfg2.get("meta_model_path", os.getenv("META_MODEL_PATH", "")) or "").strip()
            reload_sec = int(cfg2.get("meta_model_reload_sec", int(os.getenv("META_MODEL_RELOAD_SEC", "60"))))
            meta_path_ch = (cfg2.get("meta_model_path_challenger", os.getenv("META_MODEL_CHALLENGER_PATH", "")) or "").strip()
            meta_ab_share = float(cfg2.get("meta_ab_challenger_share", float(os.getenv("META_AB_CHALLENGER_SHARE", "0.0")) ) or 0.0)
            meta_ab_share = max(0.0, min(1.0, meta_ab_share))
            meta_ab_salt = (cfg2.get(MetaKeys.AB_SALT, os.getenv("META_AB_SALT", "ab_v1")) or "ab_v1")
            meta_freeze = bool(int(cfg2.get("meta_model_freeze", int(os.getenv("META_MODEL_FREEZE", "0"))) or 0))
            meta_freeze_mode = (cfg2.get("meta_freeze_mode", os.getenv("META_FREEZE_MODE", "OPEN")) or "OPEN").upper()
            meta_allow_legacy = bool(int(cfg2.get("meta_allow_legacy_schema", int(os.getenv("META_ALLOW_LEGACY_SCHEMA", "0"))) or 0))

            # P25: model signature + schema pinning (runtime)
            meta_require_sig = bool(int(cfg2.get("meta_require_signature", int(os.getenv("META_MODEL_REQUIRE_SIGNATURE", "1"))) or 1))
            meta_enforce_requires_schema = bool(int(cfg2.get("meta_enforce_requires_schema", int(os.getenv("META_MODEL_ENFORCE_REQUIRES_SCHEMA", "1"))) or 1))
            meta_schema_pin_name = (cfg2.get("meta_schema_pin_name", os.getenv("META_MODEL_SCHEMA", "")) or "")
            meta_schema_pin_hash = (cfg2.get("meta_schema_pin_hash", os.getenv("META_MODEL_SCHEMA_HASH", "")) or "")


            if meta_enable and meta_path:
                mm_champ = self._load_meta_model_slot("champion", meta_path, now_ts, reload_sec)
                mm_chal = self._load_meta_model_slot("challenger", meta_path_ch, now_ts, reload_sec) if meta_path_ch else None

                if mm_champ is not None:
                    # 1. Build features using schema registry (Train==Serve parity)
                    model_schema_name = str(getattr(mm_champ, "schema_name", "") or "legacy")
                    model_schema_vers = int(getattr(mm_champ, "schema_version", 0) or 0)
                    model_schema_hash = str(getattr(mm_champ, "schema_hash", "") or getattr(mm_champ, "feature_cols_hash", "") or "")

                    # Runtime snapshots for v4+ (optional; safe if missing)
                    bs = getattr(runtime, "book_state", None)
                    runtime_snap = getattr(bs, "snap", None) if bs is not None else None
                    runtime_prev_snap = getattr(bs, "prev_snap", None) if bs is not None else None

                    # Code-side schema registry
                    SCHEMAS = {
                        META_FEAT_V1_NAME: dict(name=META_FEAT_V1_NAME, version=META_FEAT_V1_VERSION, hash=META_FEAT_V1_HASH, builder=build_meta_features_v1),
                        META_FEAT_V2_NAME: dict(name=META_FEAT_V2_NAME, version=META_FEAT_V2_VERSION, hash=META_FEAT_V2_HASH, builder=build_meta_features_v2),
                        META_FEAT_V3_NAME: dict(name=META_FEAT_V3_NAME, version=META_FEAT_V3_VERSION, hash=META_FEAT_V3_HASH, builder=build_meta_features_v3),
                        META_FEAT_V4_NAME: dict(name=META_FEAT_V4_NAME, version=META_FEAT_V4_VERSION, hash=META_FEAT_V4_HASH, builder=build_meta_features_v4),
                        META_FEAT_V5_NAME: dict(name=META_FEAT_V5_NAME, version=META_FEAT_V5_VERSION, hash=META_FEAT_V5_HASH, builder=build_meta_features_v5),
                        META_FEAT_V6_NAME: dict(name=META_FEAT_V6_NAME, version=META_FEAT_V6_VERSION, hash=META_FEAT_V6_HASH, builder=build_meta_features_v6),
                        META_FEAT_V7_NAME: dict(name=META_FEAT_V7_NAME, version=META_FEAT_V7_VERSION, hash=META_FEAT_V7_HASH, builder=build_meta_features_v7),
                        META_FEAT_V8_NAME: dict(name=META_FEAT_V8_NAME, version=META_FEAT_V8_VERSION, hash=META_FEAT_V8_HASH, builder=build_meta_features_v8),
                        META_FEAT_V9_NAME: dict(name=META_FEAT_V9_NAME, version=META_FEAT_V9_VERSION, hash=META_FEAT_V9_HASH, builder=build_meta_features_v9),
                        META_FEAT_V10_NAME: dict(name=META_FEAT_V10_NAME, version=META_FEAT_V10_VERSION, hash=META_FEAT_V10_HASH, builder=build_meta_features_v10),
                        META_FEAT_V13_OF_NAME: dict(name=META_FEAT_V13_OF_NAME, version=META_FEAT_V13_OF_VERSION, hash=META_FEAT_V13_OF_HASH, builder=build_meta_features_v13_of),
                        META_FEAT_V14_OF_NAME: dict(name=META_FEAT_V14_OF_NAME, version=META_FEAT_V14_OF_VERSION, hash=META_FEAT_V14_OF_HASH, builder=build_meta_features_v14_of),
                        META_FEAT_V15_OF_NAME: dict(name=META_FEAT_V15_OF_NAME, version=META_FEAT_V15_OF_VERSION, hash=META_FEAT_V15_OF_HASH, builder=build_meta_features_v15_of),
                    }

                    schema_cfg = SCHEMAS.get(model_schema_name)
                    if schema_cfg is None:
                        # Unknown schema: default to v1 features; mismatched models will be forced to SHADOW (unless legacy allowed).
                        schema_cfg = SCHEMAS[META_FEAT_V1_NAME]

                    local_schema_name = str(schema_cfg["name"])
                    local_schema_vers = int(schema_cfg["version"])
                    local_schema_hash = (schema_cfg.get("hash", "") or "")
                    builder = schema_cfg["builder"]

                    evidence[MetaKeys.SCHEMA_NAME] = local_schema_name
                    evidence[MetaKeys.SCHEMA_VERSION] = local_schema_vers
                    evidence[MetaKeys.SCHEMA_HASH] = local_schema_hash
                    evidence[MetaKeys.MODEL_SCHEMA_NAME] = model_schema_name
                    evidence[MetaKeys.MODEL_SCHEMA_VERSION] = model_schema_vers
                    evidence[MetaKeys.MODEL_SCHEMA_HASH] = model_schema_hash

                    # 2. Schema Guard: mismatch => SHADOW (unless explicitly allowed)
                    is_compatible = (model_schema_name == local_schema_name and model_schema_vers == local_schema_vers)
                    if model_schema_hash and local_schema_hash and model_schema_hash != local_schema_hash:
                        is_compatible = False

                    if not is_compatible:
                        if not meta_allow_legacy and meta_mode == "ENFORCE":
                            meta_mode = "SHADOW"
                            meta_reason = f"SCHEMA_MISMATCH(model={model_schema_name}.{model_schema_vers}:{model_schema_hash} code={local_schema_name}.{local_schema_vers}:{local_schema_hash})->SHADOW"

                    # 3. Build meta features for the chosen schema
                    feat, feat_missing = builder(
                        evidence=evidence,
                        indicators=indicators,
                        runtime_snap=runtime_snap,
                        runtime_prev_snap=runtime_prev_snap,
                        indicators_with_v4=indicators_with_v4,
                        legs=legs,
                        have=int(have),
                        need=int(need),
                        ok_soft=int(ok_soft),
                        rule_score=float(score),
                        exec_risk_norm=float(exec_risk_norm),
                        exec_risk_bps=float(exec_risk_bps),
                        ml_scenario=str(ml_scenario),
                    )

                    # --------------------------------------------------------------
                    # B5 Golden Replay: optional export of the exact feature vector
                    # used by MetaModelLR (train==serve parity checks).
                    #
                    # Default OFF (payload size). Enable only for sampled captures.
                    # --------------------------------------------------------------
                    try:
                        export_meta = bool(int(cfg2.get(
                            "golden_replay_export_meta_features",
                            int(os.getenv("GOLDEN_REPLAY_EXPORT_META_FEATURES", "0"))
                        )))
                    except Exception:
                        export_meta = False
                    if export_meta:
                        try:
                            max_k = int(cfg2.get(
                                "golden_replay_export_meta_features_max",
                                int(os.getenv("GOLDEN_REPLAY_EXPORT_META_FEATURES_MAX", "256"))
                            ))
                        except Exception:
                            max_k = 256
                        try:
                            mm_feats = list(getattr(mm_champ, "features", []) or [])
                            export_cols = mm_feats[:max(0, max_k)]
                            export_vec = {k: float(feat.get(k, 0.0) or 0.0) for k in export_cols}
                            evidence[MetaKeys.FEATURES_EXPORT] = export_vec
                            evidence[MetaKeys.FEATURES_EXPORT_N] = int(len(export_vec))
                            evidence[MetaKeys.FEATURES_EXPORT_COLS_HASH] = hashlib.sha1(
                                (",".join(mm_feats)).encode("utf-8")
                            ).hexdigest()
                            evidence[MetaKeys.FEATURES_EXPORT_SCHEMA] = str(local_schema_name)
                            evidence[MetaKeys.FEATURES_EXPORT_SCHEMA_HASH] = str(local_schema_hash)
                        except Exception:
                            pass

                    # 4. Export meta schema info to evidence (for drift monitoring)
                    evidence[MetaKeys.SCHEMA_NAME] = local_schema_name
                    evidence[MetaKeys.SCHEMA_VERSION] = local_schema_vers
                    evidence[MetaKeys.SCHEMA_HASH] = local_schema_hash
                    evidence[MetaKeys.MODEL_SCHEMA_NAME] = model_schema_name
                    evidence[MetaKeys.MODEL_SCHEMA_VERSION] = model_schema_vers
                    evidence[MetaKeys.MODEL_SCHEMA_HASH] = model_schema_hash

                    evidence[MetaKeys.MISSING_FEATURE_COUNT] = len(feat_missing)
                    # Cap list to avoid huge logs if everything is missing
                    evidence[MetaKeys.MISSING_FEATURES] = feat_missing[:32]

                    # 4. Metrics Emission (P10)
                    # Emission strict on MM features.
                    try:
                        mm_feats = getattr(mm_champ, "features", []) or []
                        missing_set = set(feat_missing)
                        # We use model_schema_name as 'schema' label
                        sch_label = str(model_schema_name)

                        for f in mm_feats:
                            meta_feature_seen_total(runtime, schema=sch_label, feature=f)
                            if f in missing_set:
                                meta_feature_missing_total(runtime, schema=sch_label, feature=f)
                                feature_missing_total(runtime, feature=f)
                    except Exception:
                        pass

                    # 4b. Feature coverage guard (P29)
                    # If too many model features are missing, downgrade ENFORCE -> SHADOW.
                    try:
                        mm_feats2 = getattr(mm_champ, "features", []) or []
                        cov_obj = compute_meta_feature_coverage(mm_feats2, feat_missing, max_list=32)

                        evidence[MetaKeys.MODEL_FEATURE_TOTAL] = int(cov_obj.model_total)
                        evidence[MetaKeys.MODEL_FEATURE_MISSING] = int(cov_obj.model_missing)
                        evidence[MetaKeys.FEATURE_COVERAGE] = float(cov_obj.coverage)
                        evidence[MetaKeys.FEATURE_MISSING_RATE] = float(cov_obj.missing_rate)
                        evidence[MetaKeys.MODEL_MISSING_FEATURES] = list(cov_obj.missing_model_features)

                        sch_label = str(model_schema_name)
                        dist(runtime, "meta_feature_coverage", float(cov_obj.coverage), schema=sch_label)
                        dist(runtime, "meta_feature_missing_rate", float(cov_obj.missing_rate), schema=sch_label)

                        # Thresholds (env < cfg2); keep conservative defaults
                        min_cov = float(cfg2.get("meta_min_feature_coverage", os.getenv("META_MIN_FEATURE_COVERAGE", "0.85")))
                        max_miss = int(float(cfg2.get("meta_max_missing_model_features", os.getenv("META_MAX_MISSING_MODEL_FEATURES", "999"))))

                        new_mode, cov_reason = apply_meta_coverage_guard(
                            meta_mode=(meta_mode or ""),
                            cov=cov_obj,
                            min_coverage=float(min_cov),
                            max_missing=int(max_miss),
                        )
                        if cov_reason:
                            # Preserve an existing reason if already set later by other checks
                            evidence[MetaKeys.COVERAGE_GUARD_REASON] = cov_reason
                            if not meta_reason:
                                meta_reason = cov_reason
                        meta_mode = new_mode
                    except Exception:
                        pass

                    # SID already derived above (used for A/B + enforce)
                    from services.observability.metrics_registry import ml_inference_time_us

                    t0_inf = time.perf_counter()
                    meta_p_champion = float(mm_champ.predict_proba(feat))
                    dt_inf_champ = (time.perf_counter() - t0_inf) * 1_000_000
                    ml_inference_time_us.labels(symbol=symbol, model="champion").observe(dt_inf_champ)

                    if mm_chal is not None:
                        t0_inf_chal = time.perf_counter()
                        meta_p_challenger = float(mm_chal.predict_proba(feat))
                        dt_inf_chal = (time.perf_counter() - t0_inf_chal) * 1_000_000
                        ml_inference_time_us.labels(symbol=symbol, model="challenger").observe(dt_inf_chal)
                    else:
                        meta_p_challenger = -1.0

                    # Deterministic A/B: select which model's p to apply for this sid
                    meta_arm = _ab_pick_arm(sid=sid, share=meta_ab_share, salt=meta_ab_salt) if (mm_chal is not None and meta_ab_share > 0.0) else "champion"
                    meta_p = float(meta_p_challenger if meta_arm == "challenger" and meta_p_challenger >= 0.0 else meta_p_champion)
                    if meta_p < meta_p_min:
                        meta_veto = 1
                        meta_reason = "meta_p_below_min"

                    # Optional safety freeze (fail-open default): if enabled, meta veto is disabled
                    if meta_freeze:
                        if meta_freeze_mode == "CLOSED":
                            meta_veto = 1
                            meta_reason = "meta_freeze_closed"
                        else:
                            meta_veto = 0
                            meta_reason = "meta_freeze_open"

                    # --- Canary share for ENFORCE (deterministic, per-regime) ---
                    # Determine regime bucket for per-regime share selection
                    rb = str(indicators.get("regime_bucket", "") or indicators.get("regime_group", "") or getattr(runtime, "last_regime", "") or "").lower()
                    if "news" in rb or "fomc" in rb or "cpi" in rb:
                        bucket = "news"
                    elif "trend" in rb or "bull" in rb or "bear" in rb:
                        bucket = "trend"
                    elif "range" in rb or "chop" in rb or "meanrev" in rb:
                        bucket = "range"
                    else:
                        bucket = "other"

                    # Per-regime share: use per-bucket key if enabled, otherwise legacy meta_enforce_share
                    use_per_regime = bool(int(cfg2.get("meta_enforce_per_regime", 0) or 0))
                    if use_per_regime:
                        share_key = f"meta_enforce_share_{bucket}"
                        meta_enforce_share = float(cfg2.get(share_key, cfg2.get(MetaKeys.ENFORCE_SHARE, 1.0)) or 1.0)
                    else:
                        meta_enforce_share = float(cfg2.get(MetaKeys.ENFORCE_SHARE, 1.0) or 1.0)
                    meta_enforce_share = max(0.0, min(1.0, meta_enforce_share))
                    meta_enforce_salt = (cfg2.get(MetaKeys.ENFORCE_SALT, "enf_v1") or "enf_v1")

                    hkey = f"{meta_enforce_salt}:{sid}"
                    apply_enforce = 1 if (_hash01(hkey) < meta_enforce_share) else 0

                    # ENFORCE only on canary subset
                    if meta_mode == "ENFORCE" and apply_enforce == 1 and int(ok) == 1 and (not gate_vetoed) and meta_veto == 1:
                        ok = 0
                        # mark bit
                        try:
                            if dec is not None:
                                dec.gate_bits = int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_META_VETO
                        except Exception:
                            pass
                        final_reason = f"{final_reason}|meta_veto"
        except Exception:
            pass

        # Update evidence with meta fields
        evidence[MetaKeys.ENABLE] = int(meta_enable)
        evidence[MetaKeys.MODE] = str(meta_mode)
        evidence[MetaKeys.P_MIN] = float(meta_p_min)
        evidence[MetaKeys.P] = float(meta_p if meta_p is not None else -1.0)
        evidence[MetaKeys.VETO] = int(meta_veto)
        evidence[MetaKeys.REASON] = str(meta_reason)

        # A/B fields (for outcome attribution)
        try:
            evidence[MetaKeys.ARM] = str(locals().get(MetaKeys.ARM, "") or "")
            evidence[MetaKeys.AB_SHARE] = float(locals().get(MetaKeys.AB_SHARE, 0.0) or 0.0)
            evidence[MetaKeys.AB_SALT] = str(locals().get(MetaKeys.AB_SALT, "") or "")
            evidence[MetaKeys.P_CHAMPION] = float(locals().get(MetaKeys.P_CHAMPION, -1.0) or -1.0)
            evidence[MetaKeys.P_CHALLENGER] = float(locals().get(MetaKeys.P_CHALLENGER, -1.0) or -1.0)
        except Exception:
            pass

        # Rollout fields (for observability)
        try:
            # Recompute regime bucket for evidence (same logic as above)
            rb_ev = str(indicators.get("regime_bucket", "") or indicators.get("regime_group", "") or getattr(runtime, "last_regime", "") or "").lower()
            if "news" in rb_ev or "fomc" in rb_ev or "cpi" in rb_ev:
                bucket_ev = "news"
            elif "trend" in rb_ev or "bull" in rb_ev or "bear" in rb_ev:
                bucket_ev = "trend"
            elif "range" in rb_ev or "chop" in rb_ev or "meanrev" in rb_ev:
                bucket_ev = "range"
            else:
                bucket_ev = "other"

            # Use per-regime share if enabled, otherwise legacy
            use_per_regime_ev = bool(int(cfg2.get("meta_enforce_per_regime", 0) or 0))
            if use_per_regime_ev:
                share_key_ev = f"meta_enforce_share_{bucket_ev}"
                meta_enforce_share_ev = float(cfg2.get(share_key_ev, cfg2.get(MetaKeys.ENFORCE_SHARE, 1.0)) or 1.0)
            else:
                meta_enforce_share_ev = float(cfg2.get(MetaKeys.ENFORCE_SHARE, 1.0) or 1.0)

            meta_enforce_salt = (cfg2.get(MetaKeys.ENFORCE_SALT, "enf_v1") or "enf_v1")
            sid = str(indicators.get("sid", "") or indicators.get("stable_sid", "") or "")
            if not sid:
                sid = f"{symbol}|{now_ts}|{direction}|{scenario}"
            evidence[MetaKeys.ENFORCE_SHARE] = float(meta_enforce_share_ev)
            evidence[MetaKeys.ENFORCE_COV_BUCKET] = str(bucket_ev)
            evidence[MetaKeys.ENFORCE_SALT] = str(meta_enforce_salt)
            evidence[MetaKeys.ENFORCE_KEY] = str(sid)
            evidence[MetaKeys.ENFORCE_APPLIED] = int(apply_enforce if meta_mode == "ENFORCE" else 0)
        except Exception:
            evidence[MetaKeys.ENFORCE_SHARE] = 1.0
            evidence[MetaKeys.ENFORCE_COV_BUCKET] = "other"
            evidence[MetaKeys.ENFORCE_KEY] = ""
            evidence[MetaKeys.ENFORCE_APPLIED] = 0

        _t_stage = _snap_stage("meta_model", _t_stage)

        # ------------------------------------------------------------------
        # Phase 8: Horizon ATR Canary Gate (hz_gate).
        # Controls introduction of horizon-aware ATR policy to ENFORCE.
        #
        # ENV / cfg2 controls:
        #   HZ_ENFORCE_MODE   : SHADOW (default) | CANARY | ENFORCE
        #   HZ_ENFORCE_SHARE  : 0..1 fraction of traffic in CANARY mode (default 0.05)
        #   HZ_ENFORCE_SALT   : sticky routing salt (default "hz_atr_v5")
        #   HZ_ENFORCE_SYMBOLS: comma-sep whitelist; empty = all symbols
        #
        # In SHADOW: enriches evidence only, never blocks.
        # In CANARY: blocks only for symbols in whitelist + sticky share < threshold.
        # In ENFORCE: all symbols, full enforcement.
        # Rollback: set HZ_ENFORCE_MODE=SHADOW (no deploy needed).
        # ------------------------------------------------------------------
        try:
            hz_mode = (cfg2.get("hz_enforce_mode", os.getenv("HZ_ENFORCE_MODE", "SHADOW")) or "SHADOW").upper()
            hz_share = float(cfg2.get("hz_enforce_share", float(os.getenv("HZ_ENFORCE_SHARE", "0.05"))) or 0.05)
            hz_share = max(0.0, min(1.0, hz_share))
            hz_salt = (cfg2.get("hz_enforce_salt", os.getenv("HZ_ENFORCE_SALT", "hz_atr_v5")) or "hz_atr_v5")
            hz_symbols_raw = (cfg2.get("hz_enforce_symbols", os.getenv("HZ_ENFORCE_SYMBOLS", "")) or "")
            hz_symbols = {s.strip().upper() for s in hz_symbols_raw.split(",") if s.strip()} if hz_symbols_raw else set()

            # Determine if this signal's symbol is in the canary whitelist
            sym_upper = symbol.upper()
            hz_symbol_ok = (not hz_symbols) or (sym_upper in hz_symbols)

            # Sticky routing: deterministic by symbol|session|kind
            _session = (indicators.get("session", "") or "")
            _kind = str(indicators.get("scenario_v4", "") or scenario)
            hz_routing_key = f"{hz_salt}:{sym_upper}|{_session}|{_kind}"
            hz_in_canary = hz_symbol_ok and (_hash01(hz_routing_key) < hz_share)

            # hz_gate decision: was horizon-aware ATR policy active?
            hz_active = (hz_mode == "ENFORCE") or (hz_mode == "CANARY" and hz_in_canary)
            hz_veto = 0  # placeholder: real veto logic goes here when calibration is ready

            # Record gate status in evidence for observability
            evidence[HzGateKeys.MODE] = str(hz_mode)
            evidence[HzGateKeys.ACTIVE] = int(hz_active)
            evidence[HzGateKeys.SHARE] = float(hz_share)
            evidence[HzGateKeys.SYMBOL_OK] = int(hz_symbol_ok)
            evidence[HzGateKeys.IN_CANARY] = int(hz_in_canary)
            evidence[HzGateKeys.VETO] = int(hz_veto)

            # Phase 8 block: only apply when hz_active and calibration ready
            # (hz_veto currently = 0; will be wired to horizon policy check later)
            if hz_mode == "ENFORCE" and hz_active and int(ok) == 1 and hz_veto == 1:
                ok = 0
                final_reason = f"hz_gate_enforce|{final_reason}"
        except Exception:
            evidence[HzGateKeys.MODE] = "ERR"
            evidence[HzGateKeys.ACTIVE] = 0
            evidence[HzGateKeys.VETO] = 0

        evidence[CtxKeys.ENABLE] = int(bool(cfg2.get("ofc_ctx_enable", False)))
        evidence[CtxKeys.MODE] = str(ctx_mode)
        evidence[CtxKeys.KEY] = str(ctx_key)
        evidence[CtxKeys.BUNDLE_VER] = str(getattr(self._ofc_ctx_bundle, "version", "") if self._ofc_ctx_bundle else "")
        evidence[CtxKeys.EXEC_MODEL_VER] = str(getattr(exec_pred, "model_version", "")) if exec_pred is not None else ""
        evidence[CtxKeys.RULE_MODEL_VER] = str(getattr(rule_pred, "model_version", "")) if rule_pred is not None else ""
        evidence[CtxKeys.P_RULE_RAW] = float(getattr(rule_pred, "p_rule_raw", -1.0)) if rule_pred is not None else -1.0
        evidence[CtxKeys.P_RULE_CAL] = float(getattr(rule_pred, "p_rule_cal", -1.0)) if rule_pred is not None else -1.0
        evidence[CtxKeys.COST_P50_BPS] = float(getattr(exec_pred, "cost_p50_bps", -1.0)) if exec_pred is not None else -1.0
        evidence[CtxKeys.COST_P90_BPS] = float(getattr(exec_pred, "cost_p90_bps", -1.0)) if exec_pred is not None else -1.0
        evidence[CtxKeys.EXEC_RISK_REF_BPS] = float(getattr(exec_pred, "exec_risk_ref_bps_ctx", exec_ref)) if exec_pred is not None else float(exec_ref)
        evidence[CtxKeys.SCORE_MIN] = float(getattr(rule_pred, "score_min_ctx", cfg2.get("of_score_min", 0.40))) if rule_pred is not None else float(cfg2.get("of_score_min", 0.40))
        evidence[CtxKeys.REASON] = str(getattr(ctx_decision, "reason", "")) if ctx_decision is not None else ""
        evidence[CtxKeys.INFER_LATENCY_US] = int(ctx_infer_latency_us)

        # --------------------------------------------------------------
        # B5 Golden Replay: optional capture sidecar (inputs + runtime snapshot)
        # so replay harness can run even if outer logger only stores OFConfirmV3.
        # Default OFF (payload size). Enable only on sampled captures.
        # --------------------------------------------------------------
        try:
            cap_enable = bool(int(cfg2.get(
                "golden_replay_capture_enable",
                int(os.getenv("GOLDEN_REPLAY_CAPTURE_ENABLE", "0"))
            )))
        except Exception:
            cap_enable = False
        if cap_enable:
            with contextlib.suppress(Exception):
                evidence["golden_replay_inputs_v1"] = {
                    "symbol": symbol,
                    "tf": tf,
                    "direction": direction,
                    "tick_ts_ms": tick_ts_ms,
                    "price": price,
                    "delta_z": float(delta_z),
                    "dq_policy_hash": (indicators.get("dq_policy_hash") or ""),
                    "dq_policy_feature_manifest_hash_v1": (indicators.get("dq_policy_feature_manifest_hash_v1") or ""),
                    "runtime_snapshot": OFConfirmEngine.export_runtime_snapshot(runtime, indicators),
                }

        legacy_reason = final_reason
        score_veto_family = {
            "score_veto",
            "vol_shock_score_veto",
            "saw_chop_score_veto",
        }
        ctx_shadow_disagree = 0
        if ctx_decision is not None:
            ctx_allow = bool(getattr(ctx_decision, "allow", False))
            ctx_shadow_disagree = 1 if int(ok) != int(ctx_allow) else 0
            evidence[CtxKeys.SHADOW_DISAGREE] = int(ctx_shadow_disagree)
            evidence[CtxKeys.ALLOW] = int(ctx_allow)
            evidence[CtxKeys.EDGE_NET_P50_BPS] = float(getattr(ctx_decision, "edge_net_p50_bps", -999.0))
            evidence[CtxKeys.EDGE_NET_P90_BPS] = float(getattr(ctx_decision, "edge_net_p90_bps", -999.0))
            evidence[CtxKeys.FALLBACK_LEVEL] = str(getattr(ctx_decision, "fallback_level", ""))
            if ctx_mode == "tighten_only" and int(ok) == 1 and not ctx_allow:
                ok = 0
                final_reason = f"ctx_tighten:{getattr(ctx_decision, 'reason', 'deny')}"
            elif ctx_mode == "replace_score_veto" and str(legacy_reason) in score_veto_family:
                ok = 1 if ctx_allow else 0
                final_reason = f"ctx_replace:{getattr(ctx_decision, 'reason', 'deny')}"

        # Final safeguard: critical vetoes must never be bypassed by late-stage
        # ok-rewrites (e.g. ctx_replace_score_veto at ~5547 can flip ok=1 even
        # when burst_veto/hard_veto/gate_vetoed already fired). Mirror the
        # earlier guard at the scenario_v4 boundary so ofc.ok is consistent
        # with what downstream readers expect.
        if burst_veto == 1 or hard_veto or gate_vetoed:
            ok = 0

        ofc = OFConfirmV3(
            v=3,
            symbol=symbol,
            ts_ms=now_ts,
            direction=direction,
            scenario=str(getattr(dec, "scenario", scenario) if dec else scenario),
            ok=int(ok),
            score=float(score),
            have=int(have),
            need=int(need),
            gate_bits=int(getattr(dec, "gate_bits", 0)),
            reason=str(final_reason),
            evidence=evidence,
            contrib=contrib,
        )

        # v14_of: write og_* (rule-gate consensus) keys into shared `indicators` dict
        # so they flow through signal_pipeline → signals:of:inputs → ML dataset.
        # Fail-open: build_og_payload returns 16 zero-valued keys if any input is malformed.
        # Every fail-open path increments `og_payload_fail_open_total{reason}` so
        # silent drift to all-zero og_* features is alertable (>1% over 5m).
        try:
            from core.v14_of_features import build_og_payload, _record_fail_open as _og_fail
        except Exception:
            try:
                from core.v14_of_features import _record_fail_open as _og_fail
                _og_fail("import_error")
            except Exception:
                pass
        else:
            try:
                indicators.update(build_og_payload(ofc=ofc, dec=dec, indicators=indicators))
            except Exception:
                try:
                    _og_fail("build_raised")
                except Exception:
                    pass

        # v14_of Group OE + Phase 7.8/7.9/7.9b: copy external/derivative/composite
        # feature keys from inference-time `indicators_with_v4` into outbound
        # `indicators`, otherwise they vectorize to 0.0 in the offline dataset
        # (train/serve skew). Pure copy, no I/O. See external_features_payload_v1.py.
        try:
            from core.external_features_payload_v1 import (
                build_external_features_payload,
                _record_fail_open as _ext_fail,
            )
        except Exception:
            # Module unavailable — cannot import _record_fail_open from the same
            # failed module. Fail-open: skip external features, no veto.
            pass
        else:
            try:
                # indicators_with_v4 is created upstream in this same `build()` method
                # (around line 2961). It contains Phase 7.x/8.x populates with stale
                # guards already applied. NameError → fall-through to fail-open path.
                indicators.update(
                    build_external_features_payload(indicators_with_v4, indicators)
                )
            except Exception:
                with contextlib.suppress(Exception):
                    _ext_fail("build_raised")

        # v12_of new groups (MA/MB/MC/MD/ME/MX, 21 keys): tick_processor lives
        # in reference/ — its inject_v12_of_features call is dead code in prod.
        # Without this wiring those 21 v13_of-base keys are ABSENT in the
        # outbound payload (audit 2026-05-19) → vectorizer fail-opens to 0.0,
        # but per-key missing counter never increments because the key never
        # arrives. Insert here so v13_tracker.compute_interactions() below
        # can read populated v12 keys for NX cross-products.
        try:
            from core.v12_of_features import inject_v12_of_features
            inject_v12_of_features(runtime=runtime, now_ms=now_ts, indicators=indicators)
        except Exception:
            pass

        # v13_of Groups NA/NB/NC/NE/NF + NX interactions: merge V13RuntimeTracker
        # snapshot into outbound `indicators` so the registry vectorizer sees
        # real values (otherwise 28 v13_of keys are constant 0 — fixes train/serve
        # skew identified 2026-05-16).
        try:
            v13_tracker = getattr(runtime, "v13_tracker", None)
            if v13_tracker is not None:
                v13_snap = v13_tracker.snapshot()
                if v13_snap:
                    indicators.update(v13_snap)
                    indicators.update(
                        v13_tracker.compute_interactions(v13_snap, indicators)
                    )
        except Exception:
            pass

        # v13_of Group ND: cross-asset/macro runtime attrs loaded by
        # maybe_load_crossasset_v13() into SymbolRuntime but never forwarded
        # to indicators. Without this, all 4 ND keys are constant 0.0 in the
        # offline dataset (train/serve skew).
        try:
            for _nd_key in (
                "btc_dominance_momentum",
                "oi_weighted_funding",
                "total_market_oi_delta",
                "liq_heatmap_distance_bps",
            ):
                if _nd_key not in indicators:
                    indicators[_nd_key] = float(getattr(runtime, _nd_key, 0.0) or 0.0)
        except Exception:
            pass

        # logic to write to NDJSON.
        #
        # LOAD SHEDDING: skip capture if lagging to protect hot-path.
        #
        do_capture = (cfg2.get("ofc_capture", 0) == 1) or (__import__("os").environ.get("OFC_CAPTURE_ENABLE") == "1")
        if do_capture and is_shedding:
            do_capture = False
            indicators["ofc_capture_shedded"] = 1

        if do_capture:
            try:
                from core_snapshot.ofc_capture_v1 import maybe_capture_ofc_v1
                _rt = locals().get("runtime") or getattr(self, "runtime", None)
                _ind = locals().get("indicators") or locals().get("ind") or {}
                _ts = locals().get("now_ts_ms") or locals().get("ts_ms") or 0
                if not _ts:
                    _ts = int(_get_ny_time_millis())
                maybe_capture_ofc_v1(engine=self, runtime=_rt, indicators=_ind, cfg2=cfg2, ofc=ofc, dec=dec, now_ts_ms=int(_ts))
            except Exception:
                pass

        # Calib capture (optional)
        if not is_shedding:
            try:
                _rt = locals().get("runtime") or getattr(self, "runtime", None)
                _ind = locals().get("indicators") or locals().get("ind") or {}
                _ts = locals().get("now_ts_ms") or locals().get("ts_ms") or now_ts
                _emit_cont_ctx_calib_capture_v1(runtime=_rt, indicators=_ind, cfg2=cfg2, ofc=ofc, dec=dec, now_ts_ms=int(_ts))
            except Exception:
                pass
        else:
             indicators["calib_capture_shedded"] = 1

        _t_stage = _snap_stage("capture_export", _t_stage)

        return ofc, dec


    def restore_cancel_gate_state(self, state: dict[str, Any]) -> None:
        """Restore cancellation gate snapshot if available."""
        if not isinstance(state, dict):
            return
        g = getattr(self, "_cancel_spike_gate", None)
        if g is None:
            # Try to instantiate if possible
            try:
                self._cancel_spike_gate = CancellationSpikeGate()  # type: ignore
                g = self._cancel_spike_gate
            except Exception:
                return
        restore = getattr(g, "restore_state", None)
        if callable(restore):
            restore(state)

    def export_cancel_gate_state(self) -> dict[str, Any] | None:
        """Export cancel gate state for replay (fail-open)."""
        g = getattr(self, '_cancel_spike_gate', None)
        if g is None:
            return None
        for meth in ('export_state', 'to_state_dict', 'state_dict'):
            fn = getattr(g, meth, None)
            if callable(fn):
                try:
                    st = fn()
                    return st if isinstance(st, dict) else {'state': st}
                except Exception:
                    return None
        try:
            return {k: v for k, v in getattr(g, '__dict__', {}).items() if isinstance(v, (int, float, str, bool)) or v is None}
        except Exception:
            return None

    @staticmethod
    def export_runtime_snapshot(runtime: Any, indicators: dict[str, Any] | None = None) -> dict[str, Any]:
        """Minimal runtime snapshot for deterministic replay.

        Contract philosophy:
          - Always include the *keys* (even if value is None) so we can detect
            capture regressions (missing fields after refactors).
          - Keep it JSON-safe: primitives + shallow dicts only.
          - Only include fields that OFConfirmEngine reads via getattr(runtime, ...).

        NOTE: This is *not* a full runtime dump. It is a replay contract.
        """
        ind = indicators or {}

        def _pick(obj: Any, keys: tuple[str, ...]) -> dict[str, Any] | None:
            if obj is None:
                return None
            out: dict[str, Any] = {}
            for k in keys:
                try:
                    v = _get_attr_or_key(obj, k, None)
                except Exception:
                    v = None
                if v is None:
                    continue
                if isinstance(v, (str, int, float, bool)) or v is None:
                    out[k] = v
                elif isinstance(v, dict):
                    vv: dict[str, Any] = {}
                    for kk, x in v.items():
                        if isinstance(x, (str, int, float, bool)) or x is None:
                            vv[str(kk)] = x
                    out[k] = vv
                else:
                    with contextlib.suppress(Exception):
                        out[k] = str(v)
            return out

        snap: dict[str, Any] = {
            "schema": 3,
            "symbol": None,
            "ts_ms": None,
            # scalar-ish
            "last_regime": None,
            "liq_regime": None,
            "book_churn_hi": None,
            "cont_ctx_ts_ms": None,
            "pressure_hi": None,
            # dict-ish
            "dynamic_cfg": None,
            # events / ctx
            "last_bar": None,
            "last_obi_event": None,
            "last_iceberg_event": None,
            "last_ofi_event": None,
            "last_sweep": None,
            "last_reclaim": None,
            "last_wp": None,
            "last_div": None,
            "last_fp_edge": None,
            # capture convenience
            "now_ts_ms_used": None,
        }

        # Scalars
        for k in ("last_regime", "liq_regime", "book_churn_hi", "cont_ctx_ts_ms", "pressure_hi", "symbol", "ts_ms", "ts"):
            try:
                v = getattr(runtime, k, None)
            except Exception:
                v = None
            if v is None:
                continue
            if k == "ts":
                snap["ts_ms"] = v
            else:
                if isinstance(v, (str, int, float, bool)):
                    snap[k] = v
                else:
                    with contextlib.suppress(Exception):
                        snap[k] = str(v)

        # pressure_hi: prefer deterministic inputs
        try:
            if "pressure_hi" in ind:
                snap["pressure_hi"] = int(ind.get("pressure_hi", 0) or 0)
            elif "pressure_hi_flag" in ind:
                snap["pressure_hi"] = int(ind.get("pressure_hi_flag", 0) or 0)
            else:
                ph = getattr(runtime, "pressure_hi", None)
                if ph is not None:
                    snap["pressure_hi"] = int(ph or 0)
        except Exception:
            pass

        # dynamic_cfg (JSON primitives only)
        try:
            dyn = getattr(runtime, "dynamic_cfg", None)
            if isinstance(dyn, dict):
                snap["dynamic_cfg"] = {str(kk): vv for kk, vv in dyn.items() if isinstance(vv, (str, int, float, bool)) or vv is None}
        except Exception:
            pass

        # last_bar (microbar footprint-lite)
        try:
            bar = getattr(runtime, "last_bar", None)
            if bar is not None:
                snap["last_bar"] = _pick(bar, (
                    "end_ts_ms",
                    "open", "high", "low", "close",
                    "fp_enabled",
                    "fp_absorption_bias",
                    "fp_ladder_low_len", "fp_ladder_high_len",
                    "fp_poc_on_edge",
                    "fp_eff_quote", "fp_eff_delta",
                    "fp_quote_delta",
                    "fp_n_buckets",
                    "fp_max_imbalance",
                    "fp_absorb_score",
                    "fp_progress",
                    "fp_peak_delta",
                    "fp_bucket_px",
                ))
        except Exception:
            pass

        # Events used by evidence modules (store as dict)
        try:
            obi = getattr(runtime, "last_obi_event", None)
            snap["last_obi_event"] = _pick(obi, ("ts_ms", "direction", "obi", "obi_z", "stable_secs", "stable", "trend_dir"))
        except Exception:
            pass
        try:
            ice = getattr(runtime, "last_iceberg_event", None)
            snap["last_iceberg_event"] = _pick(ice, ("ts_ms", "side", "refresh", "duration", "price", "qty", "rate", "strength", "distance_bps"))
        except Exception:
            pass
        try:
            ofi = getattr(runtime, "last_ofi_event", None)
            snap["last_ofi_event"] = _pick(ofi, ("ts_ms", "direction", "ofi", "ofi_z", "stable_secs", "stable"))
        except Exception:
            pass
        try:
            sweep = getattr(runtime, "last_sweep", None)
            snap["last_sweep"] = _pick(sweep, ("ts_ms", "kind", "direction_bias"))
        except Exception:
            pass
        try:
            reclaim = getattr(runtime, "last_reclaim", None)
            snap["last_reclaim"] = _pick(reclaim, ("ts_ms", "hold_bars", "direction_bias", "level", "pool_id"))
        except Exception:
            pass
        try:
            wp = getattr(runtime, "last_wp", None)
            snap["last_wp"] = _pick(wp, ("weak_any",))
        except Exception:
            pass
        try:
            dv = getattr(runtime, "last_div", None)
            snap["last_div"] = _pick(dv, ("ts_ms", "kind"))
        except Exception:
            pass
        try:
            fp = getattr(runtime, "last_fp_edge", None)
            snap["last_fp_edge"] = _pick(fp, ("ts_ms", "p90", "value", "strength", "bias", "range_expansion"))
        except Exception:
            pass

        # deterministic time input (helpful when tick_ts_ms==0 in capture)
        try:
            nts = int(ind.get("now_ts_ms_used", 0) or 0)
            if nts > 0:
                snap["now_ts_ms_used"] = nts
        except Exception:
            pass

        return snap

    @staticmethod
    def _json_sanitize(obj: Any, *, max_depth: int = 6, max_items: int = 2000) -> Any:
        """Best-effort JSON-safe conversion (deterministic).
        - Keeps primitives
        - Recurses dict/list/tuple up to max_depth
        - Unknown types -> str(obj)
        """
        def _walk(x: Any, d: int) -> Any:
            if x is None or isinstance(x, (bool, int, float, str)):
                return x
            if d <= 0:
                return str(x)
            if isinstance(x, dict):
                out: dict[str, Any] = {}
                # deterministic key order
                for i, k in enumerate(sorted(x.keys(), key=lambda z: str(z))):
                    if i >= max_items:
                        break
                    try:
                        out[str(k)] = _walk(x.get(k), d - 1)
                    except Exception:
                        out[str(k)] = None
                return out
            if isinstance(x, (list, tuple)):
                out_l: list[Any] = []
                for i, v in enumerate(x):
                    if i >= max_items:
                        break
                    out_l.append(_walk(v, d - 1))
                return out_l
            return str(x)
        return _walk(obj, int(max_depth))

    def export_cfg_snapshot(self, cfg: dict[str, Any]) -> dict[str, Any]:
        """Capture JSON-safe cfg snapshot for deterministic replay."""
        try:
            if not isinstance(cfg, dict):
                return {}
            out = self._json_sanitize(cfg)
            return out if isinstance(out, dict) else {}
        except Exception:
            return {}

    @classmethod
    def runtime_snapshot_schema(cls) -> dict[str, Any]:
        """Contract for runtime_snapshot (capture/replay)."""
        return {
            "top": [
                "last_regime",
                "liq_regime",
                "book_churn_hi",
                "pressure_hi",
                "cont_ctx_ts_ms",
                "dynamic_cfg",
                "last_obi_event",
                "last_iceberg_event",
                "last_ofi_event",
                "last_sweep",
                "last_reclaim",
                "last_wp",
                "last_div",
                "last_fp_edge",
                "last_bar",
                "now_ts_ms_used",
            ],
            "nested": {
                "last_sweep": ["ts_ms", "kind", "direction_bias"],
                "last_reclaim": ["ts_ms", "hold_bars", "direction_bias", "level", "pool_id"],
                "last_wp": ["weak_any"],
                "last_div": ["ts_ms", "kind"],
                "last_fp_edge": ["ts_ms", "p90", "value", "strength", "bias", "range_expansion"],
                "last_bar": [
                    "id", "ts_ms", "fp_enabled",
                    "fp_absorption_bias",
                    "fp_ladder_low_len", "fp_ladder_high_len",
                    "fp_poc_on_edge",
                    "fp_eff_quote", "fp_eff_delta", "fp_quote_delta",
                    "fp_move_bp",
                ],
                "last_obi_event": ["ts_ms", "direction", "obi", "stable_secs", "obi_z", "stacking", "concentration"],
                "last_iceberg_event": ["ts_ms", "side", "refresh", "duration", "price"],
                "last_ofi_event": ["ts_ms", "direction", "ofi", "ofi_z", "stable_secs", "stable", "stability_score"],
            },
            "schema": 3,
        }

    @classmethod
    def validate_runtime_snapshot_contract(cls, snap: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        """Validate snapshot against schema; returns (ok, report)."""
        schema = cls.runtime_snapshot_schema()
        top = set(schema.get("top") or [])
        nested = schema.get("nested") or {}

        missing_top = []
        for k in sorted(top):
            if k in ("now_ts_ms_used",):
                continue
            if k not in snap:
                if k in ("last_iceberg_event", "last_ofi_event", "last_sweep", "last_reclaim", "last_fp_edge", "last_bar"):
                    continue
                missing_top.append(k)

        missing_nested: dict[str, Any] = {}
        for obj_key, fields in (nested.items() if isinstance(nested, dict) else []):
            obj = snap.get(obj_key)
            if obj is None or not isinstance(obj, dict):
                continue
            miss = [f for f in fields if f not in obj]
            if miss:
                missing_nested[obj_key] = miss

        ok = (not missing_top) and (not missing_nested)
        return ok, {"missing_top": missing_top, "missing_nested": missing_nested, "schema": schema.get("schema")}

    def build_runtime_from_snapshot(self, snap: dict[str, Any]) -> Any:
        """Build SimpleNamespace runtime from snapshot (replay-safe)."""
        rt = SimpleNamespace()
        for k, v in (snap or {}).items():
            with contextlib.suppress(Exception):
                setattr(rt, k, v)
        if not hasattr(rt, 'dynamic_cfg'):
            rt.dynamic_cfg = {}
        return rt
