from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Tuple, List
import hashlib
import json
import math
import os
import time
from types import SimpleNamespace

from core_snapshot.policy_snapshot_v1 import build_dq_policy_snapshot, build_feature_manifest_v1, to_public_dict

from core.book_evidence import compute_obi_flags, compute_iceberg_flags, compute_ofi_flags
from core.meta_model_lr import MetaModelLR
from core.meta_feature_coverage import compute_meta_feature_coverage, apply_meta_coverage_guard
from core.meta_model_guard import validate_meta_model
from core.of_evidence import compute_sweep_recent, compute_reclaim_recent, compute_absorption_flags
from core.strong_of_gate import eval_reversal, eval_continuation, hidden_trend_dir
from core.absorption_level_score import compute_absorption_level_score
from core.of_confirm_contract import OFConfirmV3, pack_bits
from core.cfg_merge import merged_cfg
from core.ofc_bundle_loader_v1 import OFCBundleLoaderV1
from core.ofc_context_key_v1 import iter_ctx_fallback_keys, make_ctx_key
from core.retention import MAXLEN_GLOBAL
from core.ofc_context_v1 import build_ofc_context
from core.strong_need_policy import compute_strong_need_same_tick
from common.metrics_stage import (
    veto_total, dist,
    meta_feature_seen_total,
    meta_feature_missing_total,
    feature_missing_total,
)
from core.fp_edge_evidence import compute_fp_edge_absorb
from core.book_microstructure_v2 import compute_queue_imbalance_topn, compute_ofi_multilevel_topn
from core.scenario_v4 import classify_v4
from core.meta_features_v1 import (
    META_FEAT_V1_NAME,
    META_FEAT_V1_VERSION,
    META_FEAT_V1_HASH,
    META_FEAT_V1_COLS,
    build_meta_features_v1,
)
from core.meta_features_v2 import (
    META_FEAT_V2_COLS, META_FEAT_V2_HASH,
    META_FEAT_V2_NAME,
    META_FEAT_V2_VERSION,
    build_meta_features_v2,
)
from core.burst_gate_v1 import eval_burst_gate
from core.liq_pressure_gate_v1 import eval_liq_pressure_gate
from core.taker_flow_gate_v1 import eval_taker_flow_gate
from core.fill_prob_proxy import compute_fill_prob_proxy
from core.meta_features_v3 import (
    META_FEAT_V3_NAME,
    META_FEAT_V3_VERSION,
    META_FEAT_V3_HASH,
    META_FEAT_V3_COLS,
    build_meta_features_v3,
)
from core.book_microstructure_v4 import compute_microstructure_v4
from core.meta_features_v4 import (
    META_FEAT_V4_NAME,
    META_FEAT_V4_VERSION,
    META_FEAT_V4_HASH,
    META_FEAT_V4_COLS,
    META_FEAT_V4_TRANSFORMS,
    build_meta_features_v4,
)
from core.meta_features_v5 import (
    META_FEAT_V5_NAME,
    META_FEAT_V5_VERSION,
    META_FEAT_V5_HASH,
    META_FEAT_V5_COLS,
    META_FEAT_V5_TRANSFORMS,
    build_meta_features_v5,
)
from core.meta_features_v6 import (
    META_FEAT_V6_NAME,
    META_FEAT_V6_VERSION,
    META_FEAT_V6_HASH,
    META_FEAT_V6_COLS,
    META_FEAT_V6_TRANSFORMS,
    build_meta_features_v6,
)
from core.meta_features_v7 import (
    META_FEAT_V7_NAME,
    META_FEAT_V7_VERSION,
    META_FEAT_V7_HASH,
    META_FEAT_V7_COLS,
    META_FEAT_V7_TRANSFORMS,
    build_meta_features_v7,
)

from core.meta_features_v8 import (
    META_FEAT_V8_NAME,
    META_FEAT_V8_VERSION,
    META_FEAT_V8_HASH,
    META_FEAT_V8_COLS,
    META_FEAT_V8_TRANSFORMS,
    build_meta_features_v8,
)

from core.meta_features_v9 import (
    META_FEAT_V9_NAME,
    META_FEAT_V9_VERSION,
    META_FEAT_V9_HASH,
    META_FEAT_V9_COLS,
    META_FEAT_V9_TRANSFORMS,
    build_meta_features_v9,
)
from core.meta_features_v10 import (
    META_FEAT_V10_NAME,
    META_FEAT_V10_VERSION,
    META_FEAT_V10_HASH,
    META_FEAT_V10_COLS,
    META_FEAT_V10_TRANSFORMS,
    build_meta_features_v10,
)

# Optional gates (may live in the full repo). Keep engine importable even in
# partial archives; engine remains functional with graceful degradation.
try:
    from services.cancellation_spike_gate import CancellationSpikeGate  # type: ignore
except Exception:  # pragma: no cover
    CancellationSpikeGate = None  # type: ignore
try:
    from services.ml_confirm_gate import MLConfirmGate  # type: ignore
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
META_SCHEMA_REGISTRY: Dict[str, Tuple[int, str]] = {
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
}

META_SCHEMA_V2P = (META_FEAT_V2_NAME, META_FEAT_V3_NAME, META_FEAT_V4_NAME, META_FEAT_V5_NAME, META_FEAT_V6_NAME, META_FEAT_V7_NAME, META_FEAT_V8_NAME, META_FEAT_V9_NAME, META_FEAT_V10_NAME)

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
    evidence: Dict[str, Any]
    contrib: Dict[str, float]    # score contributions per key

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# Process-level shared caches for MetaModelLR to prevent redundant I/O across engine instances.
_SHARED_META_MODELS: Dict[str, Any] = {}
_SHARED_META_STATS: Dict[str, Tuple[float, int]] = {} # path -> (mtime, size)
_SHARED_CONT_CTX_CAPTURE_CLIENT: Optional[Any] = None
_SHARED_CONT_CTX_CAPTURE_CLIENT_URL: str = ""


def _get_sync_redis_client_for_cont_ctx_capture(cfg: Dict[str, Any]) -> Optional[Any]:
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
        if _SHARED_CONT_CTX_CAPTURE_CLIENT is not None and _SHARED_CONT_CTX_CAPTURE_CLIENT_URL == url:
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
    indicators: Dict[str, Any],
    cfg2: Dict[str, Any],
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
        strong_gate_missing = str(indicators.get("strong_gate_missing", "") or "")

        sid_seed = "|".join([
            symbol,
            str(signal_ts_ms),
            direction,
            scenario_base,
            str(int(round(float(indicators.get("price", indicators.get("last_price", 0.0)) or 0.0) * 100.0))),
        ])
        signal_id = hashlib.sha1(sid_seed.encode("utf-8")).hexdigest()

        payload = {
            "schema": "1",
            "event": "ofc_cont_ctx_capture",
            "signal_id": signal_id,
            "symbol": symbol,
            "ts_ms": str(signal_ts_ms),
            "tf": str(indicators.get("tf") or ""),
            "direction": direction,
            "scenario": scenario_base,
            "scenario_v4": str(indicators.get("scenario_v4", scenario_base) or scenario_base),
            "ok": str(int(getattr(ofc, "ok", 0) or 0)),
            "ok_soft": str(int(indicators.get("ok_soft", 0) or 0)),
            "have": str(int(getattr(ofc, "have", 0) or 0)),
            "need": str(int(getattr(ofc, "need", 0) or 0)),
            "score": str(float(getattr(ofc, "score", 0.0) or 0.0)),
            "reason": str(getattr(ofc, "reason", "") or ""),
            "strong_gate_missing": strong_gate_missing,
            "trend_dir_source": str(indicators.get("trend_dir_source", "") or ""),
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
            or "stream:ofc:cont_ctx_capture"
        ).strip()
        maxlen = int(cfg2.get("cont_ctx_calib_capture_maxlen", os.getenv("CONT_CTX_CALIB_CAPTURE_MAXLEN", str(MAXLEN_GLOBAL))) or MAXLEN_GLOBAL)
        
        from services.observability.metrics_registry import ml_telemetry_io_time_us
        t0_xadd = time.perf_counter()
        client.xadd(stream, payload, maxlen=maxlen, approximate=True)
        dt_xadd = (time.perf_counter() - t0_xadd) * 1_000_000
        ml_telemetry_io_time_us.labels(symbol=str(symbol), op="xadd_cont_ctx").observe(dt_xadd)
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

    def __init__(self, version: int = 3, cancel_gate: Optional[Any] = None, ml_gate: Optional[Any] = None) -> None:
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
        self._replay_now_ms: Optional[int] = None
        # Startup timestamp (ms) - used by _should_apply_dq_veto for warmup checks.
        self._start_ms: int = get_ny_time_millis()
        self._ofc_ctx_loader = None
        self._ofc_ctx_bundle = None
        self._ofc_ctx_last_check_ms = 0

    @property
    def ml_gate(self) -> Optional[Any]:
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
    def _ensure_ofc_ctx_bundle(self, cfg: Dict[str, Any]) -> None:
        try:
            enabled = bool(cfg.get("ofc_ctx_enable", False))
            path = str(cfg.get("ofc_ctx_bundle_path", "") or "")
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
        indicators: Dict[str, Any],
        score: float,
        score_raw: float,
        exec_risk_bps: float,
        exec_risk_norm: float,
        exec_ref: float,
        spread_bps: float,
        slip_bps: float,
        score_min: float,
        now_ts: int,
    ) -> Dict[str, float]:
        dt_h = int((int(now_ts) // 1000) // 3600 % 24)
        dt_d = int((int(now_ts) // 1000) // 86400 + 3) % 7  # stable UTC weekday proxy
        h_ang = (2.0 * math.pi * float(dt_h)) / 24.0
        d_ang = (2.0 * math.pi * float(dt_d)) / 7.0
        out: Dict[str, float] = {
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
    def snapshot_cancel_gate_state(self, symbol: str) -> Optional[Dict[str, Any]]:
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
                full = fn(str(symbol))
                if isinstance(full, dict) and "symbols" in full:
                    return full["symbols"].get(str(symbol), None)
                return full
            return fn(str(symbol))
        except Exception:
            return None

    def restore_cancel_gate_state(self, symbol: str, state: Optional[Dict[str, Any]]) -> bool:
        """Restore CancellationSpikeGate state for a symbol. Returns True if applied."""
        if not state:
            return False
        try:
            gate = getattr(self, "_cancel_spike_gate", None)
            if gate is None:
                # lazy init to keep call safe in replay tool
                self._cancel_spike_gate = CancellationSpikeGate()
                gate = self._cancel_spike_gate
            fn = getattr(gate, "restore_state", None)
            if fn is None:
                # Fallback to restore if restore_state doesn't exist
                fn = getattr(gate, "restore", None)
                if fn is None:
                    return False
                fn(state, symbol=str(symbol))
                return True
            fn(str(symbol), dict(state))
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Gate state snapshot API (for deterministic golden replay)
    # ------------------------------------------------------------------

    def cancel_gate_snapshot(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Serialize CancellationSpikeGate state.

        If symbol is provided, returns the per-symbol payload.
        Otherwise returns the full container snapshot.
        """
        try:
            return self._cancel_spike_gate.snapshot(symbol)
        except Exception:
            return {"version": 1, "symbols": {}}

    def cancel_gate_restore(self, snap: Dict[str, Any], symbol: Optional[str] = None) -> None:
        """Restore CancellationSpikeGate state."""
        try:
            self._cancel_spike_gate.restore(snap, symbol=symbol)
        except Exception:
            return

    def cancel_gate_reset(self, symbol: Optional[str] = None) -> None:
        """Clear CancellationSpikeGate state (per symbol or all)."""
        try:
            self._cancel_spike_gate.reset(symbol=symbol)
        except Exception:
            return

    def _should_apply_dq_veto(self, cfg: Dict[str, Any]) -> bool:
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

    def export_gate_state(self, *, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Export internal state of stateful gates (fail-open).

        Used by OFC_CAPTURE to guarantee deterministic offline replay.

        """
        try:
            out: Dict[str, Any] = {"version": 1, "gates": {}}

            g = getattr(self, "_cancel_spike_gate", None)

            if g is not None and hasattr(g, "export_state"):
                out["gates"]["cancel_spike"] = g.export_state(symbol=symbol)

            return out

        except Exception:
            return {"version": 1, "gates": {}}

    def import_gate_state(self, state: Dict[str, Any], *, replace: bool = False) -> None:
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
    def export_cancel_spike_state(self, *, symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
        try:
            g = getattr(self, "_cancel_spike_gate", None)

            if g is None or not hasattr(g, "export_state"):
                return None

            return g.export_state(symbol=symbol)

        except Exception:
            return None

    def import_cancel_spike_state(self, state: Dict[str, Any], *, replace: bool = False) -> None:
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

    def _resolve_now_ts(self, tick_ts_ms: int, indicators: Dict[str, Any]) -> int:
        """
        Canonical time source for build().
        Priority:
          1) tick_ts_ms (if >0)
          2) indicators['now_ts_ms'] (if >0)
          3) deterministic _now_ms() (prod: wall clock, replay: frozen)
        """
        if int(tick_ts_ms or 0) > 0:
            return int(tick_ts_ms)
        v = self._i(indicators.get("now_ts_ms", 0), 0)
        if v > 0:
            return int(v)
        return int(self._now_ms())

    def _load_meta_model_slot(self, slot: str, path: str, now_ms: int, reload_sec: int) -> Optional[Any]:
        """
        Fail-open loader with coarse reload interval and process-level caching.
        NOTE: in replay mode we must not refresh by wall-clock timers.
        """
        try:
            slot = str(slot or "champion").lower()
            path = str(path or "").strip()
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
                                from core.feature_engineering import RobustScalerPack, RobustScalerParams
                                params = {}
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

    def _load_meta_model(self, path: str, now_ms: int, reload_sec: int) -> Optional[MetaModelLR]:
        """Backward-compatible champion loader."""
        return self._load_meta_model_slot("champion", path, now_ms, reload_sec)

    def build(
        self,
        *,
        symbol: str,
        tf: str,
        direction: str,
        tick_ts_ms: int,
        price: float,
        delta_z: float,
        snap_t0: Optional[Any] = None,
        snap_prev: Optional[Any] = None,
        runtime: Any,
        cfg: Dict[str, Any],
        indicators: Dict[str, Any],
        absorption: Optional[Dict[str, Any]] = None,
        worker_lag_ms: float = 0.0,
    ) -> Tuple[Optional[OFConfirmV3], Optional[Any]]:
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
            try:
                ofconfirm_build_stages_us.labels(symbol=str(symbol), stage=stage_name).observe(dt_us)
            except Exception:
                pass
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
            price=float(price),
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
        try:
            indicators["now_ts_ms_used"] = int(now_ts)
        except Exception:
            pass

        # --- Book health gate for book-based evidences (OBI/Iceberg/OFI) ---
        book_ok = _i(indicators.get("book_health_ok", 1), 1)
        
        # --- Data health gate (stricter than book_ok) ---
        # If overall data_health is low, we fail-closed ONLY for evidences that depend on book/time.
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

        kind = str(indicators.get("sweep_kind", "") or "")
        indicators["sweep_eqh"] = int(1 if (sweep_recent and kind == "EQH_SWEEP") else 0)
        indicators["sweep_eql"] = int(1 if (sweep_recent and kind == "EQL_SWEEP") else 0)

        div_ok = 0
        div_fallback = 0
        div_source = "none"
        try:
            cvd_q = int(indicators.get("cvd_quarantine_active", 0) or 0)
            dbias = str(indicators.get("sweep_dir_bias", "") or "").upper()
            div = _get_attr_or_key(runtime, "last_div", None)
            
            if cvd_q != 1:
                # Primary path: use multi-bar divergence object
                if sweep_recent and div is not None:
                    dkind = str(_get_attr_or_key(div, "kind", "") or "").lower()
                    if dbias == "SHORT" and dkind.startswith("bearish"):
                        div_ok = 1
                        div_source = "divergence_object"
                    elif dbias == "LONG" and dkind.startswith("bullish"):
                        div_ok = 1
                        div_source = "divergence_object"
            else:
                # Fallback path: use snapshot delta_tick during CVD baseline quarantine
                delta_val = float(indicators.get("delta_tick", indicators.get("delta", 0.0) or 0.0) or 0.0)
                
                # P1-10: Strict time scoping. Delta is an indicator, must not be newer than signal!
                evidence_ts = int(indicators.get("ts_ms", indicators.get("event_ts", 0)) or 0)
                signal_ts = int(now_ts)  # now_ts is the tick_ts_ms for the current signal
                time_ok = True
                if evidence_ts > 0 and signal_ts > 0 and evidence_ts > signal_ts:
                    time_ok = False
                
                if sweep_recent and time_ok:
                    if dbias == "SHORT" and delta_val < 0.0:
                        div_fallback = 1
                        div_source = "delta_tick_fallback"
                    elif dbias == "LONG" and delta_val > 0.0:
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
            now_ts_ms=int(now_ts),
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
                 rg = str(_get_attr_or_key(runtime, 'last_regime', 'na') or 'na').lower()
                 if "bull" in rg: 
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
                    try:
                         indicators["of_debug_fail"] = f"no_trend:regime={getattr(runtime, 'last_regime', 'na')}"
                    except Exception: 
                         pass

        scenario_v4 = scenario
        policy_reason = "ok"

        # proxy: news/vol shock
        news_flag = int(indicators.get("news_risk", 0) or indicators.get("calendar_risk", 0) or 0)
        reg = str(getattr(runtime, "last_regime", "") or "").lower()
        vol_shock = bool(news_flag == 1 or ("news" in reg) or ("shock" in reg))

        # proxy: saw/chop/spoof-ish
        # churn_hi: keep simple + safe (NO getattr with >3 args)
        try:
            churn_hi = bool(int(indicators.get("book_churn_hi", getattr(runtime, "book_churn_hi", 0) or 0) or 0))
        except Exception:
            try:
                churn_hi = bool(int(getattr(runtime, "book_churn_hi", 0) or 0))
            except Exception:
                churn_hi = False
        saw_chop = bool(int(indicators.get("saw_chop", 0) or 0) == 1 or churn_hi)

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
                        weak_progress=bool(wp_any),
                        iceberg_strict=bool(iceberg_strict),
                        reclaim_recent=bool(reclaim_recent),
                        cfg=cfg,
                    )
                    abs_lvl_ok = bool(abs_lvl.ok)
                    abs_lvl_score = float(abs_lvl.score)
                    abs_lvl_bias = str(abs_lvl.bias)
                    abs_lvl_dir_match = bool(abs_lvl.dir_match)

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
        fp_edge_absorb = bool(fp_edge_ok)
        # optional derive from abs_lvl if not provided
        if (not fp_edge_absorb) and bool(abs_lvl_ok):
            try:
                poc_edge = bool(int(indicators.get("abs_lvl_poc_edge", 0) or 0) == 1)
                score_min = float(cfg.get("fp_edge_abs_lvl_score_min", 0.55) or 0.55)
                fp_edge_absorb = bool(poc_edge and float(abs_lvl_score) >= score_min and bool(abs_lvl_dir_match))
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
            regime = str(_get_attr_or_key(runtime, 'last_regime', 'na') or 'na')
        except Exception:
            regime = "na"
        try:
            unstable = bool(int(dyn.get("abs_lvl_th_unstable", 0) or 0))
        except Exception:
            unstable = False
        # pressure_hi: deterministic sources only (no runtime.pressure calls => replayable)
        try:
            if "pressure_hi" in indicators:
                pressure_hi = bool(int(indicators.get("pressure_hi", 0) or 0) == 1)
            elif isinstance(dyn, dict) and "pressure_hi" in dyn:
                pressure_hi = bool(int(dyn.get("pressure_hi", 0) == 1))
            else:
                ph = getattr(runtime, "pressure_hi", None)
                if ph is not None:
                    try:
                        pressure_hi = bool(int(ph or 0) == 1) if not isinstance(ph, bool) else bool(ph)
                    except Exception:
                        pressure_hi = bool(ph)
                else:
                    pressure_hi = bool(getattr(runtime, "pressure").is_pressure_hi(int(now_ts), float(cfg2.get("pressure_hi_per_min", 4.0))))
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
            unstable=bool(unstable),
            cfg=cfg2,
        )
        # Apply need overrides into cfg2 for eval_* (same-tick)
        cfg2["strong_need_reversal"] = int(nd.need_rev)
        cfg2["strong_need_continuation"] = int(nd.need_cont)
        # We don't store it back to cfg2 as a key used by eval_*, but we keep for audit if needed

        if scenario == "reversal":
            # C1: OFI substitutes OBI stability for the microstructure leg (safe: does not increase have count).
            ofi_leg = bool(ofi_dir_ok and ofi_stable)
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
                src = str(indicators.get("trend_dir_source", ""))
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

            ofi_leg = bool(ofi_dir_ok and ofi_stable)

            from core.compat_utils import _filter_kwargs_for_callable
            
            continuation_kwargs = {
                "direction": direction,
                "trend_dir": trend_dir,
                "hidden_ctx_recent": bool(hidden_ctx_recent),
                "iceberg_strict": bool(iceberg_strict),
                "obi_stable": bool(obi_stable),
                "cont_ctx_recent": bool(cont_ctx_recent),
                "abs_lvl_ok": bool(abs_lvl_ok),
                "ofi_leg": bool(ofi_leg),
                "fp_edge_absorb": bool(fp_edge_absorb),
                "cfg": cfg2,
                "trend_dir_source": str(indicators.get("trend_dir_source", "none")),
            }

            dec = eval_continuation(**_filter_kwargs_for_callable(eval_continuation, **continuation_kwargs))

        # Attach need escalation diagnostics
        try:
            if dec is not None:
                setattr(dec, "need_reason", str(nd.reason))
        except Exception:
            pass

        # -------------------------------------------------------
        # A3) Execution-risk penalty (mandatory): spread + slippage
        # -------------------------------------------------------
        spread_bps = _f(indicators.get("spread_bps", None), -1.0)
        slip_bps = _f(indicators.get("expected_slippage_bps", None), -1.0)

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
        # FIX (Unit Scale): Link exec_risk_ref_bps to the symbol's dist_bp_threshold.
        # This provides a natural, volatility-adjusted reference instead of hardcoded 10.0.
        try:
             # Try to get it from cfg first
             ref_base = float(cfg.get("dist_bp_threshold", 0.0) or 0.0)
             if ref_base <= 0.0:
                 # Fallback to instrument_config defaults
                 from core.instrument_config import get_default_dist_bp_threshold
                 ref_base = float(get_default_dist_bp_threshold(symbol) or 20.0)
        except Exception:
             ref_base = 20.0
             
        # exec_ref is typically 1.0 * dist_bp for normal, maybe 0.7 * dist_bp for strict
        exec_ref = ref_base * float(cfg.get("exec_risk_ref_mult", 1.0) or 1.0)
        
        # Adaptive reference for low liquidity / thin regimes
        liq_regime = str(indicators.get("liq_regime", getattr(runtime, "liq_regime", "na")) or "na")
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
                direction=str(direction),
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

            w_fill = float(cfg.get("exec_fill_pen_w", 0.20) or 0.20)
            exec_fill_pen = w_fill * (1.0 - float(fp["fill_prob_proxy"]))
            indicators["exec_fill_pen"] = float(exec_fill_pen)
            try:
                exec_pen = float(exec_pen) + float(exec_fill_pen)
            except Exception:
                pass
        except Exception:
            pass

        # --- Score (0..1), stable under feature additions ---
        # We use weighted-mean aggregation by default so adding OFI/FP-edge doesn't saturate score.
        contrib: Dict[str, float] = {}
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
        ofi_leg = bool(ofi_dir_ok and ofi_stable)
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

        agg = str(cfg.get("of_score_agg", "weighted_mean") or "weighted_mean").lower()
        
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
                setattr(dec, "gate_bits", int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_TAKER_FLOW)
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
        dq_reason = str(dq_meta.get("dq_reason", "ok"))
        dq_reasons = dq_meta.get("dq_reasons", [])
        dq_health = float(dq_meta.get("dq_health_score", 1.0))
        dq_bucket = str(dq_meta.get("dq_reason_bucket", "ok"))
        dq_uptime_sec = int(dq_meta.get("uptime_sec", 0) or 0)
        dq_runtime_start_ts_ms = dq_meta.get("runtime_start_ts_ms")
        dq_veto_suppressed = int(dq_meta.get("dq_veto_suppressed", 0) or 0)
        dq_veto_suppressed_reason = str(dq_meta.get("dq_veto_suppressed_reason", "") or "")

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
        liqmap_window_used = str(cfg2.get("liqmap_gate_window", "5m") or "5m")
        liqmap_mode = "OFF"
        try:
            # Evaluate unconditionally if implementation is available.
            # The gate itself decides OFF/SHADOW/ENFORCE based on cfg2.
            if evaluate_liqmap_gate_v1 is not None:
                lm = evaluate_liqmap_gate_v1(direction=str(direction), indicators=indicators, cfg2=cfg2)
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
                        setattr(dec, "scenario", scenario)
                    setattr(dec, "gate_bits", int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_LIQMAP)
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
                self._news_gate = NewsGate(redis_client=None)
                _ng = self._news_gate

            ndec = _ng.decide(
                now_ts_ms=int(now_ts),
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
                    setattr(dec, "scenario", scenario)
                setattr(dec, "gate_bits", int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_NEWS)

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
        ctx_mode = str(cfg2.get("ofc_ctx_mode", "off") or "off").lower()
        ctx = build_ofc_context(
            symbol=str(symbol),
            direction=str(direction),
            ts_ms=int(now_ts),
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
            now_ts=int(now_ts),
        )
        indicators["ctx_key"] = str(ctx_key)
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
            meta_name = str(cfg2.get("meta_schema_name", "meta_feat_v8"))
            reg = globals().get("META_SCHEMA_REGISTRY", {})
            ver, h = (0, "")
            try:
                ver, h = reg.get(meta_name, (0, ""))
            except Exception:
                ver, h = (0, "")

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
                dec.reason = str(reason)
                dec.gate_bits = 0
            except Exception:
                # If even this fails, we keep dec=None and rely on fail-open behavior.
                pass

        # --- B2 scenario policies enforcement (post-score, pre-final reason) ---
        hard_veto = ""

        # Score threshold (double filter)
        # NOTE: scenario-specific thresholds are applied later if scenario_v4 is enabled.
        score_min = _f(cfg.get("of_score_min", 0.40), 0.40)
        if ok == 1 and score < score_min:
             # Logic: if score is too low, we can veto even if 2-of-3 passed (optional but recommended)
             # But we only do this if it's not shadow mode in the caller. 
             # We'll just return ok=0 and let the service decide.
             ok = 0
             hard_veto = "score_veto"
             # Optional: log if we vetoed by score
        
        indicators["ok_soft"] = int(ok)

        
        # P14: DQ Gate Veto
        if dq_veto and str(cfg2.get("dq_gate_mode", "off")).lower() in ("enforce", "both", "veto", "hard"):
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
        _lm_mode = str(cfg2.get("liqmap_gate_mode", "shadow") or "shadow").lower()
        _lm_shadow = int(indicators.get("liqmap_gate_shadow_veto", 0) or 0)
        _lm_veto = int(indicators.get("liqmap_gate_veto", 0) or 0)
        if (_lm_veto == 1 and _lm_mode in ("enforce", "both", "veto", "hard")) or (_lm_shadow == 1 and _lm_mode in ("both",)):
            try:
                ok = 0
                _r = str(indicators.get("liqmap_gate_veto_reason", indicators.get("liqmap_gate_reason", "veto")) or "veto")
                hard_veto = f"liqmap_{_r}"
                # Map liqmap reason to canonical code (P2-4: prevent cardinality explosion).
                veto_total(self, reason_code="VETO_LIQMAP_RR")
            except Exception:
                pass
        # P16: News Agent Reco Gate Veto
        _news_veto = int(indicators.get("news_gate_veto", 0) or 0)
        _news_mode = str(cfg2.get("news_gate_mode", "enforce") or "enforce").lower()
        if _news_veto == 1 and _news_mode in ("enforce", "both", "veto", "hard"):
            try:
                ok = 0
                hard_veto = "news_gate"
                # Canonical code from VetoReason registry (P2-4).
                veto_total(self, reason_code="VETO_NEWS_RECO_HARD", kind="news_gate", symbol=str(symbol))
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
            if not hasattr(self, "_cancel_spike_gate") or getattr(self, "_cancel_spike_gate") is None:
                try:
                    self._cancel_spike_gate = CancellationSpikeGate()  # type: ignore
                except Exception:
                    self._cancel_spike_gate = None

            if self._cancel_spike_gate is not None:
                # Deterministic replay support: allow caller to pass gate state
                # (captured via OFC_CAPTURE) to fully reproduce decisions.
                cgs = indicators.get("cancel_gate_state", None)
                if isinstance(cgs, dict) and hasattr(self._cancel_spike_gate, "restore_state"):
                    try:
                        self._cancel_spike_gate.restore_state(cgs)
                    except Exception:
                        pass

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
                                setattr(dec, "gate_bits", int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_CANCEL_SPIKE)
                        except Exception:
                            pass
                        # Use canonical VETO_CANCEL_SPIKE code; raw gate_reason is
                        # preserved in gate_meta for debugging (P2-4).
                        veto_total(runtime, reason_code="VETO_CANCEL_SPIKE", kind="cancel_spike", symbol=str(symbol))
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
                    setattr(dec, "ok_soft", 1)
                    setattr(dec, "soft_reason", soft_reason)
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
            soft_reason = str(indicators.get("range_soft_reason", ""))

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
            tr = str(indicators.get("taker_flow_gate_reason", "taker_flow_contra") or "taker_flow_contra")
            final_reason = f"taker_flow:{tr}(veto)|{final_reason}"
            try:
                setattr(dec, "gate_bits", int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_TAKER_FLOW)
            except Exception:
                pass

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
                    sv4 = str(indicators.get("scenario_v4", "") or "")
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
                sb = evidence.get('score_breakdown', {}) if isinstance(evidence, dict) else {}
                sb_small = {
                    'agg': str(sb.get('agg', '')) ,
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
                    else:
                        indicators_with_v4["max_signal_age_ratio"] = 0.0
            except Exception:
                indicators_with_v4.setdefault("max_signal_age_ratio", 0.0)

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
                    symbol=str(symbol),
                    ts_ms=int(now_ts),
                    direction=str(direction),
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
                ml_inference_time_us.labels(symbol=str(symbol), scenario=str(ml_scenario)).observe(int(t_ml_dt * 1_000_000.0))
            except Exception:
                pass
            evidence["ml"] = ml_dec.to_dict()

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
            evidence["ml"] = {"mode": "ERR", "error": str(_e)[:200]}

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
            meta_mode = str(cfg2.get("meta_model_mode", os.getenv("META_MODEL_MODE", "SHADOW"))).upper()
            meta_p_min = float(cfg2.get("meta_p_min", float(os.getenv("META_P_MIN", "0.55"))))
            meta_enable = bool(int(cfg2.get("meta_model_enable", int(os.getenv("META_MODEL_ENABLE", "0")))))
            meta_path = str(cfg2.get("meta_model_path", os.getenv("META_MODEL_PATH", "")) or "").strip()
            reload_sec = int(cfg2.get("meta_model_reload_sec", int(os.getenv("META_MODEL_RELOAD_SEC", "60"))))
            meta_path_ch = str(cfg2.get("meta_model_path_challenger", os.getenv("META_MODEL_CHALLENGER_PATH", "")) or "").strip()
            meta_ab_share = float(cfg2.get("meta_ab_challenger_share", float(os.getenv("META_AB_CHALLENGER_SHARE", "0.0")) ) or 0.0)
            meta_ab_share = max(0.0, min(1.0, meta_ab_share))
            meta_ab_salt = str(cfg2.get("meta_ab_salt", os.getenv("META_AB_SALT", "ab_v1")) or "ab_v1")
            meta_freeze = bool(int(cfg2.get("meta_model_freeze", int(os.getenv("META_MODEL_FREEZE", "0"))) or 0))
            meta_freeze_mode = str(cfg2.get("meta_freeze_mode", os.getenv("META_FREEZE_MODE", "OPEN")) or "OPEN").upper()
            meta_allow_legacy = bool(int(cfg2.get("meta_allow_legacy_schema", int(os.getenv("META_ALLOW_LEGACY_SCHEMA", "0"))) or 0))

            # P25: model signature + schema pinning (runtime)
            meta_require_sig = bool(int(cfg2.get("meta_require_signature", int(os.getenv("META_MODEL_REQUIRE_SIGNATURE", "1"))) or 1))
            meta_enforce_requires_schema = bool(int(cfg2.get("meta_enforce_requires_schema", int(os.getenv("META_MODEL_ENFORCE_REQUIRES_SCHEMA", "1"))) or 1))
            meta_schema_pin_name = str(cfg2.get("meta_schema_pin_name", os.getenv("META_MODEL_SCHEMA", "")) or "")
            meta_schema_pin_hash = str(cfg2.get("meta_schema_pin_hash", os.getenv("META_MODEL_SCHEMA_HASH", "")) or "")


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
                    }

                    schema_cfg = SCHEMAS.get(model_schema_name)
                    if schema_cfg is None:
                        # Unknown schema: default to v1 features; mismatched models will be forced to SHADOW (unless legacy allowed).
                        schema_cfg = SCHEMAS[META_FEAT_V1_NAME]

                    local_schema_name = str(schema_cfg["name"])
                    local_schema_vers = int(schema_cfg["version"])
                    local_schema_hash = str(schema_cfg.get("hash", "") or "")
                    builder = schema_cfg["builder"]

                    evidence["meta_schema_name"] = local_schema_name
                    evidence["meta_schema_version"] = local_schema_vers
                    evidence["meta_schema_hash"] = local_schema_hash
                    evidence["meta_model_schema_name"] = model_schema_name
                    evidence["meta_model_schema_version"] = model_schema_vers
                    evidence["meta_model_schema_hash"] = model_schema_hash

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
                            evidence["meta_features_export"] = export_vec
                            evidence["meta_features_export_n"] = int(len(export_vec))
                            evidence["meta_features_export_cols_hash"] = hashlib.sha1(
                                (",".join(mm_feats)).encode("utf-8")
                            ).hexdigest()
                            evidence["meta_features_export_schema"] = str(local_schema_name)
                            evidence["meta_features_export_schema_hash"] = str(local_schema_hash)
                        except Exception:
                            pass

                    # 4. Export meta schema info to evidence (for drift monitoring)
                    evidence["meta_schema_name"] = local_schema_name
                    evidence["meta_schema_version"] = local_schema_vers
                    evidence["meta_schema_hash"] = local_schema_hash
                    evidence["meta_model_schema_name"] = model_schema_name
                    evidence["meta_model_schema_version"] = model_schema_vers
                    evidence["meta_model_schema_hash"] = model_schema_hash

                    evidence["meta_missing_feature_count"] = len(feat_missing)
                    # Cap list to avoid huge logs if everything is missing
                    evidence["meta_missing_features"] = feat_missing[:32]

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

                        evidence["meta_model_feature_total"] = int(cov_obj.model_total)
                        evidence["meta_model_feature_missing"] = int(cov_obj.model_missing)
                        evidence["meta_model_feature_coverage"] = float(cov_obj.coverage)
                        evidence["meta_model_feature_missing_rate"] = float(cov_obj.missing_rate)
                        evidence["meta_model_missing_features"] = list(cov_obj.missing_model_features)

                        sch_label = str(model_schema_name)
                        dist(runtime, "meta_feature_coverage", float(cov_obj.coverage), schema=sch_label)
                        dist(runtime, "meta_feature_missing_rate", float(cov_obj.missing_rate), schema=sch_label)

                        # Thresholds (env < cfg2); keep conservative defaults
                        min_cov = float(cfg2.get("meta_min_feature_coverage", os.getenv("META_MIN_FEATURE_COVERAGE", "0.85")))
                        max_miss = int(float(cfg2.get("meta_max_missing_model_features", os.getenv("META_MAX_MISSING_MODEL_FEATURES", "999"))))

                        new_mode, cov_reason = apply_meta_coverage_guard(
                            meta_mode=str(meta_mode or ""),
                            cov=cov_obj,
                            min_coverage=float(min_cov),
                            max_missing=int(max_miss),
                        )
                        if cov_reason:
                            # Preserve an existing reason if already set later by other checks
                            evidence["meta_coverage_guard_reason"] = cov_reason
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
                    ml_inference_time_us.labels(symbol=str(symbol), model="champion").observe(dt_inf_champ)

                    if mm_chal is not None:
                        t0_inf_chal = time.perf_counter()
                        meta_p_challenger = float(mm_chal.predict_proba(feat))
                        dt_inf_chal = (time.perf_counter() - t0_inf_chal) * 1_000_000
                        ml_inference_time_us.labels(symbol=str(symbol), model="challenger").observe(dt_inf_chal)
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
                        meta_enforce_share = float(cfg2.get(share_key, cfg2.get("meta_enforce_share", 1.0)) or 1.0)
                    else:
                        meta_enforce_share = float(cfg2.get("meta_enforce_share", 1.0) or 1.0)
                    meta_enforce_share = max(0.0, min(1.0, meta_enforce_share))
                    meta_enforce_salt = str(cfg2.get("meta_enforce_salt", "enf_v1") or "enf_v1")

                    hkey = f"{meta_enforce_salt}:{sid}"
                    apply_enforce = 1 if (_hash01(hkey) < meta_enforce_share) else 0

                    # ENFORCE only on canary subset
                    if meta_mode == "ENFORCE" and apply_enforce == 1 and int(ok) == 1 and (not gate_vetoed) and meta_veto == 1:
                        ok = 0
                        # mark bit
                        try:
                            if dec is not None:
                                setattr(dec, "gate_bits", int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_META_VETO)
                        except Exception:
                            pass
                        final_reason = f"{final_reason}|meta_veto"
        except Exception:
            pass

        # Update evidence with meta fields
        evidence["meta_enable"] = int(meta_enable)
        evidence["meta_mode"] = str(meta_mode)
        evidence["meta_p_min"] = float(meta_p_min)
        evidence["meta_p"] = float(meta_p if meta_p is not None else -1.0)
        evidence["meta_veto"] = int(meta_veto)
        evidence["meta_reason"] = str(meta_reason)

        # A/B fields (for outcome attribution)
        try:
            evidence["meta_arm"] = str(locals().get("meta_arm", "") or "")
            evidence["meta_ab_share"] = float(locals().get("meta_ab_share", 0.0) or 0.0)
            evidence["meta_ab_salt"] = str(locals().get("meta_ab_salt", "") or "")
            evidence["meta_p_champion"] = float(locals().get("meta_p_champion", -1.0) or -1.0)
            evidence["meta_p_challenger"] = float(locals().get("meta_p_challenger", -1.0) or -1.0)
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
                meta_enforce_share_ev = float(cfg2.get(share_key_ev, cfg2.get("meta_enforce_share", 1.0)) or 1.0)
            else:
                meta_enforce_share_ev = float(cfg2.get("meta_enforce_share", 1.0) or 1.0)
            
            meta_enforce_salt = str(cfg2.get("meta_enforce_salt", "enf_v1") or "enf_v1")
            sid = str(indicators.get("sid", "") or indicators.get("stable_sid", "") or "")
            if not sid:
                sid = f"{symbol}|{now_ts}|{direction}|{scenario}"
            evidence["meta_enforce_share"] = float(meta_enforce_share_ev)
            evidence["meta_enforce_bucket"] = str(bucket_ev)
            evidence["meta_enforce_salt"] = str(meta_enforce_salt)
            evidence["meta_enforce_key"] = str(sid)
            evidence["meta_enforce_applied"] = int(apply_enforce if meta_mode == "ENFORCE" else 0)
        except Exception:
            evidence["meta_enforce_share"] = 1.0
            evidence["meta_enforce_bucket"] = "other"
            evidence["meta_enforce_key"] = ""
            evidence["meta_enforce_applied"] = 0
        
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
            hz_mode = str(cfg2.get("hz_enforce_mode", os.getenv("HZ_ENFORCE_MODE", "SHADOW")) or "SHADOW").upper()
            hz_share = float(cfg2.get("hz_enforce_share", float(os.getenv("HZ_ENFORCE_SHARE", "0.05"))) or 0.05)
            hz_share = max(0.0, min(1.0, hz_share))
            hz_salt = str(cfg2.get("hz_enforce_salt", os.getenv("HZ_ENFORCE_SALT", "hz_atr_v5")) or "hz_atr_v5")
            hz_symbols_raw = str(cfg2.get("hz_enforce_symbols", os.getenv("HZ_ENFORCE_SYMBOLS", "")) or "")
            hz_symbols = {s.strip().upper() for s in hz_symbols_raw.split(",") if s.strip()} if hz_symbols_raw else set()

            # Determine if this signal's symbol is in the canary whitelist
            sym_upper = str(symbol).upper()
            hz_symbol_ok = (not hz_symbols) or (sym_upper in hz_symbols)

            # Sticky routing: deterministic by symbol|session|kind
            _session = str(indicators.get("session", "") or "")
            _kind = str(indicators.get("scenario_v4", "") or scenario)
            hz_routing_key = f"{hz_salt}:{sym_upper}|{_session}|{_kind}"
            hz_in_canary = hz_symbol_ok and (_hash01(hz_routing_key) < hz_share)

            # hz_gate decision: was horizon-aware ATR policy active?
            hz_active = (hz_mode == "ENFORCE") or (hz_mode == "CANARY" and hz_in_canary)
            hz_veto = 0  # placeholder: real veto logic goes here when calibration is ready

            # Record gate status in evidence for observability
            evidence["hz_gate_mode"] = str(hz_mode)
            evidence["hz_gate_active"] = int(hz_active)
            evidence["hz_gate_share"] = float(hz_share)
            evidence["hz_gate_symbol_ok"] = int(hz_symbol_ok)
            evidence["hz_gate_in_canary"] = int(hz_in_canary)
            evidence["hz_gate_veto"] = int(hz_veto)

            # Phase 8 block: only apply when hz_active and calibration ready
            # (hz_veto currently = 0; will be wired to horizon policy check later)
            if hz_mode == "ENFORCE" and hz_active and int(ok) == 1 and hz_veto == 1:
                ok = 0
                final_reason = f"hz_gate_enforce|{final_reason}"
        except Exception:
            evidence["hz_gate_mode"] = "ERR"
            evidence["hz_gate_active"] = 0
            evidence["hz_gate_veto"] = 0

        evidence["ctx_enable"] = int(bool(cfg2.get("ofc_ctx_enable", False)))
        evidence["ctx_mode"] = str(ctx_mode)
        evidence["ctx_key"] = str(ctx_key)
        evidence["ctx_bundle_ver"] = str(getattr(self._ofc_ctx_bundle, "version", "") if self._ofc_ctx_bundle else "")
        evidence["ctx_exec_model_ver"] = str(getattr(exec_pred, "model_version", "")) if exec_pred is not None else ""
        evidence["ctx_rule_model_ver"] = str(getattr(rule_pred, "model_version", "")) if rule_pred is not None else ""
        evidence["ctx_p_rule_raw"] = float(getattr(rule_pred, "p_rule_raw", -1.0)) if rule_pred is not None else -1.0
        evidence["ctx_p_rule_cal"] = float(getattr(rule_pred, "p_rule_cal", -1.0)) if rule_pred is not None else -1.0
        evidence["ctx_cost_p50_bps"] = float(getattr(exec_pred, "cost_p50_bps", -1.0)) if exec_pred is not None else -1.0
        evidence["ctx_cost_p90_bps"] = float(getattr(exec_pred, "cost_p90_bps", -1.0)) if exec_pred is not None else -1.0
        evidence["ctx_exec_risk_ref_bps"] = float(getattr(exec_pred, "exec_risk_ref_bps_ctx", exec_ref)) if exec_pred is not None else float(exec_ref)
        evidence["ctx_score_min"] = float(getattr(rule_pred, "score_min_ctx", cfg2.get("of_score_min", 0.40))) if rule_pred is not None else float(cfg2.get("of_score_min", 0.40))
        evidence["ctx_reason"] = str(getattr(ctx_decision, "reason", "")) if ctx_decision is not None else ""
        evidence["ctx_infer_latency_us"] = int(ctx_infer_latency_us)

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
            try:
                evidence["golden_replay_inputs_v1"] = {
                    "symbol": str(symbol),
                    "tf": str(tf),
                    "direction": str(direction),
                    "tick_ts_ms": int(tick_ts_ms),
                    "price": float(price),
                    "delta_z": float(delta_z),
                    "dq_policy_hash": str(indicators.get("dq_policy_hash") or ""),
                    "dq_policy_feature_manifest_hash_v1": str(indicators.get("dq_policy_feature_manifest_hash_v1") or ""),
                    "runtime_snapshot": OFConfirmEngine.export_runtime_snapshot(runtime, indicators),
                }
            except Exception:
                pass

        legacy_reason = str(final_reason)
        score_veto_family = {
            "score_veto",
            "vol_shock_score_veto",
            "saw_chop_score_veto",
        }
        ctx_shadow_disagree = 0
        if ctx_decision is not None:
            ctx_allow = bool(getattr(ctx_decision, "allow", False))
            ctx_shadow_disagree = 1 if int(ok) != int(ctx_allow) else 0
            evidence["ctx_shadow_disagree"] = int(ctx_shadow_disagree)
            evidence["ctx_allow"] = int(ctx_allow)
            evidence["ctx_edge_net_p50_bps"] = float(getattr(ctx_decision, "edge_net_p50_bps", -999.0))
            evidence["ctx_edge_net_p90_bps"] = float(getattr(ctx_decision, "edge_net_p90_bps", -999.0))
            evidence["ctx_fallback_level"] = str(getattr(ctx_decision, "fallback_level", ""))
            if ctx_mode == "tighten_only" and int(ok) == 1 and not ctx_allow:
                ok = 0
                final_reason = f"ctx_tighten:{getattr(ctx_decision, 'reason', 'deny')}"
            elif ctx_mode == "replace_score_veto" and str(legacy_reason) in score_veto_family:
                ok = 1 if ctx_allow else 0
                final_reason = f"ctx_replace:{getattr(ctx_decision, 'reason', 'deny')}"

        ofc = OFConfirmV3(
            v=3,
            symbol=str(symbol),
            ts_ms=int(now_ts),
            direction=str(direction),
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
                    import time as _time
                    _ts = int(_get_ny_time_millis())
                maybe_capture_ofc_v1(engine=self, runtime=_rt, indicators=_ind, cfg2=cfg2, ofc=ofc, dec=dec, now_ts_ms=int(_ts))
            except Exception:
                pass
        
        # Calib capture (optional)
        if not is_shedding:
            try:
                _rt = locals().get("runtime") or getattr(self, "runtime", None)
                _ind = locals().get("indicators") or locals().get("ind") or {}
                _ts = locals().get("now_ts_ms") or locals().get("ts_ms") or int(now_ts)
                _emit_cont_ctx_calib_capture_v1(runtime=_rt, indicators=_ind, cfg2=cfg2, ofc=ofc, dec=dec, now_ts_ms=int(_ts))
            except Exception:
                pass
        else:
             indicators["calib_capture_shedded"] = 1
             
        _t_stage = _snap_stage("capture_export", _t_stage)
        
        return ofc, dec


    def restore_cancel_gate_state(self, state: Dict[str, Any]) -> None:
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

    def export_cancel_gate_state(self) -> Optional[Dict[str, Any]]:
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
    def export_runtime_snapshot(runtime: Any, indicators: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Minimal runtime snapshot for deterministic replay.

        Contract philosophy:
          - Always include the *keys* (even if value is None) so we can detect
            capture regressions (missing fields after refactors).
          - Keep it JSON-safe: primitives + shallow dicts only.
          - Only include fields that OFConfirmEngine reads via getattr(runtime, ...).

        NOTE: This is *not* a full runtime dump. It is a replay contract.
        """
        ind = indicators or {}

        def _pick(obj: Any, keys: Tuple[str, ...]) -> Optional[Dict[str, Any]]:
            if obj is None:
                return None
            out: Dict[str, Any] = {}
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
                    vv: Dict[str, Any] = {}
                    for kk, x in v.items():
                        if isinstance(x, (str, int, float, bool)) or x is None:
                            vv[str(kk)] = x
                    out[k] = vv
                else:
                    try:
                        out[k] = str(v)
                    except Exception:
                        pass
            return out

        snap: Dict[str, Any] = {
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
                    try:
                        snap[k] = str(v)
                    except Exception:
                        pass

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
    def runtime_snapshot_schema() -> Dict[str, Any]:
        """Schema for runtime_snapshot (contract).

        Used by tools/tests to detect drift when engine starts reading new fields.
        """
        return {
            "schema": 3,
            "required_top": [
                "schema",
                "dynamic_cfg",
                "last_regime",
                "liq_regime",
                "book_churn_hi",
                "cont_ctx_ts_ms",
                "pressure_hi",
                "last_bar",
                "last_obi_event",
                "last_iceberg_event",
                "last_ofi_event",
                "last_sweep",
                "last_reclaim",
                "last_wp",
                "last_div",
                "last_fp_edge",
                "now_ts_ms_used",
            ],
            "required_nested": {
                "last_bar": [
                    "end_ts_ms", "open", "high", "low", "close",
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
                ],
                "last_div": ["ts_ms", "kind"],
            },
        }

    @staticmethod
    def validate_runtime_snapshot_contract(snap: Dict[str, Any]) -> Tuple[bool, list[str]]:
        """Validate snapshot keys presence. Fail-open; returns (ok, missing_paths)."""
        missing: list[str] = []
        sch = OFConfirmEngine.runtime_snapshot_schema()
        top = sch.get("required_top", [])
        for k in top:
            if k not in snap:
                missing.append(f"runtime_snapshot.{k}")
        nested = sch.get("required_nested", {}) or {}
        for parent, keys in nested.items():
            if parent not in snap:
                continue
            obj = snap.get(parent)
            if obj is None:
                continue
            if not isinstance(obj, dict):
                missing.append(f"runtime_snapshot.{parent}:not_dict")
                continue
            for k in keys:
                if k not in obj:
                    missing.append(f"runtime_snapshot.{parent}.{k}")
        return (len(missing) == 0), missing

    def validate_runtime_snapshot(self, snap: Dict[str, Any]) -> List[str]:
        """Validate snapshot has required fields. Returns list of missing keys."""
        missing: List[str] = []
        if not isinstance(snap, dict):
            return ['snap_not_dict']
        for k in ('schema', 'symbol', 'dynamic_cfg', 'last_regime', 'liq_regime', 'book_churn_hi', 'pressure_hi', 'cont_ctx_ts_ms'):
            if k not in snap:
                missing.append(k)
        return missing

    
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
                out: Dict[str, Any] = {}
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
                out_l: List[Any] = []
                for i, v in enumerate(x):
                    if i >= max_items:
                        break
                    out_l.append(_walk(v, d - 1))
                return out_l
            return str(x)
        return _walk(obj, int(max_depth))

    def export_cfg_snapshot(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        """Capture JSON-safe cfg snapshot for deterministic replay."""
        try:
            if not isinstance(cfg, dict):
                return {}
            out = self._json_sanitize(cfg)
            return out if isinstance(out, dict) else {}
        except Exception:
            return {}

    @classmethod
    def runtime_snapshot_schema(cls) -> Dict[str, Any]:
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
    def validate_runtime_snapshot_contract(cls, snap: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
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

        missing_nested: Dict[str, Any] = {}
        for obj_key, fields in (nested.items() if isinstance(nested, dict) else []):
            obj = snap.get(obj_key)
            if obj is None or not isinstance(obj, dict):
                continue
            miss = [f for f in fields if f not in obj]
            if miss:
                missing_nested[obj_key] = miss

        ok = (not missing_top) and (not missing_nested)
        return ok, {"missing_top": missing_top, "missing_nested": missing_nested, "schema": schema.get("schema")}

    def build_runtime_from_snapshot(self, snap: Dict[str, Any]) -> Any:
        """Build SimpleNamespace runtime from snapshot (replay-safe)."""
        rt = SimpleNamespace()
        for k, v in (snap or {}).items():
            try:
                setattr(rt, k, v)
            except Exception:
                pass
        if not hasattr(rt, 'dynamic_cfg'):
            setattr(rt, 'dynamic_cfg', {})
        return rt
