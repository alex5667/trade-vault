from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Tuple
from types import SimpleNamespace
import hashlib
import math
import os
import time

from core.book_evidence import compute_obi_flags, compute_iceberg_flags, compute_ofi_flags
from core.meta_model_lr import MetaModelLR
from core.meta_features_v1 import (
    META_FEATURE_SCHEMA_VERSION,
    META_FEATURE_COLS_HASH,
    build_meta_features,
    meta_missing_stats,
)
from core.of_evidence import compute_sweep_recent, compute_reclaim_recent, compute_absorption_flags
from core.strong_of_gate import eval_reversal, eval_continuation, hidden_trend_dir
from core.absorption_level_score import compute_absorption_level_score
from core.of_confirm_contract import OFConfirmV3, pack_bits
from core.cfg_merge import merged_cfg
from core.strong_need_policy import compute_strong_need_same_tick
from services.cancellation_spike_gate import CancellationSpikeGate
from services.ml_confirm_gate import MLConfirmGate
from common.metrics_stage import veto_total, dist
from core.fp_edge_evidence import compute_fp_edge_absorb
from core.scenario_v4 import classify_v4

def _get_attr_or_key(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _clamp01(x: float) -> float:
    try:
        if not math.isfinite(x): return 0.0 # Защита от NaN/Inf
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


class OFConfirmEngine:
    """
    Replay determinism support:
      - set_replay_time_ms(ts): freezes engine "now" and disables time-based reloads
      - _now_ms(): uses frozen time in replay
    """
    # Use a high bit to avoid clashing with existing gate bits.
    GATE_BIT_CANCEL_SPIKE = 1 << 28
    GATE_BIT_META_VETO = 1 << 27

    def __init__(self, version: int = 3, cancel_gate: Optional[CancellationSpikeGate] = None, ml_gate: Optional[MLConfirmGate] = None) -> None:
        self.version = int(version)
        self._cancel_spike_gate = cancel_gate # will lazy init in build() if None
        # ML gate: lazy init in build() if None (OFF/SHADOW/ENFORCE controlled by env)
        self._ml_gate = ml_gate  # lazy init in build() if None
        self._meta_model = None  # lazy-loaded MetaModelLR
        self._meta_model_path = ""
        self._meta_model_mtime = 0.0
        self._meta_model_last_check_ms = 0
        # Replay determinism support
        self._replay_mode: bool = False
        self._replay_now_ms: Optional[int] = None

        # --- Кэшируем переменные окружения один раз при инициализации ---
        self._env_cache = {
            "SPREAD_BPS_MISSING_DEFAULT": float(os.getenv("SPREAD_BPS_MISSING_DEFAULT", "15.0")),
            "SLIPPAGE_BPS_MISSING_DEFAULT": float(os.getenv("SLIPPAGE_BPS_MISSING_DEFAULT", "4.0")),
            "META_MODEL_ENABLE": int(os.getenv("META_MODEL_ENABLE", "0")),
            "META_MODEL_MODE": str(os.getenv("META_MODEL_MODE", "SHADOW")).upper(),
            "META_P_MIN": float(os.getenv("META_P_MIN", "0.55")),
            "META_MODEL_PATH": str(os.getenv("META_MODEL_PATH", "")).strip(),
            "META_MODEL_RELOAD_SEC": int(os.getenv("META_MODEL_RELOAD_SEC", "60")),
            "META_FEATURE_SCHEMA_REQUIRED": str(os.getenv("META_FEATURE_SCHEMA_REQUIRED", META_FEATURE_SCHEMA_VERSION)),
            "META_ALLOW_LEGACY_SCHEMA": int(os.getenv("META_ALLOW_LEGACY_SCHEMA", "0")),
            "OF_SOFT_SCORE_MIN": float(os.getenv("OF_SOFT_SCORE_MIN", "0.60")),
            "OF_SOFT_EXEC_RISK_NORM_MAX": float(os.getenv("OF_SOFT_EXEC_RISK_NORM_MAX", "0.65")),
        }

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

    # ------------------------------------------------------------------
    # Stateful-gate determinism helpers (golden replay)
    # ------------------------------------------------------------------
    def snapshot_cancel_gate_state(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Return JSON-serializable state of CancellationSpikeGate.

        Used by OFC_CAPTURE to persist state for deterministic golden replay.
        """
        try:
            g = self._cancel_spike_gate
            if g is None:
                return {}
            if hasattr(g, "snapshot_state"):
                return g.snapshot_state(symbol=symbol)
        except Exception:
            pass
        return {}

    def restore_cancel_gate_state(self, state: Dict[str, Any]) -> None:
        """Restore CancellationSpikeGate state produced by snapshot_cancel_gate_state()."""
        try:
            if not isinstance(state, dict):
                return
            if self._cancel_spike_gate is None:
                self._cancel_spike_gate = CancellationSpikeGate()
            g = self._cancel_spike_gate
            if g is not None and hasattr(g, "restore_state"):
                g.restore_state(state)
        except Exception:
            pass

    def _now_ms(self) -> int:
        """
        Deterministic clock.
        In replay: returns frozen ts (if set), else 0 (explicit).
        In prod: wall clock ms.
        """
        if self._replay_mode:
            return int(self._replay_now_ms or 0)
        return int(time.time() * 1000)

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

    def _load_meta_model(self, path: str, now_ms: int, reload_sec: int) -> Optional[MetaModelLR]:
        """
        Fail-open loader with coarse reload interval.
        NOTE: in replay mode we must not refresh by wall-clock timers.
        """
        try:
            path = str(path or "").strip()
            if not path:
                return None

            # In replay mode: freeze meta model (load once outside, or keep current)
            # This removes nondeterminism from periodic reload checks.
            if getattr(self, "_replay_mode", False):
                return getattr(self, "_meta_model", None)

            if (now_ms - int(getattr(self, "_meta_model_last_check_ms", 0) or 0)) < int(reload_sec * 1000):
                # do not stat too often
                return getattr(self, "_meta_model", None)

            self._meta_model_last_check_ms = int(now_ms)

            mtime = os.path.getmtime(path)
            if getattr(self, "_meta_model", None) is None or path != getattr(self, "_meta_model_path", "") or mtime != getattr(self, "_meta_model_mtime", 0.0):
                mm = MetaModelLR.load(path)
                self._meta_model = mm
                self._meta_model_path = path
                self._meta_model_mtime = float(mtime)
            return getattr(self, "_meta_model", None)
        except Exception:
            return None

    @staticmethod
    def runtime_snapshot_schema() -> Dict[str, str]:
        """Schema for replay-safe runtime snapshot.

        This is the minimal set of runtime-derived fields that build() and evidence
        helpers may read. Keep it in sync with runtime usage to prevent "silent drift".
        """
        return {
            "liq_regime": "str",
            "book_churn_hi": "int(0/1)",
            "pressure_hi": "int(0/1)",
            "cont_ctx_ts_ms": "int(ms)",
            "last_div": "dict(ts_ms:int, kind?:str, direction_bias?:str)|None",
            "last_obi_event": "dict(ts_ms:int, direction:str, obi:float, stable_secs:float, obi_z?:float, stacking?:float, concentration?:float)|None",
            "last_iceberg_event": "dict(ts_ms:int, side:str, refresh:int, duration:float, dist_bp?:float)|None",
            "last_ofi_event": "dict(ts_ms:int, direction:str, ofi:float, stable_secs:float, ofi_z?:float, stability_score?:float)|None",
            "last_sweep": "dict(ts_ms:int, kind?:str, direction_bias?:str)|None",
            "last_reclaim": "dict(ts_ms:int, hold_bars?:int, direction_bias?:str, level?:float, pool_id?:str)|None",
        }

    @staticmethod
    def export_runtime_snapshot(runtime: Any, indicators: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Export a JSON-safe snapshot of runtime fields needed for deterministic replay."""

        def _pick_dict(d: Any, keys: Tuple[str, ...]) -> Optional[Dict[str, Any]]:
            if not isinstance(d, dict):
                return None
            out: Dict[str, Any] = {}
            for k in keys:
                if k in d:
                    out[k] = d.get(k)
            return out or None

        def _pick_obj(obj: Any, keys: Tuple[str, ...]) -> Optional[Dict[str, Any]]:
            if obj is None:
                return None
            if isinstance(obj, dict):
                return _pick_dict(obj, keys)
            out: Dict[str, Any] = {}
            for k in keys:
                try:
                    if hasattr(obj, k):
                        out[k] = getattr(obj, k)
                except Exception:
                    continue
            return out or None

        snap: Dict[str, Any] = {}

        # Regime / health
        try:
            snap["liq_regime"] = str(getattr(runtime, "liq_regime", "na") or "na")
        except Exception:
            snap["liq_regime"] = "na"

        try:
            snap["book_churn_hi"] = int(getattr(runtime, "book_churn_hi", 0) or 0)
        except Exception:
            snap["book_churn_hi"] = 0

        # pressure_hi: prefer deterministic indicator (if already computed)
        v = None
        try:
            if isinstance(indicators, dict):
                v = indicators.get("pressure_hi", None)
        except Exception:
            v = None
        if v is None:
            try:
                v = getattr(runtime, "pressure_hi", None)
            except Exception:
                v = None
        try:
            snap["pressure_hi"] = int(v or 0)
        except Exception:
            snap["pressure_hi"] = 0

        # Continuation/divergence context
        try:
            snap["cont_ctx_ts_ms"] = int(getattr(runtime, "cont_ctx_ts_ms", 0) or 0)
        except Exception:
            snap["cont_ctx_ts_ms"] = 0
        try:
            snap["last_div"] = _pick_obj(getattr(runtime, "last_div", None), ("ts_ms", "kind", "direction_bias"))
        except Exception:
            snap["last_div"] = None

        # Book events (already dict in runtime)
        try:
            snap["last_obi_event"] = _pick_dict(getattr(runtime, "last_obi_event", None), ("ts_ms", "direction", "obi", "stable_secs", "obi_z", "stacking", "concentration"))
        except Exception:
            snap["last_obi_event"] = None
        try:
            snap["last_iceberg_event"] = _pick_dict(getattr(runtime, "last_iceberg_event", None), ("ts_ms", "side", "refresh", "duration", "dist_bp"))
        except Exception:
            snap["last_iceberg_event"] = None
        try:
            snap["last_ofi_event"] = _pick_dict(getattr(runtime, "last_ofi_event", None), ("ts_ms", "direction", "ofi", "stable_secs", "ofi_z", "stability_score"))
        except Exception:
            snap["last_ofi_event"] = None

        # OF events (objects in runtime)
        try:
            snap["last_sweep"] = _pick_obj(getattr(runtime, "last_sweep", None), ("ts_ms", "kind", "direction_bias"))
        except Exception:
            snap["last_sweep"] = None
        try:
            snap["last_reclaim"] = _pick_obj(getattr(runtime, "last_reclaim", None), ("ts_ms", "hold_bars", "direction_bias", "level", "pool_id"))
        except Exception:
            snap["last_reclaim"] = None

        # Drop empty values to keep payload small
        out2: Dict[str, Any] = {}
        for k, v2 in snap.items():
            if v2 is None:
                continue
            if isinstance(v2, str) and not v2:
                continue
            out2[k] = v2
        return out2

    def export_cancel_gate_state(self) -> Dict[str, Any]:
        """Export cancellation gate state for deterministic replay."""
        gate = getattr(self, "_cancel_spike_gate", None)
        if gate is None:
            return {}
        # Prefer explicit API if present
        try:
            if hasattr(gate, "get_state"):
                st = gate.get_state()
                if isinstance(st, dict):
                    return {"type": gate.__class__.__name__, "state": st}
        except Exception:
            pass
        # Common pattern: gate.state dict
        try:
            st2 = getattr(gate, "state", None)
            if isinstance(st2, dict):
                return {"type": gate.__class__.__name__, "state": dict(st2)}
        except Exception:
            pass
        # Fallback: shallow scalar snapshot
        d = getattr(gate, "__dict__", {}) or {}
        out: Dict[str, Any] = {}
        for k, v in d.items():
            if v is None or isinstance(v, (int, float, str, bool)):
                out[k] = v
        return {"type": gate.__class__.__name__, "state": out}

    def restore_cancel_gate_state(self, state: Optional[Dict[str, Any]]) -> None:
        """Restore cancellation gate state exported by export_cancel_gate_state()."""
        if not state or not isinstance(state, dict):
            return
        if getattr(self, "_cancel_spike_gate", None) is None:
            try:
                self._cancel_spike_gate = CancellationSpikeGate()
            except Exception:
                return

        gate = getattr(self, "_cancel_spike_gate", None)
        if gate is None:
            return
        st = state.get("state") if isinstance(state.get("state", None), dict) else state
        if not isinstance(st, dict):
            return

        try:
            if hasattr(gate, "set_state"):
                gate.set_state(st)
                return
        except Exception:
            pass
        try:
            if hasattr(gate, "state") and isinstance(getattr(gate, "state"), dict):
                getattr(gate, "state").update(st)
                return
        except Exception:
            pass
        # Last resort: set attrs
        for k, v in st.items():
            try:
                setattr(gate, k, v)
            except Exception:
                continue

    def build(
        self,
        *,
        symbol: str,
        tf: str,
        direction: str,
        tick_ts_ms: int,
        price: float,
        delta_z: float,
        runtime: Any,
        cfg: Dict[str, Any],
        indicators: Dict[str, Any],
        absorption: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[OFConfirmV3], Optional[Any]]:
        """
        Returns:
          (of_confirm, gate_decision)

        Centralizes evidence computation, scenario evaluation, and continuous scoring.
        """
        # Deterministic time source (replay-safe)
        now_ts = self._resolve_now_ts(tick_ts_ms, indicators)

        # Optional runtime snapshot (from golden replay capture)
        rt_snap: Optional[Dict[str, Any]] = None
        try:
            rs = indicators.get("runtime_snapshot", None)
            if isinstance(rs, dict):
                rt_snap = rs
        except Exception:
            rt_snap = None

        def _rt(name: str, default: Any = None) -> Any:
            if isinstance(rt_snap, dict) and name in rt_snap:
                return rt_snap.get(name, default)
            return _get_attr_or_key(runtime, name, default)

        def _as_obj(x: Any) -> Any:
            # replay payload may store objects as dict; convert for getattr()-based helpers
            if isinstance(x, dict):
                try:
                    return SimpleNamespace(**x)
                except Exception:
                    return x
            return x
        
        # --- Book evidence (OBI/Iceberg) ---
        obi_dir_ok, obi_stable, obi_stable_secs, obi_val = compute_obi_flags(
            direction=direction,
            now_ts_ms=now_ts,
            last_event=_rt("last_obi_event", None),
            cfg=cfg,
            indicators=indicators,
        )
        iceberg_dir_ok, iceberg_strict, iceberg_refresh, iceberg_duration = compute_iceberg_flags(
            direction=direction,
            price=float(price),
            now_ts_ms=now_ts,
            last_event=_rt("last_iceberg_event", None),
            cfg=cfg,
            indicators=indicators,
        )

        # --- OFI evidence (first-class) ---
        # C1: OFI becomes an alternative microstructure leg for Have/Need by safely substituting OBI stable.
        # IMPORTANT: OFI is treated as "book/time-dependent" evidence -> it will be vetoed when book_ok=0.
        ofi_dir_ok, ofi_stable, ofi_stable_secs, ofi_val, ofi_z, ofi_stability_score = compute_ofi_flags(
            direction=direction,
            now_ts_ms=now_ts,
            last_event=_rt("last_ofi_event", None),
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
            last_sweep=_as_obj(_rt("last_sweep", None)),
            cfg=cfg,
            indicators=indicators,
        )
        reclaim_recent, reclaim_hold_bars = compute_reclaim_recent(
            direction=direction,
            now_ts_ms=now_ts,
            last_reclaim=_as_obj(_rt("last_reclaim", None)),
            cfg=cfg,
            indicators=indicators,
        )

        # --- Absorption ---
        abs_ok, abs_vol = compute_absorption_flags(
            direction=direction,
            absorption=absorption,
            cfg=cfg,
            indicators=indicators,
        )

        # --- Weak progress (computed on bar_close) ---
        wp_any = bool(getattr(getattr(runtime, "last_wp", None), "weak_any", False))
        indicators["weak_progress"] = 1 if wp_any else 0

        # --- FP edge absorb (A2) ---
        # Use runtime.last_fp_edge (produced by footprint edge detector on microbars).
        # This evidence is useful to confirm absorption at edge without range expansion (anti-fake-impulse).
        fp_edge_ok, fp_edge_strength, fp_edge_rng, fp_edge_bias = compute_fp_edge_absorb(
            direction=direction,
            now_ts_ms=int(now_ts),
            last_edge=getattr(runtime, "last_fp_edge", None),
            cfg=cfg,
            indicators=indicators,
        )

        # --- Scenario selection ---
        scenario = "reversal" if sweep_recent else "continuation"
        dec = None
        fallback_reason = "unknown"

        # Continuation needs a trend direction (from hidden divergence kind if available)
        trend_dir = None
        if scenario == "continuation":
            # Best practice: if CVD is quarantined, ignore hidden divergence (avoid false trend from broken baseline)
            cvd_q = int(indicators.get("cvd_quarantine_active", 0) or 0)
            div = None if cvd_q == 1 else _rt("last_div", None)
            if cvd_q == 1:
                indicators["hidden_div_ignored"] = 1
            trend_dir = hidden_trend_dir(getattr(div, "kind", None) if div else None)
            
            # FAILBACK: If no hidden divergence, use REGIME as trend definition (Trend Following)
            if trend_dir is None:
                 rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                 if "bull" in rg: 
                     trend_dir = "LONG"
                 elif "bear" in rg: 
                     trend_dir = "SHORT"
            else:
                 indicators["hidden_div_used"] = 1

            if trend_dir is None:
                scenario = "none"
                fallback_reason = "no_sweep_and_no_trend"
                try:
                     indicators["of_debug_fail"] = f"no_trend:regime={getattr(runtime, 'last_regime', 'na')}"
                except Exception: 
                     pass

        scenario_v4 = scenario
        policy_reason = ""

        # proxy: news/vol shock
        news_flag = int(indicators.get("news_risk", 0) or indicators.get("calendar_risk", 0) or 0)
        reg = str(getattr(runtime, "last_regime", "") or "").lower()
        vol_shock = bool(news_flag == 1 or ("news" in reg) or ("shock" in reg))

        # proxy: saw/chop/spoof-ish
        churn_hi = bool(int(_rt("book_churn_hi", 0) or 0) == 1 or int(indicators.get("book_churn_hi", 0) or 0) == 1)
        saw_chop = bool(int(indicators.get("saw_chop", 0) or 0) == 1 or churn_hi)

        if vol_shock:
            scenario_v4 = "vol_shock_news_proxy"
            policy_reason = "vol_shock_proxy"
        elif saw_chop:
            scenario_v4 = "saw_chop_spoof_proxy"
            policy_reason = "saw_chop_proxy"

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

        # --- Strong gate decision (need is scenario-dependent, can be escalated same-tick) ---
        # Merge dynamic cfg (runtime.dynamic_cfg) into local cfg view.
        dyn = getattr(runtime, "dynamic_cfg", {}) or {}
        cfg2 = merged_cfg(cfg, dyn)

        # Determine regime / instability / pressure / churn same-tick inputs
        try:
            regime = str(getattr(runtime, "last_regime", "na") or "na")
        except Exception:
            regime = "na"
        try:
            unstable = bool(int(dyn.get("abs_lvl_th_unstable", 0) or 0))
        except Exception:
            unstable = False
        # pressure_hi: deterministic source order
        #  1) indicators['pressure_hi'] (captured)
        #  2) runtime_snapshot['pressure_hi'] (captured)
        #  3) runtime.pressure.is_pressure_hi(...) (live-only, non-replay-safe)
        pressure_hi = False
        try:
            v_ph = indicators.get("pressure_hi", None)
            if v_ph is None and isinstance(rt_snap, dict):
                v_ph = rt_snap.get("pressure_hi", None)
            if v_ph is not None:
                pressure_hi = bool(int(v_ph))
            else:
                pr = getattr(runtime, "pressure", None)
                if pr is not None and hasattr(pr, "is_pressure_hi"):
                    pressure_hi = bool(pr.is_pressure_hi(int(now_ts), float(cfg2.get("pressure_hi_per_min", 4.0))))
        except Exception:
            pressure_hi = False
        # churn_hi from runtime
        try:
            churn_hi = bool(int(_rt("book_churn_hi", 0) or 0))
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
            cont_ts = int(_rt("cont_ctx_ts_ms", 0) or 0)
            cont_valid = int(cfg2.get("cont_ctx_valid_ms", 120_000))
            cont_ctx_recent = (cont_ts > 0 and 0 <= now_ts_for_cont - cont_ts <= cont_valid)
            
            # hidden ctx recent
            div = _rt("last_div", None)
            hidden_ms = int(cfg2.get("hidden_ctx_valid_ms", 120_000))
            div_ts = int(getattr(div, "ts_ms", now_ts_for_cont))
            hidden_ctx_recent = (div is not None and 0 <= now_ts_for_cont - div_ts <= hidden_ms)
            
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
            spread_bps = _f(cfg.get("spread_bps_missing_default", self._env_cache["SPREAD_BPS_MISSING_DEFAULT"]), 15.0)
            indicators["spread_bps_missing"] = 1
        if slip_bps < 0:
            slip_bps = _f(cfg.get("expected_slippage_bps_missing_default", self._env_cache["SLIPPAGE_BPS_MISSING_DEFAULT"]), 4.0)
            indicators["expected_slippage_missing"] = 1

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
            score = _clamp01(raw_sum)
        else:
            score = _clamp01(raw_sum / max(1e-9, w_sum))
            
        # Apply penalty after base score
        score = score - float(exec_pen)

        ok = 0
        have = 0
        need = 0
        if dec is not None:
            ok = 1 if bool(dec.ok) else 0
            have = int(dec.have)
            need = int(dec.need)

        # Score threshold (double filter)
        # NOTE: scenario-specific thresholds are applied later if scenario_v4 is enabled.
        score_min = _f(cfg.get("of_score_min", 0.65), 0.65)
        if ok == 1 and score < score_min:
             # Logic: if score is too low, we can veto even if 2-of-3 passed (optional but recommended)
             # But we only do this if it's not shadow mode in the caller. 
             # We'll just return ok=0 and let the service decide.
             ok = 0
             # Optional: log if we vetoed by score

        # --- B2 scenario policies enforcement (post-score, pre-final reason) ---
        hard_veto = ""
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
        gate_reason = ""
        gate_meta = {}
        gate_vetoed = False
        ok_pre_gate = int(ok)
        try:
            # Optional deterministic replay hook: restore gate state from capture.
            # This is a no-op in prod unless the caller injects this key.
            try:
                st = indicators.get("cancel_gate_state", None)
                if st is None:
                    st = indicators.get("cancel_spike_gate_state", None)
                if isinstance(st, dict) and st:
                    self.restore_cancel_gate_state(st)
            except Exception:
                pass
            if not hasattr(self, "_cancel_spike_gate") or getattr(self, "_cancel_spike_gate") is None:
                self._cancel_spike_gate = CancellationSpikeGate()

            # prefer explicit keys from indicators
            c_bid = _f(indicators.get("cancel_bid_rate_ema", 0.0), 0.0)
            c_ask = _f(indicators.get("cancel_ask_rate_ema", 0.0), 0.0)
            t_buy = _f(indicators.get("taker_buy_rate_ema", 0.0), 0.0)
            t_sell = _f(indicators.get("taker_sell_rate_ema", 0.0), 0.0)

            # bucket monotonicity or bar_id
            b_id = indicators.get("bucket_id", indicators.get("bar_id"))
            if b_id is None and bar is not None:
                b_id = getattr(bar, "id", None)

            gd = self._cancel_spike_gate.check(
                symbol=str(symbol),
                direction=str(direction),
                cancel_bid_rate_ema=float(c_bid),
                cancel_ask_rate_ema=float(c_ask),
                taker_buy_rate_ema=float(t_buy),
                taker_sell_rate_ema=float(t_sell),
                bucket_id=int(b_id) if b_id is not None else None,
                cfg2=cfg2,
            )
            gate_reason = str(gd.reason)
            gate_meta = dict(getattr(gd, "meta", {}) or {})

            # Attach diagnostics
            indicators["cancel_spike_reason"] = gate_reason
            indicators["cancel_spike_ready"] = int(gate_meta.get("ready", 0))

            if (not bool(gd.allow)) and ok_pre_gate == 1:
                ok = 0
                gate_vetoed = True
                try:
                    if dec is not None:
                        # Mark as gate bit
                        setattr(dec, "gate_bits", int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_CANCEL_SPIKE)
                except Exception:
                    pass
                veto_total(runtime, reason_code=gate_reason, kind="cancel_spike", symbol=str(symbol))

            # Distributions
            try:
                dist(runtime, "cancel_spike_ratio_support", float(gate_meta.get("ratio_support", 0.0)),
                     kind="cancel_spike", symbol=str(symbol), side=str(gate_meta.get("support_side", "")), dir=str(gate_meta.get("direction", "")))
                dist(runtime, "cancel_spike_z_support", float(gate_meta.get("z_support", 0.0)),
                     kind="cancel_spike", symbol=str(symbol), side=str(gate_meta.get("support_side", "")), dir=str(gate_meta.get("direction", "")))
            except Exception:
                pass
        except Exception:
            pass

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
            scn_v4 = classify_v4(
                sweep_recent=bool(sweep_recent),
                trend_dir=trend_dir,
                pressure_hi=bool(pressure_hi),
                churn_hi=bool(churn_hi),
                exec_risk_bps=float(exec_risk_bps),
                liq_regime=str(liq_regime),
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
        }

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
                soft_reason = ""
                
                if not is_hard_pass and (have == need - 1):
                    if score >= soft_min_score and exec_risk_norm <= soft_max_risk:
                        is_soft_pass = True
                        soft_reason = "range_soft_fail"
                
                ok = 1 if is_hard_pass else 0
                
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

        # Required legs by scenario v4 (for missing_legs in UI/Telegram)
        req = []
        if scenario_v4 == "range_meanrev":
            req = ["absorption", "obi_stable", "iceberg_strict"]  # coarse view; abs_lvl/fp_edge/ofi are alternatives
        elif scenario_v4 == "vol_shock_news_proxy":
            req = ["absorption", "obi_stable", "reclaim_recent", "iceberg_strict", "vol_shock_exec_risk_ok"]
        elif scenario_v4 == "saw_chop_spoof_proxy":
            req = ["iceberg_strict", "ofi_stable", "fp_edge_absorb", "saw_chop_exec_risk_ok"] # Hard evidence required
        elif scenario_base == "continuation":
            req = ["obi_stable", "ofi_leg", "weak_progress"] # Standard continuation legs
        elif scenario_base == "reversal":
            req = ["absorption", "obi_stable", "reclaim_recent"] # Standard reversal legs
        missing = [k for k in req if int(legs.get(k, 0)) == 0]

        # --- Soft-fail (analytics-only) ---
        ok_soft = 0
        try:
            if int(ok) == 0 and int(need) > 0 and int(have) == int(need) - 1:
                # FIX (2026-02-01): Lower default soft_score_min to 0.60 to capture near-misses (missed by 1 leg but decent score).
                # Formerly 0.78 was too strict (stricter than passing score 0.65).
                # Priority: ENV > cfg > default
                # import os removed

                soft_score_min = float(
                    self._env_cache["OF_SOFT_SCORE_MIN"] or 
                    cfg.get("soft_score_min") or 
                    0.60
                )
                soft_exec_max = float(
                    self._env_cache["OF_SOFT_EXEC_RISK_NORM_MAX"] or 
                    cfg.get("soft_exec_risk_norm_max") or 
                    0.65
                )
                if float(score) >= soft_score_min and float(exec_risk_norm) <= soft_exec_max:
                    ok_soft = 1
        except Exception:
            pass

        final_reason = str(getattr(dec, "reason", fallback_reason))
        if dec is not None:
             final_reason = f"{final_reason}({have}/{need})"

        if gate_vetoed and gate_reason:
            final_reason = f"{gate_reason}(veto)"

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

        evidence = {
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
            "fp_move_bp": float(getattr(bar, "fp_move_bp", 0.0) if bar else 0.0),
            "fp_eff_quote": float(getattr(bar, "fp_eff_quote", 0.0) if bar else 0.0),
            "fp_quote_delta": float(getattr(bar, "fp_quote_delta", 0.0) if bar else 0.0),
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
            "cancel_spike_ready": int(gate_meta.get("ready", 0) if isinstance(gate_meta, dict) else 0),
            "cancel_spike_ratio_support": float(gate_meta.get("ratio_support", 0.0) if isinstance(gate_meta, dict) else 0.0),
            "cancel_spike_z_support": float(gate_meta.get("z_support", 0.0) if isinstance(gate_meta, dict) else 0.0),
        }

        # ------------------------------------------------------------------
        # ML confirm gate (Step C1/D/4): after hard vetoes, before final decision.
        # Modes:
        #   OFF    -> no effect
        #   SHADOW -> attach p_edge but never block
        #   ENFORCE-> require p_edge >= threshold (fail policy applied inside MLConfirmGate)
        # ------------------------------------------------------------------
        try:
            if self._ml_gate is None:
                self._ml_gate = MLConfirmGate.from_env()

            # Prefer scenario_v4 for ML bucketization when dec.scenario is legacy (reversal/continuation)
            # This ensures ML v10.4 util_mh always gets v4 scenario for correct bucket selection and util_floor_by_bucket
            ml_scenario = str(getattr(dec, "scenario", "") if dec else "") or str(scenario)

            # If legacy scenario, try to use indicators["scenario_v4"] (set by engine / strategy)
            if ml_scenario.lower() in ("reversal", "continuation", "none"):
                sv4 = ""
                try:
                    sv4 = str(indicators.get("scenario_v4", "") or "")
                except Exception:
                    sv4 = ""
                if sv4:
                    ml_scenario = sv4
                # Fallback: use computed scenario_v4 if available (from line 712)
                elif scenario_v4 and scenario_v4.lower() not in ("reversal", "continuation", "none"):
                    ml_scenario = scenario_v4

            # Ensure scenario_v4 is in indicators for ML feature extraction
            indicators_with_v4 = dict(indicators, delta_z=float(delta_z))
            if scenario_v4:
                indicators_with_v4["scenario_v4"] = str(scenario_v4)

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
                ok_rule=int(ok),
            )
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

        # Add scenario v4 / explainability fields to evidence
        evidence.update({
            "scenario_v4": str(scenario_v4),
            "need_reason": str(nd.reason if nd is not None else ""),
            "policy_reason": str(policy_reason),
            
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
            "meta_feature_schema": str(META_FEATURE_SCHEMA_VERSION),
            "meta_feature_cols_hash": str(META_FEATURE_COLS_HASH),
            "meta_missing_feature_count": 0,
            "meta_missing_feature_rate": 0.0,
            "meta_missing_critical_count": 0,
            "meta_schema_mismatch": 0,
            "meta_schema_reason": "",
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
        meta_reason = ""
        apply_enforce = 0  # 1 if ENFORCE should be applied (canary-share check)
        try:
            meta_enable = bool(int(cfg2.get("meta_model_enable", self._env_cache["META_MODEL_ENABLE"])))
            meta_mode = str(cfg2.get("meta_model_mode", self._env_cache["META_MODEL_MODE"])).upper()
            meta_p_min = float(cfg2.get("meta_p_min", self._env_cache["META_P_MIN"]))
            meta_path = str(cfg2.get("meta_model_path", self._env_cache["META_MODEL_PATH"] or "")).strip()
            reload_sec = int(cfg2.get("meta_model_reload_sec", self._env_cache["META_MODEL_RELOAD_SEC"]))

            if meta_enable and meta_path:
                mm = self._load_meta_model(meta_path, now_ts, reload_sec)
                if mm is not None:
                    # feature dict: stable + interpretable (missing => 0 in model)
                    meta_ctx = {
                        "rule_score": float(score),
                        "have": float(have),
                        "need": float(need),
                        "ok_soft": float(ok_soft),
                        "exec_risk_norm": float(exec_risk_norm),
                        "exec_risk_bps": float(exec_risk_bps),
                    }

                    feat, meta_feat_stats = build_meta_features(
                        evidence=evidence if isinstance(evidence, dict) else {},
                        indicators=indicators if isinstance(indicators, dict) else {},
                        indicators_with_v4=indicators_with_v4 if isinstance(indicators_with_v4, dict) else {},
                        legs=legs if isinstance(legs, dict) else {},
                        runtime=runtime,
                        meta_ctx=meta_ctx,
                    )

                    meta_schema_required = str(
                        cfg2.get(
                            "meta_feature_schema_required",
                            self._env_cache["META_FEATURE_SCHEMA_REQUIRED"],
                        )
                        or META_FEATURE_SCHEMA_VERSION
                    )
                    mm_schema = str(getattr(mm, "schema_version", "") or "")
                    meta_schema_mismatch = 0
                    meta_schema_reason = ""
                    if meta_schema_required:
                        if not mm_schema:
                            allow_legacy = bool(
                                int(cfg2.get("meta_allow_legacy_schema", self._env_cache["META_ALLOW_LEGACY_SCHEMA"]))
                            )
                            if meta_mode == "ENFORCE" and not allow_legacy:
                                meta_schema_mismatch = 1
                                meta_schema_reason = "meta_schema_unknown"
                                meta_mode = "SHADOW"
                        elif mm_schema != meta_schema_required:
                            meta_schema_mismatch = 1
                            meta_schema_reason = "meta_schema_mismatch"
                            if meta_mode == "ENFORCE":
                                meta_mode = "SHADOW"

                    model_feats = list(getattr(mm, "features", []) or [])
                    if model_feats:
                        meta_feat_stats_model = meta_missing_stats(
                            feat=feat,
                            present=meta_feat_stats.get("present", {}),
                            schema_version=META_FEATURE_SCHEMA_VERSION,
                            feature_names=model_feats,
                        )
                    else:
                        meta_feat_stats_model = meta_feat_stats

                    evidence["meta_feature_schema"] = str(META_FEATURE_SCHEMA_VERSION)
                    evidence["meta_feature_cols_hash"] = str(META_FEATURE_COLS_HASH)
                    evidence["meta_missing_feature_count"] = int(meta_feat_stats_model.get("missing_count", 0) or 0)
                    evidence["meta_missing_feature_rate"] = float(meta_feat_stats_model.get("missing_rate", 0.0) or 0.0)
                    evidence["meta_missing_critical_count"] = int(meta_feat_stats_model.get("missing_critical_count", 0) or 0)
                    evidence["meta_schema_mismatch"] = int(meta_schema_mismatch)
                    if meta_schema_reason:
                        evidence["meta_schema_reason"] = str(meta_schema_reason)

                    # 3b. Coverage (P30): model-feature coverage for canary + dashboards
                    try:
                        mm_feats = getattr(mm, "features", []) or []
                        missing_set = set(feat_missing)
                        total_m = int(len(mm_feats))
                        miss_m = 0
                        if total_m > 0:
                            for f in mm_feats:
                                if f in missing_set:
                                    miss_m += 1
                            cov = max(0.0, min(1.0, 1.0 - (miss_m / float(total_m))))
                            miss_rate = max(0.0, min(1.0, miss_m / float(total_m)))
                        else:
                            cov = 0.0
                            miss_rate = 1.0
                        evidence["meta_model_feature_total"] = int(total_m)
                        evidence["meta_model_feature_missing"] = int(miss_m)
                        evidence["meta_feature_coverage"] = float(cov)
                        evidence["meta_feature_missing_rate"] = float(miss_rate)
                        sch_label_cov = str(model_schema_name)
                        dist(self, "meta_feature_coverage", float(cov), schema=sch_label_cov, kind="of_confirm", symbol=symbol)
                        dist(self, "meta_feature_missing_rate", float(miss_rate), schema=sch_label_cov, kind="of_confirm", symbol=symbol)
                    except Exception:
                        pass

                    try:
                        dist(
                            self,
                            "meta_missing_feature_rate",
                            float(evidence.get("meta_missing_feature_rate", 0.0) or 0.0),
                            kind="of_confirm",
                            symbol=symbol,
                        )
                        dist(self, "meta_schema_mismatch", float(meta_schema_mismatch), kind="of_confirm", symbol=symbol)
                    except Exception:
                        pass
                    meta_p = float(mm.predict_proba(feat))
                    if meta_p < meta_p_min:
                        meta_veto = 1
                        meta_reason = "meta_p_below_min"

                    # --- Canary share for ENFORCE (deterministic) ---
                    # Priority:
                    #   1) per-coverage buckets (meta_enforce_per_cov=1)
                    #   2) per-regime buckets (meta_enforce_per_regime=1)
                    #   3) global meta_enforce_share
                    rb = str(indicators.get("regime_bucket", "") or indicators.get("regime_group", "") or getattr(runtime, "last_regime", "") or "").lower()
                    if "news" in rb or "fomc" in rb or "cpi" in rb:
                        bucket = "news"
                    elif "trend" in rb or "bull" in rb or "bear" in rb:
                        bucket = "trend"
                    elif "range" in rb or "chop" in rb or "meanrev" in rb:
                        bucket = "range"
                    else:
                        bucket = "other"

                    # Coverage bucket (P30)
                    try:
                        cov_v = float(evidence.get("meta_feature_coverage", 1.0) or 1.0)
                    except Exception:
                        cov_v = 1.0
                    try:
                        cov_a_ge = float(cfg2.get("meta_cov_bucket_a_ge", 0.98) or 0.98)
                        cov_b_ge = float(cfg2.get("meta_cov_bucket_b_ge", 0.95) or 0.95)
                        cov_c_ge = float(cfg2.get("meta_cov_bucket_c_ge", 0.90) or 0.90)
                    except Exception:
                        cov_a_ge, cov_b_ge, cov_c_ge = 0.98, 0.95, 0.90

                    if cov_v >= cov_a_ge:
                        cov_bucket = "a"
                    elif cov_v >= cov_b_ge:
                        cov_bucket = "b"
                    elif cov_v >= cov_c_ge:
                        cov_bucket = "c"
                    else:
                        cov_bucket = "d"

                    use_per_cov = bool(int(cfg2.get("meta_enforce_per_cov", 0) or 0))
                    use_per_regime = bool(int(cfg2.get("meta_enforce_per_regime", 0) or 0))

                    if use_per_cov:
                        share_key = f"meta_enforce_share_cov_{cov_bucket}"
                        meta_enforce_share = float(cfg2.get(share_key, cfg2.get("meta_enforce_share", 1.0)) or 1.0)
                    elif use_per_regime:
                        share_key = f"meta_enforce_share_{bucket}"
                        meta_enforce_share = float(cfg2.get(share_key, cfg2.get("meta_enforce_share", 1.0)) or 1.0)
                    else:
                        meta_enforce_share = float(cfg2.get("meta_enforce_share", 1.0) or 1.0)
                    meta_enforce_share = max(0.0, min(1.0, float(meta_enforce_share)))

                    meta_enforce_salt = str(cfg2.get("meta_enforce_salt", "enf_v1") or "enf_v1")

                    # Prefer stable SID provided by strategy / inputs; fallback to synthetic key
                    sid = str(indicators.get("sid", "") or indicators.get("stable_sid", "") or "")
                    if not sid:
                        sid = f"{symbol}|{now_ts}|{direction}|{scenario}"

                    hkey = f"{meta_enforce_salt}|{sid}"
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
        
        # Rollout fields (for observability)
        try:
            # Recompute regime bucket for evidence (low cardinality)
            rb_ev = str(indicators.get("regime_bucket", "") or indicators.get("regime_group", "") or getattr(runtime, "last_regime", "") or "").lower()
            if "news" in rb_ev or "fomc" in rb_ev or "cpi" in rb_ev:
                bucket_ev = "news"
            elif "trend" in rb_ev or "bull" in rb_ev or "bear" in rb_ev:
                bucket_ev = "trend"
            elif "range" in rb_ev or "chop" in rb_ev or "meanrev" in rb_ev:
                bucket_ev = "range"
            else:
                bucket_ev = "other"

            # Coverage bucket (P30)
            try:
                cov_ev = float(evidence.get("meta_feature_coverage", 1.0) or 1.0)
            except Exception:
                cov_ev = 1.0
            try:
                cov_a_ge = float(cfg2.get("meta_cov_bucket_a_ge", 0.98) or 0.98)
                cov_b_ge = float(cfg2.get("meta_cov_bucket_b_ge", 0.95) or 0.95)
                cov_c_ge = float(cfg2.get("meta_cov_bucket_c_ge", 0.90) or 0.90)
            except Exception:
                cov_a_ge, cov_b_ge, cov_c_ge = 0.98, 0.95, 0.90
            
            if cov_ev >= cov_a_ge:
                cov_bucket_ev = "a"
            elif cov_ev >= cov_b_ge:
                cov_bucket_ev = "b"
            elif cov_ev >= cov_c_ge:
                cov_bucket_ev = "c"
            else:
                cov_bucket_ev = "d"

            use_per_cov_ev = bool(int(cfg2.get("meta_enforce_per_cov", 0) or 0))
            use_per_regime_ev = bool(int(cfg2.get("meta_enforce_per_regime", 0) or 0))

            meta_enforce_share_global = float(cfg2.get("meta_enforce_share", 1.0) or 1.0)
            meta_enforce_share_cov = float(cfg2.get(f"meta_enforce_share_cov_{cov_bucket_ev}", meta_enforce_share_global) or meta_enforce_share_global)
            meta_enforce_share_regime = float(cfg2.get(f"meta_enforce_share_{bucket_ev}", meta_enforce_share_global) or meta_enforce_share_global)

            if use_per_cov_ev:
                meta_enforce_share_ev = meta_enforce_share_cov
                bucket_type = "cov"
            elif use_per_regime_ev:
                meta_enforce_share_ev = meta_enforce_share_regime
                bucket_type = "regime"
            else:
                meta_enforce_share_ev = meta_enforce_share_global
                bucket_type = "global"
            meta_enforce_share_ev = max(0.0, min(1.0, float(meta_enforce_share_ev)))

            meta_enforce_salt = str(cfg2.get("meta_enforce_salt", "enf_v1") or "enf_v1")
            sid = str(indicators.get("sid", "") or indicators.get("stable_sid", "") or "")
            if not sid:
                sid = f"{symbol}|{now_ts}|{direction}|{scenario}"

            evidence["meta_enforce_share"] = float(meta_enforce_share_ev)
            evidence["meta_enforce_share_global"] = float(meta_enforce_share_global)
            evidence["meta_enforce_share_cov"] = float(meta_enforce_share_cov)
            evidence["meta_enforce_share_regime"] = float(meta_enforce_share_regime)
            evidence["meta_enforce_bucket"] = str(bucket_ev)
            evidence["meta_enforce_bucket_type"] = str(bucket_type)
            evidence["meta_enforce_cov_bucket"] = str(cov_bucket_ev)
            evidence["meta_enforce_salt"] = str(meta_enforce_salt)
            evidence["meta_enforce_key"] = str(sid)
            evidence["meta_enforce_applied"] = int(apply_enforce if meta_mode == "ENFORCE" else 0)
        except Exception:
            evidence["meta_enforce_share"] = 1.0
            evidence["meta_enforce_share_global"] = 1.0
            evidence["meta_enforce_share_cov"] = 1.0
            evidence["meta_enforce_share_regime"] = 1.0
            evidence["meta_enforce_bucket"] = "other"
            evidence["meta_enforce_bucket_type"] = "global"
            evidence["meta_enforce_cov_bucket"] = "a"
            evidence["meta_enforce_salt"] = "enf_v1"
            evidence["meta_enforce_key"] = ""
            evidence["meta_enforce_applied"] = 0

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

        # ------------------------------------------------------------------
        # B6: deterministic NDJSON capture for golden replay parity.
        # Fail-open: capture must never affect trading decisions.
        #
        # Enabled by:
        #   - cfg2: ofc_capture_enable=1
        #   - env : OFC_CAPTURE=1 or OFC_CAPTURE_ENABLE=1
        # Optional deterministic sampling:
        #   - cfg2/ofc_capture_sample_ppm or env OFC_CAPTURE_SAMPLE_PPM (default 1000 ppm = 0.1%)
        # ------------------------------------------------------------------
        try:
            from core_snapshot.ofc_capture_v1 import maybe_capture_ofc_v1  # type: ignore
            maybe_capture_ofc_v1(engine=self, runtime=runtime, indicators=indicators, cfg2=cfg2, ofc=ofc, dec=dec, now_ts_ms=int(now_ts))
        except Exception:
            pass
        return ofc, dec
