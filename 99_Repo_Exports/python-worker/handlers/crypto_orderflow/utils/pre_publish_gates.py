from __future__ import annotations

import functools
import os

from utils.time_utils import get_ny_time_millis


@functools.lru_cache(maxsize=1024)
def _cached_getenv(k, d=None): return os.getenv(k, d)
import math
import time
from dataclasses import dataclass
from typing import Any
from core.signal_payload import GateDecisionV1

from core.atr_floor_policy import compute_atr_bps_threshold
from domain.gate_profile import strict_enabled
from domain.time_utils import normalize_ts_ms, session_from_ts_ms
from handlers.crypto_orderflow.utils.drift_reader import load_drift_active_factor
from services.atr_horizon_canary import should_enforce_horizon_gate
from services.atr_horizon_shadow_gate import compute_horizon_dq_shadow


@functools.lru_cache(maxsize=1024)
def _env_bool(name: str, default: bool = False) -> bool:
    v = (_cached_getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


@functools.lru_cache(maxsize=1024)
def _env_float(name: str, default: float) -> float:
    try:
        return float(_cached_getenv(name, str(default)) or default)
    except Exception:
        return default


def _norm_symbol(sym: Any) -> str:
    return (sym or "").strip().upper().replace("/", "").replace("-", "")


def _parse_csv_set(v: str) -> set[str]:
    out: set[str] = set()
    for x in (v or "").split(","):
        s = x.strip().lower()
        if s:
            out.add(s)
    return out



def _get_regime(ctx: Any) -> str:
    from contexts import MARKET_REGIME_NA, normalize_regime_label
    reg = str(getattr(ctx, "regime", None) or getattr(getattr(ctx, "of", None), "regime", MARKET_REGIME_NA))
    return normalize_regime_label(reg)


def _get_epoch_ms(ctx: Any) -> int | None:
    ts = getattr(ctx, "ts_event_ms", None) or getattr(ctx, "ts", None)
    try:
        return int(ts) if ts is not None else None
    except Exception:
        return None


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except Exception:
        return default






@dataclass
class HardDataQualityGate:
    """
    Hard veto on *data quality* issues that strongly correlate with bad fills and churn.

    Controlled by ENV (all optional; defaults are fail-open unless enabled):
      DATA_HARD_GATE_ENABLED=1/0
      DATA_HARD_GATE_ENABLED__{SYMBOL}=1/0   (per-symbol override; takes priority)
      DATA_REQUIRE_EPOCH_TS=1/0              (veto if ts_event_ms isn't epoch ms)
      DATA_ATR_STALE_MAX_MS=180000           (3m default when enabled)
      DATA_STRICT_MISSING_ATR_TS=1/0         (veto if atr_ts_ms missing)
      DATA_STRICT_TOUCH_FRESH=1/0            (veto if ctx.touch_is_stale==True)
      DATA_VETO_FLAGS="stale_l2,time_quarantine,..." (matches ctx.data_quality_flags)
    """
    enabled: bool
    require_epoch_ts: bool
    atr_stale_max_ms: int
    strict_missing_atr_ts: bool
    strict_touch_fresh: bool
    veto_flags: set[str]

    @classmethod
    def from_env(cls) -> HardDataQualityGate:
        return cls(
            enabled=_env_bool("DATA_HARD_GATE_ENABLED", False),
            require_epoch_ts=_env_bool("DATA_REQUIRE_EPOCH_TS", False),
            atr_stale_max_ms=int(_env_float("DATA_ATR_STALE_MAX_MS", 180000.0)),
            strict_missing_atr_ts=_env_bool("DATA_STRICT_MISSING_ATR_TS", False),
            strict_touch_fresh=_env_bool("DATA_STRICT_TOUCH_FRESH", False),
            veto_flags=_parse_csv_set(_cached_getenv("DATA_VETO_FLAGS", "") or ""),
        )

    def _is_enabled_for(self, symbol: str) -> bool:
        """Per-symbol override: DATA_HARD_GATE_ENABLED__BTCUSDT takes priority over global."""
        sym = _norm_symbol(symbol)
        per_sym_key = f"DATA_HARD_GATE_ENABLED__{sym}"
        per_sym_val = _cached_getenv(per_sym_key)
        if per_sym_val is not None:
            return per_sym_val.strip().lower() in {"1", "true", "yes", "on"}
        return self.enabled

    def evaluate(self, *, ctx: Any, symbol: str, kind: str) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_epoch_ms(ctx) or 0
        
        def _make_res(decision: str, reason: str, notes: dict[str, Any] = None) -> GateDecisionV1:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="dq_integrity", gate="HardDataQualityGate", decision=decision,
                reason_code=reason, severity="CRITICAL" if decision == "DENY" else "INFO",
                profile="hard", fail_policy="CLOSED", ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash="",
                notes=(notes or {})
            )

        if not self._is_enabled_for(symbol):
            return _make_res("ABSTAIN", "OK", {"msg": "disabled"})

        # 1) Epoch timestamp sanity
        if self.require_epoch_ts:
            if ts_ev_ms < 946684800000:
                return _make_res("DENY", "VETO_BAD_TS_NOT_EPOCH", {"ts_ms": ts_ev_ms})

        # 2) ATR staleness
        now_ms = ts_ev_ms
        of = getattr(ctx, "of", None)
        atr_ts = (
            getattr(ctx, "atr_ts_ms", None)
            or (getattr(of, "atr_ts_ms", None) if of is not None else None)
            or (getattr(of, "atr_updated_ms", None) if of is not None else None)
        )
        if atr_ts is None:
            if self.strict_missing_atr_ts:
                return _make_res("DENY", "VETO_ATR_TS_MISSING")
        else:
            try: age = int(now_ms) - int(atr_ts)
            except Exception: age = 0
            if now_ms and age > int(self.atr_stale_max_ms):
                return _make_res("DENY", "VETO_ATR_STALE", {"age_ms": age})

        # 3) Touch snapshot staleness
        if self.strict_touch_fresh:
            if bool(getattr(ctx, "touch_is_stale", True)):
                return _make_res("DENY", "VETO_TOUCH_STALE")

        # 4) Quality flags veto
        if self.veto_flags:
            flags = getattr(ctx, "data_quality_flags", None)
            if isinstance(flags, list):
                for f in flags:
                    if isinstance(f, str) and f.strip().lower() in self.veto_flags:
                        return _make_res("DENY", "VETO_QUALITY_FLAG", {"flag": f})

        # Phase 2.3A: horizon-aware shadow
        _emit_shadow = _cached_getenv("ATR_HORIZON_DQ_SHADOW_ENABLE", "1") == "1"
        if _emit_shadow:
            try:
                shadow = compute_horizon_dq_shadow(ctx)
                ctx.dq_horizon_shadow = shadow
                canary = should_enforce_horizon_gate(
                    symbol=str(getattr(ctx, "symbol", "") or ""),
                    sid=str(getattr(ctx, "sid", "") or getattr(ctx, "signal_id", "") or ""),
                    regime=str(getattr(ctx, "regime", "") or ""),
                    scenario=str(getattr(ctx, "scenario", "") or ""),
                )
                if bool(canary.get("should_enforce", False)) and not bool(shadow.get("allow_shadow", True)):
                    return _make_res("DENY", shadow.get("shadow_reason_code") or "DQ_HZ_DENY", {"canary": True})
            except Exception:
                pass

        return _make_res("ALLOW", "OK")


@dataclass
class RegimeSessionGate:
    """
    Symbol×kind×regime gating with liquidity thresholds.

    ENV resolution uses double-underscore tokens (matches your style):
      RS_GATE_ENABLED=1

      RS_DENY__BTCUSDT__breakout__range=1
      RS_ALLOW_ONLY_REGIMES__BTCUSDT__breakout="trend,expansion"

      RS_SPREAD_MAX_BPS__BTCUSDT__breakout__range=8
      RS_DEPTH_MIN__BTCUSDT__breakout__range=0
      RS_BURST_FLIP_MAX__BTCUSDT__breakout__range=0.65

    Fallback chain (first found wins):
      KEY__SYM__KIND__REGIME
      KEY__SYM__KIND
      KEY__SYM
      KEY__KIND__REGIME
      KEY__KIND
      KEY__REGIME
      KEY_DEFAULT
    """
    enabled: bool
    spread_max_bps_default: float
    depth_min_default: float
    burst_flip_max_default: float

    @classmethod
    def from_env(cls) -> RegimeSessionGate:
        return cls(
            enabled=_env_bool("RS_GATE_ENABLED", False),
            spread_max_bps_default=_env_float("RS_SPREAD_MAX_BPS_DEFAULT", 0.0),  # 0 => disabled by default
            depth_min_default=_env_float("RS_DEPTH_MIN_DEFAULT", 0.0),
            burst_flip_max_default=_env_float("RS_BURST_FLIP_MAX_DEFAULT", 0.0),
        )

    def _pick_float(self, key: str, sym: str, kind: str, regime: str, default: float) -> float:
        cand = [
            f"{key}__{sym}__{kind}__{regime}",
            f"{key}__{sym}__{kind}",
            f"{key}__{sym}",
            f"{key}__{kind}__{regime}",
            f"{key}__{kind}",
            f"{key}__{regime}",
            f"{key}_DEFAULT",
        ]
        for name in cand:
            if _cached_getenv(name) is None:
                continue
            return _env_float(name, default)
        return default

    def _pick_bool(self, key: str, sym: str, kind: str, regime: str) -> bool | None:
        # FIX #4: support both sym-specific and global (no-sym) deny rules.
        # Lookup order: SYM+KIND+REGIME first, then KIND+REGIME (global).
        for name in (
            f"{key}__{sym}__{kind}__{regime}",   # per-symbol (highest priority)
            f"{key}__{kind}__{regime}",           # global kind+regime
        ):
            if _cached_getenv(name) is not None:
                return _env_bool(name, False)
        return None

    def evaluate(self, *, ctx: Any, symbol: str, kind: str) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_epoch_ms(ctx) or 0
        
        def _make_res(decision: str, reason: str, notes: dict[str, Any] = None) -> GateDecisionV1:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="regime_session", gate="RegimeSessionGate", decision=decision,
                reason_code=reason, severity="WARN" if decision == "DENY" else "INFO",
                profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash="",
                notes=(notes or {})
            )

        if not self.enabled:
            return _make_res("ABSTAIN", "OK", {"msg": "disabled"})

        sym = _norm_symbol(symbol)
        kind_l = (kind or "").strip().lower()
        regime = _get_regime(ctx)

        from contexts import MARKET_REGIME_NA
        if regime == MARKET_REGIME_NA:
            strict_req = str(_cached_getenv("RS_STRICT_REGIME", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
            if strict_req:
                return _make_res("DENY", "VETO_RS_UNKNOWN_REGIME", {"regime": regime})

        deny = self._pick_bool("RS_DENY", sym, kind_l, regime)
        if deny is True:
            return _make_res("DENY", "VETO_RS_DENY_RULE", {"rule": f"{sym}/{kind_l}/{regime}"})

        allow_env = _cached_getenv(f"RS_ALLOW_ONLY_REGIMES__{sym}__{kind_l}", "") or ""
        if allow_env.strip():
            allowed = _parse_csv_set(allow_env)
            if regime not in allowed:
                return _make_res("DENY", "VETO_RS_REGIME_NOT_ALLOWED", {"regime": regime, "allowed": sorted(allowed)})

        of = getattr(ctx, "of", None)
        spread_bps = _safe_float(getattr(ctx, "spread_bps", None), 0.0)
        if of is not None:
            spread_bps = max(spread_bps, _safe_float(getattr(of, "spread_bps", None), 0.0))
        
        depth_bid_5 = _safe_float(getattr(ctx, "depth_bid_5", None), 0.0)
        depth_ask_5 = _safe_float(getattr(ctx, "depth_ask_5", None), 0.0)
        burst_flip_ratio = _safe_float(getattr(ctx, "burst_flip_ratio", None), 0.0)
        if of is not None:
            depth_bid_5 = max(depth_bid_5, _safe_float(getattr(of, "depth_bid_5", None), 0.0))
            depth_ask_5 = max(depth_ask_5, _safe_float(getattr(of, "depth_ask_5", None), 0.0))
            burst_flip_ratio = max(burst_flip_ratio, _safe_float(getattr(of, "burst_flip_ratio", None), 0.0))

        rs_drift_tighten = (_cached_getenv("RS_DRIFT_TIGHTEN", "1") or "").strip().lower() in {"1","true","yes","on"}
        drift_factor = 1.0
        drift_score = 0.0
        drift_feat = ""
        if rs_drift_tighten:
            try:
                redis_client = getattr(ctx, "redis", None) or getattr(ctx, "_redis", None)
                tsm = int(normalize_ts_ms(getattr(ctx, "ts_ms", None) or getattr(ctx, "ts", None) or 0))
                if tsm > 0:
                    sess = str(getattr(ctx, "session", None) or session_from_ts_ms(tsm) or "na")
                    tfv = str(getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "na")
                    ven = str(getattr(ctx, "venue", None) or "na")
                    drift_factor, drift_score, drift_feat = load_drift_active_factor(
                        redis_client, symbol=str(sym).upper(), venue=str(ven),
                        session=str(sess), tf=str(tfv), kind=str(kind_l),
                    )
                    if not math.isfinite(drift_factor) or drift_factor <= 0: drift_factor = 1.0
            except Exception: drift_factor = 1.0

        sp_max = self._pick_float("RS_SPREAD_MAX_BPS", sym, kind_l, regime, self.spread_max_bps_default)
        if sp_max > 0.0 and spread_bps > sp_max:
            return _make_res("DENY", "VETO_RS_SPREAD", {"spread_bps": spread_bps, "limit": sp_max})

        d_min = self._pick_float("RS_DEPTH_MIN", sym, kind_l, regime, self.depth_min_default)
        strict = strict_enabled()
        try: power = int(float(_cached_getenv("RS_DRIFT_POWER", "2" if strict else "1")))
        except Exception: power = 2 if strict else 1
        drift_mult = float(drift_factor) ** float(max(0, power))
        d_min_eff = float(d_min) * drift_mult
        if d_min_eff > 0.0 and min(depth_bid_5, depth_ask_5) < d_min_eff:
            return _make_res("DENY", "VETO_RS_DEPTH", {"min_depth": min(depth_bid_5, depth_ask_5), "limit": d_min_eff, "drift": drift_factor})

        d_min_20 = self._pick_float("RS_DEPTH20_MIN", sym, kind_l, regime, 0.0)
        if d_min_20 > 0.0:
            depth_bid_20 = _safe_float(getattr(ctx, "depth_bid_20", None), 0.0)
            depth_ask_20 = _safe_float(getattr(ctx, "depth_ask_20", None), 0.0)
            if of is not None:
                depth_bid_20 = max(depth_bid_20, _safe_float(getattr(of, "depth_bid_20", None), 0.0))
                depth_ask_20 = max(depth_ask_20, _safe_float(getattr(of, "depth_ask_20", None), 0.0))
            if depth_bid_20 > 0 and depth_ask_20 > 0:
                d_min_20_eff = float(d_min_20) * drift_mult
                if d_min_20_eff > 0.0 and min(depth_bid_20, depth_ask_20) < d_min_20_eff:
                    return _make_res("DENY", "VETO_RS_DEPTH20", {"min_depth20": min(depth_bid_20, depth_ask_20), "limit": d_min_20_eff, "drift": drift_factor})

        bf_max = self._pick_float("RS_BURST_FLIP_MAX", sym, kind_l, regime, self.burst_flip_max_default)
        if bf_max > 0.0 and burst_flip_ratio > bf_max:
            return _make_res("DENY", "VETO_RS_BURST_FLIP", {"burst_flip": burst_flip_ratio, "limit": bf_max})

        return _make_res("ALLOW", "OK")


@dataclass
class ConsistencyGate:
    """
    Simple logical rules to reject "feature disagreement" pseudo-signals.
    Controlled by ENV:
      CONSISTENCY_GATE_ENABLED=1

    Uses your existing knobs when possible:
      BREAKOUT_REQUIRE_OBI / BREAKOUT_REQUIRE_OBI20
      BREAKOUT_MIN_MICROPRICE_SHIFT_BPS
      BTC_DELTA_Z_THRESHOLD, ETH_DELTA_Z_THRESHOLD (fallback DELTA_Z_THRESHOLD)
      BTC_OBI_THRESHOLD, ETH_OBI_THRESHOLD (fallback OBI_THRESHOLD)
      CRYPTO_OBI_SPIKE_THR

    Absorption touch requirement is OPTIONAL (off by default):
      ABSORPTION_REQUIRE_TOUCH_REFILL=1
      ABSORPTION_TOUCH_REFILL_MIN_RHO=0.10
    """
    enabled: bool
    absorption_require_touch_refill: bool
    absorption_touch_refill_min_rho: float

    @classmethod
    def from_env(cls) -> ConsistencyGate:
        return cls(
            enabled=_env_bool("CONSISTENCY_GATE_ENABLED", False),
            absorption_require_touch_refill=_env_bool("ABSORPTION_REQUIRE_TOUCH_REFILL", False),
            absorption_touch_refill_min_rho=_env_float("ABSORPTION_TOUCH_REFILL_MIN_RHO", 0.0),
        )

    def _base(self, symbol: str) -> str:
        s = _norm_symbol(symbol)
        if s.endswith("USDT") and len(s) > 4:
            return s[:-4]
        return s

    def _z_thr(self, symbol: str) -> float:
        base = self._base(symbol)
        # Prefer BTC_DELTA_Z_THRESHOLD etc
        if _cached_getenv(f"{base}_DELTA_Z_THRESHOLD") is not None:
            return _env_float(f"{base}_DELTA_Z_THRESHOLD", 2.0)
        return _env_float("DELTA_Z_THRESHOLD", 2.0)

    def _obi_thr(self, symbol: str) -> float:
        base = self._base(symbol)
        if _cached_getenv(f"{base}_OBI_THRESHOLD") is not None:
            return _env_float(f"{base}_OBI_THRESHOLD", 0.35)
        return _env_float("OBI_THRESHOLD", 0.35)

    def evaluate(self, *, ctx: Any, symbol: str, kind: str, side: str) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_epoch_ms(ctx) or 0
        
        def _make_res(decision: str, reason: str, notes: dict[str, Any] = None) -> GateDecisionV1:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="consistency", gate="ConsistencyGate", decision=decision,
                reason_code=reason, severity="WARN" if decision == "DENY" else "INFO",
                profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash="",
                notes=(notes or {})
            )

        if not self.enabled:
            return _make_res("ABSTAIN", "OK", {"msg": "disabled"})

        kind_l = (kind or "").strip().lower()
        of = getattr(ctx, "of", None)
        z = _safe_float(getattr(of, "z_delta", None), 0.0) if of is not None else _safe_float(getattr(ctx, "z_delta", None), 0.0)
        obi = _safe_float(getattr(of, "obi", None), 0.0) if of is not None else _safe_float(getattr(ctx, "obi", None), 0.0)
        obi_20 = _safe_float(getattr(of, "obi_20", None), 0.0) if of is not None else _safe_float(getattr(ctx, "obi_20", None), 0.0)
        mps = _safe_float(getattr(of, "microprice_shift_bps_20", None), 0.0) if of is not None else _safe_float(getattr(ctx, "microprice_shift_bps_20", None), 0.0)
        weak_progress = bool(getattr(of, "weak_progress", False)) if of is not None else bool(getattr(ctx, "weak_progress", False))

        z_thr = self._z_thr(symbol)
        obi_thr = self._obi_thr(symbol)

        if kind_l == "breakout":
            if z < z_thr: return _make_res("DENY", "VETO_BREAKOUT_Z_LOW", {"z": z, "thr": z_thr})
            if _env_bool("BREAKOUT_REQUIRE_OBI", False) and obi < obi_thr: return _make_res("DENY", "VETO_BREAKOUT_OBI_LOW", {"obi": obi, "thr": obi_thr})
            if _env_bool("BREAKOUT_REQUIRE_OBI20", False) and obi_20 < obi_thr: return _make_res("DENY", "VETO_BREAKOUT_OBI20_LOW", {"obi20": obi_20, "thr": obi_thr})
            mps_min = _env_float("BREAKOUT_MIN_MICROPRICE_SHIFT_BPS", 0.0)
            if mps_min > 0.0 and mps < mps_min: return _make_res("DENY", "VETO_BREAKOUT_MICROSHIFT_LOW", {"mps": mps, "thr": mps_min})
            return _make_res("ALLOW", "OK")

        if kind_l == "extreme":
            ex_thr = _env_float("EXTREME_Z_THRESHOLD", max(3.0, z_thr * 1.5))
            if z < ex_thr: return _make_res("DENY", "VETO_EXTREME_Z_LOW", {"z": z, "thr": ex_thr})
            ex_c2t_max = _env_float("EXTREME_L3_MAX_CANCEL_TO_TRADE", 1e9)
            if ex_c2t_max < 1e9:
                s = (side or "").strip().upper()
                c2t_field = "cancel_to_trade_ask" if s == "LONG" else ("cancel_to_trade_bid" if s == "SHORT" else "")
                if c2t_field:
                    c2t = _safe_float(getattr(of, c2t_field, None), 0.0) if of is not None else _safe_float(getattr(ctx, c2t_field, None), 0.0)
                    if c2t > ex_c2t_max: return _make_res("DENY", "VETO_EXTREME_CANCEL_TO_TRADE_HIGH", {"c2t": c2t, "limit": ex_c2t_max})
            return _make_res("ALLOW", "OK")

        if kind_l == "obi_spike":
            thr = _env_float("CRYPTO_OBI_SPIKE_THR", 0.7)
            obi_avg = _safe_float(getattr(of, "obi_avg", None), 0.0) if of is not None else _safe_float(getattr(ctx, "obi_avg", None), 0.0)
            if abs(obi_avg) < thr: return _make_res("DENY", "VETO_OBI_SPIKE_WEAK", {"obi_avg": obi_avg, "thr": thr})
            req_sustained = _env_bool("CONS_OBI_SPIKE_REQUIRE_SUSTAINED", _env_bool("OBI_SPIKE_REQUIRE_SUSTAINED", True))
            if req_sustained:
                sustained = bool(getattr(of, "obi_sustained", False)) if of is not None else bool(getattr(ctx, "obi_sustained", False))
                if not sustained: return _make_res("DENY", "VETO_OBI_SPIKE_NOT_SUSTAINED")
            return _make_res("ALLOW", "OK")

        if kind_l == "absorption":
            if z < z_thr: return _make_res("DENY", "VETO_ABS_Z_LOW", {"z": z, "thr": z_thr})
            if not weak_progress: return _make_res("DENY", "VETO_ABS_WEAK_PROGRESS_FALSE")
            if self.absorption_require_touch_refill:
                s = (side or "").strip().upper()
                want_ask = (s == "SHORT")
                tag = str(getattr(ctx, "touch_ask_tag" if want_ask else "touch_bid_tag", "none") or "none").lower()
                rho = _safe_float(getattr(ctx, "touch_ask_rho" if want_ask else "touch_bid_rho", None), 0.0)
                if bool(getattr(ctx, "touch_is_stale", True)): return _make_res("DENY", "VETO_ABS_TOUCH_STALE")
                if tag != "refill": return _make_res("DENY", "VETO_ABS_NO_REFILL_TAG", {"tag": tag})
                if self.absorption_touch_refill_min_rho > 0.0 and rho < self.absorption_touch_refill_min_rho:
                    return _make_res("DENY", "VETO_ABS_REFILL_RHO_LOW", {"rho": rho, "thr": self.absorption_touch_refill_min_rho})
            return _make_res("ALLOW", "OK")

        return _make_res("ALLOW", "OK", {"msg": "unknown_kind_fail_open"})


def _b2s(x: Any) -> str:
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="ignore")
    return str(x if x is not None else "")


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return d
        return v
    except Exception:
        return d


class SmtCoherenceGate:
    """
    Reads bundle state from Redis:
      key: smt:bundle:v1:{bundle_id}
      fields: leader, leader_dir (UP/DOWN), leader_confirm (0/1), coh (0..1), ts_ms

    Modes:
      - observe: NEVER veto; only write audit fields to ctx
      - veto: veto ONLY countertrend signals against confirmed leader when coh is high

    IMPORTANT:
      - fail-open: missing/invalid bundle state -> never veto
      - no protocol breaks: audit uses dynamic setattr() on ctx
    """
    def __init__(
        self,
        *,
        enabled: bool,
        mode: str,
        bundle_id: str,
        coh_min: float,
        state_stale_ms: int,
        diag_stream: str,
        diag_enabled: bool,
        diag_maxlen: int,
    ) -> None:
        self.enabled = bool(enabled)
        self.mode = (mode or "observe").strip().lower()
        if self.mode not in ("observe", "veto"):
            self.mode = "observe"
        self.bundle_id = (bundle_id or "").strip()
        self.coh_min = float(coh_min)
        self.state_stale_ms = int(max(0, state_stale_ms))
        self.diag_stream = (diag_stream or "")
        self.diag_enabled = bool(diag_enabled)
        self.diag_maxlen = int(max(1000, diag_maxlen))

    @classmethod
    def from_env(cls) -> SmtCoherenceGate:
        def _i(name: str, d: int) -> int:
            try:
                return int(float(_cached_getenv(name, str(d))))
            except Exception:
                return d
        def _f(name: str, d: float) -> float:
            try:
                return float(_cached_getenv(name, str(d)))
            except Exception:
                return d
        enabled = _cached_getenv("SMT_GATE_ENABLED", "1").strip() not in ("0", "false", "no", "off")
        return cls(
            enabled=enabled,
            mode=(_cached_getenv("SMT_LEADER_MODE", "observe") or "observe"),
            bundle_id=(_cached_getenv("SMT_COH_BUNDLE", "") or "").strip(),
            coh_min=_f("SMT_COH_MIN", 0.65),
            state_stale_ms=_i("SMT_STATE_STALE_MS", 5_000),
            diag_stream=str(_cached_getenv("SMT_DIAG_STREAM", "") or ""),
            diag_enabled=_cached_getenv("SMT_DIAG_ENABLED", "0").strip() in ("1", "true", "yes", "on"),
            diag_maxlen=_i("SMT_DIAG_MAXLEN", 20000),
        )

    def _read_state(self, redis_client: Any) -> dict[str, str] | None:
        if not self.bundle_id:
            return None
        key = f"smt:bundle:v1:{self.bundle_id}"
        try:
            d = redis_client.hgetall(key) or {}
        except Exception:
            return None
        dd: dict[str, str] = {}
        try:
            for k, v in dict(d).items():
                dd[_b2s(k)] = _b2s(v)
        except Exception:
            return None
        if not dd:
            return None
        return dd

    def _diag(self, redis_client: Any, *, fields: dict[str, str]) -> None:
        if not self.diag_enabled or not self.diag_stream:
            return
        try:
            redis_client.xadd(
                self.diag_stream,
                fields=fields,
                maxlen=self.diag_maxlen,
                approximate=True,
            )
        except Exception:
            return

    def evaluate(self, *, ctx: Any, redis_client: Any, symbol: str, kind: str, side: str) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_epoch_ms(ctx) or 0
        
        def _make_res(decision: str, reason: str, notes: dict[str, Any] = None) -> GateDecisionV1:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="smt", gate="SmtCoherenceGate", decision=decision,
                reason_code=reason, severity="WARN" if decision == "DENY" else "INFO",
                profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash="",
                notes=(notes or {})
            )

        if not self.enabled:
            return _make_res("ABSTAIN", "OK", {"msg": "disabled"})

        st = self._read_state(redis_client)
        now = int(math.floor(get_ny_time_millis()))
        leader, leader_dir, leader_confirm, coh, st_ts, stale = "", "", 0, 0.0, 0, True

        if st is not None:
            leader, leader_dir = st.get("leader", ""), st.get("leader_dir", "")
            leader_confirm = int(_safe_float(st.get("leader_confirm") or 0.0, 0.0))
            coh = float(_safe_float(st.get("coh") or 0.0, 0.0))
            st_ts = int(_safe_float(st.get("ts_ms") or 0.0, 0.0))
            if st_ts > 0: stale = (abs(now - st_ts) > self.state_stale_ms) if self.state_stale_ms > 0 else False

        # audit into ctx
        sig_ud = (side or "").upper()
        if sig_ud not in ("LONG", "SHORT"): sig_ud = "NA"
        lead_ud = "LONG" if str(leader_dir).upper() == "UP" else "SHORT" if str(leader_dir).upper() == "DOWN" else "NA"
        countertrend = (sig_ud in ("LONG", "SHORT")) and (sig_ud != lead_ud)
        
        try:
            ctx.smt_leader_dir = leader_dir
            ctx.smt_coh = float(coh)
            ctx.smt_state_stale = bool(stale)
        except Exception: pass

        if st is None or stale or not leader_dir:
            return _make_res("ALLOW", "OK", {"msg": "no_state_or_stale"})

        if self.mode == "observe":
            return _make_res("ALLOW", "OK", {"msg": "observe_only", "countertrend": countertrend})

        if countertrend and int(leader_confirm) == 1 and float(coh) >= float(self.coh_min):
            return _make_res("DENY", "VETO_SMT_COUNTERTREND", {"leader_dir": leader_dir, "coh": coh})

        return _make_res("ALLOW", "OK")


@dataclass
class AtrFloorGate:
    """
    Veto signals if volatility (ATR in BPS) is below a floor mapped by regime.
    
    ENV:
      ATR_FLOOR_GATE_ENABLED=1
      ATR_FLOOR_BPS_T0=5.0
      ATR_FLOOR_BPS_T1=10.0
      ATR_FLOOR_BPS_T2=15.0
      ATR_FLOOR_FAIL_OPEN=1
    """
    enabled: bool
    t0_bps: float
    t1_bps: float
    t2_bps: float
    fail_open: bool

    @classmethod
    def from_env(cls) -> AtrFloorGate:
        return cls(
            enabled=_env_bool("ATR_FLOOR_GATE_ENABLED", False),
            t0_bps=_env_float("ATR_FLOOR_BPS_T0", 5.0),
            t1_bps=_env_float("ATR_FLOOR_BPS_T1", 10.0),
            t2_bps=_env_float("ATR_FLOOR_BPS_T2", 15.0),
            fail_open=_env_bool("ATR_FLOOR_FAIL_OPEN", True),
        )

    def evaluate(self, *, ctx: Any, symbol: str, kind: str) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_epoch_ms(ctx) or 0
        
        def _make_res(decision: str, reason: str, notes: dict[str, Any] = None) -> GateDecisionV1:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="atr_floor", gate="AtrFloorGate", decision=decision,
                reason_code=reason, severity="WARN" if decision == "DENY" else "INFO",
                profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash="",
                notes=(notes or {})
            )

        if not self.enabled:
            return _make_res("ABSTAIN", "OK", {"msg": "disabled"})

        indicators = getattr(ctx, "indicators", {})
        atr_bps = indicators.get("atr_bps") or indicators.get("atr_bps_exec")
        if atr_bps is None:
            if self.fail_open: return _make_res("ALLOW", "OK", {"msg": "atr_missing_fail_open"})
            return _make_res("DENY", "VETO_ATR_MISSING")

        regime = _get_regime(ctx)
        cfg = getattr(ctx, "config", {})
        tier, rg, threshold = compute_atr_bps_threshold(
            regime=regime, cfg=cfg, t0=self.t0_bps, t1=self.t1_bps, t2=self.t2_bps
        )

        if float(atr_bps) < float(threshold):
            return _make_res("DENY", "VETO_ATR_FLOOR", {"atr": atr_bps, "thr": threshold, "tier": tier, "regime": rg})

        return _make_res("ALLOW", "OK")

@dataclass
class BreadthGate:
    """
    Veto signals during low-breadth/divergent market regimes.
    
    ENV:
      BREADTH_GATE_ENABLED=1
      BREADTH_MIN_RET_24H=...
      BREADTH_MIN_VOL_Z=...
      BREADTH_REQUIRE_LEADER_CONFIRM=1
    """
    mode: str
    canary_share: float
    min_ret_24h: float
    min_vol_z: float
    require_leader_confirm: bool

    @classmethod
    def from_env(cls) -> BreadthGate:
        import os
        mode_env = os.getenv("BREADTH_GATE_MODE", "").strip().lower()
        if not mode_env:
            enabled = _env_bool("BREADTH_GATE_ENABLED", False)
            mode = "enforce" if enabled else "off"
        else:
            mode = mode_env

        return cls(
            mode=mode,
            canary_share=max(0.0, min(1.0, _env_float("BREADTH_GATE_CANARY_SHARE", 0.0))),
            min_ret_24h=_env_float("BREADTH_MIN_RET_24H", -100.0),
            min_vol_z=_env_float("BREADTH_MIN_VOL_Z", -50.0),
            require_leader_confirm=_env_bool("BREADTH_REQUIRE_LEADER_CONFIRM", False),
        )

    def evaluate(self, *, ctx: Any, symbol: str, kind: str, side: str) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_epoch_ms(ctx) or 0
        
        def _make_res(decision: str, reason: str, notes: dict[str, Any] = None) -> GateDecisionV1:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="breadth", gate="BreadthGate", decision=decision,
                reason_code=reason, severity="WARN" if decision == "DENY" else "INFO",
                profile=self.mode, fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash="",
                notes=(notes or {})
            )

        if self.mode == "off":
            return _make_res("ABSTAIN", "OK", {"msg": "mode_off"})

        def _get_val(k: str) -> float:
            if hasattr(ctx, "indicators"): return _safe_float(ctx.indicators.get(k, 0.0), 0.0)
            if isinstance(ctx, dict): return _safe_float(ctx.get(k, 0.0), 0.0)
            return _safe_float(getattr(ctx, k, None), 0.0)

        ret_24h = _get_val("market_breadth_ret_24h")
        vol_z = _get_val("market_breadth_volume_z")
        leader_confirm = _get_val("leader_btc_eth_confirm")
        veto_reason = None
        notes = {}

        sig_side = (side or "").upper()
        if sig_side == "LONG":
            if ret_24h < self.min_ret_24h:
                veto_reason, notes = "VETO_BREADTH_RET_LOW", {"ret": ret_24h, "min": self.min_ret_24h}
            elif self.require_leader_confirm and leader_confirm < 0:
                veto_reason, notes = "VETO_BREADTH_LEADER_DIVERGENCE", {"confirm": leader_confirm}
        elif sig_side == "SHORT":
            if ret_24h > -self.min_ret_24h:
                veto_reason, notes = "VETO_BREADTH_RET_HIGH", {"ret": ret_24h, "max": -self.min_ret_24h}
            elif self.require_leader_confirm and leader_confirm > 0:
                veto_reason, notes = "VETO_BREADTH_LEADER_DIVERGENCE", {"confirm": leader_confirm}

        if not veto_reason and vol_z < self.min_vol_z:
            veto_reason, notes = "VETO_BREADTH_VOL_LOW", {"vol_z": vol_z, "min": self.min_vol_z}

        if veto_reason:
            if self.mode == "enforce":
                return _make_res("DENY", veto_reason, notes)
            elif self.mode == "canary":
                sticky_key = f"{symbol}|{kind}|{side}|{str(getattr(ctx, 'signal_id', ''))}"
                import hashlib
                h = hashlib.sha1(sticky_key.encode("utf-8")).hexdigest()
                u = (int(h[:8], 16) % 10_000) / 10_000.0
                if u < self.canary_share: return _make_res("DENY", veto_reason, {**notes, "canary": True})
                return _make_res("ALLOW", veto_reason.replace("VETO_", "SHADOW_VETO_"), {**notes, "canary_shadow": True})
            return _make_res("ALLOW", veto_reason.replace("VETO_", "SHADOW_VETO_"), {**notes, "shadow": True})

        return _make_res("ALLOW", "OK")
