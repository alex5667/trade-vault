from __future__ import annotations

import os
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set

from domain.time_utils import normalize_ts_ms, session_from_ts_ms
from domain.gate_profile import strict_enabled
from handlers.crypto_orderflow.utils.drift_reader import load_drift_active_factor


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _norm_symbol(sym: Any) -> str:
    return str(sym or "").strip().upper().replace("/", "").replace("-", "")


def _parse_csv_set(v: str) -> Set[str]:
    out: Set[str] = set()
    for x in (v or "").split(","):
        s = x.strip().lower()
        if s:
            out.add(s)
    return out


def _get_regime(ctx: Any) -> str:
    # Prefer ctx.regime (SignalContext) then ctx.of.regime (OrderflowContext)
    r = getattr(ctx, "regime", None)
    if isinstance(r, str) and r.strip():
        return r.strip().lower()
    of = getattr(ctx, "of", None)
    r2 = getattr(of, "regime", None) if of is not None else None
    if isinstance(r2, str) and r2.strip():
        return r2.strip().lower()
    return "unknown"


def _get_epoch_ms(ctx: Any) -> Optional[int]:
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




@dataclass(frozen=True)
class GateDecision:
    apply: bool
    veto: bool
    reason_code: str
    notes: str = ""


@dataclass
class HardDataQualityGate:
    """
    Hard veto on *data quality* issues that strongly correlate with bad fills and churn.

    Controlled by ENV (all optional; defaults are fail-open unless enabled):
      DATA_HARD_GATE_ENABLED=1/0
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
    veto_flags: Set[str]

    @classmethod
    def from_env(cls) -> "HardDataQualityGate":
        return cls(
            enabled=_env_bool("DATA_HARD_GATE_ENABLED", False),
            require_epoch_ts=_env_bool("DATA_REQUIRE_EPOCH_TS", False),
            atr_stale_max_ms=int(_env_float("DATA_ATR_STALE_MAX_MS", 180000.0)),
            strict_missing_atr_ts=_env_bool("DATA_STRICT_MISSING_ATR_TS", False),
            strict_touch_fresh=_env_bool("DATA_STRICT_TOUCH_FRESH", False),
            veto_flags=_parse_csv_set(os.getenv("DATA_VETO_FLAGS", "") or ""),
        )

    def evaluate(self, *, ctx: Any, symbol: str, kind: str) -> GateDecision:
        if not self.enabled:
            return GateDecision(apply=False, veto=False, reason_code="OK", notes="disabled")

        # 1) Epoch timestamp sanity (protect against "minutes_of_day" etc.)
        ts_ms = _get_epoch_ms(ctx)
        if self.require_epoch_ts:
            # 2000-01-01 epoch ms ~= 946684800000; anything below is very likely NOT epoch ms
            if ts_ms is None or ts_ms < 946684800000:
                return GateDecision(True, True, "VETO_BAD_TS_NOT_EPOCH", "require_epoch_ts")

        # 2) ATR staleness (needs ctx.of.atr_ts_ms)
        now_ms = ts_ms or 0
        of = getattr(ctx, "of", None)
        atr_ts = (
            getattr(ctx, "atr_ts_ms", None)
            or (getattr(of, "atr_ts_ms", None) if of is not None else None)
            or (getattr(of, "atr_updated_ms", None) if of is not None else None)
        )
        if atr_ts is None:
            if self.strict_missing_atr_ts:
                return GateDecision(True, True, "VETO_ATR_TS_MISSING", "strict_missing_atr_ts")
        else:
            try:
                age = int(now_ms) - int(atr_ts)
            except Exception:
                age = 0
            if now_ms and age > int(self.atr_stale_max_ms):
                return GateDecision(True, True, "VETO_ATR_STALE", f"age_ms={age}")

        # 3) Touch snapshot staleness (important for L2/L3 confirmation logic)
        if self.strict_touch_fresh:
            if bool(getattr(ctx, "touch_is_stale", True)):
                return GateDecision(True, True, "VETO_TOUCH_STALE", "strict_touch_fresh")

        # 4) Quality flags veto (pipeline-produced flags)
        if self.veto_flags:
            flags = getattr(ctx, "data_quality_flags", None)
            if isinstance(flags, list):
                for f in flags:
                    if isinstance(f, str) and f.strip().lower() in self.veto_flags:
                        return GateDecision(True, True, "VETO_QUALITY_FLAG", f"flag={f}")

        return GateDecision(True, False, "OK", "")


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
    def from_env(cls) -> "RegimeSessionGate":
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
            if os.getenv(name) is None:
                continue
            return _env_float(name, default)
        return float(default)

    def _pick_bool(self, key: str, sym: str, kind: str, regime: str) -> Optional[bool]:
        name = f"{key}__{sym}__{kind}__{regime}"
        if os.getenv(name) is None:
            return None
        return _env_bool(name, False)

    def evaluate(self, *, ctx: Any, symbol: str, kind: str) -> GateDecision:
        if not self.enabled:
            return GateDecision(False, False, "OK", "disabled")

        sym = _norm_symbol(symbol)
        kind_l = (kind or "").strip().lower()
        regime = _get_regime(ctx)

        # Hard deny by matrix rule
        deny = self._pick_bool("RS_DENY", sym, kind_l, regime)
        if deny is True:
            return GateDecision(True, True, "VETO_RS_DENY_RULE", f"{sym}/{kind_l}/{regime}")

        # Allow-only regimes per sym+kind
        allow_env = os.getenv(f"RS_ALLOW_ONLY_REGIMES__{sym}__{kind_l}", "") or ""
        if allow_env.strip():
            allowed = _parse_csv_set(allow_env)
            if regime not in allowed:
                return GateDecision(True, True, "VETO_RS_REGIME_NOT_ALLOWED", f"regime={regime} allowed={sorted(allowed)}")

        of = getattr(ctx, "of", None)
        spread_bps = _safe_float(getattr(ctx, "spread_bps", None), 0.0)
        if of is not None:
            spread_bps = max(spread_bps, _safe_float(getattr(of, "spread_bps", None), 0.0))
        # ------------------------------------------------------------------
        # STRICT: depth поля в вашем ctx гарантированы именно так:
        #   depth_bid_5, depth_ask_5, depth_bid_20, depth_ask_20
        # Никаких l2_depth_* нет — намеренно НЕ читаем их, чтобы не словить регресс.
        # ------------------------------------------------------------------
        depth_bid_5 = _safe_float(getattr(of, "depth_bid_5", None), 0.0) if of is not None else 0.0
        depth_ask_5 = _safe_float(getattr(of, "depth_ask_5", None), 0.0) if of is not None else 0.0
        burst_flip_ratio = _safe_float(getattr(of, "burst_flip_ratio", None), 0.0) if of is not None else 0.0

        # ------------------------------------------------------------------
        # NEW: drift-aware tightening
        # Если drift alarm активен -> повышаем требуемую глубину:
        #   d_min_eff = d_min * drift_factor
        # Fail-open: если redis/ts недоступны -> drift_factor=1.0 (поведение прежнее).
        # ------------------------------------------------------------------
        rs_drift_tighten = (os.getenv("RS_DRIFT_TIGHTEN", "1") or "").strip().lower() in {"1","true","yes","on"}
        drift_factor = 1.0
        drift_score = float("nan")
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
                        redis_client,
                        symbol=str(sym).upper(),
                        venue=str(ven),
                        session=str(sess),
                        tf=str(tfv),
                        kind=str(kind_l),
                    )
                    if not math.isfinite(drift_factor) or drift_factor <= 0:
                        drift_factor = 1.0
            except Exception:
                drift_factor = 1.0

        sp_max = self._pick_float("RS_SPREAD_MAX_BPS", sym, kind_l, regime, self.spread_max_bps_default)
        if sp_max > 0.0 and spread_bps > sp_max:
            return GateDecision(True, True, "VETO_RS_SPREAD", f"spread_bps={spread_bps:.2f} > {sp_max:.2f}")

        d_min = self._pick_float("RS_DEPTH_MIN", sym, kind_l, regime, self.depth_min_default)

        # Drift tightening power:
        #   - default profile: power=1  (умеренно)
        #   - strict profile : power=2  (очень агрессивно)
        # Можно вручную задать RS_DRIFT_POWER=1|2|...
        strict = strict_enabled()
        try:
            power = int(float(os.getenv("RS_DRIFT_POWER", "2" if strict else "1")))
        except Exception:
            power = 2 if strict else 1
        if power < 0:
            power = 0
        drift_mult = float(drift_factor) ** float(power)
        d_min_eff = float(d_min) * drift_mult
        if d_min_eff > 0.0 and min(depth_bid_5, depth_ask_5) < d_min_eff:
            note = f"min_depth={min(depth_bid_5, depth_ask_5):.3f} < {d_min_eff:.3f}"
            if float(drift_factor) > 1.0:
                note = f"{note} | drift x{float(drift_factor):.2f} score={float(drift_score):.1f} feat={drift_feat}"
            return GateDecision(True, True, "VETO_RS_DEPTH", note)

        # Depth20 check (separate from depth5)
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
                    note = f"min_depth20={min(depth_bid_20, depth_ask_20):.3f} < {d_min_20_eff:.3f}"
                    if float(drift_factor) > 1.0:
                        note = f"{note} | drift x{float(drift_factor):.2f} score={float(drift_score):.1f} feat={drift_feat}"
                    return GateDecision(True, True, "VETO_RS_DEPTH20", note)

        bf_max = self._pick_float("RS_BURST_FLIP_MAX", sym, kind_l, regime, self.burst_flip_max_default)
        if bf_max > 0.0 and burst_flip_ratio > bf_max:
            return GateDecision(True, True, "VETO_RS_BURST_FLIP", f"burst_flip={burst_flip_ratio:.3f} > {bf_max:.3f}")

        return GateDecision(True, False, "OK", "")


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
    def from_env(cls) -> "ConsistencyGate":
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
        if os.getenv(f"{base}_DELTA_Z_THRESHOLD") is not None:
            return _env_float(f"{base}_DELTA_Z_THRESHOLD", 2.0)
        return _env_float("DELTA_Z_THRESHOLD", 2.0)

    def _obi_thr(self, symbol: str) -> float:
        base = self._base(symbol)
        if os.getenv(f"{base}_OBI_THRESHOLD") is not None:
            return _env_float(f"{base}_OBI_THRESHOLD", 0.35)
        return _env_float("OBI_THRESHOLD", 0.35)

    def evaluate(self, *, ctx: Any, symbol: str, kind: str, side: str) -> GateDecision:
        if not self.enabled:
            return GateDecision(False, False, "OK", "disabled")

        kind_l = (kind or "").strip().lower()
        of = getattr(ctx, "of", None)
        z = _safe_float(getattr(of, "z_delta", None), 0.0) if of is not None else _safe_float(getattr(ctx, "z_delta", None), 0.0)
        obi = _safe_float(getattr(of, "obi", None), 0.0) if of is not None else _safe_float(getattr(ctx, "obi", None), 0.0)
        obi_20 = _safe_float(getattr(of, "obi_20", None), 0.0) if of is not None else _safe_float(getattr(ctx, "obi_20", None), 0.0)
        mps = _safe_float(getattr(of, "microprice_shift_bps_20", None), 0.0) if of is not None else _safe_float(getattr(ctx, "microprice_shift_bps_20", None), 0.0)
        weak_progress = bool(getattr(of, "weak_progress", False)) if of is not None else bool(getattr(ctx, "weak_progress", False))

        z_thr = self._z_thr(symbol)
        obi_thr = self._obi_thr(symbol)

        # Breakout: require delta z + optionally OBI agreement + microprice shift
        if kind_l == "breakout":
            if z < z_thr:
                return GateDecision(True, True, "VETO_BREAKOUT_Z_LOW", f"z={z:.3f} < {z_thr:.3f}")
            if _env_bool("BREAKOUT_REQUIRE_OBI", False) and obi < obi_thr:
                return GateDecision(True, True, "VETO_BREAKOUT_OBI_LOW", f"obi={obi:.3f} < {obi_thr:.3f}")
            if _env_bool("BREAKOUT_REQUIRE_OBI20", False) and obi_20 < obi_thr:
                return GateDecision(True, True, "VETO_BREAKOUT_OBI20_LOW", f"obi20={obi_20:.3f} < {obi_thr:.3f}")
            mps_min = _env_float("BREAKOUT_MIN_MICROPRICE_SHIFT_BPS", 0.0)
            if mps_min > 0.0 and mps < mps_min:
                return GateDecision(True, True, "VETO_BREAKOUT_MICROSHIFT_LOW", f"mps={mps:.3f} < {mps_min:.3f}")
            return GateDecision(True, False, "OK", "")

        # Extreme: require stronger z by env (otherwise it is often pure noise)
        if kind_l == "extreme":
            ex_thr = _env_float("EXTREME_Z_THRESHOLD", max(3.0, z_thr * 1.5))
            if z < ex_thr:
                return GateDecision(True, True, "VETO_EXTREME_Z_LOW", f"z={z:.3f} < {ex_thr:.3f}")
            return GateDecision(True, False, "OK", "")

        # OBI spike: require sustained skew to avoid single-bucket blips
        if kind_l == "obi_spike":
            thr = _env_float("CRYPTO_OBI_SPIKE_THR", 0.7)
            obi_avg = _safe_float(getattr(of, "obi_avg", None), 0.0) if of is not None else _safe_float(getattr(ctx, "obi_avg", None), 0.0)
            if abs(obi_avg) < thr:
                return GateDecision(True, True, "VETO_OBI_SPIKE_WEAK", f"obi_avg={obi_avg:.3f} < thr={thr:.3f}")
            if _env_bool("OBI_SPIKE_REQUIRE_SUSTAINED", True):
                sustained = bool(getattr(of, "obi_sustained", False)) if of is not None else bool(getattr(ctx, "obi_sustained", False))
                if not sustained:
                    return GateDecision(True, True, "VETO_OBI_SPIKE_NOT_SUSTAINED", "obi_sustained=False")
            return GateDecision(True, False, "OK", "")

        # Absorption: at minimum require weak_progress and sufficient z (optional touch refill requirement)
        if kind_l == "absorption":
            if z < z_thr:
                return GateDecision(True, True, "VETO_ABS_Z_LOW", f"z={z:.3f} < {z_thr:.3f}")
            if not weak_progress:
                return GateDecision(True, True, "VETO_ABS_WEAK_PROGRESS_FALSE", "weak_progress=False")
            if self.absorption_require_touch_refill:
                # Heuristic mapping (can be tuned):
                #   SHORT absorption at resistance -> ask refill expected
                #   LONG absorption at support    -> bid refill expected
                s = (side or "").strip().upper()
                want_ask = (s == "SHORT")
                tag = str(getattr(ctx, "touch_ask_tag" if want_ask else "touch_bid_tag", "none") or "none").lower()
                rho = _safe_float(getattr(ctx, "touch_ask_rho" if want_ask else "touch_bid_rho", None), 0.0)
                if bool(getattr(ctx, "touch_is_stale", True)):
                    return GateDecision(True, True, "VETO_ABS_TOUCH_STALE", "touch_is_stale=True")
                if tag != "refill":
                    return GateDecision(True, True, "VETO_ABS_NO_REFILL_TAG", f"tag={tag}")
                if self.absorption_touch_refill_min_rho > 0.0 and rho < self.absorption_touch_refill_min_rho:
                    return GateDecision(True, True, "VETO_ABS_REFILL_RHO_LOW", f"rho={rho:.3f} < {self.absorption_touch_refill_min_rho:.3f}")
            return GateDecision(True, False, "OK", "")

        # Unknown kind => fail-open
        return GateDecision(True, False, "OK", "unknown_kind_fail_open")


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
        self.diag_stream = str(diag_stream or "")
        self.diag_enabled = bool(diag_enabled)
        self.diag_maxlen = int(max(1000, diag_maxlen))

    @classmethod
    def from_env(cls) -> "SmtCoherenceGate":
        def _i(name: str, d: int) -> int:
            try:
                return int(float(os.getenv(name, str(d))))
            except Exception:
                return d
        def _f(name: str, d: float) -> float:
            try:
                return float(os.getenv(name, str(d)))
            except Exception:
                return d
        enabled = os.getenv("SMT_GATE_ENABLED", "1").strip() not in ("0", "false", "no", "off")
        return cls(
            enabled=enabled,
            mode=(os.getenv("SMT_LEADER_MODE", "observe") or "observe"),
            bundle_id=(os.getenv("SMT_COH_BUNDLE", "") or "").strip(),
            coh_min=_f("SMT_COH_MIN", 0.65),
            state_stale_ms=_i("SMT_STATE_STALE_MS", 5_000),
            diag_stream=str(os.getenv("SMT_DIAG_STREAM", "") or ""),
            diag_enabled=os.getenv("SMT_DIAG_ENABLED", "0").strip() in ("1", "true", "yes", "on"),
            diag_maxlen=_i("SMT_DIAG_MAXLEN", 20000),
        )

    def _read_state(self, redis_client: Any) -> Optional[Dict[str, str]]:
        if not self.bundle_id:
            return None
        key = f"smt:bundle:v1:{self.bundle_id}"
        try:
            d = redis_client.hgetall(key) or {}
        except Exception:
            return None
        dd: Dict[str, str] = {}
        try:
            for k, v in dict(d).items():
                dd[_b2s(k)] = _b2s(v)
        except Exception:
            return None
        if not dd:
            return None
        return dd

    def _diag(self, redis_client: Any, *, fields: Dict[str, str]) -> None:
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

    def evaluate(self, *, ctx: Any, redis_client: Any, symbol: str, kind: str, side: str) -> GateDecision:
        if not self.enabled:
            return GateDecision(True, False, "OK", "smt_gate_disabled")

        st = self._read_state(redis_client)
        now = int(math.floor(time.time() * 1000))

        leader = ""
        leader_dir = ""
        leader_confirm = 0
        coh = 0.0
        st_ts = 0
        stale = True

        if st is not None:
            leader = str(st.get("leader") or "")
            leader_dir = str(st.get("leader_dir") or "")
            leader_confirm = int(_safe_float(st.get("leader_confirm") or 0.0, 0.0))
            coh = float(_safe_float(st.get("coh") or 0.0, 0.0))
            st_ts = int(_safe_float(st.get("ts_ms") or 0.0, 0.0))
            if st_ts > 0:
                stale = (abs(now - st_ts) > self.state_stale_ms) if self.state_stale_ms > 0 else False

        # --- audit into ctx (never breaks protocol) ---
        try:
            setattr(ctx, "smt_mode", self.mode)
            setattr(ctx, "smt_bundle", self.bundle_id)
            setattr(ctx, "smt_leader", leader)
            setattr(ctx, "smt_leader_dir", leader_dir)
            setattr(ctx, "smt_leader_confirm", int(leader_confirm))
            setattr(ctx, "smt_coh", float(coh))
            setattr(ctx, "smt_state_ts_ms", int(st_ts))
            setattr(ctx, "smt_state_stale", bool(stale))
        except Exception:
            pass

        # fail-open: no state / stale / invalid => never veto
        if st is None or stale or not leader_dir:
            self._diag(redis_client, fields={
                "event": "SMT_GATE",
                "mode": self.mode,
                "bundle": self.bundle_id,
                "symbol": str(symbol),
                "kind": str(kind),
                "side": str(side),
                "veto": "0",
                "reason": "NO_STATE_OR_STALE",
                "coh": f"{coh:.6f}",
                "leader": leader,
                "leader_dir": leader_dir,
                "leader_confirm": str(int(leader_confirm)),
                "ts_ms": str(now),
            })
            return GateDecision(True, False, "OK", "no_state_or_stale")

        # Map signal side -> direction
        sig_dir = str(side or "").upper()
        if sig_dir not in ("LONG", "SHORT"):
            sig_dir = "NA"
        lead_dir = "LONG" if str(leader_dir).upper() == "UP" else "SHORT"
        countertrend = (sig_dir in ("LONG", "SHORT")) and (sig_dir != lead_dir)

        # observe mode never veto
        if self.mode == "observe":
            self._diag(redis_client, fields={
                "event": "SMT_GATE",
                "mode": self.mode,
                "bundle": self.bundle_id,
                "symbol": str(symbol),
                "kind": str(kind),
                "side": str(side),
                "veto": "0",
                "reason": "OBSERVE_ONLY",
                "coh": f"{coh:.6f}",
                "leader": leader,
                "leader_dir": leader_dir,
                "leader_confirm": str(int(leader_confirm)),
                "countertrend": "1" if countertrend else "0",
                "ts_ms": str(now),
            })
            return GateDecision(True, False, "OK", "observe_only")

        # veto mode: only strict condition
        if countertrend and int(leader_confirm) == 1 and float(coh) >= float(self.coh_min):
            try:
                setattr(ctx, "smt_veto", True)
                setattr(ctx, "smt_veto_reason", "COUNTERTREND_VS_CONFIRMED_LEADER")
            except Exception:
                pass
            self._diag(redis_client, fields={
                "event": "SMT_GATE",
                "mode": self.mode,
                "bundle": self.bundle_id,
                "symbol": str(symbol),
                "kind": str(kind),
                "side": str(side),
                "veto": "1",
                "reason": "VETO_COUNTERTREND",
                "coh": f"{coh:.6f}",
                "coh_min": f"{float(self.coh_min):.6f}",
                "leader": leader,
                "leader_dir": leader_dir,
                "leader_confirm": str(int(leader_confirm)),
                "ts_ms": str(now),
            })
            return GateDecision(True, True, "VETO_SMT_LEADER_CT", "countertrend_vs_confirmed_leader")

        self._diag(redis_client, fields={
            "event": "SMT_GATE",
            "mode": self.mode,
            "bundle": self.bundle_id,
            "symbol": str(symbol),
            "kind": str(kind),
            "side": str(side),
            "veto": "0",
            "reason": "PASS",
            "coh": f"{coh:.6f}",
            "leader": leader,
            "leader_dir": leader_dir,
            "leader_confirm": str(int(leader_confirm)),
            "countertrend": "1" if countertrend else "0",
            "ts_ms": str(now),
        })
        return GateDecision(True, False, "OK", "pass")
