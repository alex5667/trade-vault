from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque
from typing import Any, Deque, Dict, Optional
import math
import os
import statistics


def _f(x: Any, default: float = 0.0) -> float:
    """Fail-open float parsing used on hot path."""
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def _i(x: Any, d: int = 0) -> int:
    """Fail-open int conversion."""
    try:
        return int(x)
    except Exception:
        return d


def _b(x: Any, default: bool = False) -> bool:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return x
        s = str(x).strip().lower()
        if s in ("1", "true", "yes", "on"):
            return True
        if s in ("0", "false", "no", "off"):
            return False
        return default
    except Exception:
        return default


def _ema(prev: float, x: float, alpha: float) -> float:
    """Classic EMA with sane init. If prev is missing/invalid -> set to x."""
    if not math.isfinite(x):
        return prev
    if prev <= 0.0 or (not math.isfinite(prev)):
        return x
    return prev + alpha * (x - prev)


def _robust_z(x: float, hist: Deque[float], eps: float = 1e-9) -> float:
    """
    Robust z-score via median/MAD. Cheap enough for small windows.
    Caller should pass hist WITHOUT current x (we do that in check()).
    """
    n = len(hist)
    if n < 5:
        return 0.0
    med = statistics.median(hist)
    abs_dev = [abs(v - med) for v in hist]
    mad = statistics.median(abs_dev)
    # 1.4826 converts MAD to sigma-equivalent for normal distribution
    sigma = 1.4826 * mad
    return (x - med) / max(eps, sigma)


@dataclass
class CancelSpikeParams:
    """
    Gate params.
    Default values come from ENV but can be overridden per-symbol via cfg2.
    """
    enable: bool = True
    mode: str = "veto"  # "monitor" | "veto"

    # Baseline tracking (slow EMA)
    alpha_slow: float = 0.02

    # Spike criteria
    ratio_th: float = 3.0
    abs_th: float = 0.0
    min_baseline: float = 0.0

    # Optional robust stats
    use_robust_z: bool = True
    window: int = 120
    min_samples: int = 30
    z_th: float = 3.5

    # "Pull without aggression" (anti fake-impulse)
    min_taker_rate: float = 0.0

    @staticmethod
    def from_env() -> "CancelSpikeParams":
        def g(name: str, d: Any) -> Any:
            v = os.getenv(name)
            return d if v is None else v

        p = CancelSpikeParams()
        p.enable = _b(g("OF_CANCEL_SPIKE_ENABLE", "1"), True)
        p.mode = str(g("OF_CANCEL_SPIKE_MODE", p.mode))
        p.alpha_slow = _f(g("OF_CANCEL_SPIKE_ALPHA_SLOW", p.alpha_slow), p.alpha_slow)
        p.ratio_th = _f(g("OF_CANCEL_SPIKE_RATIO_TH", p.ratio_th), p.ratio_th)
        p.abs_th = _f(g("OF_CANCEL_SPIKE_ABS_TH", p.abs_th), p.abs_th)
        p.min_baseline = _f(g("OF_CANCEL_SPIKE_MIN_BASELINE", p.min_baseline), p.min_baseline)
        p.use_robust_z = _b(g("OF_CANCEL_SPIKE_USE_ROBUST_Z", "1"), True)
        p.window = int(_f(g("OF_CANCEL_SPIKE_WINDOW", p.window), p.window))
        p.min_samples = int(_f(g("OF_CANCEL_SPIKE_MIN_SAMPLES", p.min_samples), p.min_samples))
        p.z_th = _f(g("OF_CANCEL_SPIKE_Z_TH", p.z_th), p.z_th)
        p.min_taker_rate = _f(g("OF_CANCEL_SPIKE_MIN_TAKER_RATE", p.min_taker_rate), p.min_taker_rate)
        return p

    def merged_with_cfg(self, cfg: Dict[str, Any]) -> "CancelSpikeParams":
        """
        Per-symbol overrides from cfg2 (dynamic cfg already merged by caller).
        """
        out = CancelSpikeParams(**self.__dict__)
        try:
            if "cancel_spike_enable" in cfg:
                out.enable = bool(int(cfg.get("cancel_spike_enable", 1)))
        except Exception:
            pass
        try:
            if "cancel_spike_mode" in cfg:
                out.mode = str(cfg.get("cancel_spike_mode", out.mode))
        except Exception:
            pass
        # floats/ints fail-open
        out.alpha_slow = _f(cfg.get("cancel_spike_alpha_slow", out.alpha_slow), out.alpha_slow)
        out.ratio_th = _f(cfg.get("cancel_spike_ratio_th", out.ratio_th), out.ratio_th)
        out.abs_th = _f(cfg.get("cancel_spike_abs_th", out.abs_th), out.abs_th)
        out.min_baseline = _f(cfg.get("cancel_spike_min_baseline", out.min_baseline), out.min_baseline)
        out.use_robust_z = _b(cfg.get("cancel_spike_use_robust_z", out.use_robust_z), out.use_robust_z)
        out.window = int(_f(cfg.get("cancel_spike_window", out.window), out.window))
        out.min_samples = int(_f(cfg.get("cancel_spike_min_samples", out.min_samples), out.min_samples))
        out.z_th = _f(cfg.get("cancel_spike_z_th", out.z_th), out.z_th)
        out.min_taker_rate = _f(cfg.get("cancel_spike_min_taker_rate", out.min_taker_rate), out.min_taker_rate)
        # normalize mode
        m = out.mode.strip().lower()
        out.mode = "veto" if m == "veto" else "monitor"
        # clamp window to avoid pathological memory use
        out.window = max(10, min(5000, out.window))
        return out


@dataclass(kw_only=True)
class GateDecision:
    allow: bool
    reason: str
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class _SymState:
    base_bid: float = 0.0
    base_ask: float = 0.0
    hist_bid: Deque[float] = field(default_factory=lambda: deque(maxlen=120))
    hist_ask: Deque[float] = field(default_factory=lambda: deque(maxlen=120))
    last_bucket_id: Optional[int] = None


class CancellationSpikeGate:
    """
    Cancellation Spike gate (L3-lite).

    Design principles:
    - Fail-open: never throws, never blocks if data missing.
    - Deterministic per bucket_id: avoid double processing of the same bucket.
    - Warmup-aware: no veto until baseline and enough samples exist.
    """

    def __init__(self, params: Optional[CancelSpikeParams] = None):
        self._defaults = params or CancelSpikeParams.from_env()
        self._st: Dict[str, _SymState] = {}

    def check(
        self,
        *,
        symbol: str,
        direction: str,
        cancel_bid_rate_ema: float,
        cancel_ask_rate_ema: float,
        taker_buy_rate_ema: float,
        taker_sell_rate_ema: float,
        bucket_id: Optional[int],
        cfg2: Optional[Dict[str, Any]] = None,
    ) -> GateDecision:
        """
        Returns GateDecision:
          allow: False only if mode=veto and spike criteria matched.
          reason: compact code used in OFConfirmV3.reason and metrics tags.
          meta: compact diagnostics for evidence/metrics.
        """
        try:
            p = self._defaults.merged_with_cfg(cfg2 or {})
            if not p.enable:
                return GateDecision(True, "cancel_spike_disabled", {"mode": p.mode})

            d = str(direction).upper()
            if d not in ("LONG", "SHORT"):
                # Unknown direction => do not gate
                return GateDecision(True, "cancel_spike_no_direction", {"mode": p.mode})

            st = self._st.get(symbol)
            if st is None:
                st = _SymState()
                st.hist_bid = deque(maxlen=p.window)
                st.hist_ask = deque(maxlen=p.window)
                self._st[symbol] = st
            else:
                # if window changed dynamically, rebuild deques with new maxlen
                if st.hist_bid.maxlen != p.window:
                    st.hist_bid = deque(list(st.hist_bid), maxlen=p.window)
                if st.hist_ask.maxlen != p.window:
                    st.hist_ask = deque(list(st.hist_ask), maxlen=p.window)

            # bucket monotonicity (determinism / no double-count)
            if bucket_id is not None:
                try:
                    b = int(bucket_id)
                    if st.last_bucket_id is not None and b <= st.last_bucket_id:
                        return GateDecision(True, "cancel_spike_duplicate_bucket", {"bucket_id": b, "mode": p.mode})
                    st.last_bucket_id = b
                except Exception:
                    pass

            bid = max(0.0, _f(cancel_bid_rate_ema, 0.0))
            ask = max(0.0, _f(cancel_ask_rate_ema, 0.0))
            tkr_buy = max(0.0, _f(taker_buy_rate_ema, 0.0))
            tkr_sell = max(0.0, _f(taker_sell_rate_ema, 0.0))

            # Support side = cancellations that remove liquidity supporting our intended direction
            support_side = "bid" if d == "LONG" else "ask"
            opp_side = "ask" if support_side == "bid" else "bid"
            support = bid if support_side == "bid" else ask
            opp = ask if opp_side == "ask" else bid
            base_support = st.base_bid if support_side == "bid" else st.base_ask
            base_opp = st.base_ask if opp_side == "ask" else st.base_bid

            ratio_support = support / max(1e-9, base_support) if base_support > 0.0 else 0.0
            ratio_opp = opp / max(1e-9, base_opp) if base_opp > 0.0 else 0.0

            # robust z vs history (excluding current)
            z_support = 0.0
            z_opp = 0.0
            if p.use_robust_z:
                hs = st.hist_bid if support_side == "bid" else st.hist_ask
                ho = st.hist_ask if opp_side == "ask" else st.hist_bid
                z_support = _robust_z(support, hs)
                z_opp = _robust_z(opp, ho)

            # readiness (warmup)
            hs = st.hist_bid if support_side == "bid" else st.hist_ask
            ready = (len(hs) >= p.min_samples) and (base_support >= p.min_baseline)

            def _is_spike(x: float, base: float, ratio: float, z: float, n_hist: int) -> bool:
                if x < p.abs_th:
                    return False
                if base < p.min_baseline:
                    return False
                if ratio >= p.ratio_th:
                    return True
                if p.use_robust_z and n_hist >= p.min_samples and abs(z) >= p.z_th:
                    return True
                return False

            spike_support = _is_spike(support, base_support, ratio_support, z_support, len(hs))
            ho = st.hist_ask if opp_side == "ask" else st.hist_bid
            spike_opp = _is_spike(opp, base_opp, ratio_opp, z_opp, len(ho))

            # “Pull without aggression”:
            # If opposite-side cancellations spike but the matching taker aggression is low,
            # the move can be a "vacuum" / spoof-like impulse (less reliable).
            dir_taker = tkr_buy if d == "LONG" else tkr_sell
            veto_pull_wo_aggr = bool(spike_opp and (dir_taker < p.min_taker_rate))

            allow = True
            reason = "cancel_spike_ok"
            veto_kind = "none"

            if ready:
                if spike_support:
                    allow = False
                    veto_kind = "support_pulled"
                    # LONG -> bids pulled; SHORT -> asks pulled
                    reason = "cancel_spike_" + support_side + "_support_pulled"
                elif veto_pull_wo_aggr:
                    allow = False
                    veto_kind = "pull_without_aggr"
                    # LONG: asks pulled but taker buy low; SHORT: bids pulled but taker sell low
                    reason = "cancel_spike_" + opp_side + "_pull_without_aggr"
            else:
                # warmup: never veto
                allow = True
                reason = "cancel_spike_warmup"

            # monitor mode never blocks
            if p.mode != "veto":
                allow = True
                reason = "cancel_spike_monitor_" + reason

            meta = {
                "mode": p.mode,
                "ready": int(ready),
                "direction": d,
                "support_side": support_side,
                "support": float(support),
                "base_support": float(base_support),
                "ratio_support": float(ratio_support),
                "z_support": float(z_support),
                "opp_side": opp_side,
                "opp": float(opp),
                "base_opp": float(base_opp),
                "ratio_opp": float(ratio_opp),
                "z_opp": float(z_opp),
                "taker_buy_rate_ema": float(tkr_buy),
                "taker_sell_rate_ema": float(tkr_sell),
                "dir_taker": float(dir_taker),
                "veto_kind": veto_kind,
            }

            # --- state update AFTER decision (important) ---
            st.base_bid = _ema(st.base_bid, bid, p.alpha_slow)
            st.base_ask = _ema(st.base_ask, ask, p.alpha_slow)
            st.hist_bid.append(bid)
            st.hist_ask.append(ask)

            return GateDecision(allow=allow, reason=reason, meta=meta)
        except Exception:
            # hard fail-open
            return GateDecision(True, "cancel_spike_error_fail_open", {})

    # ------------------------------------------------------------------
    # Deterministic replay helpers
    # ------------------------------------------------------------------

    def export_state(self, *, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Export gate state as JSON-serializable dict (fail-open)."""
        try:
            if symbol is not None:
                st = self._st.get(str(symbol))
                if st is None:
                    return {"version": 1, "symbols": {}}
                return {
                    "version": 1,
                    "symbols": {
                        str(symbol): {
                            "base_bid": float(st.base_bid),
                            "base_ask": float(st.base_ask),
                            "hist_bid": list(st.hist_bid),
                            "hist_ask": list(st.hist_ask),
                            "last_bucket_id": st.last_bucket_id,
                            "hist_maxlen": int(st.hist_bid.maxlen or 0),
                        }
                    },
                }
            # all symbols
            symbols: Dict[str, Any] = {}
            for sym, st in (self._st or {}).items():
                try:
                    symbols[str(sym)] = {
                        "base_bid": float(st.base_bid),
                        "base_ask": float(st.base_ask),
                        "hist_bid": list(st.hist_bid),
                        "hist_ask": list(st.hist_ask),
                        "last_bucket_id": st.last_bucket_id,
                        "hist_maxlen": int(st.hist_bid.maxlen or 0),
                    }
                except Exception:
                    continue
            return {"version": 1, "symbols": symbols}
        except Exception:
            return {"version": 1, "symbols": {}}

    def import_state(self, state: Dict[str, Any], *, replace: bool = False) -> None:
        """Restore state previously exported by export_state() (fail-open)."""
        try:
            if not isinstance(state, dict):
                return

            symbols = state.get("symbols", {}) or {}
            if not isinstance(symbols, dict):
                return

            if replace:
                self._st = {}

            for sym, o in symbols.items():
                if not isinstance(o, dict):
                    continue

                st = _SymState()
                st.base_bid = _f(o.get("base_bid", 0.0), 0.0)
                st.base_ask = _f(o.get("base_ask", 0.0), 0.0)
                maxlen = int(_f(o.get("hist_maxlen", 0), 0)) or 120
                maxlen = max(10, min(5000, maxlen))
                st.hist_bid = deque([_f(x, 0.0) for x in (o.get("hist_bid", []) or [])], maxlen=maxlen)
                st.hist_ask = deque([_f(x, 0.0) for x in (o.get("hist_ask", []) or [])], maxlen=maxlen)
                try:
                    v = o.get("last_bucket_id", None)
                    st.last_bucket_id = None if v is None else int(v)
                except Exception:
                    st.last_bucket_id = None

                self._st[str(sym)] = st

        except Exception:
            return

    def reset_symbol_state(self, symbol: str) -> None:
        try:
            self._st.pop(str(symbol), None)
        except Exception:
            return

    # ------------------------------------------------------------------
    # Snapshot/restore API (compatible with diff, wraps export_state/import_state)
    # ------------------------------------------------------------------

    def snapshot(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Serialize state (compatible with diff API).

        Shape:
          {"version": 1, "symbols": {"BTCUSDT": {...}, ...}}
        or if symbol specified:
          {...}  (symbol payload)
        """
        try:
            if symbol:
                sym = str(symbol).upper()
                st = self._st.get(sym)
                if not st:
                    return {"symbol": sym, "present": False}
                return {
                    "symbol": sym,
                    "present": True,
                    "last_bucket_id": st.last_bucket_id,
                    "n_samples": len(st.hist_bid),  # approximate sample count (both sides should be same length)
                    "bid": {
                        "baseline_ema": float(st.base_bid),
                        "hist": list(st.hist_bid),
                        "hist_maxlen": int(st.hist_bid.maxlen or 0),
                    },
                    "ask": {
                        "baseline_ema": float(st.base_ask),
                        "hist": list(st.hist_ask),
                        "hist_maxlen": int(st.hist_ask.maxlen or 0),
                    },
                }
            # full snapshot (compatible with export_state format)
            return self.export_state()
        except Exception:
            return {"version": 1, "symbols": {}}

    def restore(self, snap: Dict[str, Any], symbol: Optional[str] = None) -> None:
        """Restore state from snapshot (compatible with diff API)."""
        try:
            if not isinstance(snap, dict):
                return

            if symbol:
                payload = snap
                # allow passing the full container snapshot too
                if "symbols" in snap and isinstance(snap.get("symbols"), dict):
                    payload = snap["symbols"].get(str(symbol).upper(), {})
                self._restore_one(payload)
                return

            # full snapshot - try import_state first (backward compat)
            if "symbols" in snap:
                self.import_state(snap, replace=False)
                return

            # maybe a single-symbol payload
            self._restore_one(snap)
        except Exception:
            return

    def _restore_one(self, payload: Dict[str, Any]) -> None:
        """Restore one symbol from payload (internal helper)."""
        if not isinstance(payload, dict):
            return
        sym = str(payload.get("symbol", "") or "").upper()
        if not sym:
            return
        if payload.get("present") is False:
            self._st.pop(sym, None)
            return

        st = _SymState()
        st.last_bucket_id = payload.get("last_bucket_id", None)
        try:
            if st.last_bucket_id is not None:
                st.last_bucket_id = int(st.last_bucket_id)
        except Exception:
            st.last_bucket_id = None

        # Map to internal structure (base_bid/base_ask instead of bid/ask)
        # Support both formats: new (bid/ask with baseline_ema) and legacy (base_bid/base_ask)
        if "bid" in payload and isinstance(payload.get("bid"), dict):
            bid_data = payload["bid"]
            st.base_bid = _f(bid_data.get("baseline_ema", 0.0), 0.0)
            hist_bid = bid_data.get("hist", [])
            maxlen_bid = _i(bid_data.get("hist_maxlen", 0), 0) or 120
        elif "base_bid" in payload:
            # Legacy format from export_state
            st.base_bid = _f(payload.get("base_bid", 0.0), 0.0)
            hist_bid = payload.get("hist_bid", [])
            maxlen_bid = _i(payload.get("hist_maxlen", 0), 0) or 120
        else:
            st.base_bid = 0.0
            hist_bid = []
            maxlen_bid = 120

        if "ask" in payload and isinstance(payload.get("ask"), dict):
            ask_data = payload["ask"]
            st.base_ask = _f(ask_data.get("baseline_ema", 0.0), 0.0)
            hist_ask = ask_data.get("hist", [])
            maxlen_ask = _i(ask_data.get("hist_maxlen", 0), 0) or 120
        elif "base_ask" in payload:
            # Legacy format from export_state
            st.base_ask = _f(payload.get("base_ask", 0.0), 0.0)
            hist_ask = payload.get("hist_ask", [])
            maxlen_ask = _i(payload.get("hist_maxlen", 0), 0) or 120
        else:
            st.base_ask = 0.0
            hist_ask = []
            maxlen_ask = 120

        maxlen_bid = max(10, min(5000, maxlen_bid))
        maxlen_ask = max(10, min(5000, maxlen_ask))

        if not isinstance(hist_bid, list):
            hist_bid = []
        if not isinstance(hist_ask, list):
            hist_ask = []

        st.hist_bid = deque((float(x) for x in hist_bid[-maxlen_bid:]), maxlen=maxlen_bid)
        st.hist_ask = deque((float(x) for x in hist_ask[-maxlen_ask:]), maxlen=maxlen_ask)

        self._st[sym] = st

    def reset(self, symbol: Optional[str] = None) -> None:
        """Clear state (per symbol or all)."""
        try:
            if symbol:
                self._st.pop(str(symbol).upper(), None)
            else:
                self._st.clear()
        except Exception:
            return

    # ------------------------------------------------------------------
    # Diff-compatible API (snapshot_state/restore_state)
    # ------------------------------------------------------------------
    def snapshot_state(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Snapshot state for golden replay (diff-compatible API).
        
        Args:
            symbol: Optional symbol to snapshot. If None, snapshots all symbols.
            
        Returns:
            Dict with format: {"ver": 1, "symbols": {"SYMBOL": {"last_bucket": int, "long": {...}, "short": {...}}, ...}}
            For single symbol, returns the same format but only for that symbol.
        """
        try:
            out: Dict[str, Any] = {"ver": 1, "symbols": {}}
            items = [(symbol, self._st.get(symbol))] if symbol else list(self._st.items())
            for sym, st in items:
                if not sym or not isinstance(st, _SymState):
                    continue
                # For compatibility with diff format, we need to track per-direction stats
                # Since current implementation doesn't track per-direction, we use a simplified format
                out_st: Dict[str, Any] = {"last_bucket": int(st.last_bucket_id) if st.last_bucket_id is not None else -1}

                # Create Welford-like stats for ratio tracking (simplified)
                # We'll use bid side as proxy for LONG, ask side for SHORT
                # Since we don't track ratios directly, we approximate from baseline
                # Format: {"n": int, "mean": float, "m2": float}
                w_long = {
                    "n": int(len(st.hist_bid)),
                    "mean": float(st.base_bid),
                    "m2": 0.0,  # Not tracked in current implementation
                }
                w_short = {
                    "n": int(len(st.hist_ask)),
                    "mean": float(st.base_ask),
                    "m2": 0.0,  # Not tracked in current implementation
                }

                out_st["long"] = {"w_ratio": w_long}
                out_st["short"] = {"w_ratio": w_short}

                out["symbols"][str(sym)] = out_st
            return out
        except Exception:
            return {"ver": 1, "symbols": {}}

    def restore_state(self, snapshot: Dict[str, Any]) -> None:
        """Restore state from snapshot (diff-compatible API).
        
        Args:
            snapshot: Dict with format from snapshot_state().
        """
        if not isinstance(snapshot, dict):
            return
        symbols = snapshot.get("symbols", None)
        if not isinstance(symbols, dict):
            return
        for sym, st in symbols.items():
            if not isinstance(sym, str) or not isinstance(st, dict):
                continue
            last_bucket = int(st.get("last_bucket", -1) or -1)

            # Restore per-direction stats
            sym_st = self._st.get(sym)
            if sym_st is None:
                sym_st = _SymState()
                self._st[sym] = sym_st

            sym_st.last_bucket_id = last_bucket if last_bucket >= 0 else None

            for side in ("long", "short"):
                ss = st.get(side, None)
                if not isinstance(ss, dict):
                    continue
                wj = ss.get("w_ratio", None)
                if isinstance(wj, dict):
                    # Restore Welford stats (simplified - we don't track m2 in current impl)
                    n = int(wj.get("n", 0) or 0)
                    mean = float(wj.get("mean", 0.0) or 0.0)
                    # Map to internal state: long -> bid, short -> ask
                    if side == "long":
                        sym_st.base_bid = mean
                        # Initialize history with mean value (simplified restoration)
                        sym_st.hist_bid = deque([mean] * min(n, 120), maxlen=120)
                    else:
                        sym_st.base_ask = mean
                        sym_st.hist_ask = deque([mean] * min(n, 120), maxlen=120)
        # Legacy compatibility: also support old format
        if "symbol" in snapshot and "state" in snapshot:
            # Old format: restore single symbol
            sym = str(snapshot.get("symbol", ""))
            if sym and not snapshot.get("empty", False):
                d = snapshot.get("state", {})
                if isinstance(d, dict):
                    st = _SymState()
                    st.last_bucket_id = _i(d.get("last_bucket_id", -1), -1)
                    if st.last_bucket_id == -1:
                        st.last_bucket_id = None
                    mean_val = _f(d.get("mean", 0.0), 0.0)
                    st.base_bid = mean_val
                    st.base_ask = mean_val
                    st.hist_bid = deque(maxlen=120)
                    st.hist_ask = deque(maxlen=120)
                    self._st[sym] = st
