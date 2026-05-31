"""
EntryPolicyGate: spread shock / burst flip / cancel-to-trade + feature drift alarm.

Goals:
  - avoid cutting too many signals by default (soft mode is audit/tighten, not veto)
  - allow switching to hard mode from docker-compose via env

Modes:
  GATE_PROFILE=default|soft|strict|hard
    - default/soft: never veto by entry-policy alone; only annotate ctx and optionally tighten
    - strict: may veto on extreme spread shock (configurable)
    - hard: veto on threshold breach

Feature drift alarm:
  - tracks baseline distributions for a few features (EMA mean/absdev)
  - if drift spikes: either (a) tighten by annotating ctx, or (b) veto in hard profile
  - designed fail-open: never breaks signal publishing if Redis is down
"""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from domain.time_utils import normalize_ts_ms, session_from_ts_ms


@dataclass(frozen=True)
class GateDecision:
    apply: bool
    veto: bool
    reason_code: str
    notes: str = ""


def _env_bool(name: str, default: bool) -> bool:
    try:
        v = os.getenv(name, "")
        if v == "":
            return bool(default)
        return v.strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return bool(default)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _hset_compat(redis_client: Any, key: str, mapping: Dict[str, Any]) -> None:
    """
    Compatibility wrapper:
      - real redis-py: hset(name, mapping={...})
      - FakeRedis in tests: may accept hset(key, mapping) or hset(key, **kwargs)
    """
    if redis_client is None:
        return
    try:
        redis_client.hset(key, mapping=mapping)
        return
    except TypeError:
        pass
    try:
        redis_client.hset(key, mapping)
        return
    except Exception:
        return


def _expire_compat(redis_client: Any, key: str, ttl_s: int) -> None:
    if redis_client is None:
        return
    try:
        fn = getattr(redis_client, "expire", None)
        if callable(fn):
            fn(key, int(ttl_s))
    except Exception:
        pass


def _b2s(x: Any) -> str:
    """Decode bytes to str, else str(x)."""
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="ignore")
    return str(x)


def _spread_bps_from_ctx(ctx: Any) -> float:
    # Prefer already computed ctx.spread_bps; fallback to bid/ask
    sp = _safe_float(getattr(ctx, "spread_bps", None), 0.0)
    if sp > 0:
        return float(sp)
    bid = _safe_float(getattr(ctx, "bid", None) or getattr(ctx, "b", None), 0.0)
    ask = _safe_float(getattr(ctx, "ask", None) or getattr(ctx, "a", None), 0.0)
    mid = _safe_float(getattr(ctx, "mid", None) or getattr(ctx, "price", None), 0.0)
    if mid > 0 and ask > 0 and bid > 0 and ask >= bid:
        return float((ask - bid) / mid * 10_000.0)
    return 0.0


class _FeatureDriftTracker:
    """
    Minimal drift tracker:
      - per feature stores EMA mean (mu) and EMA abs deviation (mad)
      - score = abs(x - mu) / max(eps, mad)
    Fail-open: if Redis missing/down => returns (0.0, {})
    """
    def __init__(self, redis_client: Any) -> None:
        self.redis = redis_client

    def update_and_score(self, *, key: str, x: float, alpha: float, eps: float, now_ms: int) -> Tuple[float, Dict[str, float]]:
        if self.redis is None:
            return 0.0, {}
        try:
            d = self.redis.hgetall(key) or {}
        except Exception:
            return 0.0, {}

        dd: Dict[str, str] = {}
        try:
            for k, v in dict(d).items():
                dd[_b2s(k)] = _b2s(v)
        except Exception:
            dd = {}

        try:
            n = int(float(dd.get("n") or 0))
            mu = float(dd.get("mu") or 0.0)
            mad = float(dd.get("mad") or 0.0)
        except Exception:
            n, mu, mad = 0, 0.0, 0.0

        if n <= 0:
            mu = float(x)
            mad = float(max(eps, abs(x)))
            n = 1
        else:
            mu = (1.0 - alpha) * mu + alpha * float(x)
            mad = (1.0 - alpha) * mad + alpha * float(abs(float(x) - mu))
            n += 1

        denom = max(float(eps), float(mad))
        z = float(abs(float(x) - float(mu)) / denom)

        try:
            _hset_compat(self.redis, key, {"n": n, "mu": mu, "mad": mad, "last_ts_ms": int(now_ms)})
            _expire_compat(self.redis, key, int(float(os.getenv("FEATURE_DRIFT_TTL_S", "86400"))))
        except Exception:
            pass

        return z, {"n": float(n), "mu": float(mu), "mad": float(mad)}


class EntryPolicyGate:
    @staticmethod
    def from_env() -> "EntryPolicyGate":
        return EntryPolicyGate()

    def __init__(self) -> None:
        # Toggle
        self.enabled = _env_bool("ENTRY_POLICY_ENABLED", True)

        # Conservative defaults to avoid "cutting too many signals".
        self.spread_shock_bps = _safe_float(os.getenv("ENTRY_SPREAD_SHOCK_BPS", "35"), 35.0)
        self.spread_shock_bps_hard = _safe_float(os.getenv("ENTRY_SPREAD_SHOCK_BPS_HARD", "60"), 60.0)
        self.burst_flip_max = _safe_float(os.getenv("ENTRY_BURST_FLIP_MAX", "0.85"), 0.85)
        self.c2t_max = _safe_float(os.getenv("ENTRY_C2T_MAX", "8.0"), 8.0)

        # Feature drift (off by default)
        self.drift_enabled = _env_bool("FEATURE_DRIFT_ENABLED", False)
        self.drift_z = _safe_float(os.getenv("FEATURE_DRIFT_Z", "6.0"), 6.0)
        self.drift_alpha = _safe_float(os.getenv("FEATURE_DRIFT_ALPHA", "0.02"), 0.02)
        self.drift_eps = _safe_float(os.getenv("FEATURE_DRIFT_EPS", "1e-6"), 1e-6)

        # Optional diagnostics stream (audit)
        self.diag_stream = str(os.getenv("ENTRY_POLICY_DIAG_STREAM", "") or "")

    def evaluate(self, *, ctx: Any, symbol: str, kind: str) -> GateDecision:
        if not self.enabled:
            return GateDecision(False, False, "OK", "disabled")

        profile = (os.getenv("GATE_PROFILE", "") or "").strip().lower()
        if profile in {"", "normal"}:
            profile = "default"

        # Strict timestamp normalization (single source of truth)
        ts_raw = getattr(ctx, "ts_ms", None) or getattr(ctx, "ts", None) or 0
        tsm = int(normalize_ts_ms(ts_raw))
        sess = "na"
        if tsm > 0:
            try:
                sess = str(getattr(ctx, "session", None) or session_from_ts_ms(tsm) or "na")
            except Exception:
                sess = "na"

        spread_bps = _spread_bps_from_ctx(ctx)
        burst_flip = _safe_float(
            getattr(ctx, "burst_flip_ratio", None)
            or getattr(ctx, "burst_flip", None)
            or getattr(ctx, "flip_ratio", None),
            0.0,
        )
        c2t = _safe_float(
            getattr(ctx, "cancel_to_trade", None)
            or getattr(ctx, "cancel_to_trade_ratio", None)
            or getattr(ctx, "c2t_ratio", None),
            0.0,
        )

        soft_flags = []
        if spread_bps > 0 and spread_bps >= self.spread_shock_bps:
            soft_flags.append(f"spread_shock={spread_bps:.1f}bps")
        if burst_flip > 0 and burst_flip >= self.burst_flip_max:
            soft_flags.append(f"burst_flip={burst_flip:.3f}")
        if c2t > 0 and c2t >= self.c2t_max:
            soft_flags.append(f"c2t={c2t:.3f}")

        # Feature drift alarm (optional; fail-open)
        drift_hit = False
        drift_notes = ""
        if self.drift_enabled:
            try:
                redis_client = getattr(ctx, "redis", None)
                tr = _FeatureDriftTracker(redis_client)
                now_ms = int(time.time() * 1000)

                venue = str(getattr(ctx, "venue", None) or "na").lower()
                tf = str(getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "na").lower()
                knd = str(kind or "na").lower()
                dims = f"{str(symbol).upper()}:{venue}:{sess}:{tf}:{knd}"

                # mentioned: obi, z_delta, spread_bps, depth_*
                obi = _safe_float(getattr(ctx, "obi", None) or getattr(ctx, "obi_total", None), 0.0)
                z_delta = _safe_float(getattr(ctx, "z_delta", None) or getattr(ctx, "delta_z", None), 0.0)
                depth_bid_5 = _safe_float(getattr(ctx, "depth_bid_5", None), 0.0)
                depth_ask_5 = _safe_float(getattr(ctx, "depth_ask_5", None), 0.0)
                depth_bid_20 = _safe_float(getattr(ctx, "depth_bid_20", None), 0.0)
                depth_ask_20 = _safe_float(getattr(ctx, "depth_ask_20", None), 0.0)
                dmin = min([x for x in (depth_bid_5, depth_ask_5, depth_bid_20, depth_ask_20) if x > 0] or [0.0])

                z_sp, _ = tr.update_and_score(key=f"drift:spread_bps:{dims}", x=float(spread_bps), alpha=self.drift_alpha, eps=max(1e-3, self.drift_eps), now_ms=now_ms)
                z_obi, _ = tr.update_and_score(key=f"drift:obi:{dims}", x=float(obi), alpha=self.drift_alpha, eps=max(1e-6, self.drift_eps), now_ms=now_ms)
                z_zd, _ = tr.update_and_score(key=f"drift:z_delta:{dims}", x=float(z_delta), alpha=self.drift_alpha, eps=max(1e-6, self.drift_eps), now_ms=now_ms)
                z_dep, _ = tr.update_and_score(key=f"drift:depth_min:{dims}", x=float(dmin), alpha=self.drift_alpha, eps=max(1e-6, self.drift_eps), now_ms=now_ms)

                zmax = max(z_sp, z_obi, z_zd, z_dep)
                if zmax >= float(self.drift_z):
                    drift_hit = True
                    drift_notes = f"zmax={zmax:.2f} (spread={z_sp:.2f} obi={z_obi:.2f} z={z_zd:.2f} depth={z_dep:.2f})"
            except Exception:
                drift_hit = False

        # Annotate ctx for downstream tightening (EdgeCostGate multiplies K)
        try:
            if soft_flags:
                setattr(ctx, "entry_policy_flags", list(soft_flags))
                # Mild in default/soft; stronger in strict/hard.
                setattr(ctx, "entry_policy_tighten_k", 1.10 if profile in {"default", "soft"} else 1.25)
            if drift_hit:
                setattr(ctx, "feature_drift_alarm", 1)
                setattr(ctx, "feature_drift_notes", drift_notes[:256])
                setattr(ctx, "feature_drift_tighten_k", 1.15 if profile in {"default", "soft"} else 1.35)
        except Exception:
            pass

        # Optional audit stream (never affects decision)
        if self.diag_stream:
            try:
                redis_client = getattr(ctx, "redis", None)
                if redis_client is not None and (soft_flags or drift_hit):
                    ev = {
                        "ts_ms": int(time.time() * 1000),
                        "symbol": str(symbol),
                        "kind": str(kind),
                        "session": str(sess),
                        "spread_bps": float(spread_bps),
                        "burst_flip_ratio": float(burst_flip),
                        "cancel_to_trade": float(c2t),
                        "soft_flags": soft_flags,
                        "drift": int(drift_hit),
                        "drift_notes": drift_notes[:256],
                        "profile": profile,
                    }
                    redis_client.xadd(self.diag_stream, {"data": json.dumps(ev, ensure_ascii=False)})
            except Exception:
                pass

        # Decision policy:
        #   default/soft: do not veto (не режем поток)
        #   strict: veto only on extreme spread shock
        #   hard: veto on policy flags and/or drift
        if profile in {"default", "soft"}:
            return GateDecision(True, False, "OK", "audit_only")

        if spread_bps > 0 and spread_bps >= self.spread_shock_bps_hard:
            return GateDecision(True, True, "VETO_SPREAD_SHOCK", f"spread_bps={spread_bps:.1f}")

        if profile == "hard":
            if soft_flags:
                return GateDecision(True, True, "VETO_ENTRY_POLICY", ";".join(soft_flags)[:256])
            if drift_hit:
                return GateDecision(True, True, "VETO_FEATURE_DRIFT", drift_notes[:256])

        return GateDecision(True, False, "OK", "pass")


def write_entry_policy_diag(redis_client: Any, *, stream: str, maxlen: int, event: Dict[str, Any]) -> None:
    """
    Standalone diagnostic helper for out-of-band entry policy logging.
    Used by CryptoOrderFlowHandler to log veto/delay decisions to a dedicated stream.
    """
    if not stream or redis_client is None:
        return
    try:
        # standard outbox JSON packing: {"data": "<json>"}
        payload = {"data": json.dumps(event, ensure_ascii=False, separators=(",", ":"))}
        redis_client.xadd(stream, payload, maxlen=int(maxlen), approximate=True)
    except Exception:
        pass
