from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from domain.time_utils import normalize_ts_ms, session_from_ts_ms
from domain.gate_profile import strict_enabled
from handlers.crypto_orderflow.utils.drift_reader import (
    drift_active_key_v1
    drift_active_key_v2
    drift_state_key_v1
    drift_state_key_v2
)


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if math.isfinite(v):
            return float(v)
    except Exception:
        pass
    return float(default)


def _canon_tf(x: Any) -> str:
    s = str(x or "").strip().lower()
    return s or "na"


def _b2s(x: Any) -> str:
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="ignore")
    return str(x)


def _hgetall_str(redis_client: Any, key: str) -> Dict[str, str]:
    try:
        raw = redis_client.hgetall(key) or {}
    except Exception:
        return {}
    out: Dict[str, str] = {}
    try:
        for k, v in dict(raw).items():
            out[_b2s(k)] = _b2s(v)
    except Exception:
        return {}
    return out


def _hset_map(redis_client: Any, key: str, mapping: Dict[str, Any]) -> None:
    """
    Compatible with both:
      - redis-py: hset(name, mapping={...})
      - older forms: hset(name, key, value) in a loop
    """
    try:
        if hasattr(redis_client, "hset"):
            try:
                redis_client.hset(key, mapping=mapping)  # type: ignore
                return
            except TypeError:
                pass
    except Exception:
        pass
    try:
        for k, v in mapping.items():
            try:
                redis_client.hset(key, k, v)
            except Exception:
                continue
    except Exception:
        return


@dataclass
class DriftConfig:
    enabled: bool
    include_kind: bool
    base_alpha: float
    fast_alpha: float
    z_threshold: float
    tighten_mult: float
    min_samples: int
    active_ttl_ms: int
    diag_stream: str

    @classmethod
    def from_env(cls) -> "DriftConfig":
        strict = strict_enabled()
        # Default profile vs strict profile (can be overridden by ENV).
        d_base_alpha = "0.01" if strict else "0.005"
        d_fast_alpha = "0.10" if strict else "0.05"
        d_z_thr      = "2.2"  if strict else "3.0"
        d_mult       = "1.25" if strict else "0.5"
        d_min_n      = "12"   if strict else "30"
        d_ttl        = "600000" if strict else "120000"
        return cls(
            enabled=_env_bool("FEATURE_DRIFT_ENABLED", True)
            include_kind=_env_bool("FEATURE_DRIFT_INCLUDE_KIND", False)
            base_alpha=_safe_float(os.getenv("FEATURE_DRIFT_BASE_ALPHA", d_base_alpha), float(d_base_alpha))
            fast_alpha=_safe_float(os.getenv("FEATURE_DRIFT_FAST_ALPHA", d_fast_alpha), float(d_fast_alpha))
            z_threshold=_safe_float(os.getenv("FEATURE_DRIFT_Z_THRESHOLD", d_z_thr), float(d_z_thr))
            tighten_mult=_safe_float(os.getenv("FEATURE_DRIFT_TIGHTEN_MULT", d_mult), float(d_mult))
            min_samples=int(_safe_float(os.getenv("FEATURE_DRIFT_MIN_SAMPLES", d_min_n), int(d_min_n)))
            active_ttl_ms=int(_safe_float(os.getenv("FEATURE_DRIFT_ACTIVE_TTL_MS", d_ttl), int(d_ttl)))
            diag_stream=str(os.getenv("FEATURE_DRIFT_DIAG_STREAM", "") or "").strip()
        )


# Compatibility alias
FeatureDriftConfig = DriftConfig


class FeatureDriftAlarm:

    """
    Feature drift alarm:
      - maintains baseline (slow EMA) + fast (fast EMA) per feature
      - computes drift score ~ max z-score across features
      - when score >= threshold and enough samples:
          writes drift:active key with factor>1 for gates to tighten automatically

    Features:
      - obi
      - z_delta
      - spread_bps
      - depth_min_5   (STRICT from ctx.depth_bid_5/ctx.depth_ask_5)
      - depth_min_20  (STRICT from ctx.depth_bid_20/ctx.depth_ask_20)
    """

    def __init__(self, *, cfg: Optional[DriftConfig] = None) -> None:
        self.cfg = cfg or DriftConfig.from_env()

    @classmethod
    def from_env(cls) -> "FeatureDriftAlarm":
        return cls(cfg=DriftConfig.from_env())

    def update(self, *, redis_client: Any, ctx: Any, symbol: str, kind: str) -> None:
        """
        Called on every signal (fail-open).
        """
        cfg = self.cfg
        if not cfg.enabled or redis_client is None:
            return

        # TS: strict normalization (must be epoch ms)
        tsm = int(normalize_ts_ms(getattr(ctx, "ts_ms", None) or getattr(ctx, "ts", None) or 0))
        if tsm <= 0:
            return

        sym = (symbol or getattr(ctx, "symbol", "") or "").upper()
        ven = str(getattr(ctx, "venue", None) or "na").lower()
        sess = str(getattr(ctx, "session", None) or session_from_ts_ms(tsm) or "na").lower()
        tfv = _canon_tf(getattr(ctx, "tf", None) or getattr(ctx, "timeframe", None) or "na")
        knd = str(kind or getattr(ctx, "kind", None) or getattr(ctx, "signal_kind", None) or getattr(ctx, "strategy", None) or "na").strip().lower() or "na"

        feats = self._extract_features(ctx)
        if not feats:
            return

        # select keys
        if cfg.include_kind:
            state_key = drift_state_key_v2(sym, ven, sess, tfv, knd)
            active_key = drift_active_key_v2(sym, ven, sess, tfv, knd)
        else:
            state_key = drift_state_key_v1(sym, ven, sess, tfv)
            active_key = drift_active_key_v1(sym, ven, sess, tfv)

        st = _hgetall_str(redis_client, state_key)

        # load counters
        try:
            n = int(float(st.get("n") or st.get("samples") or 0))
        except Exception:
            n = 0
        n2 = n + 1

        # update EMA stats per feature: baseline (slow) and fast
        base_a = float(cfg.base_alpha)
        fast_a = float(cfg.fast_alpha)
        eps = 1e-9

        updated: Dict[str, Any] = {"n": str(n2), "last_ts_ms": str(int(tsm))}

        # Track maximum drift feature.
        best_score = 0.0
        best_feat = ""

        for fn, x in feats.items():
            if not math.isfinite(x):
                continue
            # baseline
            b_mu = _safe_float(st.get(f"b_mu:{fn}"), x)
            b_var = _safe_float(st.get(f"b_var:{fn}"), 0.0)
            # fast
            f_mu = _safe_float(st.get(f"f_mu:{fn}"), x)
            f_var = _safe_float(st.get(f"f_var:{fn}"), 0.0)

            # EMA mean update
            b_mu2 = (1.0 - base_a) * b_mu + base_a * x
            f_mu2 = (1.0 - fast_a) * f_mu + fast_a * x

            # EMA variance update (EWMA of squared deviation)
            b_dev = (x - b_mu2)
            f_dev = (x - f_mu2)
            b_var2 = (1.0 - base_a) * b_var + base_a * (b_dev * b_dev)
            f_var2 = (1.0 - fast_a) * f_var + fast_a * (f_dev * f_dev)

            updated[f"b_mu:{fn}"] = f"{b_mu2:.12g}"
            updated[f"b_var:{fn}"] = f"{b_var2:.12g}"
            updated[f"f_mu:{fn}"] = f"{f_mu2:.12g}"
            updated[f"f_var:{fn}"] = f"{f_var2:.12g}"

            # drift score: compare fast mean vs baseline mean using baseline sigma
            sigma = math.sqrt(max(b_var2, eps))
            z = abs(f_mu2 - b_mu2) / sigma
            if math.isfinite(z) and z > best_score:
                best_score = float(z)
                best_feat = str(fn)

        updated["score"] = f"{best_score:.6g}"
        updated["feature"] = best_feat

        # Compute active factor if drift is large enough and enough samples
        active = 0
        factor = 1.0
        if n2 >= int(cfg.min_samples) and best_score >= float(cfg.z_threshold):
            active = 1
            # factor grows moderately with score (avoid over-cutting signals)
            # ratio = score/threshold; factor = 1 + tighten_mult * clamp(ratio-1, 0..2)
            ratio = float(best_score) / max(float(cfg.z_threshold), 1e-6)
            bump = max(0.0, min(2.0, ratio - 1.0))
            factor = 1.0 + float(cfg.tighten_mult) * bump

        updated["active"] = str(active)
        updated["factor"] = f"{factor:.6g}"

        # Write state
        _hset_map(redis_client, state_key, updated)

        # Write active marker with TTL (so it naturally cools off)
        try:
            if active:
                _hset_map(redis_client, active_key, {
                    "factor": f"{factor:.6g}"
                    "score": f"{best_score:.6g}"
                    "feature": best_feat
                    "last_ts_ms": str(int(tsm))
                })
                # TTL
                try:
                    redis_client.pexpire(active_key, int(cfg.active_ttl_ms))
                except Exception:
                    # fallback seconds expire
                    try:
                        redis_client.expire(active_key, int(max(1, cfg.active_ttl_ms // 1000)))
                    except Exception:
                        pass
        except Exception:
            pass

        # Optional diagnostics stream
        if cfg.diag_stream:
            try:
                payload = {
                    "ts_ms": int(tsm)
                    "symbol": sym
                    "venue": ven
                    "session": sess
                    "tf": tfv
                    "kind": knd if cfg.include_kind else "na"
                    "score": float(best_score)
                    "feature": best_feat
                    "active": int(active)
                    "factor": float(factor)
                    "features": feats
                }
                redis_client.xadd(cfg.diag_stream, {"data": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))})
            except Exception:
                pass

    def _extract_features(self, ctx: Any) -> Dict[str, float]:
        """
        STRICT extraction for depth fields:
          depth_bid_5, depth_ask_5, depth_bid_20, depth_ask_20
        """
        out: Dict[str, float] = {}

        obi = _safe_float(getattr(ctx, "obi", None) or getattr(ctx, "obi_val", None), float("nan"))
        if math.isfinite(obi):
            out["obi"] = float(obi)

        z = _safe_float(getattr(ctx, "z_delta", None) or getattr(ctx, "delta_z", None) or getattr(ctx, "z", None), float("nan"))
        if math.isfinite(z):
            out["z_delta"] = float(z)

        sp = _safe_float(getattr(ctx, "spread_bps", None), float("nan"))
        if math.isfinite(sp) and sp > 0:
            out["spread_bps"] = float(sp)

        d_b5 = _safe_float(getattr(ctx, "depth_bid_5", None), 0.0)
        d_a5 = _safe_float(getattr(ctx, "depth_ask_5", None), 0.0)
        if d_b5 > 0 and d_a5 > 0:
            out["depth_min_5"] = float(min(d_b5, d_a5))

        d_b20 = _safe_float(getattr(ctx, "depth_bid_20", None), 0.0)
        d_a20 = _safe_float(getattr(ctx, "depth_ask_20", None), 0.0)
        if d_b20 > 0 and d_a20 > 0:
            out["depth_min_20"] = float(min(d_b20, d_a20))

        return out
