from __future__ import annotations

import os
import math
from dataclasses import dataclass
from typing import Any, Dict, Optional

from services.reliability_curves import (
    load_bucket_rate
)


# =============================================================================
# Confidence post-calibration (Variant A).
#
# We DO NOT change signal generation logic here.
# We only compute and attach:
#   - confidence_adjusted
#   - confidence_adjust_reason/meta
#
# This is meant to run at TradeMonitor "entry point" (normalize_signal)
# i.e. right before create_position, so you can later:
#   - rank/route signals by adjusted confidence
#   - optionally tighten gates in a separate profile
#
# Fail-open:
#   - If Redis missing, curves not ready, or fields missing -> no adjustment.
#
# Formula (simple, stable):
#   adj = clip01(base + alpha * (rate_ctx - rate_global))
# But for hard/hardest profiles we additionally:
#   - require sufficient samples (global + ctx)
#   - require meaningful effect (min_delta)
#   - require statistical evidence (z-score)
#   - shrink ctx rate towards global (hierarchical prior) for stability
# =============================================================================


def _env_int(k: str, default: int) -> int:
    try:
        return int(float(os.getenv(k, str(default))))
    except Exception:
        return int(default)


def _env_float(k: str, default: float) -> float:
    try:
        return float(os.getenv(k, str(default)))
    except Exception:
        return float(default)


def _env_bool(k: str, default: bool) -> bool:
    v = os.getenv(k, None)
    if v is None or str(v).strip() == "":
        return bool(default)
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "on"}:
        return True
    if s in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _canon_target(x: str) -> str:
    x = (x or "").strip().lower()
    if x in {"tp1", "tp1_hit"}:
        return "tp1"
    if x in {"tp2", "tp2_hit"}:
        return "tp2"
    if x in {"win", "pnl"}:
        return "win"
    if x in {"tp1_not_sl", "tp1nosl", "tp1_no_sl"}:
        return "tp1_not_sl"
    # default in most systems: tp2
    return x or "tp2"


def _clamp01(x: float) -> float:
    if not math.isfinite(x):
        return 0.0
    return max(0.0, min(1.0, float(x)))


def _pick_adjust_target() -> str:
    """
    Keep adjuster target aligned with writer targets.
    Priority:
      1) RELIABILITY_ADJUST_TARGET
      2) first value in RELIABILITY_TARGETS
      3) tp2 (recommended default)
    """
    t = (os.getenv("RELIABILITY_ADJUST_TARGET", "") or "").strip()
    if t:
        return _canon_target(t)
    raw = (os.getenv("RELIABILITY_TARGETS", "tp2") or "tp2").strip().lower()
    raw = raw.replace(",", "|")
    first = raw.split("|")[0].strip() if raw else "tp2"
    return _canon_target(first or "tp2")


def _pick_profile() -> str:
    p = (os.getenv("RELIABILITY_ADJ_PROFILE", "soft") or "").strip().lower()
    if p not in {"soft", "hard", "hardest"}:
        return "soft"
    return p


def maybe_apply_confidence_adjustment(
    redis_client: Any
    *
    envelope: Dict[str, Any]
    strategy: str
    symbol: str
    tf: str
    direction: str
) -> None:
    """
    Thin helper used by TradeMonitor (or any other caller).
    It mutates envelope in-place (fail-open).

    Fields are intentionally compact & stable:
      confidence_adjusted
      confidence_adjust_ctx
      confidence_adjust_delta
      confidence_adjust_delta_rate
      confidence_adjust_bucket
      confidence_adjust_n_ctx
      confidence_adjust_n_glob
      confidence_adjust_target
      confidence_adjust_profile
      confidence_adjust_notes
    """
    try:
        res = maybe_adjust_confidence(
            redis_client
            envelope=envelope
            strategy=strategy
            symbol=symbol
            tf=tf
            direction=direction
        )
        if res is None:
            return
        envelope["confidence_adjusted"] = float(res.adjusted)
        envelope["confidence_adjust_ctx"] = str(res.ctx_key or "na")
        envelope["confidence_adjust_delta"] = float(res.delta)
        envelope["confidence_adjust_delta_rate"] = float(res.delta_rate)
        envelope["confidence_adjust_bucket"] = int(res.bucket)
        envelope["confidence_adjust_n_ctx"] = int(res.n_ctx)
        envelope["confidence_adjust_n_glob"] = int(res.n_glob)
        envelope["confidence_adjust_target"] = str(res.target)
        envelope["confidence_adjust_profile"] = str(res.profile)
        if res.notes:
            envelope["confidence_adjust_notes"] = str(res.notes)[:512]
    except Exception:
        # fail-open: must never affect signal flow
        return


def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if not math.isfinite(v):
            return None
        return v
    except Exception:
        return None


def _bucket_confidence(conf: float, *, step: int) -> int:
    if not math.isfinite(conf):
        return -1
    x = float(conf)
    if x <= 1.0:
        x *= 100.0
    x = max(0.0, min(100.0, x))
    b = int(round(x / float(step)) * step)
    return max(0, min(100, b))


def _dir_to_ud(direction: str) -> str:
    d = (direction or "").strip().upper()
    if d == "LONG":
        return "UP"
    if d == "SHORT":
        return "DOWN"
    return "NA"


def _boolish(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return int(x) != 0
    s = str(x).strip().lower()
    return s in {"1", "true", "yes", "on"}


def _extract_ctx(envelope: Dict[str, Any]) -> Dict[str, Any]:
    ctx = envelope.get("ctx")
    return ctx if isinstance(ctx, dict) else {}


def _canon_kind(s: Any) -> str:
    v = str(s or "").strip().lower()
    return v or "na"


def _canon_regime(s: Any) -> str:
    v = str(s or "").strip().lower()
    return v or "na"


def _canon_venue(s: Any) -> str:
    v = str(s or "").strip().lower()
    return v or "na"


def _extract_venue(envelope: Dict[str, Any]) -> str:
    v = envelope.get("venue")
    if v:
        return _canon_venue(v)
    ctx = _extract_ctx(envelope)
    return _canon_venue(ctx.get("venue") if isinstance(ctx, dict) else None)


def _extract_kind(envelope: Dict[str, Any]) -> str:
    return _canon_kind(envelope.get("kind") or "na")


def _extract_entry_regime(envelope: Dict[str, Any]) -> str:
    return _canon_regime(envelope.get("entry_regime") or envelope.get("regime") or envelope.get("regime_label") or "na")




def _clamp01(x: float) -> float:
    if not math.isfinite(x):
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return float(x)


def _z_score_diff(p1: float, n1: int, p2: float, n2: int) -> float:
    """
    Two-proportion z-score (best-effort, fail-open).
    If variance degenerates -> return 0 (meaning "not significant").
    """
    try:
        if n1 <= 0 or n2 <= 0:
            return 0.0
        p1 = _clamp01(p1)
        p2 = _clamp01(p2)
        v1 = p1 * (1.0 - p1) / float(n1)
        v2 = p2 * (1.0 - p2) / float(n2)
        denom = math.sqrt(max(1e-12, v1 + v2))
        return abs(p1 - p2) / denom
    except Exception:
        return 0.0


def _shrink_ctx_to_global(h_ctx: int, n_ctx: int, p_glob: float, prior_n: int) -> float:
    """
    Hierarchical shrinkage:
      p_ctx_shrunk = (h_ctx + prior_n * p_glob) / (n_ctx + prior_n)
    This makes hard-profile stable and prevents overreaction on sparse ctx.
    """
    try:
        if n_ctx <= 0:
            return float(p_glob)
        pn = max(0, int(prior_n))
        if pn <= 0:
            return float(h_ctx) / float(n_ctx)
        return (float(h_ctx) + float(pn) * float(p_glob)) / (float(n_ctx) + float(pn))
    except Exception:
        return float(p_glob)


def _extract_base_confidence(envelope: Dict[str, Any]) -> Optional[float]:
    """
    Extract base confidence from envelope and/or ctx.
    We support multiple synonyms to avoid tight coupling.
    """
    for k in ("confidence", "final_score", "score", "conf", "prob", "p"):
        v = _safe_float(envelope.get(k))
        if v is not None:
            return v
    ctx = _extract_ctx(envelope)
    for k in ("confidence", "final_score", "score", "conf", "prob", "p"):
        v = _safe_float(ctx.get(k))
        if v is not None:
            return v
    return None


def _smt_ctx_key(envelope: Dict[str, Any], *, coh_thr: float, direction: str) -> Optional[str]:
    ctx = _extract_ctx(envelope)
    if "smt_leader_confirm" not in ctx and "smt_coh" not in ctx and "smt_leader_dir" not in ctx:
        return None
    leader_confirm = 1 if _boolish(ctx.get("smt_leader_confirm")) else 0
    coh = _safe_float(ctx.get("smt_coh"))
    coh_hi = 1 if (coh is not None and float(coh) >= float(coh_thr)) else 0
    leader_dir = str(ctx.get("smt_leader_dir") or "NA").strip().upper()
    sig_ud = _dir_to_ud(direction)
    align = 1 if (leader_dir in {"UP", "DOWN"} and sig_ud in {"UP", "DOWN"} and leader_dir == sig_ud) else 0
    return f"smtc{leader_confirm}_coh{coh_hi}_al{align}"


@dataclass(frozen=True)
class ConfidenceAdjustResult:
    adjusted: float
    base: float
    delta: float
    notes: str = ""
    # ---- audit/meta (added for TradeMonitor) ----
    ctx_key: str = "na"
    delta_rate: float = 0.0
    bucket: int = -1
    n_ctx: int = 0
    n_glob: int = 0
    target: str = "tp2"
    profile: str = "soft"


def maybe_adjust_confidence(
    redis_client: Any
    *
    envelope: Dict[str, Any]
    strategy: str
    symbol: str
    tf: str
    direction: str
) -> Optional[ConfidenceAdjustResult]:
    """
    Post-calibration confidence adjuster using reliability curves.

    Profiles:
      - soft    : very conservative, mainly protects from overreacting on small samples
      - hard    : stricter gating (bigger samples, stronger evidence)
      - hardest : MAX stability (recommended if you want "do-no-harm" behavior):
          * never adjusts when SMT-context is absent (ctx_key == "na")
          * requires large global+context samples
          * requires very strong evidence (z >= 2.58 ~ 99% CI)
          * uses heavier shrinkage to global (larger prior)
          * tighter adjustment cap
        This minimizes the risk of "cutting" signals indirectly by overfitting confidence.

    Returns ConfidenceAdjustResult or None (meaning: keep base confidence).
    """
    if redis_client is None:
        return None
    enabled = bool(int(os.getenv("RELIABILITY_ADJUST_ENABLED", "0") or "0"))
    if not enabled:
        return None

    # ------------------------------------------------------------------
    # Core configuration (common to all profiles)
    # ------------------------------------------------------------------
    profile = _pick_profile()
    target = _pick_adjust_target()
    step = max(1, min(20, _env_int("RELIABILITY_BUCKET_STEP", 5)))
    coh_thr = float(_env_float("RELIABILITY_SMT_COH_THR", 0.65))
    alpha = float(_env_float("RELIABILITY_ADJ_ALPHA", 0.5))
    min_bucket_samples = int(float(os.getenv("RELIABILITY_ADJ_MIN_BUCKET_SAMPLES", "50") or "50"))

    # ------------------------------------------------------------------
    # Profile parameters (default conservative values, overridable via env)
    # ------------------------------------------------------------------
    # soft: basic protection from small samples, uses raw difference
    # hard: requires larger samples + statistical evidence + shrinkage
    # hardest: MAX stability (recommended for "do-no-harm"):
    #   - requires SMT context present
    #   - very large samples + strong evidence (99% CI)
    #   - heavy shrinkage + tight adjustment cap
    min_n_global = _env_int("RELIABILITY_ADJ_MIN_SAMPLES_GLOBAL", 100)
    min_n_ctx = _env_int("RELIABILITY_ADJ_MIN_SAMPLES_CTX", 100)
    min_delta = _env_float("RELIABILITY_ADJ_MIN_DELTA", 0.03)  # 3pp minimum effect
    min_z = _env_float("RELIABILITY_ADJ_MIN_Z", 1.96)          # ~95% CI evidence threshold
    prior_n = _env_int("RELIABILITY_ADJ_PRIOR_N", 50)          # shrinkage strength (higher = trust global more)
    max_abs = _env_float("RELIABILITY_ADJ_MAX_ABS", 0.15)      # cap absolute adjustment

    base = _extract_base_confidence(envelope)
    if base is None or not math.isfinite(float(base)):
        return None
    base = _clamp01(float(base))

    # ------------------------------------------------------------------
    # Extract signal metadata for reliability lookup
    # ------------------------------------------------------------------
    venue = _extract_venue(envelope)
    kind = _extract_kind(envelope)
    regime = _extract_entry_regime(envelope)

    # Bucket the confidence score (e.g., 0.52 -> bucket 50 with step=5)
    b = _bucket_confidence(float(base), step=step)
    if b < 0:
        return None

    # ctx_key is derived ONLY from compact SMT context flags:
    #   smtc{0/1}_coh{0/1}_al{0/1}
    ctx_key = _smt_ctx_key(envelope, coh_thr=coh_thr, direction=direction) or "na"

    # keep for audit
    ctx_key_final = str(ctx_key or "na")

    # ------------------------------------------------------------------
    # HARDEST profile = maximum stability.
    # NOTE: values can be overridden via env, but these are conservative defaults.
    # ------------------------------------------------------------------
    if profile == "hardest":
        # Require a real SMT context. If SMT fields are absent => ctx_key="na" => skip.
        if (ctx_key or "na") == "na":
            return None
        # Much larger sample requirements
        min_n_global = int(_env_int("RELIABILITY_ADJ_MIN_SAMPLES_GLOBAL", 1000))
        min_n_ctx = int(_env_int("RELIABILITY_ADJ_MIN_SAMPLES_CTX", 500))
        # Only adjust if effect is meaningful
        min_delta = float(_env_float("RELIABILITY_ADJ_MIN_DELTA", 0.04))   # 4pp
        # Strong evidence only (~99% CI)
        min_z = float(_env_float("RELIABILITY_ADJ_MIN_Z", 2.58))
        # Heavier shrinkage to global (reduces context noise)
        prior_n = int(_env_int("RELIABILITY_ADJ_PRIOR_N", 150))
        # Tighter cap: even if context is strong, we limit impact
        max_abs = float(_env_float("RELIABILITY_ADJ_MAX_ABS", 0.10))

    # Read global vs ctx bucket rates (v4 -> v3 -> v2 fallback is inside load_bucket_rate)
    p_g, n_g = load_bucket_rate(
        redis_client
        target=target
        strategy=strategy
        symbol=symbol
        tf=tf
        venue=venue
        kind=kind, regime=regime
        ctx_key="na"
        bucket=b
    )

    p_c, n_c = load_bucket_rate(
        redis_client
        target=target
        strategy=strategy
        symbol=symbol
        tf=tf
        venue=venue
        kind=kind, regime=regime
        ctx_key=ctx_key
        bucket=b
    )

    if p_g is None or p_c is None:
        return None

    # Convert to integers for consistent arithmetic (samples are always integers)
    n_gi = int(n_g)
    n_ci = int(n_c)

    p_glob = _clamp01(float(p_g))
    p_ctx = _clamp01(float(p_c))

    # ------------------------------------------------------------------
    # Sample size guards (fail-safe against overfitting)
    # ------------------------------------------------------------------
    # Always require minimum bucket samples (even for soft profile)
    if n_gi < min_bucket_samples or n_ci < min_bucket_samples:
        return None

    # Profile-level guards: require larger samples for hard/hardest profiles
    if profile in {"hard", "hardest"}:
        if n_gi < min_n_global or n_ci < min_n_ctx:
            return None

    # soft profile: simplest rule (stable baseline behavior)
    if profile == "soft":
        delta_rate = float(p_ctx - p_glob)
        delta = float(alpha) * float(delta_rate)
        adjusted = _clamp01(float(base) + float(delta))
        return ConfidenceAdjustResult(
            adjusted=float(adjusted)
            base=float(base)
            delta=float(delta)
            ctx_key=ctx_key_final
            delta_rate=float(delta_rate)
            bucket=int(b)
            n_ctx=n_ci
            n_glob=n_gi
            target=str(target)
            profile="soft"
            notes=f"soft:tgt={target} b={b} ctx={ctx_key_final} pG={p_glob:.3f}(n={n_gi}) pC={p_ctx:.3f}(n={n_ci})"
        )

    # hard/hardest: shrink ctx -> global to reduce overfit / sparse noise
    h_c = int(round(float(p_ctx) * float(n_ci)))
    p_ctx_shrunk = _clamp01(_shrink_ctx_to_global(h_c, n_ci, p_glob, int(prior_n)))

    delta_rate = float(p_ctx_shrunk - p_glob)
    if abs(delta_rate) < float(min_delta):
        return None

    # Conservative significance check: ctx effective n includes prior (harder to pass).
    n_eff_ctx = n_ci + max(0, int(prior_n))
    z = _z_score_diff(p_ctx_shrunk, n_eff_ctx, p_glob, n_gi)
    if float(z) < float(min_z):
        return None

    raw_adj = float(alpha) * float(delta_rate)
    if abs(raw_adj) > float(max_abs):
        raw_adj = float(max_abs) * (1.0 if raw_adj > 0 else -1.0)

    adjusted = _clamp01(float(base) + raw_adj)
    return ConfidenceAdjustResult(
        adjusted=float(adjusted)
        base=float(base)
        delta=float(raw_adj)
        ctx_key=ctx_key_final
        delta_rate=float(delta_rate)
        bucket=int(b)
        n_ctx=n_ci
        n_glob=n_gi
        target=str(target)
        profile=str(profile)
        notes=f"{profile}:tgt={target} b={b} ctx={ctx_key_final} pG={p_glob:.3f}(n={n_gi}) pC~={p_ctx_shrunk:.3f}(n={n_ci},prior={prior_n}) z={z:.2f}"
    )
