from __future__ import annotations

# core/feature_engineering.py
import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        v = float(x)
        if not math.isfinite(v):
            return d
        return v
    except Exception:
        return d


def clip(x: float, lo: float | None = None, hi: float | None = None) -> float:
    if x != x:  # NaN
        return 0.0
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def log1p_signed(x: float) -> float:
    """Signed log1p transform: sign(x)*log1p(|x|)."""
    ax = abs(float(x))
    return math.copysign(math.log1p(ax), float(x))


def apply_transform(x: float, spec: Any) -> float:
    """Apply a lightweight, deterministic transform spec.

    Supported specs (examples):
      {"type":"log1p"}
      {"type":"log1p_signed"}
      {"type":"clip","lo":0,"hi":50}
      {"type":"winsor","lo":-3,"hi":3}   # online equivalent = clip to precomputed bounds
      {"type":"identity"}

    Unknown spec -> identity.
    """
    if spec is None:
        return x
    if isinstance(spec, str):
        t = spec
        s: dict[str, Any] = {"type": t}
    elif isinstance(spec, dict):
        s = spec
    else:
        return x

    t = str(s.get("type") or s.get("name") or "identity").lower()

    try:
        if t in ("identity", "none"):
            return x
        if t in ("log1p",):
            # assumes x >= 0, fallback to signed if negative
            if x < 0:
                return log1p_signed(x)
            return math.log1p(float(x))
        if t in ("log1p_signed", "signed_log1p"):
            return log1p_signed(x)
        if t in ("clip", "clamp"):
            lo = s.get("lo", None)
            hi = s.get("hi", None)
            lo_f = float(lo) if lo is not None else None
            hi_f = float(hi) if hi is not None else None
            return clip(float(x), lo_f, hi_f)
        if t in ("winsor", "winsorize"):
            # online winsorization = clip to precomputed bounds
            lo = s.get("lo", None)
            hi = s.get("hi", None)
            lo_f = float(lo) if lo is not None else None
            hi_f = float(hi) if hi is not None else None
            return clip(float(x), lo_f, hi_f)
    except Exception:
        return x

    return x


def apply_robust_scale(x: Any, *, center: float, scale: float, eps: float = 1e-9):
    """Robust scaling helper.

    Supports both scalar floats and numpy vectors (for batch transforms).
    """
    sc = float(scale)
    if not math.isfinite(sc) or abs(sc) < eps:
        sc = 1.0
    c = float(center)
    if np is not None and hasattr(x, "__array__"):
        return (np.asarray(x, dtype=float) - c) / sc
    try:
        return (float(x) - c) / sc
    except Exception:
        return 0.0


@dataclass
class RobustScalerPack:
    # feature -> {"center": median, "scale": mad_scaled}
    params: dict[str, dict[str, float]]

    def scale(self, name: str, x: float) -> float:
        p = self.params.get(name)
        if not p:
            return x
        return apply_robust_scale(x, center=float(p.get("center", 0.0)), scale=float(p.get("scale", 1.0)))

    def transform(self, X: np.ndarray, feature_names: list[str] | None = None) -> np.ndarray:  # type: ignore
        """Transform array X using robust scaling per feature.
        
        Args:
            X: numpy array of shape (n_samples, n_features)
            feature_names: optional list of feature names matching columns (if None, tries to infer from params keys)
        
        Returns:
            Transformed array with same shape
        """
        import numpy as np
        if X is None or X.size == 0:
            return X
        X_arr = np.asarray(X, dtype=np.float64)
        if len(X_arr.shape) != 2:
            raise ValueError(f"X must be 2D array, got shape {X_arr.shape}")

        X_out = X_arr.copy()
        # If params is empty, return as-is
        if not self.params:
            return X_out

        # If feature_names provided, use them; otherwise try to infer from params keys
        if feature_names is None:
            # Try to get feature names from params (assumes params keys are feature names)
            feature_names = list(self.params.keys())

        # Scale each column by corresponding feature name
        n_features = X_arr.shape[1]
        for i in range(n_features):
            if i < len(feature_names):
                feature_name = feature_names[i]
                if feature_name in self.params:
                    # Apply robust scaling to this column
                    p = self.params[feature_name]
                    center = float(p.get("center", 0.0))
                    scale = float(p.get("scale", 1.0))
                    X_out[: i] = apply_robust_scale(X_arr[: i], center=center, scale=scale)

        return X_out

    @staticmethod
    def fit(X: np.ndarray, feature_names: list[str] | None = None) -> RobustScalerPack:  # type: ignore
        """Fit robust scaler on array X.
        
        Computes median (center) and MAD*1.4826 (scale) per feature.
        
        Args:
            X: numpy array of shape (n_samples, n_features)
            feature_names: optional list of feature names (if None, uses indices)
        
        Returns:
            RobustScalerPack with fitted params
        """
        import numpy as np
        if X is None or X.size == 0:
            return RobustScalerPack(params={})

        X_arr = np.asarray(X, dtype=np.float64)
        if len(X_arr.shape) != 2:
            raise ValueError(f"X must be 2D array, got shape {X_arr.shape}")

        n_features = X_arr.shape[1]
        params: dict[str, dict[str, float]] = {}

        # MAD constant (makes MAD equivalent to std for normal distribution)
        MAD_CONST = 1.4826

        for i in range(n_features):
            col = X_arr[: i]
            # Remove NaN/Inf
            col_clean = col[np.isfinite(col)]
            if len(col_clean) == 0:
                center = 0.0
                scale = 1.0
            else:
                center = float(np.median(col_clean))
                mad = float(np.median(np.abs(col_clean - center)))
                scale = max(1e-9, mad * MAD_CONST)

            feature_name = feature_names[i] if feature_names and i < len(feature_names) else f"f_{i}"
            params[feature_name] = {"center": center, "scale": scale}

        return RobustScalerPack(params=params)


def bucketize(x: float, edges: Sequence[float]) -> int:
    """Return bucket index in [0..len(edges)] for sorted edges."""
    v = float(x)
    i = 0
    for e in edges:
        try:
            if v <= float(e):
                return i
        except Exception:
            pass
        i += 1
    return i


def derive_session_label(ts_ms: int, *, tz: str = "UTC", cfg: dict[str, Any] | None = None) -> str:
    """Deterministic session label derived ONLY from ts_ms.

    Default uses UTC hour buckets. You can override via cfg["session_hours"]:
      {"asia": [0,7], "eu": [7,13], "us": [13,21], "off": [21,24]}
    """
    # NOTE: keep deterministic; no wall-clock.
    # We intentionally ignore tz conversions here; exchange timestamps should already be UTC.
    try:
        h = int((int(ts_ms) // 1000) % 86400) // 3600
    except Exception:
        h = 0

    sh = (cfg or {}).get("session_hours") if isinstance((cfg or {}).get("session_hours"), dict) else None
    if not sh:
        # default: 4 buckets
        if 0 <= h < 7:
            return "asia"
        if 7 <= h < 13:
            return "eu"
        if 13 <= h < 21:
            return "us"
        return "off"

    for name, rng in sh.items():
        try:
            if not isinstance(rng, (list, tuple)) or len(rng) != 2:
                continue
            a, b = int(rng[0]), int(rng[1])
            if a <= h < b:
                return str(name)
        except Exception:
            continue
    return "other"


def derive_regime_label(x: Any, *, fallback_score: float | None = None, cfg: dict[str, Any] | None = None) -> str:
    """Derive a regime label.

    Priority:
      1) if x is already a string label -> normalized
      2) else use fallback_score (0..1) to bucketize by thresholds

    cfg["regime_thresholds"] default [0.33, 0.66] -> low/mid/high
    """
    if isinstance(x, str) and x.strip():
        return x.strip().lower()

    thr = (cfg or {}).get("regime_thresholds")
    edges = [0.33, 0.66]
    if isinstance(thr, (list, tuple)) and len(thr) >= 2:
        try:
            edges = [float(thr[0]), float(thr[1])]
        except Exception:
            edges = [0.33, 0.66]

    sc = fallback_score
    if sc is None:
        return "unknown"

    try:
        v = float(sc)
    except Exception:
        return "unknown"

    if v <= edges[0]:
        return "low"
    if v <= edges[1]:
        return "mid"
    return "high"


def get_numeric_feature(
    *,
    name: str,
    indicators: dict[str, Any],
    transforms: dict[str, Any] | None = None,
    scaler: RobustScalerPack | None = None,
) -> float:
    x = _f(indicators.get(name, 0.0), 0.0)
    if transforms and name in transforms:
        x = apply_transform(x, transforms.get(name))
    if scaler:
        x = scaler.scale(name, x)
    return float(x)



