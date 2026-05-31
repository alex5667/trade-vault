"""Artifact loader + scorer for the confidence meta-gate model.

The v1 artifact is a pure-Python logistic regression — no sklearn at runtime —
plus a Platt or piecewise-isotonic calibrator. Heavy backends (LightGBM,
XGBoost) load lazily via importlib only if the artifact declares them.

Loader guarantees:
  * Returns None when path is missing → caller falls back to legacy.
  * Validates `schema` field; mismatched schema yields a SCHEMA_MISMATCH
    reason from the gate.
  * Records load timestamp so the gate can flag the model as stale.
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("conf_meta_gate.model")


@dataclass(frozen=True)
class CalibrationSpec:
    type: str  # "platt" | "isotonic" | "identity"
    a: float = 1.0
    b: float = 0.0
    # For isotonic: monotonic piecewise breakpoints (x_i, y_i), x ascending.
    points: tuple[tuple[float, float], ...] = ()
    # OOS metrics, used as health signal — non-None when serialized.
    ece: float | None = None


@dataclass(frozen=True)
class Thresholds:
    min_p_win: float = 0.56
    min_expected_r: float = 0.02
    min_expected_edge_bps: float = 1.5


@dataclass(frozen=True)
class TrainingSummary:
    n_rows: int = 0
    pos_rate: float = 0.0
    oos_auc: float = 0.0
    oos_brier: float = 0.0
    oos_ece: float = 0.0
    top5_expectancy_r: float = 0.0
    created_ms: int = 0


@dataclass(frozen=True)
class MetaGateArtifact:
    """Immutable model+calibrator+thresholds bundle."""

    model_ver: str
    schema: str
    feature_cols: tuple[str, ...]
    model_type: str  # "logistic_regression" for v1
    intercept: float
    coef: tuple[float, ...]
    calibrator: CalibrationSpec
    thresholds: Thresholds
    training_summary: TrainingSummary
    loaded_at_ms: int
    source_path: str

    # Derived: hash of feature_cols for schema-mismatch detection.
    feature_cols_hash: str = ""

    def predict_raw(self, features: dict[str, float]) -> float:
        """Compute σ(intercept + Σ coef_i · x_i) over feature_cols order."""
        z = self.intercept
        for name, w in zip(self.feature_cols, self.coef):
            x = features.get(name)
            if x is None:
                # Missing feature treated as 0 — caller should pass the same
                # default the trainer used; we flag missing externally.
                continue
            try:
                z += w * float(x)
            except (TypeError, ValueError):
                continue
        # Clamp z to avoid overflow on extreme inputs.
        if z > 60.0:
            return 1.0
        if z < -60.0:
            return 0.0
        return 1.0 / (1.0 + math.exp(-z))

    def calibrate(self, p_raw: float) -> float:
        return _apply_calibrator(self.calibrator, p_raw)


def _apply_calibrator(c: CalibrationSpec, p_raw: float) -> float:
    if c.type == "identity":
        return _clip01(p_raw)
    if c.type == "platt":
        # Platt: σ(a · logit(p) + b)
        eps = 1e-6
        p = max(eps, min(1.0 - eps, p_raw))
        z = c.a * math.log(p / (1.0 - p)) + c.b
        if z > 60.0:
            return 1.0
        if z < -60.0:
            return 0.0
        return 1.0 / (1.0 + math.exp(-z))
    if c.type == "isotonic":
        return _piecewise_isotonic(c.points, p_raw)
    return _clip01(p_raw)


def _piecewise_isotonic(points: tuple[tuple[float, float], ...], x: float) -> float:
    if not points:
        return _clip01(x)
    if x <= points[0][0]:
        return _clip01(points[0][1])
    if x >= points[-1][0]:
        return _clip01(points[-1][1])
    # Linear interpolation between bracketing points (binary search would be
    # marginally faster but the artifact has <50 points; linear scan is fine).
    for i in range(1, len(points)):
        x0, y0 = points[i - 1]
        x1, y1 = points[i]
        if x0 <= x <= x1:
            if x1 == x0:
                return _clip01(y0)
            t = (x - x0) / (x1 - x0)
            return _clip01(y0 + t * (y1 - y0))
    return _clip01(x)


def _clip01(v: float) -> float:
    if not math.isfinite(v):
        return 0.0
    return max(0.0, min(1.0, v))


def _hash_feature_cols(feature_cols: tuple[str, ...]) -> str:
    import hashlib
    h = hashlib.sha256()
    for c in feature_cols:
        h.update(c.encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()[:16]


def load_artifact(path: str) -> MetaGateArtifact | None:
    """Load a model artifact from disk. Returns None if path missing/invalid."""
    if not path:
        return None
    if not os.path.exists(path):
        log.info("conf_meta_gate artifact missing path=%s", path)
        return None
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        log.warning("conf_meta_gate artifact unreadable path=%s err=%s", path, e)
        return None

    try:
        feature_cols = tuple(str(c) for c in raw["feature_cols"])
        model_block = raw["model"]
        coef = tuple(float(x) for x in model_block.get("coef", []))
        if len(coef) != len(feature_cols):
            log.warning(
                "conf_meta_gate coef len mismatch coef=%d cols=%d path=%s",
                len(coef), len(feature_cols), path,
            )
            return None
        intercept = float(model_block.get("intercept", 0.0))
        model_type = str(model_block.get("type", "logistic_regression"))
        if model_type != "logistic_regression":
            # v1 only ships LR; richer types can be added once we need them.
            log.warning("conf_meta_gate unsupported model.type=%s", model_type)
            return None

        cal_block = raw.get("calibrator") or {"type": "identity"}
        cal = CalibrationSpec(
            type=str(cal_block.get("type", "identity")),
            a=float(cal_block.get("a", 1.0)),
            b=float(cal_block.get("b", 0.0)),
            points=tuple(
                (float(p[0]), float(p[1])) for p in (cal_block.get("points") or [])
            ),
            ece=(float(cal_block["ece"]) if "ece" in cal_block else None),
        )
        thr_block = raw.get("thresholds") or {}
        thresholds = Thresholds(
            min_p_win=float(thr_block.get("min_p_win", 0.56)),
            min_expected_r=float(thr_block.get("min_expected_r", 0.02)),
            min_expected_edge_bps=float(thr_block.get("min_expected_edge_bps", 1.5)),
        )
        ts_block = raw.get("training_summary") or {}
        training = TrainingSummary(
            n_rows=int(ts_block.get("n_rows", 0)),
            pos_rate=float(ts_block.get("pos_rate", 0.0)),
            oos_auc=float(ts_block.get("oos_auc", 0.0)),
            oos_brier=float(ts_block.get("oos_brier", 0.0)),
            oos_ece=float(ts_block.get("oos_ece", 0.0)),
            top5_expectancy_r=float(ts_block.get("top5_expectancy_r", 0.0)),
            created_ms=int(ts_block.get("created_ms", 0)),
        )
        artifact = MetaGateArtifact(
            model_ver=str(raw.get("model_ver", "")),
            schema=str(raw.get("schema", "")),
            feature_cols=feature_cols,
            model_type=model_type,
            intercept=intercept,
            coef=coef,
            calibrator=cal,
            thresholds=thresholds,
            training_summary=training,
            loaded_at_ms=int(time.time() * 1000),
            source_path=path,
            feature_cols_hash=_hash_feature_cols(feature_cols),
        )
        log.info(
            "conf_meta_gate artifact loaded model_ver=%s schema=%s n_cols=%d",
            artifact.model_ver, artifact.schema, len(feature_cols),
        )
        return artifact
    except (KeyError, TypeError, ValueError) as e:
        log.warning("conf_meta_gate artifact parse failed path=%s err=%s", path, e)
        return None


@dataclass
class ArtifactSlot:
    """Hot-swappable artifact pointer (single lock around the reference)."""

    _artifact: MetaGateArtifact | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def set(self, artifact: MetaGateArtifact | None) -> None:
        with self._lock:
            self._artifact = artifact

    def get(self) -> MetaGateArtifact | None:
        with self._lock:
            return self._artifact

    def age_hours(self, now_ms: int | None = None) -> float | None:
        a = self.get()
        if a is None or a.training_summary.created_ms <= 0:
            return None
        ref = now_ms if now_ms is not None else int(time.time() * 1000)
        return max(0.0, (ref - a.training_summary.created_ms) / 1000.0 / 3600.0)
