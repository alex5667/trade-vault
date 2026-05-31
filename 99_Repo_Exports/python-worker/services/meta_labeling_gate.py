"""
meta_labeling_gate.py — Phase 2.1: Online meta-label signal gate.

Scores incoming signals with the trained meta-labeling model (P(TP | features))
and emits PASS or VETO META_LOW_PROB based on per-regime thresholds.

SHADOW by default: META_LABEL_GATE_ENABLED=0 → score but never veto.
When enabled (=1), low-probability signals are blocked before publish.

The model state is hot-reloaded from Redis every META_LABEL_MODEL_TTL_SEC (default 300).
Missing model → PASS (fail-open).

ENV:
  META_LABEL_GATE_ENABLED   = 0          SHADOW mode (score but don't veto)
  META_LABEL_MODEL_KEY      = meta_label_model:state
  META_LABEL_REDIS_URL      = redis://redis-worker-1:6379/0
  META_LABEL_MODEL_TTL_SEC  = 300        Model reload interval
  META_LABEL_THR_DEFAULT    = 0.45       Default P(TP) gate threshold
  META_LABEL_LOG_SAMPLE     = 0.05       Fraction of decisions to log (reduce noise)

Prometheus counters (injected externally via register_metrics):
  meta_label_gate_score_total{regime, decision}   PASS / SHADOW_VETO / VETO
  meta_label_gate_prob_histogram{regime}
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any

log = logging.getLogger("meta_labeling_gate")

_VETO_REASON = "META_LOW_PROB"


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)

def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except Exception:
        return d

def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")

def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


class MetaLabelGate:
    """
    Online meta-label gate: scores a signal and returns PASS/VETO.

    Thread-safe: each call is stateless except for the cached model.
    Model is hot-reloaded from Redis when TTL expires.
    """

    def __init__(
        self,
        rc: Any,
        *,
        enabled: bool = False,
        model_key: str = "meta_label_model:state",
        model_ttl_sec: int = 300,
        default_threshold: float = 0.45,
        log_sample: float = 0.05,
        ensemble_reader: Any = None,
    ) -> None:
        self.rc = rc
        self.enabled = enabled
        self.model_key = model_key
        self.model_ttl_sec = model_ttl_sec
        self.default_threshold = default_threshold
        self.log_sample = log_sample

        self._state: dict | None = None
        self._state_loaded_ms: float = 0.0
        self._autopilot_enabled: bool = False
        self._autopilot_checked_ms: float = 0.0

        # Optional ensemble blender — when ENSEMBLE_WEIGHTS_READ_ENABLED=1
        # and the symbol has weights in Redis, blend meta-label prob with
        # other available P(win) sources (e.g. indicators["p_edge"]).
        # Lazy-init on first use to avoid Redis lookup at import time.
        self._ensemble_reader = ensemble_reader

        self._metrics: dict[str, Any] = {}

    def register_metrics(self, metrics: dict[str, Any]) -> None:
        """Inject Prometheus metric objects {score_total, prob_histogram}."""
        self._metrics = metrics

    # ── Model loading ─────────────────────────────────────────────────────────

    def _load_model(self) -> dict | None:
        """Load model state from Redis if TTL expired. Returns state or None."""
        now_ms = time.time() * 1000
        if (now_ms - self._state_loaded_ms) < self.model_ttl_sec * 1000:
            return self._state  # cache hit

        try:
            raw = self.rc.get(self.model_key)
            if raw:
                self._state = json.loads(str(raw))
                self._state_loaded_ms = now_ms
                log.debug("meta_labeling_gate: model reloaded (n=%d auc=%.3f)",
                          self._state.get("n_samples", 0),
                          self._state.get("roc_auc_oos", 0.0))
            else:
                self._state = None
                self._state_loaded_ms = now_ms  # avoid hammering Redis on miss
        except Exception as e:
            log.debug("meta_labeling_gate: model load error: %s", e)
            self._state = None

        return self._state

    def _is_enabled(self) -> bool:
        """Return True if gate enforcement is active (ENV or autopilot flag)."""
        if self.enabled:
            return True
        # Re-check autopilot flag at same cadence as model TTL
        now_ms = time.time() * 1000
        if (now_ms - self._autopilot_checked_ms) >= self.model_ttl_sec * 1000:
            try:
                from orderflow_services.calibration_autopilot_v1 import read_autopilot_flag
                self._autopilot_enabled = read_autopilot_flag(
                    self.rc, "meta_label_gate_enabled"
                )
            except Exception:
                pass
            self._autopilot_checked_ms = now_ms
        return self._autopilot_enabled

    # ── Ensemble blending (optional) ──────────────────────────────────────────

    def _get_ensemble_reader(self) -> Any:
        if self._ensemble_reader is None:
            try:
                from services.ensemble_weights_reader import EnsembleWeightsReader
                self._ensemble_reader = EnsembleWeightsReader(self.rc)
            except Exception as e:
                log.debug("meta_labeling_gate: ensemble reader init error: %s", e)
                self._ensemble_reader = False  # type: ignore[assignment]
        return self._ensemble_reader if self._ensemble_reader is not False else None

    def _maybe_blend(
        self,
        meta_prob: float,
        indicators: dict[str, Any],
        symbol: str,
    ) -> tuple[float, dict[str, float]]:
        """Blend meta_prob with other in-indicator P(win) sources when available.

        Returns (blended_prob, sources_used). When ENSEMBLE_WEIGHTS_READ_ENABLED
        is off, or no extra sources present, returns (meta_prob, {}).
        """
        if not symbol:
            return meta_prob, {}
        reader = self._get_ensemble_reader()
        if reader is None:
            return meta_prob, {}
        # Collect additional source probs from indicators.
        # Convention: keys ending with `_prob` or named `p_edge` are P(win)-like.
        sources: dict[str, float] = {"meta_label": meta_prob}
        p_edge = indicators.get("p_edge")
        if p_edge is not None:
            try:
                p = float(p_edge)
                if 0.0 < p < 1.0:
                    sources["p_edge"] = p
            except Exception:
                pass
        for k, v in indicators.items():
            if k.endswith("_prob") and k != "meta_label_prob":
                try:
                    p = float(v)
                    if 0.0 < p < 1.0:
                        sources[k] = p
                except Exception:
                    continue
        if len(sources) < 2:
            return meta_prob, {}
        try:
            blended = reader.blend(symbol, sources)
            return float(blended), sources
        except Exception as e:
            log.debug("meta_labeling_gate: blend error: %s", e)
            return meta_prob, {}

    # ── Gate evaluation ───────────────────────────────────────────────────────

    def evaluate(
        self,
        indicators: dict[str, Any],
        regime: str = "unknown",
        symbol: str = "",
    ) -> tuple[str, float, str | None]:
        """
        Score a signal using the meta-labeling model.

        Returns:
            (decision, prob, veto_reason)
            decision: "PASS" | "SHADOW_VETO" | "VETO"
            prob: calibrated P(TP) in [0, 1]
            veto_reason: e.g. "META_LOW_PROB" or None
        """
        state = self._load_model()
        if state is None:
            return "PASS", 0.5, None  # fail-open: no model available

        try:
            from calibration.meta_labeling_model import predict_prob, get_threshold
            prob = predict_prob(indicators, state)
            threshold = get_threshold(state, regime)
        except Exception as e:
            log.debug("meta_labeling_gate evaluate error: %s", e)
            return "PASS", 0.5, None  # fail-open on error

        # Optional: blend with other P(win) sources via ensemble weights.
        # No-op unless ENSEMBLE_WEIGHTS_READ_ENABLED=1 AND ≥2 source probs.
        prob, blend_sources = self._maybe_blend(prob, indicators, symbol)

        above_thr = prob >= threshold

        if above_thr:
            decision = "PASS"
            veto_reason = None
        else:
            if self._is_enabled():
                decision = "VETO"
            else:
                decision = "SHADOW_VETO"  # shadow: score but log, don't block
            veto_reason = _VETO_REASON

        # Metrics
        try:
            c = self._metrics.get("score_total")
            if c:
                c.labels(regime=regime, decision=decision).inc()
            h = self._metrics.get("prob_histogram")
            if h:
                h.labels(regime=regime).observe(prob)
        except Exception:
            pass

        # Sampling-based log
        import random
        if random.random() < self.log_sample:
            log.info(
                "meta_label_gate symbol=%s regime=%s prob=%.3f thr=%.3f decision=%s blend=%s",
                symbol or "?", regime, prob, threshold, decision,
                ",".join(blend_sources.keys()) if blend_sources else "none",
            )

        return decision, prob, veto_reason

    def should_veto(
        self,
        indicators: dict[str, Any],
        regime: str = "unknown",
        symbol: str = "",
    ) -> tuple[bool, float, str | None]:
        """
        Simplified interface: returns (should_veto, prob, reason).

        should_veto=True only when enabled=True AND prob < threshold.
        In SHADOW mode, should_veto is always False (metrics still emitted).
        """
        decision, prob, reason = self.evaluate(indicators, regime, symbol=symbol)
        return decision == "VETO", prob, reason


# ─── Module-level singleton factory ──────────────────────────────────────────

_instance: MetaLabelGate | None = None


def get_gate(rc: Any | None = None) -> MetaLabelGate:
    """
    Return (or create) a module-level MetaLabelGate singleton.
    Pass rc on first call only; subsequent calls return cached instance.
    """
    global _instance
    if _instance is None:
        if rc is None:
            raise RuntimeError("MetaLabelGate not initialized; pass rc on first call")
        _instance = MetaLabelGate(
            rc=rc,
            enabled=_env_bool("META_LABEL_GATE_ENABLED", False),
            model_key=_env("META_LABEL_MODEL_KEY", "meta_label_model:state"),
            model_ttl_sec=_env_int("META_LABEL_MODEL_TTL_SEC", 300),
            default_threshold=_env_float("META_LABEL_THR_DEFAULT", 0.45),
            log_sample=_env_float("META_LABEL_LOG_SAMPLE", 0.05),
        )
    return _instance


def reset_gate() -> None:
    """Reset singleton (for testing)."""
    global _instance
    _instance = None
