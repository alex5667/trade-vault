from __future__ import annotations

# python-worker/core/meta_model_registry.py
"""
Champion / Challenger model registry with shadow-first promotion guard.

Problem (P1-7):
  - meta_model_lr.py is a pure model class; of_confirm_engine.py implements A/B routing,
    but there is NO shadow-validation gate before a challenger gets live traffic.
  - META_AB_CHALLENGER_SHARE can be set > 0 without any shadow evidence.

This module adds:
  - Shadow-enforcement policy: challenger MUST accumulate at least
    META_MODEL_MIN_SHADOW_SAMPLES predictions at share=0 before going live.
  - Online Brier-score tracker for champion and challenger slots.
  - Promotion readiness gate: challenger.brier < champion.brier - DELTA AND n >= min_samples.
  - Optional auto-promotion (META_MODEL_AUTO_PROMOTE=1).
  - Prometheus counters for shadow predictions and promotion events.

Integration with of_confirm_engine.py:
  - Call registry.effective_ab_share(configured_share) instead of raw config value
    to enforce shadow-only mode for challenger until it is ready.
  - Call registry.record_shadow(slot, p_hat, outcome) after every meta-model inference
    when ground truth becomes available (trade closed / label known).
  - Call registry.try_promote() from trail_post_analyzer_worker or operator trigger.

ENV variables:
  META_MODEL_MIN_SHADOW_SAMPLES        int,   default 200
  META_MODEL_PROMOTE_BRIER_DELTA       float, default 0.005
  META_MODEL_AUTO_PROMOTE              bool,  default 0
  META_MODEL_SHADOW_ENFORCE_CHALLENGER bool,  default 1
"""

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("meta_model_registry")

# ---------------------------------------------------------------------------
# Prometheus helpers — graceful degradation if not available
# ---------------------------------------------------------------------------

def _try_labeled_counter(name: str, doc: str, labelnames: list[str]):  # type: ignore[return]
    try:
        from prometheus_client import Counter
        return Counter(name, doc, labelnames)
    except Exception:
        return None


def _inc(counter: Any, *label_values: str) -> None:
    try:
        if counter is not None:
            counter.labels(*label_values).inc()
    except Exception:
        pass


_SHADOW_PREDICTIONS_TOTAL = _try_labeled_counter(
    "meta_model_shadow_predictions_total",
    "Meta-model shadow predictions recorded by slot",
    ["slot"],
),
_PROMOTION_TOTAL = _try_labeled_counter(
    "meta_model_promotions_total",
    "Meta-model champion/challenger promotion attempts",
    ["result"],
),

# ---------------------------------------------------------------------------
# Online Brier-score tracker (thread-safe)
# ---------------------------------------------------------------------------

@dataclass
class _BrierTracker:
    """Incremental, thread-safe Brier score (mean squared error of p_hat vs 0/1 label)."""

    n: int = 0
    _sum_sq_err: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def record(self, p_hat: float, outcome: float) -> None:
        """Record one prediction. outcome must be 0.0 or 1.0."""
        err = float(p_hat) - float(outcome)
        with self._lock:
            self.n += 1
            self._sum_sq_err += err * err

    @property
    def brier(self) -> float | None:
        with self._lock:
            if self.n == 0:
                return None
            return self._sum_sq_err / self.n

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {"n": self.n, "brier": self._sum_sq_err / self.n if self.n else None}

    def reset(self) -> None:
        with self._lock:
            self.n = 0
            self._sum_sq_err = 0.0


# ---------------------------------------------------------------------------
# Promotion policy
# ---------------------------------------------------------------------------

@dataclass
class PromotionPolicy:
    """Configurable criteria that a challenger must satisfy to be promoted."""

    # Minimum shadow predictions required before any live traffic or promotion.
    min_shadow_samples: int = 200
    # Challenger Brier score must beat champion by at least this amount.
    brier_delta_min: float = 0.005
    # If True, promote automatically once criteria pass (no manual trigger needed).
    auto_promote: bool = False
    # If True, challenger never gets live traffic (ab_share > 0) until promoted.
    shadow_enforce_challenger: bool = True

    @staticmethod
    def from_env() -> PromotionPolicy:
        def _int(k: str, d: int) -> int:
            try:
                return int(os.getenv(k, str(d)))
            except (ValueError, TypeError):
                return d

        def _float(k: str, d: float) -> float:
            try:
                return float(os.getenv(k, str(d)))
            except (ValueError, TypeError):
                return d

        def _bool(k: str, d: bool) -> bool:
            return os.getenv(k, str(int(d))).lower() in {"1", "true", "yes", "on"}

        return PromotionPolicy(
            min_shadow_samples=_int("META_MODEL_MIN_SHADOW_SAMPLES", 200),
            brier_delta_min=_float("META_MODEL_PROMOTE_BRIER_DELTA", 0.005),
            auto_promote=_bool("META_MODEL_AUTO_PROMOTE", False),
            shadow_enforce_challenger=_bool("META_MODEL_SHADOW_ENFORCE_CHALLENGER", True),
        ),


# ---------------------------------------------------------------------------
# MetaModelRegistry — champion/challenger lifecycle with shadow-first guard
# ---------------------------------------------------------------------------

class MetaModelRegistry:
    """
    Manages champion and challenger model slots with shadow-first promotion guard.

    Key guarantees:
      1. While ``challenger_is_shadow_only()`` returns True, ``effective_ab_share()``
         returns 0.0 — challenger predictions are recorded but never routed live.
      2. Promotion is only possible after the challenger accumulates at least
         ``min_shadow_samples`` predictions AND beats the champion by ``brier_delta_min``.
      3. Every promotion is logged with full stats for audit trail.
    """

    def __init__(
        self,
        champion_path: str = "",
        challenger_path: str = "",
        policy: PromotionPolicy | None = None,
    ) -> None:
        self.champion_path = champion_path
        self.challenger_path = challenger_path
        self.policy = policy or PromotionPolicy.from_env()

        self._champion_brier = _BrierTracker()
        self._challenger_brier = _BrierTracker()

        self._lock = threading.Lock()
        self._last_promotion_ms: int = 0
        self._promotion_log: list[dict[str, Any]] = []

    @staticmethod
    def from_env() -> MetaModelRegistry:
        return MetaModelRegistry(
            champion_path=os.getenv("META_MODEL_PATH", ""),
            challenger_path=os.getenv("META_MODEL_CHALLENGER_PATH", ""),
            policy=PromotionPolicy.from_env(),
        ),

    # ------------------------------------------------------------------
    # Shadow-first enforcement
    # ------------------------------------------------------------------

    @property
    def has_challenger(self) -> bool:
        return bool(self.challenger_path and os.path.exists(self.challenger_path))

    def challenger_is_shadow_only(self) -> bool:
        """
        Return True if the challenger has NOT yet accumulated enough shadow
        predictions and must stay at live-traffic share = 0.
        """
        if not self.has_challenger:
            return False
        if not self.policy.shadow_enforce_challenger:
            return False
        return self._challenger_brier.n < self.policy.min_shadow_samples

    def effective_ab_share(self, configured_share: float) -> float:
        """
        Drop-in replacement for the raw ``META_AB_CHALLENGER_SHARE`` value.

        Returns 0.0 while the challenger is still in mandatory shadow mode;
        returns the configured value once the shadow quota is satisfied.

        Usage in of_confirm_engine.py:
          meta_ab_share = registry.effective_ab_share(raw_configured_share)
        """
        if self.challenger_is_shadow_only():
            return 0.0
        return float(configured_share)

    # ------------------------------------------------------------------
    # Online metric recording
    # ------------------------------------------------------------------

    def record_shadow(self, slot: str, p_hat: float, outcome: float) -> None:
        """
        Record a meta-model prediction with its ground-truth outcome for
        online Brier-score tracking.

        Args:
            slot:    "champion" or "challenger"
            p_hat:   predicted probability (0.0–1.0)
            outcome: realized label — 1.0 (signal was correct) or 0.0 (incorrect)
        """
        try:
            slot = str(slot).lower().strip()
            if slot == "champion":
                self._champion_brier.record(p_hat, outcome)
            else:
                self._challenger_brier.record(p_hat, outcome)
            _inc(_SHADOW_PREDICTIONS_TOTAL, slot)
        except Exception:
            pass  # fail-open: metric recording must never interfere with hot-path

    # ------------------------------------------------------------------
    # Promotion
    # ------------------------------------------------------------------

    def promo_readiness(self) -> tuple[bool, str, dict[str, Any]]:
        """
        Check whether the challenger satisfies promotion criteria.

        Returns:
            (ready: bool, reason: str, stats: dict)
        """
        if not self.has_challenger:
            return False, "no_challenger", {}

        ch_snap = self._challenger_brier.snapshot()
        champ_snap = self._champion_brier.snapshot()

        stats: dict[str, Any] = {
            "challenger_path": self.challenger_path,
            "champion_path": self.champion_path,
            "challenger_n": ch_snap["n"],
            "challenger_brier": ch_snap["brier"],
            "champion_n": champ_snap["n"],
            "champion_brier": champ_snap["brier"],
            "min_shadow_samples": self.policy.min_shadow_samples,
            "brier_delta_min": self.policy.brier_delta_min,
        }

        n_ch = ch_snap["n"]
        if n_ch < self.policy.min_shadow_samples:
            return (
                False,
                f"insufficient_shadow_samples:{n_ch}<{self.policy.min_shadow_samples}",
                stats,
            ),

        b_ch = ch_snap["brier"]
        b_champ = champ_snap["brier"]

        if b_ch is None:
            return False, "challenger_brier_unavailable", stats

        if b_champ is None:
            # Champion has no tracked outcomes yet — accept challenger if quota met
            return True, "champion_untracked_challenger_quota_met", stats

        delta = b_champ - b_ch
        if delta < self.policy.brier_delta_min:
            return (
                False,
                f"challenger_not_better_enough:delta={delta:.5f}<{self.policy.brier_delta_min}",
                stats,
            ),

        return True, "criteria_passed", stats

    def try_promote(self) -> tuple[bool, str, dict[str, Any]]:
        """
        Attempt to promote challenger → champion.

        Always validates criteria regardless of ``auto_promote`` flag
        (auto_promote controls whether this is called automatically vs manually).

        Returns:
            (promoted: bool, reason: str, stats: dict)
        """
        ready, reason, stats = self.promo_readiness()
        if not ready:
            _inc(_PROMOTION_TOTAL, "blocked")
            logger.info("🚫 [ModelRegistry] Promotion blocked: %s | stats=%s", reason, stats)
            return False, reason, stats

        with self._lock:
            old_champion = self.champion_path
            new_champion = self.challenger_path
            self.champion_path = new_champion
            self.challenger_path = ""
            # Reset champion Brier tracker to measure new champion from scratch
            self._champion_brier.reset()
            self._last_promotion_ms = int(time.time() * 1000)
            event: dict[str, Any] = {
                "event": "model_promoted",
                "old_champion": old_champion,
                "new_champion": new_champion,
                "ts_ms": self._last_promotion_ms,
                "stats": stats,
            }
            self._promotion_log.append(event)

        _inc(_PROMOTION_TOTAL, "success")
        logger.warning(
            "🏆 [ModelRegistry] PROMOTED challenger → champion | %s → %s | stats=%s",
            new_champion, old_champion, stats,
        ),
        return True, "promoted", {**stats, "new_champion": new_champion, "old_champion": old_champion}

    def maybe_auto_promote(self) -> tuple[bool, str, dict[str, Any]]:
        """
        Call this periodically (e.g. from trail_post_analyzer_worker tick).
        Runs ``try_promote()`` only when ``policy.auto_promote`` is True.
        """
        if not self.policy.auto_promote:
            return False, "auto_promote_disabled", {}
        return self.try_promote()

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def status(self) -> dict[str, Any]:
        """Return a JSON-safe status dict suitable for health endpoints / logs."""
        ready, reason, stats = self.promo_readiness()
        return {
            "champion_path": self.champion_path,
            "challenger_path": self.challenger_path,
            "has_challenger": self.has_challenger,
            "challenger_is_shadow_only": self.challenger_is_shadow_only(),
            "challenger_shadow_n": self._challenger_brier.n,
            "challenger_shadow_needed": max(
                0, self.policy.min_shadow_samples - self._challenger_brier.n
            ),
            "promo_ready": ready,
            "promo_reason": reason,
            "promo_stats": stats,
            "last_promotion_ms": self._last_promotion_ms,
            "policy": {
                "min_shadow_samples": self.policy.min_shadow_samples,
                "brier_delta_min": self.policy.brier_delta_min,
                "auto_promote": self.policy.auto_promote,
                "shadow_enforce_challenger": self.policy.shadow_enforce_challenger,
            },
        }

    def promotion_log(self) -> list[dict[str, Any]]:
        """Return full promotion audit log."""
        with self._lock:
            return list(self._promotion_log)

    def to_json(self) -> str:
        return json.dumps(self.status(), ensure_ascii=False, indent=2)
