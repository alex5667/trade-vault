from __future__ import annotations

"""
Signal Ensemble — weighted voting layer across independent signal sources.

Sources:
  - orderflow:      CVD z_delta + DOM cluster (from _compose_confidence)
  - ta_indicators:  RSI / ADX / EMA from Go gateway Redis keys
  - microstructure: MicrostructureSpikeDetectorPro metrics (spread, OBI, book_churn)
  - regime_filter:  MarketRegimeService TREND/RANGE/MIXED/UNKNOWN

Algorithm:
  1. Collect SignalVote from each source
  2. Load dynamic weights from Redis (weights:ensemble:{symbol})
  3. Veto check: any source with veto=True → skip
  4. Weighted consensus: long_score vs short_score with configurable threshold
  5. Shadow mode: log only; enforce mode: gate signal emission

Weights are recalculated hourly by ensemble_weight_calibrator.py based on 30-day
OOS Sharpe ratio per source.
"""
import json
import logging
from dataclasses import asdict, dataclass, field
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib

try:
    import redis as redis_lib
except ImportError:
    redis_lib = None

log = logging.getLogger("signal_ensemble")


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class SignalVote:
    """Vote from a single independent signal source."""

    source: str        # "orderflow" | "ta_indicators" | "microstructure" | "regime_filter"
    direction: str     # "long" | "short" | "neutral"
    confidence: float  # 0..1, raw confidence from the source
    veto: bool = False       # hard block
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.direction not in ("long", "short", "neutral"):
            raise ValueError(f"Invalid direction: {self.direction!r}")
        self.confidence = max(0.0, min(1.0, float(self.confidence)))


@dataclass
class EnsembleDecision:
    """Result of ensemble voting."""

    action: str       # "long" | "short" | "skip"
    score: float = 0.0
    reason: str = ""
    votes: dict[str, Any] = field(default_factory=dict)
    shadow: bool = True  # True = audit-only, not used for signaling

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Source builder helpers — extract SignalVote from existing detector outputs
# ---------------------------------------------------------------------------

def build_orderflow_vote(
    conf: float,
    side_hint: str | None,
    conf_parts: dict[str, float],
) -> SignalVote:
    """
    Build an orderflow vote from _compose_confidence output.
    conf: 0..1 blended confidence
    side_hint: "LONG" | "SHORT" | None
    """
    if side_hint is None or conf < 0.05:
        direction = "neutral"
    else:
        direction = "long" if side_hint.upper() == "LONG" else "short"

    return SignalVote(
        source="orderflow",
        direction=direction,
        confidence=conf,
        veto=False,
        meta={"parts": conf_parts},
    )


def build_ta_vote(
    r: Any,
    symbol: str,
) -> SignalVote:
    """
    Read TA indicators from Redis (published by Go gateway) and produce a vote.

    Keys checked:
      - ta:last:rsi:{symbol}   → JSON {"rsi": float, ...}
      - ta:last:adx:{symbol}   → JSON {"adx": float, "pdi": float, "mdi": float, ...}
      - ta:last:ema:{symbol}   → JSON {"ema_fast": float, "ema_slow": float, ...}

    If keys are missing, returns neutral vote (TA source not yet populated).
    """
    meta: dict[str, Any] = {}
    direction = "neutral"
    confidence = 0.0
    signals_bullish = 0
    signals_bearish = 0
    total_signals = 0

    # --- RSI ---
    try:
        raw = r.get(f"ta:last:rsi:{symbol}")
        if raw:
            data = json.loads(raw)
            rsi = float(data.get("rsi", 50.0))
            meta["rsi"] = rsi
            total_signals += 1
            if rsi < 30:
                signals_bullish += 1  # oversold → long bias
            elif rsi > 70:
                signals_bearish += 1  # overbought → short bias
    except Exception:
        pass

    # --- ADX + DI ---
    try:
        raw = r.get(f"ta:last:adx:{symbol}")
        if raw:
            data = json.loads(raw)
            adx = float(data.get("adx", 0.0))
            pdi = float(data.get("pdi", 0.0))
            mdi = float(data.get("mdi", 0.0))
            meta["adx"] = adx
            meta["pdi"] = pdi
            meta["mdi"] = mdi
            if adx > 25:  # trending
                total_signals += 1
                if pdi > mdi:
                    signals_bullish += 1
                elif mdi > pdi:
                    signals_bearish += 1
    except Exception:
        pass

    # --- EMA crossover ---
    try:
        raw = r.get(f"ta:last:ema:{symbol}")
        if raw:
            data = json.loads(raw)
            ema_fast = float(data.get("ema_fast", 0.0))
            ema_slow = float(data.get("ema_slow", 0.0))
            if ema_fast > 0 and ema_slow > 0:
                meta["ema_fast"] = ema_fast
                meta["ema_slow"] = ema_slow
                total_signals += 1
                if ema_fast > ema_slow:
                    signals_bullish += 1
                else:
                    signals_bearish += 1
    except Exception:
        pass

    if total_signals == 0:
        return SignalVote(
            source="ta_indicators",
            direction="neutral",
            confidence=0.0,
            veto=False,
            meta={"status": "no_ta_keys"},
        )

    # Determine direction and confidence from majority
    if signals_bullish > signals_bearish:
        direction = "long"
        confidence = signals_bullish / total_signals
    elif signals_bearish > signals_bullish:
        direction = "short"
        confidence = signals_bearish / total_signals
    else:
        direction = "neutral"
        confidence = 0.0

    meta["bullish"] = signals_bullish
    meta["bearish"] = signals_bearish
    meta["total"] = total_signals

    return SignalVote(
        source="ta_indicators",
        direction=direction,
        confidence=confidence,
        veto=False,
        meta=meta,
    )


def build_microstructure_vote(
    m_pro: dict[str, Any],
    m_legacy: dict[str, Any],
    cl: Any,
) -> SignalVote:
    """
    Build microstructure vote from detector outputs.

    Uses z_delta direction + magnitude, OBI sustained, book churn.
    """
    z_delta = float(m_pro.get("z_delta", 0.0))
    z_speed = float(m_pro.get("z_speed", 0.0))
    z_abs = abs(z_delta)

    # OBI + book_churn from pro metrics
    obi_avg = float(m_pro.get("obi_avg", 0.0))
    book_churn = float(m_pro.get("book_churn", 0.0))

    # cluster DOM info
    cl_conf = 0.0
    if cl and isinstance(cl, dict):
        cl_conf = float(cl.get("cluster_score", 0.0)) / 100.0

    # Direction from z_delta
    if z_abs < 1.0:
        direction = "neutral"
    elif z_delta > 0:
        direction = "long"
    else:
        direction = "short"

    # Confidence: sigmoid of |z_delta| magnitude, boosted by OBI confirmation
    from math import exp
    sig_z = 1.0 / (1.0 + exp(-1.0 * (z_abs - 2.0)))  # center at z=2
    obi_boost = 0.0
    if abs(obi_avg) > 0.3:
        obi_confirms = (obi_avg > 0 and z_delta > 0) or (obi_avg < 0 and z_delta < 0)
        if obi_confirms:
            obi_boost = 0.15

    confidence = min(1.0, sig_z + obi_boost + cl_conf * 0.2)

    # Veto on extreme book churn (market maker fading)
    veto = False
    if book_churn > 5.0 and z_abs < 2.5:
        veto = True  # high churn + weak impulse → unreliable

    meta = {
        "z_delta": z_delta,
        "z_speed": z_speed,
        "obi_avg": obi_avg,
        "book_churn": book_churn,
        "cl_conf": cl_conf,
    }

    return SignalVote(
        source="microstructure",
        direction=direction,
        confidence=confidence,
        veto=veto,
        meta=meta,
    )


def build_regime_vote(
    r: Any,
    symbol: str,
    side_hint: str | None = None,
) -> SignalVote:
    """
    Read regime state from Redis and produce a vote.

    Redis key: regime:state:{symbol} → JSON with label, trend_score, range_score

    - TREND → votes with side_hint direction, high confidence
    - RANGE → votes neutral (not veto, allows other sources to decide)
    - MIXED → votes neutral with low confidence
    - UNKNOWN → veto (insufficient data to decide)
    """
    meta: dict[str, Any] = {}
    direction = "neutral"
    confidence = 0.0
    veto = False

    try:
        raw = r.get(f"regime:state:{symbol}")
        if raw:
            data = json.loads(raw)
            label = (data.get("label", "unknown")).lower()
            trend_score = float(data.get("trend_score", 0.0))
            range_score = float(data.get("range_score", 0.0))
            meta["label"] = label
            meta["trend_score"] = trend_score
            meta["range_score"] = range_score

            if label in ("trending", "trend"):
                # Strong directional signal — use side_hint
                if side_hint and side_hint.upper() in ("LONG", "SHORT"):
                    direction = side_hint.lower()
                    confidence = min(1.0, trend_score * 1.2)
                else:
                    direction = "neutral"
                    confidence = trend_score * 0.5
            elif label == "range":
                # Range regime: neutral (not veto — allows other sources)
                direction = "neutral"
                confidence = 0.0
                meta["note"] = "range_regime_neutral"
            elif label == "mixed":
                direction = "neutral"
                confidence = 0.0
            elif label == "unknown":
                veto = True
                meta["note"] = "unknown_regime_veto"
            else:
                direction = "neutral"
                confidence = 0.0
        else:
            # No regime state available → neutral (not veto, avoids blocking on cold start)
            meta["status"] = "no_regime_key"
    except Exception as e:
        meta["error"] = str(e)

    return SignalVote(
        source="regime_filter",
        direction=direction,
        confidence=confidence,
        veto=veto,
        meta=meta,
    )


# ---------------------------------------------------------------------------
# Core Ensemble
# ---------------------------------------------------------------------------

class SignalEnsemble:
    """
    Weighted voting across independent signal sources.

    Weights are dynamic — based on 30-day OOS Sharpe per source, recalculated
    hourly by ensemble_weight_calibrator.py.  Until weights are available,
    equal weights (0.25) are used.
    """

    SOURCES: list[str] = ["orderflow", "ta_indicators", "microstructure", "regime_filter"]

    # Default equal weights (cold-start / bootstrap)
    DEFAULT_WEIGHT: float = 0.25

    def __init__(
        self,
        redis_client: Any,
        symbol: str,
        *,
        mode: str = "shadow",
        threshold: float = 0.35,
        consensus_ratio: float = 1.5,
        logger: logging.Logger | None = None,
    ) -> None:
        self.r = redis_client
        self.symbol = symbol
        self.mode = mode.lower()  # "shadow" | "enforce" | "disabled"
        self.threshold = threshold
        self.consensus_ratio = consensus_ratio
        self._log = logger or log
        self._decision_count = 0

    def vote(self, signals: dict[str, SignalVote | None]) -> EnsembleDecision:
        """
        Synchronous voting — called from hub step() on each tick that passes
        the confidence threshold.

        Args:
            signals: dict mapping source name → SignalVote (or None if source unavailable)

        Returns:
            EnsembleDecision with action, score, reason, and per-source votes
        """
        shadow = self.mode != "enforce"
        weights = self._get_dynamic_weights()

        vote_details: dict[str, Any] = {}
        active_votes: dict[str, dict[str, Any]] = {}

        for source in self.SOURCES:
            sig = signals.get(source)
            if sig is None:
                vote_details[source] = {"status": "unavailable"}
                continue

            w = weights.get(source, self.DEFAULT_WEIGHT)
            vote_details[source] = {
                "direction": sig.direction,
                "confidence": round(sig.confidence, 4),
                "weight": round(w, 4),
                "weighted_confidence": round(sig.confidence * w, 4),
                "veto": sig.veto,
                "meta": sig.meta,
            }
            active_votes[source] = vote_details[source]

        # --- Veto check ---
        veto_sources = [s for s, v in active_votes.items() if v.get("veto")]
        if veto_sources:
            decision = EnsembleDecision(
                action="skip",
                score=0.0,
                reason=f"veto:{','.join(veto_sources)}",
                votes=vote_details,
                shadow=shadow,
            )
            self._log_decision(decision)
            return decision

        # --- Weighted scoring ---
        long_score = sum(
            v["weighted_confidence"]
            for v in active_votes.values()
            if v["direction"] == "long"
        )
        short_score = sum(
            v["weighted_confidence"]
            for v in active_votes.values()
            if v["direction"] == "short"
        )

        # Load per-symbol threshold (allows live tuning via Redis)
        threshold = self._get_threshold()

        # Consensus check
        action = "skip"
        score = 0.0
        reason = "no_consensus"

        if long_score > threshold and long_score > short_score * self.consensus_ratio:
            action = "long"
            score = long_score
            reason = f"long_consensus(score={long_score:.3f},thr={threshold:.3f})"
        elif short_score > threshold and short_score > long_score * self.consensus_ratio:
            action = "short"
            score = short_score
            reason = f"short_consensus(score={short_score:.3f},thr={threshold:.3f})"
        else:
            reason = f"no_consensus(long={long_score:.3f},short={short_score:.3f},thr={threshold:.3f})"

        decision = EnsembleDecision(
            action=action,
            score=round(score, 4),
            reason=reason,
            votes=vote_details,
            shadow=shadow,
        )

        self._log_decision(decision)
        return decision

    def _get_dynamic_weights(self) -> dict[str, float]:
        """Load per-source weights from Redis. Falls back to equal weights."""
        try:
            raw = self.r.hgetall(f"weights:ensemble:{self.symbol}")
            if raw and len(raw) > 0:
                return {
                    str(k): float(v)
                    for k, v in raw.items()
                    if str(k) in self.SOURCES
                },
        except Exception:
            pass
        return dict.fromkeys(self.SOURCES, self.DEFAULT_WEIGHT)

    def _get_threshold(self) -> float:
        """Load per-symbol threshold from Redis, fallback to self.threshold."""
        try:
            val = self.r.get(f"threshold:ensemble:{self.symbol}")
            if val:
                return float(val)
        except Exception:
            pass
        return self.threshold

    def _log_decision(self, decision: EnsembleDecision) -> None:
        """Log the ensemble decision. In shadow mode, write to Redis stream for analysis."""
        self._decision_count += 1

        # Always log significant decisions
        if decision.action != "skip" or self._decision_count % 1000 == 0:
            self._log.info(
                "🎯 Ensemble[%s] #%d: action=%s score=%.3f reason=%s shadow=%s",
                self.symbol,
                self._decision_count,
                decision.action,
                decision.score,
                decision.reason[:100],
                decision.shadow,
            )

        # Write to shadow log stream for offline analysis
        if decision.shadow:
            try:
                self.r.xadd(
                    f"ensemble:shadow_log:{self.symbol}",
                    {
                        "ts": str(get_ny_time_millis()),
                        "action": decision.action,
                        "score": str(decision.score),
                        "reason": decision.reason,
                        "votes": json.dumps(decision.votes, default=str),
                    },
                    maxlen=5000,
                    approximate=True,
                )
            except Exception:
                pass  # non-critical; don't block hot-path

        # Store last decision for debug reads
        with contextlib.suppress(Exception):
            self.r.set(
                f"ensemble:last_decision:{self.symbol}",
                json.dumps(decision.to_dict(), default=str),
                ex=300,
            )
