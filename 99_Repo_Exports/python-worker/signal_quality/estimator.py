"""
Signal Quality Estimator for assessing signal quality based on historical performance.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

import psycopg2
from psycopg2.extras import DictCursor

if TYPE_CHECKING:
    from scoring.scoring_engine import QualityResult


@dataclass(slots=True)
class QualityEstimate:
    """Quality assessment result for a signal."""

    offline_score: float      # 0..100 from offline historical data
    online_score: float       # 0..100 from recent rolling data
    combined_score: float     # 0..100 weighted combination
    status: str               # 'ok'/'degraded'/'disabled'
    expectancy_r_offline: float
    expectancy_r_online: float


class SignalQualityEstimator:
    """
    Estimates signal quality by combining offline historical data
    with recent online performance.
    """

    def __init__(
        self
        pg_dsn: str
        horizon: str = "R_main"
        w_offline: float = 0.7
        w_online: float = 0.3
    ) -> None:
        """
        Initialize the quality estimator.

        Args:
            pg_dsn: PostgreSQL connection string
            horizon: R horizon to use ('R_main', 'R_30m', etc.)
            w_offline: Weight for offline score (0.0-1.0)
            w_online: Weight for online score (0.0-1.0)
        """
        self._dsn = pg_dsn
        self._horizon = horizon
        self._w_offline = w_offline
        self._w_online = w_online

    def estimate(
        self
        *
        symbol: str
        signal_type: str
        side: str
        session: str
        regime: str
        feature_bucket: str
        conn: Optional[psycopg2.extensions.connection] = None
    ) -> Optional[QualityEstimate]:
        """
        Estimate quality for a signal based on historical performance.

        Args:
            symbol: Trading symbol
            signal_type: Type of signal (breakout_R1, fade_PDH, etc.)
            side: Trade side (buy/sell)
            session: Trading session (asia/europe/us)
            regime: Market regime (trend/range/mixed)
            feature_bucket: Feature cluster bucket
            conn: Optional existing DB connection (allows connection reuse in
                  batch scenarios). A new connection is opened if None.

        Returns:
            QualityEstimate or None if no data available
        """
        _owns_conn = conn is None
        if _owns_conn:
            conn = psycopg2.connect(self._dsn)

        try:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                # 1) Get offline quality by exact feature bucket
                cur.execute(
                    """
                    SELECT
                        quality_score
                        expectancy_r
                    FROM signal_quality_offline
                    WHERE symbol = %s
                      AND signal_type = %s
                      AND side = %s
                      AND COALESCE(session, '') = COALESCE(%s, '')
                      AND COALESCE(regime, '') = COALESCE(%s, '')
                      AND COALESCE(feature_bucket, '') = COALESCE(%s, '')
                      AND horizon = %s
                    """
                    (symbol, signal_type, side, session, regime, feature_bucket, self._horizon)
                )
                row_off = cur.fetchone()

                # 2) Fallback: aggregate across all buckets for this signal type
                if row_off is None or row_off["quality_score"] is None:
                    cur.execute(
                        """
                        SELECT
                            AVG(quality_score) AS quality_score
                            AVG(expectancy_r) AS expectancy_r
                        FROM signal_quality_offline
                        WHERE symbol = %s
                          AND signal_type = %s
                          AND side = %s
                          AND horizon = %s
                        """
                        (symbol, signal_type, side, self._horizon)
                    )
                    row_off = cur.fetchone()

                # Set defaults if no offline data
                if row_off is None or row_off["quality_score"] is None:
                    offline_score = 0.0
                    exp_r_off = 0.0
                else:
                    offline_score = float(row_off["quality_score"])
                    exp_r_off = float(row_off["expectancy_r"] or 0.0)

                # 3) Get online rolling quality
                cur.execute(
                    """
                    SELECT
                        quality_score_online
                        expectancy_r_recent
                        status
                    FROM signal_quality_online
                    WHERE symbol = %s
                      AND signal_type = %s
                      AND side = %s
                      AND horizon = %s
                    """
                    (symbol, signal_type, side, self._horizon)
                )
                row_on = cur.fetchone()

                # Set defaults if no online data
                if row_on is None:
                    online_score = 50.0  # Neutral score
                    exp_r_on = 0.0
                    status = "ok"
                else:
                    online_score = float(row_on["quality_score_online"])
                    exp_r_on = float(row_on["expectancy_r_recent"] or 0.0)
                    status = row_on["status"]

                # 4) Combine offline and online scores
                combined = self._w_offline * offline_score + self._w_online * online_score

                return QualityEstimate(
                    offline_score=offline_score
                    online_score=online_score
                    combined_score=combined
                    status=status
                    expectancy_r_offline=exp_r_off
                    expectancy_r_online=exp_r_on
                )
        finally:
            if _owns_conn:
                conn.close()

    def estimate_quality(
        self
        ctx,  # SignalContext or similar
        base_score: float
        base_confidence: float
    ) -> "QualityResult":
        """
        Estimate signal quality and return QualityResult.

        This is the unified interface for signal quality assessment.
        """
        from scoring.scoring_engine import QualityResult, SignalQualityLabel

        quality_estimate = self.estimate(
            symbol=getattr(ctx, "symbol", "")
            signal_type=getattr(ctx, "signal_type", "unknown")
            side=getattr(ctx, "side", "buy")
            session=getattr(ctx, "session", "")
            regime=getattr(ctx, "regime", "")
            feature_bucket=getattr(ctx, "feature_bucket", "")
        )

        label = SignalQualityLabel.C
        reasons: list[str] = []
        force_reject = False
        adjusted_confidence = base_confidence

        if quality_estimate:
            combined_score = quality_estimate.combined_score

            if combined_score >= 80:
                label = SignalQualityLabel.A
                reasons.append("high_quality_score")
            elif combined_score >= 60:
                label = SignalQualityLabel.B
                reasons.append("medium_quality_score")
            elif combined_score >= 40:
                label = SignalQualityLabel.C
                reasons.append("low_quality_score")
            else:
                label = SignalQualityLabel.REJECT
                reasons.append("very_low_quality_score")
                force_reject = True

            if combined_score < 30:
                adjusted_confidence = min(adjusted_confidence, 0.2)
                force_reject = True
                reasons.append("quality_too_low")
        else:
            reasons.append("no_quality_data")

        return QualityResult(
            confidence=adjusted_confidence
            label=label
            reasons=reasons
            force_reject=force_reject
        )
