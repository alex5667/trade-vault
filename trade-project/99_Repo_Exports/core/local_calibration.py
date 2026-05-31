"""
Local Calibration Store for metric calibration.

Provides calibration data for metrics based on symbol/regime/session/side.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, Optional

# Optional import for psycopg2
try:
    import psycopg2
    from psycopg2.extras import DictCursor
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


@dataclass(frozen=True)
class LocalKey:
    """Key for local calibration lookup."""
    symbol: str
    regime: Optional[str]
    session: Optional[str]
    side: Optional[str]


@dataclass
class MetricCalibration:
    """Calibration data for a single metric."""
    value: float
    is_extreme: bool
    threshold: float
    quantile: Optional[float] = None
    p50: Optional[float] = None
    p75: Optional[float] = None
    p90: Optional[float] = None


class LocalCalibrationStore:
    """
    Store for local calibration data.

    Provides calibration for metrics based on symbol/regime/session/side combinations.
    """

    def __init__(self) -> None:
        # key -> metric_name -> calibration_dict
        self._by_key: Dict[LocalKey, Dict[str, Dict[str, float]]] = {}

    def load_from_db(self, pg_dsn: str) -> None:
        """
        Load calibration data from database.

        Expected table structure:
        symbol, regime, session, side, metric, p50, p75, p90, extreme_threshold, samples_count
        """
        if not PSYCOPG2_AVAILABLE:
            print("⚠️ psycopg2 not available, skipping database load")
            return

        try:
            conn = psycopg2.connect(pg_dsn)
            cursor = conn.cursor(cursor_factory=DictCursor)

            # Query calibration data
            cursor.execute("""
                SELECT
                    symbol,
                    regime,
                    session,
                    side,
                    metric,
                    p50,
                    p75,
                    p90,
                    extreme_threshold,
                    samples_count
                FROM local_calibration
                WHERE samples_count > 100  -- minimum sample size
                ORDER BY symbol, regime, session, side, metric
            """)

            data: Dict[LocalKey, Dict[str, Dict[str, float]]] = {}

            for row in cursor.fetchall():
                key = LocalKey(
                    symbol=row['symbol'],
                    regime=row['regime'],
                    session=row['session'],
                    side=row['side']
                )

                if key not in data:
                    data[key] = {}

                data[key][row['metric']] = {
                    'p50': float(row['p50']) if row['p50'] is not None else None,
                    'p75': float(row['p75']) if row['p75'] is not None else None,
                    'p90': float(row['p90']) if row['p90'] is not None else None,
                    'extreme': float(row['extreme_threshold']) if row['extreme_threshold'] is not None else None,
                    'samples': int(row['samples_count'])
                }

            self._by_key = data
            print(f"✅ Loaded {len(data)} calibration keys from database")

        except Exception as e:
            print(f"❌ Error loading calibration from database: {e}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()

    def get_metric_calibration(
        self,
        key: LocalKey,
        metric: str,
    ) -> Optional[Dict[str, float]]:
        """
        Get calibration data for a specific metric.

        Returns dict with keys: p50, p75, p90, extreme, samples
        """
        # Try exact match first
        metrics = self._by_key.get(key)
        if metrics and metric in metrics:
            return metrics[metric]

        # Try fallback: same symbol/session, any regime
        fallback_key = LocalKey(
            symbol=key.symbol,
            regime=None,
            session=key.session,
            side=None
        )
        metrics = self._by_key.get(fallback_key)
        if metrics and metric in metrics:
            return metrics[metric]

        # Try fallback: same symbol, any session/regime
        fallback_key2 = LocalKey(
            symbol=key.symbol,
            regime=None,
            session=None,
            side=None
        )
        metrics = self._by_key.get(fallback_key2)
        if metrics and metric in metrics:
            return metrics[metric]

        return None

    def calibrate_metric(
        self,
        key: LocalKey,
        metric: str,
        raw_value: float,
        default_extreme_threshold: float = 2.0,
    ) -> MetricCalibration:
        """
        Calibrate a raw metric value using local calibration data.
        """
        calib = self.get_metric_calibration(key, metric)

        if not calib:
            # No calibration data - use defaults
            return MetricCalibration(
                value=raw_value,
                is_extreme=abs(raw_value) >= default_extreme_threshold,
                threshold=default_extreme_threshold,
                quantile=None,
                p50=None,
                p75=None,
                p90=None
            )

        # Use calibrated thresholds
        threshold = calib.get('extreme', default_extreme_threshold)
        is_extreme = abs(raw_value) >= threshold

        # Calculate quantile (simple interpolation)
        p50 = calib.get('p50')
        p75 = calib.get('p75')
        p90 = calib.get('p90')

        quantile = None
        if p50 is not None and p75 is not None and p90 is not None:
            abs_value = abs(raw_value)
            if abs_value <= p50:
                quantile = 0.5 * (abs_value / max(p50, 1e-9))
            elif abs_value <= p75:
                quantile = 0.5 + 0.25 * (abs_value - p50) / max(p75 - p50, 1e-9)
            elif abs_value <= p90:
                quantile = 0.75 + 0.15 * (abs_value - p75) / max(p90 - p75, 1e-9)
            else:
                quantile = 0.9 + 0.1 * min((abs_value - p90) / max(threshold - p90, 1e-9), 1.0)

        return MetricCalibration(
            value=raw_value,
            is_extreme=is_extreme,
            threshold=threshold,
            quantile=quantile,
            p50=p50,
            p75=p75,
            p90=p90
        )
