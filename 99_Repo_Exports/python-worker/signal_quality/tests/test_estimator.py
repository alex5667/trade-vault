"""
Unit tests for signal_quality.estimator — uses mocked psycopg2 (no DB).

Key design:
- The estimator calls `with conn.cursor(cursor_factory=DictCursor) as cur:` which
  invokes conn.cursor(...) → returns cursor_cm → cursor_cm.__enter__() → cursor.
  Our _make_mock_conn helper ensures this chain works correctly.
- fetchone call count depends on the code path:
  * Exact bucket hit (quality_score not None): 2 fetchone calls (offline + online)
  * Exact bucket miss (None): 3 fetchone calls (exact, fallback aggregate, online)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_quality.estimator import SignalQualityEstimator, QualityEstimate


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _make_estimator(**kwargs):
    defaults = dict(pg_dsn="postgresql://mock/db", horizon="R_main", w_offline=0.7, w_online=0.3)
    defaults.update(kwargs)
    return SignalQualityEstimator(**defaults)


def _make_mock_conn(fetchone_sequence):
    """
    Build a mock psycopg2 connection whose cursor context manager returns a
    configured cursor.

    Pattern:
      conn.cursor(...) → cursor_cm
      with cursor_cm:   → cursor_cm.__enter__() → cursor
      cursor.fetchone() → items from fetchone_sequence (in order)
    """
    cursor = MagicMock()
    cursor.fetchone.side_effect = list(fetchone_sequence)

    cursor_cm = MagicMock()
    cursor_cm.__enter__ = MagicMock(return_value=cursor)
    cursor_cm.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cursor_cm

    return conn, cursor


CALL_KWARGS = dict(
    symbol="BTCUSDT",
    signal_type="breakout_R1",
    side="buy",
    session="us",
    regime="trend",
    feature_bucket="dz:<1.5|obi:<1.0|wp:<0.3|atr:<0.7",
)


# ──────────────────────────────────────────────
# QualityEstimate dataclass
# ──────────────────────────────────────────────

class TestQualityEstimate:
    def test_slots_present(self):
        assert hasattr(QualityEstimate, "__slots__")

    def test_instantiation(self):
        qe = QualityEstimate(
            offline_score=80.0,
            online_score=70.0,
            combined_score=77.0,
            status="ok",
            expectancy_r_offline=1.2,
            expectancy_r_online=0.9,
        )
        assert qe.combined_score == pytest.approx(77.0)
        assert qe.status == "ok"

    def test_fields_accessible(self):
        qe = QualityEstimate(10.0, 20.0, 13.0, "degraded", 0.5, 0.3)
        assert qe.offline_score == 10.0
        assert qe.expectancy_r_offline == 0.5


# ──────────────────────────────────────────────
# SignalQualityEstimator.estimate
# ──────────────────────────────────────────────

class TestSignalQualityEstimatorEstimate:

    def test_no_data_defaults_applied(self):
        """Exact=None, fallback=None, online=None → offline=0.0, online=50.0 (neutral)."""
        # 3 fetchone calls because exact bucket misses → fallback query fires
        est = _make_estimator()
        conn, _ = _make_mock_conn([None, None, None])

        result = est.estimate(**CALL_KWARGS, conn=conn)
        assert isinstance(result, QualityEstimate)
        assert result.offline_score == pytest.approx(0.0)
        assert result.online_score == pytest.approx(50.0)

    def test_full_data_combined_score(self):
        """Exact bucket HIT → 2 fetchone calls (no fallback); combined = 0.7*80 + 0.3*60."""
        est = _make_estimator(w_offline=0.7, w_online=0.3)
        off_row = {"quality_score": 80.0, "expectancy_r": 1.5}
        on_row = {"quality_score_online": 60.0, "expectancy_r_recent": 0.8, "status": "ok"}
        # Exact hit: [off_row, on_row] — only 2 calls
        conn, _ = _make_mock_conn([off_row, on_row])

        result = est.estimate(**CALL_KWARGS, conn=conn)
        assert result is not None
        assert result.offline_score == pytest.approx(80.0)
        assert result.online_score == pytest.approx(60.0)
        assert result.combined_score == pytest.approx(0.7 * 80.0 + 0.3 * 60.0)
        assert result.status == "ok"

    def test_offline_fallback_used_when_bucket_miss(self):
        """Bucket miss → [None(exact), fallback_row, on_row] = 3 fetchone calls."""
        est = _make_estimator()
        fallback_row = {"quality_score": 55.0, "expectancy_r": 0.7}
        on_row = {"quality_score_online": 50.0, "expectancy_r_recent": 0.5, "status": "ok"}
        conn, _ = _make_mock_conn([None, fallback_row, on_row])

        result = est.estimate(**CALL_KWARGS, conn=conn)
        assert result is not None
        assert result.offline_score == pytest.approx(55.0)

    def test_conn_not_closed_when_caller_owns(self):
        """When caller passes conn, estimator must NOT close it."""
        est = _make_estimator()
        conn, _ = _make_mock_conn([None, None, None])

        est.estimate(**CALL_KWARGS, conn=conn)
        conn.close.assert_not_called()

    def test_conn_closed_when_estimator_owns(self):
        """When no conn passed, estimator opens and closes it."""
        est = _make_estimator()
        mock_conn, _ = _make_mock_conn([None, None, None])

        with patch("signal_quality.estimator.psycopg2.connect", return_value=mock_conn):
            est.estimate(**CALL_KWARGS)

        mock_conn.close.assert_called_once()

    def test_status_propagated_from_online(self):
        """Status from online row is propagated correctly."""
        est = _make_estimator()
        on_row = {"quality_score_online": 20.0, "expectancy_r_recent": -0.8, "status": "degraded"}
        # exact=None, fallback=None, online=degraded → 3 calls
        conn, _ = _make_mock_conn([None, None, on_row])

        result = est.estimate(**CALL_KWARGS, conn=conn)
        assert result.status == "degraded"

    def test_expectancy_defaults_zero_when_null_in_db(self):
        """NULL expectancy_r fields in DB rows default to 0.0.
        Exact hit found (quality_score=70) → only 2 fetchone calls.
        """
        est = _make_estimator()
        off_row = {"quality_score": 70.0, "expectancy_r": None}
        on_row = {"quality_score_online": 50.0, "expectancy_r_recent": None, "status": "ok"}
        conn, _ = _make_mock_conn([off_row, on_row])

        result = est.estimate(**CALL_KWARGS, conn=conn)
        assert result.expectancy_r_offline == pytest.approx(0.0)
        assert result.expectancy_r_online == pytest.approx(0.0)
