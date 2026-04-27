# -*- coding: utf-8 -*-
"""
Unit tests for analysis.label_analyzer (LabelAnalyzer).

Uses unittest.mock to avoid PyArrow/Parquet I/O.
"""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# get_stats tests — no I/O required
# ---------------------------------------------------------------------------

class TestGetStats:
    """Tests for LabelAnalyzer.get_stats() — pure DataFrame logic."""

    def _make_analyzer(self):
        """Instantiate LabelAnalyzer without checking disk / PyArrow."""
        with (
            patch("os.path.exists", return_value=True),
            patch("analysis.label_analyzer._HAS_ARROW", True),
        ):
            from analysis.label_analyzer import LabelAnalyzer
            return LabelAnalyzer("/fake/labels")

    def test_empty_df_returns_error(self):
        la = self._make_analyzer()
        result = la.get_stats(pd.DataFrame())
        assert result == {"error": "No data"}

    def test_full_df_has_all_keys(self):
        la = self._make_analyzer()
        df = pd.DataFrame({
            "ts": [1_000, 2_000, 3_000],
            "symbol": ["XAUUSD", "XAUUSD", "BTCUSDT"],
            "side": ["buy", "sell", "buy"],
            "source": ["detector_A", "detector_A", "detector_B"],
            "emitted": [True, False, True],
            "confidence": [0.8, 0.6, 0.9],
            "lot": [0.01, 0.02, 0.01],
        })
        result = la.get_stats(df)
        assert result["total_signals"] == 3
        assert result["emitted_signals"] == 2
        assert result["unique_symbols"] == 2
        assert result["date_range"]["start"] == 1_000
        assert result["date_range"]["end"] == 3_000
        assert "buy" in result["by_side"]
        assert "confidence" in result
        assert result["confidence"]["mean"] == pytest.approx(
            (0.8 + 0.6 + 0.9) / 3
        )
        assert "lot_size" in result

    def test_missing_optional_columns(self):
        la = self._make_analyzer()
        df = pd.DataFrame({"confidence": [0.7]})
        result = la.get_stats(df)
        assert result["emitted_signals"] == 0
        assert result["unique_symbols"] == 0
        assert result["by_side"] == {}
        assert result["by_source"] == {}
        assert result["lot_size"] == {}

    def test_no_confidence_column(self):
        la = self._make_analyzer()
        df = pd.DataFrame({"ts": [1, 2], "symbol": ["A", "B"]})
        result = la.get_stats(df)
        assert result["confidence"] == {}


# ---------------------------------------------------------------------------
# get_metrics_summary tests
# ---------------------------------------------------------------------------

class TestGetMetricsSummary:
    def _make_analyzer(self):
        with (
            patch("os.path.exists", return_value=True),
            patch("analysis.label_analyzer._HAS_ARROW", True),
        ):
            from analysis.label_analyzer import LabelAnalyzer
            return LabelAnalyzer("/fake/labels")

    def test_empty_df_returns_error(self):
        la = self._make_analyzer()
        result = la.get_metrics_summary(pd.DataFrame())
        assert result == {"error": "No metrics data"}

    def test_no_metrics_column(self):
        la = self._make_analyzer()
        df = pd.DataFrame({"ts": [1, 2]})
        result = la.get_metrics_summary(df)
        assert result == {"error": "No metrics data"}

    def test_metrics_numeric_aggregated(self):
        la = self._make_analyzer()
        df = pd.DataFrame({
            "metrics": [
                {"score": 0.9, "detector_source": "A", "trigger": True},
                {"score": 0.5, "detector_source": "B", "trigger": False},
            ]
        })
        result = la.get_metrics_summary(df)
        assert "score" in result
        assert result["score"]["mean"] == pytest.approx(0.7)
        assert result["score"]["min"] == pytest.approx(0.5)
        assert result["score"]["max"] == pytest.approx(0.9)

    def test_detector_usage_counted(self):
        la = self._make_analyzer()
        df = pd.DataFrame({
            "metrics": [
                {"detector_source": "A"},
                {"detector_source": "A"},
                {"detector_source": "B"},
            ]
        })
        result = la.get_metrics_summary(df)
        assert result["detector_usage"]["A"] == 2
        assert result["detector_usage"]["B"] == 1

    def test_trigger_rate(self):
        la = self._make_analyzer()
        df = pd.DataFrame({
            "metrics": [
                {"trigger": 1},
                {"trigger": 0},
                {"trigger": 1},
                {"trigger": 1},
            ]
        })
        result = la.get_metrics_summary(df)
        assert result["trigger_rate"] == pytest.approx(0.75)

    def test_extreme_rate(self):
        la = self._make_analyzer()
        df = pd.DataFrame({
            "metrics": [
                {"extreme": 0},
                {"extreme": 1},
            ]
        })
        result = la.get_metrics_summary(df)
        assert result["extreme_rate"] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# load_labels — filter logic via mock dataset
# ---------------------------------------------------------------------------

class TestLoadLabels:
    def _make_analyzer_and_mock(self, df: pd.DataFrame):
        """
        Returns (analyzer, mock_dataset) so we can inspect calls.
        """
        import pyarrow as pa
        table = pa.Table.from_pandas(df)

        mock_dataset = MagicMock()
        mock_dataset.to_table.return_value = table

        with (
            patch("os.path.exists", return_value=True),
            patch("analysis.label_analyzer._HAS_ARROW", True),
            patch("analysis.label_analyzer.ds") as mock_ds,
        ):
            mock_ds.dataset.return_value = mock_dataset
            mock_ds.field.side_effect = lambda name: MagicMock(
                __eq__=lambda self, other: f"field({name})=={other}",
                __ge__=lambda self, other: f"field({name})>={other}",
                __le__=lambda self, other: f"field({name})<={other}",
            )
            from analysis.label_analyzer import LabelAnalyzer
            la = LabelAnalyzer("/fake/labels")
            return la, mock_dataset, mock_ds

    def test_no_filters_passes_none(self):
        df = pd.DataFrame({"ts": [1], "symbol": ["X"], "confidence": [0.5]})
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            pytest.skip("pyarrow not installed")

        with (
            patch("os.path.exists", return_value=True),
            patch("analysis.label_analyzer._HAS_ARROW", True),
            patch("analysis.label_analyzer.ds") as mock_ds,
        ):
            import pyarrow as pa
            table = pa.Table.from_pandas(df)
            mock_dataset = MagicMock()
            mock_dataset.to_table.return_value = table
            mock_ds.dataset.return_value = mock_dataset
            from analysis.label_analyzer import LabelAnalyzer
            la = LabelAnalyzer("/fake/labels")
            result = la.load_labels()
            mock_dataset.to_table.assert_called_once_with(filter=None)
            assert len(result) == 1

    def test_min_confidence_filter(self):
        df = pd.DataFrame({
            "ts": [1, 2, 3],
            "confidence": [0.4, 0.7, 0.9],
        })
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            pytest.skip("pyarrow not installed")

        with (
            patch("os.path.exists", return_value=True),
            patch("analysis.label_analyzer._HAS_ARROW", True),
            patch("analysis.label_analyzer.ds") as mock_ds,
        ):
            import pyarrow as pa
            table = pa.Table.from_pandas(df)
            mock_dataset = MagicMock()
            mock_dataset.to_table.return_value = table
            mock_ds.dataset.return_value = mock_dataset
            from analysis.label_analyzer import LabelAnalyzer
            la = LabelAnalyzer("/fake/labels")
            result = la.load_labels(min_confidence=0.6)
            assert len(result) == 2  # 0.7 and 0.9 pass

    def test_json_fields_deserialized(self):
        df = pd.DataFrame({
            "ts": [1],
            "metrics": ['{"score": 0.8}'],
            "tp_levels": ['[1.0, 2.0]'],
        })
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            pytest.skip("pyarrow not installed")

        with (
            patch("os.path.exists", return_value=True),
            patch("analysis.label_analyzer._HAS_ARROW", True),
            patch("analysis.label_analyzer.ds") as mock_ds,
        ):
            import pyarrow as pa
            table = pa.Table.from_pandas(df)
            mock_dataset = MagicMock()
            mock_dataset.to_table.return_value = table
            mock_ds.dataset.return_value = mock_dataset
            from analysis.label_analyzer import LabelAnalyzer
            la = LabelAnalyzer("/fake/labels")
            result = la.load_labels()
            assert result["metrics"].iloc[0] == {"score": 0.8}
            assert result["tp_levels"].iloc[0] == [1.0, 2.0]


# ---------------------------------------------------------------------------
# export_report
# ---------------------------------------------------------------------------

class TestExportReport:
    def test_export_writes_valid_json(self, tmp_path):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            pytest.skip("pyarrow not installed")

        df = pd.DataFrame({
            "ts": [1_000, 2_000],
            "symbol": ["XAUUSD", "XAUUSD"],
            "confidence": [0.8, 0.9],
        })

        with (
            patch("os.path.exists", return_value=True),
            patch("analysis.label_analyzer._HAS_ARROW", True),
            patch("analysis.label_analyzer.ds") as mock_ds,
        ):
            import pyarrow as pa
            table = pa.Table.from_pandas(df)
            mock_dataset = MagicMock()
            mock_dataset.to_table.return_value = table
            mock_ds.dataset.return_value = mock_dataset
            from analysis.label_analyzer import LabelAnalyzer
            la = LabelAnalyzer("/fake/labels")

            out_path = str(tmp_path / "report.json")
            result_path = la.export_report(out_path)

            assert result_path == out_path
            with open(out_path) as f:
                data = json.load(f)

            assert "generated_at" in data
            assert "stats" in data
            assert "metrics" in data
            assert "sample_signals" in data
            assert data["stats"]["total_signals"] == 2


# ---------------------------------------------------------------------------
# LabelAnalyzer init guards
# ---------------------------------------------------------------------------

class TestLabelAnalyzerInit:
    def test_raises_when_directory_missing(self):
        with patch("os.path.exists", return_value=False):
            from analysis.label_analyzer import LabelAnalyzer
            with pytest.raises(ValueError, match="not found"):
                LabelAnalyzer("/missing/path")

    def test_raises_when_pyarrow_missing(self):
        with (
            patch("os.path.exists", return_value=True),
            patch("analysis.label_analyzer._HAS_ARROW", False),
        ):
            from analysis.label_analyzer import LabelAnalyzer
            with pytest.raises(ImportError, match="PyArrow"):
                LabelAnalyzer("/fake/labels")
