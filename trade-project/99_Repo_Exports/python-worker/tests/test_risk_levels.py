"""
Unit tests for signals/risk_levels.py
"""

from signals.risk_levels import compute_levels, format_sltp_text, parse_floats


class TestParseFloats:
    """Test parse_floats utility."""

    def test_empty(self):
        """Test empty string."""
        assert parse_floats("") == []
        assert parse_floats(None) == []

    def test_single(self):
        """Test single value."""
        assert parse_floats("1.5") == [1.5]

    def test_multiple(self):
        """Test multiple values."""
        assert parse_floats("1,2,3") == [1.0, 2.0, 3.0]

    def test_with_spaces(self):
        """Test with spaces."""
        assert parse_floats(" 1.5 , 2.5 , 3.5 ") == [1.5, 2.5, 3.5]

    def test_with_invalid(self):
        """Test with invalid values."""
        result = parse_floats("1,abc,3")
        assert len(result) == 2
        assert 1.0 in result
        assert 3.0 in result


class TestComputeLevels:
    """Test compute_levels function."""

    def test_long_atr_stop_rr_tp(self):
        """Test LONG with ATR stop and RR TP."""
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 0.6,
            "TP_MODE": "RR",
            "TP_RR": "1,2,3"
        }

        levels = compute_levels(entry=1875.0, atr=1.0, side="LONG", cfg=cfg)

        # SL should be entry - 0.6*ATR = 1875.0 - 0.6 = 1874.4
        assert abs(levels['sl'] - 1874.4) < 0.01

        # TPs: entry + RR * stop_dist
        # TP1: 1875.0 + 1*0.6 = 1875.6
        # TP2: 1875.0 + 2*0.6 = 1876.2
        # TP3: 1875.0 + 3*0.6 = 1876.8
        assert len(levels['tp_levels']) == 3
        assert abs(levels['tp_levels'][0] - 1875.6) < 0.01
        assert abs(levels['tp_levels'][1] - 1876.2) < 0.01
        assert abs(levels['tp_levels'][2] - 1876.8) < 0.01

        # RR should be [1, 2, 3]
        assert levels['rr'] == [1.0, 2.0, 3.0]

        # Stop dist
        assert abs(levels['stop_dist'] - 0.6) < 0.01

    def test_short_atr_stop_rr_tp(self):
        """Test SHORT with ATR stop and RR TP."""
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 0.6,
            "TP_MODE": "RR",
            "TP_RR": "1,2,3"
        }

        levels = compute_levels(entry=1875.0, atr=1.0, side="SHORT", cfg=cfg)

        # SL should be entry + 0.6*ATR = 1875.0 + 0.6 = 1875.6
        assert abs(levels['sl'] - 1875.6) < 0.01

        # TPs: entry - RR * stop_dist
        # TP1: 1875.0 - 1*0.6 = 1874.4
        # TP2: 1875.0 - 2*0.6 = 1873.8
        # TP3: 1875.0 - 3*0.6 = 1873.2
        assert len(levels['tp_levels']) == 3
        assert abs(levels['tp_levels'][0] - 1874.4) < 0.01
        assert abs(levels['tp_levels'][1] - 1873.8) < 0.01
        assert abs(levels['tp_levels'][2] - 1873.2) < 0.01

    def test_pct_stop(self):
        """Test percentage-based stop."""
        cfg = {
            "STOP_MODE": "PCT",
            "STOP_PCT": 0.2,  # 0.2%
            "TP_MODE": "RR",
            "TP_RR": "1,2"
        }

        levels = compute_levels(entry=2000.0, atr=1.0, side="LONG", cfg=cfg)

        # SL: 2000.0 - 0.2% = 2000.0 - 4.0 = 1996.0
        stop_dist = 2000.0 * 0.2 / 100.0
        assert abs(stop_dist - 4.0) < 0.01
        assert abs(levels['sl'] - 1996.0) < 0.01

        # TP1: 2000.0 + 1*4.0 = 2004.0
        # TP2: 2000.0 + 2*4.0 = 2008.0
        assert abs(levels['tp_levels'][0] - 2004.0) < 0.01
        assert abs(levels['tp_levels'][1] - 2008.0) < 0.01

    def test_points_stop(self):
        """Test fixed points stop."""
        cfg = {
            "STOP_MODE": "POINTS",
            "STOP_POINTS": 5.0,
            "TP_MODE": "RR",
            "TP_RR": "1.5"
        }

        levels = compute_levels(entry=1875.0, atr=1.0, side="LONG", cfg=cfg)

        # SL: 1875.0 - 5.0 = 1870.0
        assert abs(levels['sl'] - 1870.0) < 0.01
        assert abs(levels['stop_dist'] - 5.0) < 0.01

        # TP1: 1875.0 + 1.5*5.0 = 1882.5
        assert abs(levels['tp_levels'][0] - 1882.5) < 0.01

    def test_atr_tp_mode(self):
        """Test ATR-based TPs."""
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 0.5,
            "TP_MODE": "ATR",
            "TP_ATR_MULTS": "0.6,1.0,1.5"
        }

        levels = compute_levels(entry=1875.0, atr=2.0, side="LONG", cfg=cfg)

        # SL: 1875.0 - 0.5*2.0 = 1874.0
        assert abs(levels['sl'] - 1874.0) < 0.01

        # TP1: 1875.0 + 0.6*2.0 = 1876.2
        # TP2: 1875.0 + 1.0*2.0 = 1877.0
        # TP3: 1875.0 + 1.5*2.0 = 1878.0
        assert abs(levels['tp_levels'][0] - 1876.2) < 0.01
        assert abs(levels['tp_levels'][1] - 1877.0) < 0.01
        assert abs(levels['tp_levels'][2] - 1878.0) < 0.01

        # RR calculated relative to stop_dist
        # RR1: 1.2/1.0 = 1.2
        # RR2: 2.0/1.0 = 2.0
        # RR3: 3.0/1.0 = 3.0
        assert len(levels['rr']) == 3
        assert abs(levels['rr'][0] - 1.2) < 0.01
        assert abs(levels['rr'][1] - 2.0) < 0.01
        assert abs(levels['rr'][2] - 3.0) < 0.01

    def test_defaults(self):
        """Test with empty/minimal config (use defaults)."""
        cfg = {}

        levels = compute_levels(entry=1875.0, atr=1.0, side="LONG", cfg=cfg)

        # Should use ATR mode defaults
        assert levels['mode']['stop'] == 'ATR'
        assert levels['mode']['tp'] == 'RR'
        assert len(levels['tp_levels']) == 3  # Default 1,2,3

    def test_realistic_gold_scenario(self):
        """Test realistic  scenario."""
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 0.6,
            "TP_MODE": "RR",
            "TP_RR": "1,2,3"
        }

        # Realistic: entry=1875.50, ATR=1.30
        levels = compute_levels(
            entry=1875.50,
            atr=1.30,
            side="LONG",
            cfg=cfg
        )

        # SL: 1875.50 - 0.6*1.30 = 1875.50 - 0.78 = 1874.72
        expected_sl = 1875.50 - 0.6 * 1.30
        assert abs(levels['sl'] - expected_sl) < 0.01

        # TP1: 1875.50 + 1*0.78 = 1876.28
        # TP2: 1875.50 + 2*0.78 = 1877.06
        # TP3: 1875.50 + 3*0.78 = 1877.84
        stop_dist = 0.6 * 1.30
        assert abs(levels['tp_levels'][0] - (1875.50 + 1 * stop_dist)) < 0.01
        assert abs(levels['tp_levels'][1] - (1875.50 + 2 * stop_dist)) < 0.01
        assert abs(levels['tp_levels'][2] - (1875.50 + 3 * stop_dist)) < 0.01


class TestFormatSLTPText:
    """Test format_sltp_text function."""

    def test_basic_formatting(self):
        """Test basic text formatting."""
        levels = {
            'sl': 1874.22,
            'tp_levels': [1875.78, 1876.56, 1878.12],
            'rr': [1.0, 2.0, 3.0],
            'stop_dist': 0.78,
            'atr': 1.3,
            'mode': {'stop': 'ATR', 'tp': 'RR'}
        }

        text = format_sltp_text(1875.0, levels, 'LONG')

        assert "Entry: 1875.00" in text
        assert "SL: 1874.22" in text
        assert "TP1: 1875.78" in text
        assert "TP2: 1876.56" in text
        assert "TP3: 1878.12" in text
        assert "RR 1.0" in text
        assert "RR 2.0" in text
        assert "RR 3.0" in text

