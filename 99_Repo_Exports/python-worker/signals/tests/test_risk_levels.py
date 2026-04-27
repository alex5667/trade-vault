"""
Тесты для signals/risk_levels.py

Вычисление SL и TP уровней в различных режимах.
"""
import pytest
from signals.risk_levels import compute_levels, format_sltp_text, parse_floats


# ─────────────────────── parse_floats ───────────────────────

def test_parse_floats_basic():
    assert parse_floats("1.0,2.0,3.0") == [1.0, 2.0, 3.0]


def test_parse_floats_empty():
    assert parse_floats("") == []


def test_parse_floats_skip_invalid():
    result = parse_floats("1.0,abc,3.0")
    assert result == [1.0, 3.0]


# ─────────────────────── compute_levels: ATR mode ───────────────────────

def make_cfg(
    stop_mode="ATR",
    stop_atr_mult=1.0,
    tp_mode="ATR",
    tp_atr_mults="0.6,1.0,1.5"
):
    return {
        "STOP_MODE": stop_mode,
        "STOP_ATR_MULT": stop_atr_mult,
        "TP_MODE": tp_mode,
        "TP_ATR_MULTS": tp_atr_mults,
    }


class TestComputeLevelsATR:
    def test_long_atr_mode(self):
        cfg = make_cfg(stop_mode="ATR", stop_atr_mult=1.0,
                       tp_mode="ATR", tp_atr_mults="1.0,2.0")
        result = compute_levels(entry=1000.0, atr=10.0, side="LONG", cfg=cfg)

        assert "sl" in result
        assert "tp_levels" in result
        # SL ниже entry для LONG
        assert result["sl"] < 1000.0
        # SL = entry - 1.0 * ATR = 990.0
        assert result["sl"] == pytest.approx(990.0)
        # TP1 = entry + 1.0 * ATR = 1010.0
        assert result["tp_levels"][0] == pytest.approx(1010.0)

    def test_short_atr_mode(self):
        cfg = make_cfg(stop_mode="ATR", stop_atr_mult=1.0,
                       tp_mode="ATR", tp_atr_mults="1.0")
        result = compute_levels(entry=1000.0, atr=10.0, side="SHORT", cfg=cfg)

        # SL выше entry для SHORT
        assert result["sl"] > 1000.0
        assert result["sl"] == pytest.approx(1010.0)
        # TP ниже entry для SHORT
        assert result["tp_levels"][0] == pytest.approx(990.0)

    def test_multiple_tp_levels(self):
        cfg = make_cfg(tp_atr_mults="0.6,1.0,1.5")
        result = compute_levels(entry=1000.0, atr=10.0, side="LONG", cfg=cfg)
        # 3 уровня TP
        assert len(result["tp_levels"]) == 3
        # Должны быть упорядочены по возрастанию для LONG
        assert result["tp_levels"][0] < result["tp_levels"][1] < result["tp_levels"][2]

    def test_rr_list_matches_tp_levels(self):
        cfg = make_cfg()
        result = compute_levels(entry=1000.0, atr=10.0, side="LONG", cfg=cfg)
        assert len(result["rr"]) == len(result["tp_levels"])


# ─────────────────────── compute_levels: PCT mode ───────────────────────

class TestComputeLevelsPCT:
    def test_pct_stop_long(self):
        cfg = {
            "STOP_MODE": "PCT",
            "STOP_PCT": 1.0,  # 1% stop
            "TP_MODE": "ATR",
            "TP_ATR_MULTS": "1.0",
        }
        result = compute_levels(entry=1000.0, atr=5.0, side="LONG", cfg=cfg)
        # SL = entry - 1% of entry = 1000 - 10 = 990
        assert result["sl"] == pytest.approx(990.0)


# ─────────────────────── compute_levels: zero/invalid stop ───────────────────────

class TestComputeLevelsEdgeCases:
    def test_zero_stop_dist_returns_empty(self):
        """Если stop_dist = 0 (нет конфига) → возвращаем {}."""
        cfg = {
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 0.0,  # → stop_dist=0 → returns {}
        }
        result = compute_levels(entry=1000.0, atr=10.0, side="LONG", cfg=cfg)
        assert result == {}

    def test_missing_atr_mult_returns_empty(self):
        cfg = {}  # Никаких конфигов
        result = compute_levels(entry=1000.0, atr=10.0, side="LONG", cfg=cfg)
        assert result == {}

    def test_atr_near_zero_does_not_crash(self):
        """ATR близкий к 0 не должен делить на 0."""
        cfg = make_cfg(stop_atr_mult=1.0, tp_atr_mults="1.0")
        # atr=0 → заменяется на 1e-9
        result = compute_levels(entry=1000.0, atr=0.0, side="LONG", cfg=cfg)
        # Может вернуть {} или нет — главное не крашится
        assert isinstance(result, dict)

    def test_stop_dist_override(self):
        cfg = make_cfg(stop_atr_mult=1.0, tp_atr_mults="1.0")
        result = compute_levels(
            entry=1000.0, atr=10.0, side="LONG",
            cfg=cfg, stop_dist_override=5.0
        )
        # SL должен быть 995.0
        assert result["sl"] == pytest.approx(995.0)


# ─────────────────────── format_sltp_text ───────────────────────

class TestFormatSltp:
    def test_format_contains_entry_sl_tp(self):
        cfg = make_cfg(stop_atr_mult=1.0, tp_atr_mults="1.0,2.0")
        levels = compute_levels(entry=1875.0, atr=5.0, side="LONG", cfg=cfg)
        text = format_sltp_text(1875.0, levels, "LONG")

        assert "Entry: 1875.00" in text
        assert "SL:" in text
        assert "TP1:" in text

    def test_format_two_tp_levels(self):
        cfg = make_cfg(stop_atr_mult=1.0, tp_atr_mults="1.0,2.0")
        levels = compute_levels(entry=1875.0, atr=5.0, side="LONG", cfg=cfg)
        text = format_sltp_text(1875.0, levels, "LONG")
        assert "TP2:" in text
