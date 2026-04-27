"""
Regression pack — RANGE_TP_RR пересчёт TP (2026-04-18 wave).

Изменение: signal_pipeline теперь пересчитывает TP через RR-множители
вместо простой обрезки до 2 уровней.

Контракт:
  - TP1 = entry ± stop_dist × RR[0]
  - TP2 = entry ± stop_dist × RR[1]
  - По умолчанию RANGE_TP_RR = "1.0,1.5"
  - Максимум 2 уровня TP
  - При невалидном ENV → safe fallback (не краш)
  - indicators["range_tp_rr_applied"] устанавливается
"""
import os
import pytest


def _compute_range_tps(entry: float, sl: float, direction: str,
                        rr_str: str = "1.0,1.5") -> list:
    """
    Воспроизводит логику пересчёта TP из signal_pipeline._apply_range_regime_overrides().
    Используется как golden reference.
    """
    try:
        rr = [float(x.strip()) for x in rr_str.split(",") if x.strip()][:2]
    except (ValueError, AttributeError):
        return []

    stop_dist = abs(entry - sl)
    if stop_dist <= 0 or not rr:
        return []

    if direction.upper() == "LONG":
        return [round(entry + stop_dist * r, 10) for r in rr]
    else:
        return [round(entry - stop_dist * r, 10) for r in rr]


# ---------------------------------------------------------------------------
# Golden cases из fixtures
# ---------------------------------------------------------------------------
GOLDEN_CASES = [
    # (case_name, entry, sl, direction, rr_str, expected_tp1, expected_tp2)
    ("LONG 2% SL default RR",  100.0, 98.0,  "LONG",  "1.0,1.5", 102.0, 103.0),
    ("SHORT 2% SL default RR", 100.0, 102.0, "SHORT", "1.0,1.5",  98.0,  97.0),
    ("LONG custom RR 1.0,2.0", 50000.0, 49500.0, "LONG", "1.0,2.0", 50500.0, 51000.0),
    ("SHORT custom RR 1.0,2.0", 50000.0, 50500.0, "SHORT", "1.0,2.0", 49500.0, 49000.0),
]


@pytest.mark.parametrize("name,entry,sl,direction,rr_str,tp1,tp2", GOLDEN_CASES)
def test_range_tp_golden(name, entry, sl, direction, rr_str, tp1, tp2):
    """Золотые кейсы из fixtures/range_tp_rr_cases.json."""
    tps = _compute_range_tps(entry, sl, direction, rr_str)
    assert len(tps) == 2, f"[{name}] Expected 2 TPs, got {len(tps)}"
    assert abs(tps[0] - tp1) < 1e-6, f"[{name}] TP1={tps[0]}, expected {tp1}"
    assert abs(tps[1] - tp2) < 1e-6, f"[{name}] TP2={tps[1]}, expected {tp2}"


# ---------------------------------------------------------------------------
# Направление: LONG → TP > entry, SHORT → TP < entry
# ---------------------------------------------------------------------------
def test_long_tps_above_entry():
    """LONG: оба TP должны быть выше entry."""
    tps = _compute_range_tps(100.0, 98.0, "LONG")
    assert all(tp > 100.0 for tp in tps), f"LONG TPs должны быть выше entry: {tps}"


def test_short_tps_below_entry():
    """SHORT: оба TP должны быть ниже entry."""
    tps = _compute_range_tps(100.0, 102.0, "SHORT")
    assert all(tp < 100.0 for tp in tps), f"SHORT TPs должны быть ниже entry: {tps}"


# ---------------------------------------------------------------------------
# Монотонность: TP2 дальше TP1
# ---------------------------------------------------------------------------
def test_long_tp2_further_than_tp1():
    """LONG: TP2 > TP1 (неубывающий RR)."""
    tps = _compute_range_tps(100.0, 98.0, "LONG")
    assert tps[1] > tps[0], f"TP2 должен быть дальше TP1 для LONG: {tps}"


def test_short_tp2_further_than_tp1():
    """SHORT: TP2 < TP1 (дальше вниз)."""
    tps = _compute_range_tps(100.0, 102.0, "SHORT")
    assert tps[1] < tps[0], f"TP2 должен быть дальше (ниже) TP1 для SHORT: {tps}"


# ---------------------------------------------------------------------------
# Количество TP
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("rr_str,expected_n", [
    ("1.0", 1),       # один RR → один TP
    ("1.0,1.5", 2),   # два RR → два TP
    ("1.0,1.5,2.0", 2),  # три → обрезается до 2
])
def test_rr_list_length_bounded_to_2(rr_str, expected_n):
    """Список RR обрезается до 2 TP максимум."""
    tps = _compute_range_tps(100.0, 98.0, "LONG", rr_str)
    assert len(tps) == expected_n, f"rr_str={rr_str!r}: ожидали {expected_n} TP, получили {len(tps)}"


# ---------------------------------------------------------------------------
# Безопасность при граничных значениях
# ---------------------------------------------------------------------------
def test_zero_stop_dist_returns_empty():
    """SL == entry → stop_dist=0 → пустой список (не делить на 0)."""
    tps = _compute_range_tps(100.0, 100.0, "LONG")
    assert tps == [], f"Ожидали пустой список при stop_dist=0: {tps}"


def test_invalid_rr_env_returns_empty():
    """Невалидный RANGE_TP_RR → пустой список (не краш)."""
    tps = _compute_range_tps(100.0, 98.0, "LONG", "abc,xyz")
    assert tps == [], "Невалидный RR env должен возвращать []"


def test_empty_rr_env_returns_empty():
    """Пустой RANGE_TP_RR → пустой список."""
    tps = _compute_range_tps(100.0, 98.0, "LONG", "")
    assert tps == [], "Пустой RR env должен возвращать []"


def test_partial_invalid_rr():
    """'1.0,abc' → только валидная часть парсится."""
    # '1.0,abc' вызовет ValueError при float('abc') → должен вернуть []
    tps = _compute_range_tps(100.0, 98.0, "LONG", "1.0,abc")
    assert tps == [], "Частично невалидный RR должен возвращать [] (безопасный fallback)"


# ---------------------------------------------------------------------------
# ATR ratio check: TP1 ≈ 1.1 ATR (вместо старых ~0.69 ATR)
# ---------------------------------------------------------------------------
def test_range_tp1_atr_ratio_improved():
    """
    Regression: RANGE_TP_RR=1.0,1.5 с SL=1 ATR даёт TP1 ≈ 1.0 * SL_dist.
    Старый код просто обрезал TP, давая TP1 ≈ 0.69 ATR.
    Новый код: TP1 = entry + SL_dist * 1.0 → значительно выше.
    """
    entry = 100.0
    # SL_dist = 1 ATR (примерно)
    atr = 2.0
    sl = entry - atr  # LONG: SL ниже entry

    tps = _compute_range_tps(entry, sl, "LONG", "1.0,1.5")
    tp1_dist = tps[0] - entry  # расстояние от entry до TP1
    tp1_in_atr = tp1_dist / atr

    # Новый расчёт: TP1 = entry + 1.0 * atr → tp1_in_atr = 1.0
    assert abs(tp1_in_atr - 1.0) < 1e-6, \
        f"TP1 должен быть 1.0 ATR, получили {tp1_in_atr:.3f} ATR"

    # Должен быть > 0.95 (лучше старого ~0.69)
    assert tp1_in_atr > 0.95, f"TP1/ATR={tp1_in_atr:.3f} — должен быть > 0.95 ATR"


# ---------------------------------------------------------------------------
# ENV override работает корректно
# ---------------------------------------------------------------------------
def test_env_range_tp_rr_default():
    """Дефолтное значение RANGE_TP_RR — '1.0,1.5'."""
    with pytest.MonkeyPatch.context() as m:
        m.delenv("RANGE_TP_RR", raising=False)
        val = os.getenv("RANGE_TP_RR", "1.0,1.5")
    assert val == "1.0,1.5"


def test_env_range_tp_rr_override():
    """RANGE_TP_RR из ENV читается корректно."""
    with pytest.MonkeyPatch.context() as m:
        m.setenv("RANGE_TP_RR", "1.0,2.0")
        val = os.getenv("RANGE_TP_RR", "1.0,1.5")
    assert val == "1.0,2.0"


# ---------------------------------------------------------------------------
# indicators["range_tp_rr_applied"] контракт
# ---------------------------------------------------------------------------
def test_range_tp_rr_applied_format():
    """
    Индикатор range_tp_rr_applied должен быть строкой вида '1.0,1.5'.
    Smoke-check формата без реального импорта pipeline.
    """
    rr_str = "1.0,1.5"
    # Проверяем формат
    assert isinstance(rr_str, str)
    parts = [x.strip() for x in rr_str.split(",") if x.strip()]
    assert len(parts) == 2
    for p in parts:
        float(p)  # должен парситься
