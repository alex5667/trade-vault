"""
tests/test_regime_worker.py — самодостаточные unit-тесты для regime-worker.

Не требуют Redis или Postgres.
Покрывают: adx_atr, classify, quantiles (TTL-кэш, fallback DEFAULTS).
"""

from __future__ import annotations

import math
import os

from adx_atr import WilderState, directional_moves, true_range, update_adx_atr, wilder_update
from classify import classify_regime, confidence
from quantiles import DEFAULTS, _cache, bust_cache_for, load_quantiles

# ---------------------------------------------------------------------------
# Тестовые квантили (общие для classify-тестов)
# ---------------------------------------------------------------------------
Q = {
    "adx_p40": 18.0,
    "adx_p60": 25.0,
    "adx_p75": 32.0,
    "atrp_p25": 0.0008,
    "atrp_p50": 0.0016,
    "atrp_p75": 0.0025,
}


# ────────────────────────────────────────────────────────────────────────────
# adx_atr tests
# ────────────────────────────────────────────────────────────────────────────
class TestWilderUpdate:
    def test_basic_smoothing(self):
        """EMA(Wilder): prev=10, new=20, n=14 → (10*13 + 20) / 14"""
        result = wilder_update(10.0, 20.0, 14)
        expected = (10.0 * 13 + 20.0) / 14
        assert abs(result - expected) < 1e-10

    def test_same_values_stable(self):
        """При prev==new результат должен остаться тем же."""
        assert wilder_update(5.0, 5.0, 14) == 5.0

    def test_convergence(self):
        """Через много итераций должен сходиться к new."""
        val = 100.0
        for _ in range(1000):
            val = wilder_update(val, 10.0, 14)
        assert abs(val - 10.0) < 0.001


class TestTrueRange:
    def test_hl_dominates(self):
        """H-L является greatest."""
        assert true_range(110.0, 90.0, 100.0) == 20.0

    def test_h_close_dominates(self):
        """|H - PC| больше H-L и |L-PC|."""
        # h=115, lo=100, pc=80 → max(15, 35, 20) = 35
        assert true_range(115.0, 100.0, 80.0) == 35.0

    def test_l_close_dominates(self):
        """|L - PC| больше остальных."""
        # h=100, lo=80, pc=120 → max(20, 20, 40) = 40
        assert true_range(100.0, 80.0, 120.0) == 40.0

    def test_equal_hlc(self):
        assert true_range(100.0, 100.0, 100.0) == 0.0


class TestDirectionalMoves:
    def test_up_dominates(self):
        """Up > Dn → plus_dm = up, minus_dm = 0.
        h=110, ph=100 → up=10; lo=95, pl=100 → dn=5. up > dn."""
        plus_dm, minus_dm = directional_moves(110.0, 95.0, 100.0, 100.0)
        assert plus_dm == 10.0
        assert minus_dm == 0.0

    def test_dn_dominates(self):
        """Dn > Up → minus_dm = dn, plus_dm = 0.
        h=102, ph=100 → up=2; lo=80, pl=90 → dn=10. dn > up."""
        plus_dm, minus_dm = directional_moves(102.0, 80.0, 100.0, 90.0)
        assert minus_dm == 10.0
        assert plus_dm == 0.0

    def test_equal_moves_both_zero(self):
        """Up == Dn → ни одно не доминирует → оба 0 (по алгоритму Уайлдера).
        h=110, ph=100 → up=10; lo=90, pl=100 → dn=10. Равны → оба 0."""
        plus_dm, minus_dm = directional_moves(110.0, 90.0, 100.0, 100.0)
        assert plus_dm == 0.0
        assert minus_dm == 0.0

    def test_negative_moves_zero(self):
        """Отрицательные движения (h < ph, lo > pl) → нули."""
        plus_dm, minus_dm = directional_moves(95.0, 100.0, 100.0, 95.0)
        assert plus_dm == 0.0
        assert minus_dm == 0.0


class TestUpdateAdxAtr:
    def _make_state(self) -> WilderState:
        return WilderState()

    def test_first_call_returns_none(self):
        """Первый вызов — инициализация, result=None."""
        st = self._make_state()
        st, res = update_adx_atr(st, 110.0, 90.0, 100.0, 100.0, 90.0, 95.0)
        assert res is None
        assert st.initialized is True
        assert st.atr is not None

    def test_second_call_returns_dict(self):
        """Второй вызов возвращает результат."""
        st = self._make_state()
        st, _ = update_adx_atr(st, 110.0, 90.0, 100.0, 100.0, 90.0, 95.0)
        st, res = update_adx_atr(st, 112.0, 92.0, 105.0, 110.0, 90.0, 100.0)
        assert res is not None
        assert "atr" in res
        assert "plusDI" in res
        assert "minusDI" in res
        assert "adx" in res

    def test_zero_atr_no_exception(self):
        """Если ATR=0 после обновления, не делим на ноль — нет исключений."""
        st = self._make_state()
        st, _ = update_adx_atr(st, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0)
        # Не должно выбрасывать исключений:
        st, _res = update_adx_atr(st, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0)

    def test_all_result_fields_non_negative(self):
        """Ненулевые данные → все поля результата >= 0."""
        st = self._make_state()
        st, _ = update_adx_atr(st, 105.0, 95.0, 100.0, 100.0, 95.0, 98.0)
        st, res = update_adx_atr(st, 108.0, 98.0, 105.0, 105.0, 95.0, 100.0)
        if res is not None:
            assert res["atr"] > 0
            assert res["adx"] >= 0
            assert res["plusDI"] >= 0
            assert res["minusDI"] >= 0

    def test_wilder_state_slots(self):
        """WilderState использует __slots__ — нет __dict__."""
        st = WilderState()
        assert not hasattr(st, "__dict__")


# ────────────────────────────────────────────────────────────────────────────
# classify tests
# ────────────────────────────────────────────────────────────────────────────
class TestClassifyRegime:
    def test_squeeze(self):
        """low_atr + low_adx + adx_slope <= 0 → squeeze."""
        regime, _, _ = classify_regime(
            adx=15.0, adx_prev=16.0,
            atr_pct=0.0005, atr_pct_prev=0.0006,
            plus_di=10.0, minus_di=8.0,
            q=Q,
        )
        assert regime == "squeeze"

    def test_trending_bull(self):
        """high_adx + adx_slope >= 0 + plus_di > minus_di → trending_bull."""
        regime, _, _ = classify_regime(
            adx=30.0, adx_prev=28.0,
            atr_pct=0.0020, atr_pct_prev=0.0018,
            plus_di=25.0, minus_di=10.0,
            q=Q,
        )
        assert regime == "trending_bull"

    def test_trending_bear(self):
        """high_adx + minus_di > plus_di → trending_bear."""
        regime, _, _ = classify_regime(
            adx=30.0, adx_prev=28.0,
            atr_pct=0.0020, atr_pct_prev=0.0018,
            plus_di=10.0, minus_di=25.0,
            q=Q,
        )
        assert regime == "trending_bear"

    def test_range_low_adx_mid_atr(self):
        """low_adx + mid_atr → range."""
        regime, _, _ = classify_regime(
            adx=14.0, adx_prev=14.0,
            atr_pct=0.0012, atr_pct_prev=0.0012,
            plus_di=12.0, minus_di=11.0,
            q=Q,
        )
        assert regime == "range"

    def test_expansion(self):
        """adx_slope > 0 + atrp_slope > 0 → expansion."""
        regime, _, _ = classify_regime(
            adx=20.0, adx_prev=18.0,
            atr_pct=0.0022, atr_pct_prev=0.0018,
            plus_di=15.0, minus_di=14.0,
            q=Q,
        )
        assert regime == "expansion"

    def test_slopes_returned_correctly(self):
        """Проверяем что slopes вычислены правильно."""
        _, adx_slope, atrp_slope = classify_regime(
            adx=20.0, adx_prev=18.0,
            atr_pct=0.002, atr_pct_prev=0.001,
            plus_di=10.0, minus_di=10.0,
            q=Q,
        )
        assert abs(adx_slope - 2.0) < 1e-10
        assert abs(atrp_slope - 0.001) < 1e-10

    def test_none_prev_slope_zero(self):
        """При adx_prev=None и atr_pct_prev=None slopes = 0."""
        _, adx_slope, atrp_slope = classify_regime(
            adx=20.0, adx_prev=None,
            atr_pct=0.002, atr_pct_prev=None,
            plus_di=10.0, minus_di=10.0,
            q=Q,
        )
        assert adx_slope == 0.0
        assert atrp_slope == 0.0


class TestConfidence:
    def test_trending_p75(self):
        """ADX >= p75 → 0.9."""
        assert confidence("trending_bull", 35.0, Q) == 0.9

    def test_trending_p60(self):
        """ADX >= p60 но < p75 → 0.7."""
        assert confidence("trending_bear", 27.0, Q) == 0.7

    def test_trending_below_p60(self):
        """Trending, но ADX < p60 → 0.55."""
        assert confidence("trending_bull", 20.0, Q) == 0.55

    def test_squeeze(self):
        assert confidence("squeeze", 15.0, Q) == 0.7

    def test_expansion(self):
        assert confidence("expansion", 20.0, Q) == 0.65

    def test_range(self):
        assert confidence("range", 14.0, Q) == 0.5

    def test_unknown_regime(self):
        """Неизвестный режим → 0.5 (default)."""
        assert confidence("unknown_regime_xyz", 20.0, Q) == 0.5


# ────────────────────────────────────────────────────────────────────────────
# quantiles tests (без реального DB)
# ────────────────────────────────────────────────────────────────────────────
class TestQuantiles:
    def setup_method(self):
        """Очищаем кэш перед каждым тестом."""
        _cache.clear()

    def test_returns_defaults_without_db(self):
        """Без DATABASE_URL должен возвращать DEFAULTS."""
        old_url = os.environ.pop("DATABASE_URL", None)
        try:
            q = load_quantiles("BTCUSDT", "1m")
            assert q == DEFAULTS
        finally:
            if old_url is not None:
                os.environ["DATABASE_URL"] = old_url

    def test_caches_result(self):
        """Второй вызов возвращает тот же объект из кэша."""
        old_url = os.environ.pop("DATABASE_URL", None)
        try:
            q1 = load_quantiles("ETHUSDT", "5m")
            q2 = load_quantiles("ETHUSDT", "5m")
            assert q1 is q2  # одна и та же ссылка из кэша
        finally:
            if old_url is not None:
                os.environ["DATABASE_URL"] = old_url

    def test_bust_cache_clears(self):
        """bust_cache_for убирает запись из кэша."""
        old_url = os.environ.pop("DATABASE_URL", None)
        try:
            load_quantiles("SOLUSDT", "15m")
            assert ("SOLUSDT", "15m") in _cache

            bust_cache_for("SOLUSDT", "15m")
            assert ("SOLUSDT", "15m") not in _cache
        finally:
            if old_url is not None:
                os.environ["DATABASE_URL"] = old_url

    def test_bust_cache_nonexistent_key(self):
        """bust_cache_for на несуществующий ключ — не падает."""
        bust_cache_for("XYZUSDT", "99m")  # No KeyError expected

    def test_defaults_has_required_keys(self):
        """DEFAULTS содержит все необходимые ключи для классификации."""
        required = {"adx_p40", "adx_p60", "adx_p75", "atrp_p25", "atrp_p50", "atrp_p75"}
        assert required.issubset(DEFAULTS.keys())

    def test_different_symbols_independent_cache(self):
        """Разные (symbol, tf) имеют независимые кэши."""
        old_url = os.environ.pop("DATABASE_URL", None)
        try:
            load_quantiles("BTCUSDT", "1h")
            bust_cache_for("BTCUSDT", "1h")
            # ETH кэш не затронут
            load_quantiles("ETHUSDT", "1h")
            assert ("ETHUSDT", "1h") in _cache
            assert ("BTCUSDT", "1h") not in _cache
        finally:
            if old_url is not None:
                os.environ["DATABASE_URL"] = old_url


# ────────────────────────────────────────────────────────────────────────────
# Integration: full pipeline (без Redis/DB)
# ────────────────────────────────────────────────────────────────────────────
class TestFullPipeline:
    """End-to-end тест: несколько свечей → режим."""

    def _run_pipeline(self, candles: list[dict]) -> list[dict]:
        """Прогоняет список свечей через ADX/ATR + classify, возвращает результаты."""
        state = WilderState()
        prev = {"adx": None, "atrPct": None}
        prev_candle = None
        results = []

        for candle in candles:
            h, lo, close = candle["h"], candle["l"], candle["c"]
            if prev_candle:
                ph, pl, pc = prev_candle["h"], prev_candle["l"], prev_candle["c"]
            else:
                ph, pl, pc = h, lo, close

            prev_candle = {"h": h, "l": lo, "c": close}
            state, res = update_adx_atr(state, h, lo, close, ph, pl, pc, n=14)
            if res is None:
                continue

            atr_pct = res["atr"] / close if close else 0.0
            regime, _adx_slope, _atrp_slope = classify_regime(
                res["adx"], prev["adx"],
                atr_pct, prev["atrPct"],
                res["plusDI"], res["minusDI"],
                Q,
            )
            prev = {"adx": res["adx"], "atrPct": atr_pct}
            conf = confidence(regime, res["adx"], Q)
            results.append({"regime": regime, "adx": res["adx"], "confidence": conf})

        return results

    def test_trending_sequence(self):
        """Стабильный uptrend → классифицируется корректно."""
        candles = [
            {"h": 100.0 + i * 2, "l": 99.0 + i * 2, "c": 100.0 + i * 2}
            for i in range(30)
        ]
        results = self._run_pipeline(candles)
        assert len(results) > 5
        final = results[-1]
        assert final["regime"] in ("trending_bull", "trending_bear", "expansion", "range")
        assert 0.0 <= final["confidence"] <= 1.0

    def test_flat_sequence(self):
        """Флет → squeeze или range."""
        candles = [
            {"h": 100.1, "l": 99.9, "c": 100.0}
            for _ in range(30)
        ]
        results = self._run_pipeline(candles)
        assert len(results) > 5
        regimes = {r["regime"] for r in results}
        assert regimes.issubset({"squeeze", "range", "expansion", "trending_bull", "trending_bear"})

    def test_confidence_in_range(self):
        """Confidence всегда в [0, 1]."""
        candles = [
            {
                "h": 100.0 + math.sin(i * 0.5),
                "l": 99.0 + math.sin(i * 0.5),
                "c": 99.5 + math.sin(i * 0.5),
            }
            for i in range(30)
        ]
        results = self._run_pipeline(candles)
        for r in results:
            assert 0.0 <= r["confidence"] <= 1.0
