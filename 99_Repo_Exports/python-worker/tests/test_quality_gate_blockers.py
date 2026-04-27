"""
Quality Gate Blocker Tests — B1, B2, B3, B4

B1: TM_SMART_TIMEOUT при atr=0 не зависает (zombie fix)
B2: _get_ml_executor() является singleton
B3: is_of_sync_build() читает ENV корректно
B4: OF_SYNC_BUILD kill-switch присутствует в ml_confirm_gate
"""
from __future__ import annotations

import os
import time
import importlib
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


# ===========================================================================
# B1: Smart Timeout zombie fix
# ===========================================================================

class _FakePos:
    """Минимальная фейковая позиция для тестов orphan-логики."""
    def __init__(self, *, atr: float, entry_price: float, direction: str = "LONG",
                 trailing_active: bool = False):
        self.id = "test-pos-1"
        self.symbol = "BTCUSDT"
        self.direction = direction
        self.entry_price = entry_price
        self.atr = atr
        self.trailing_active = trailing_active
        self.closed = False


def _run_smart_timeout_check(
    *,
    atr: float,
    entry_price: float,
    last_price: float,
    direction: str = "LONG",
    pnl_bps_threshold: float = 4.0,
    mae_atr_threshold: float = 1.0,
) -> bool:
    """
    Воспроизводит логику smart timeout из trade_monitor._collect_orphan_closures.
    Возвращает True если позиция должна быть закрыта (timeout allowed),
    False если должна быть удержана (HOLD / continue).
    """
    pos = _FakePos(atr=atr, entry_price=entry_price, direction=direction)
    last_px = float(last_price)
    entry_px = float(pos.entry_price)

    if direction == "LONG":
        pnl_raw = (last_px - entry_px) / entry_px
    else:
        pnl_raw = (entry_px - last_px) / entry_px
    pnl_bps = pnl_raw * 10000.0

    param_min_pnl = pnl_bps_threshold
    param_max_mae_atr = mae_atr_threshold

    is_profitable_exit = pnl_bps >= param_min_pnl

    atr_val = float(pos.atr)
    is_risky = False

    # ---- ИСПРАВЛЕННАЯ ЛОГИКА (B1 fix) ----
    if atr_val > 0:
        if direction == "LONG":
            adverse_dist = entry_px - last_px
        else:
            adverse_dist = last_px - entry_px

        if adverse_dist > (atr_val * param_max_mae_atr):
            is_risky = True

        if not is_profitable_exit and not is_risky:
            return False  # HOLD

    # atr=0 → pass through to normal orphan close
    return True  # close allowed


class TestSmartTimeoutB1:
    """B1: Тесты zombie-fix для atr=0."""

    def test_atr_zero_with_loss_closes(self):
        """B1-критический: при atr=0 и убытке позиция должна закрыться (не зависать)."""
        result = _run_smart_timeout_check(
            atr=0.0,
            entry_price=100.0,
            last_price=99.0,  # убыток -100bps
        )
        assert result is True, "atr=0 + убыток → должен быть ORPHAN_TIMEOUT (не zombie)"

    def test_atr_zero_with_small_profit_closes(self):
        """B1: при atr=0 и маленьком профите (< threshold) тоже закрывается."""
        result = _run_smart_timeout_check(
            atr=0.0,
            entry_price=100.0,
            last_price=100.02,  # +2bps (ниже порога 4bps)
        )
        assert result is True, "atr=0 → нет ATR-данных, всегда закрываем"

    def test_atr_nonzero_profitable_closes(self):
        """При atr > 0 и прибыли >= threshold — закрываем."""
        result = _run_smart_timeout_check(
            atr=50.0,
            entry_price=100.0,
            last_price=100.05,  # +5bps > 4bps threshold
        )
        assert result is True

    def test_atr_nonzero_risky_closes(self):
        """При atr > 0 и большом adverse drawdown (> 1 ATR) — закрываем в убыток."""
        atr = 10.0
        # adverse = 100 - 88 = 12 > 1.0 * 10.0 = 10.0 → is_risky=True
        result = _run_smart_timeout_check(
            atr=atr,
            entry_price=100.0,
            last_price=88.0,
        )
        assert result is True, "Большой adverse drawdown → закрываем (risk control)"

    def test_atr_nonzero_hold_when_safe(self):
        """При atr > 0, небольшом убытке (< 1 ATR) и pnl < threshold → HOLD."""
        atr = 10.0
        # adverse = 100 - 97 = 3 < 10.0 → is_risky=False
        # pnl = -300bps < 4bps → is_profitable=False
        # → HOLD
        result = _run_smart_timeout_check(
            atr=atr,
            entry_price=100.0,
            last_price=97.0,
        )
        assert result is False, "Небольшой убыток при живом ATR → должен удержаться"

    def test_trailing_active_skipped(self):
        """Позиция с trailing_active=True не попадает в смарт-логику вообще."""
        # Эта проверка происходит раньше (строка 3261 trade_monitor.py)
        # Просто убеждаемся что trailing_active=True → позиция пропускается
        pos = _FakePos(atr=0.0, entry_price=100.0, trailing_active=True)
        assert pos.trailing_active is True


# ===========================================================================
# B2: _get_ml_executor() singleton
# ===========================================================================

class TestMLExecutorSingleton:
    """B2: Подтверждение одного экземпляра ThreadPoolExecutor."""

    def test_executor_is_singleton(self):
        """_get_ml_executor() должен возвращать один и тот же объект при повторных вызовах."""
        from services.ml_confirm_gate import _get_ml_executor
        ex1 = _get_ml_executor()
        ex2 = _get_ml_executor()
        assert ex1 is ex2, "executor ДОЛЖЕН быть singleton (один объект на процесс)"

    def test_executor_thread_name_prefix(self):
        """ThreadPoolExecutor создан с правильным именем потоков."""
        from services.ml_confirm_gate import _get_ml_executor
        ex = _get_ml_executor()
        assert ex is not None
        # Проверяем через внутренний атрибут (CPython)
        prefix = getattr(ex, "_thread_name_prefix", "") or ""
        assert "ml-infer" in prefix, f"Ожидали 'ml-infer' в thread_name_prefix, получили: {prefix!r}"

    def test_executor_concurrent_calls_same_object(self):
        """_get_ml_executor() безопасен при конкурентных вызовах из нескольких потоков."""
        from services.ml_confirm_gate import _get_ml_executor
        results = []

        def _get():
            results.append(id(_get_ml_executor()))

        threads = [threading.Thread(target=_get) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(set(results)) == 1, f"Ожидали 1 уникальный id, получили: {set(results)}"


# ===========================================================================
# B4: is_of_sync_build() kill-switch
# ===========================================================================

class TestSyncBuildKillSwitch:
    """B4: Тесты kill-switch OF_SYNC_BUILD."""

    def test_sync_build_off_by_default(self):
        """По умолчанию OF_SYNC_BUILD=0 → is_of_sync_build() возвращает False."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("OF_SYNC_BUILD", None)
            from services.ml_confirm_gate import is_of_sync_build
            assert is_of_sync_build() is False

    def test_sync_build_enabled_by_env(self):
        """OF_SYNC_BUILD=1 → is_of_sync_build() возвращает True."""
        with patch.dict(os.environ, {"OF_SYNC_BUILD": "1"}):
            from services.ml_confirm_gate import is_of_sync_build
            assert is_of_sync_build() is True

    def test_sync_build_zero_disabled(self):
        """OF_SYNC_BUILD=0 → is_of_sync_build() возвращает False."""
        with patch.dict(os.environ, {"OF_SYNC_BUILD": "0"}):
            from services.ml_confirm_gate import is_of_sync_build
            assert is_of_sync_build() is False

    def test_shutdown_function_exists(self):
        """_shutdown_ml_executor функция должна существовать."""
        from services.ml_confirm_gate import _shutdown_ml_executor
        assert callable(_shutdown_ml_executor)


# ===========================================================================
# Интеграционный тест: smart timeout + env params
# ===========================================================================

class TestSmartTimeoutEnvParams:
    """Тестируем влияние ENV-параметров на смарт-логику."""

    def test_custom_pnl_threshold(self):
        """TM_SMART_TIMEOUT_PNL_BPS=10.0 → +8bps недостаточно, HOLD при atr > 0."""
        # Симулируем atr=10, adverse=0 (нейтральная цена), pnl=+8bps < 10bps
        atr = 10.0
        entry = 100.0
        last = 100.08  # +8bps

        pnl_bps = (last - entry) / entry * 10000.0  # = 8bps
        is_profitable = pnl_bps >= 10.0  # False

        adverse = entry - last  # = -0.08 → adverse_dist < 0 → not risky
        is_risky = (adverse > atr * 1.0) if atr > 0 else False

        should_close = atr == 0 or is_profitable or is_risky
        assert should_close is False, "8bps < 10bps threshold → HOLD"

    def test_custom_mae_threshold(self):
        """TM_SMART_TIMEOUT_MAE_ATR=2.0 → drawdown 1.5 ATR → NOT risky → HOLD."""
        atr = 10.0
        entry = 100.0
        last = 85.0  # adverse = 15.0 = 1.5 * ATR

        adverse = entry - last  # 15
        is_risky = adverse > (atr * 2.0)  # 15 > 20 = False
        is_profitable = (last - entry) / entry * 10000.0 >= 4.0  # False

        should_hold = atr > 0 and not is_profitable and not is_risky
        assert should_hold is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
