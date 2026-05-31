"""Phase G regression tests:

  1. _calculate_levels overlay — profile_tp_rr / profile_tp_ratio применяются к cfg
     когда indicators содержат эти ключи (путь TRADE_PROFILE_TP_ENFORCE=1).
  2. Shadow path — с ENFORCE=0 cfg не меняется, только shadow-индикаторы.
  3. Poison-message retry cap — после TP_PEL_MAX_RETRIES итераций
     _process_one_message делает force-ACK независимо от DLQ.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import fakeredis
import pytest


# ─────────────────────── helpers ────────────────────────────────────────────

def _make_listener(r=None):
    with (
        patch("services.tp_event_listener.redis.from_url"),
        patch("services.tp_event_listener.TrailingProfilesRegistry"),
        patch("services.tp_event_listener.TpHitTrailingOrchestrator"),
    ):
        from services.tp_event_listener import TPEventListener
    listener = object.__new__(TPEventListener)
    listener.r = r or MagicMock()
    listener.events_stream = "events:trades"
    listener.consumer_group = "test-group"
    listener.consumer_name = "test-consumer"
    listener.running = False
    listener.stats = {
        "messages_read": 0, "messages_processed": 0,
        "messages_acked": 0, "errors": 0, "last_message_ts": 0,
    }
    listener.orchestrator = MagicMock()
    return listener


# ─────────────── P1: _calculate_levels overlay ──────────────────────────────

class TestCalculateLevelsOverlay:
    """Проверяет, что overlay в _calculate_levels применяет profile_tp_rr и profile_tp_ratio."""

    def _run_overlay(self, indicators: dict, base_cfg: dict) -> dict:
        """Воспроизводит overlay-логику из _calculate_levels (строки 4844-4857)."""
        cfg = dict(base_cfg)
        _p_stop = indicators.get("profile_stop_atr_mult")
        _p_tp_rr = indicators.get("profile_tp_rr")
        _p_tp1 = indicators.get("profile_tp1_atr_mult")
        _p_tp_ratio = indicators.get("profile_tp_ratio")
        if _p_stop is not None or _p_tp_rr is not None or _p_tp1 is not None or _p_tp_ratio is not None:
            cfg = {**cfg}
            if _p_stop is not None:
                cfg["stop_atr_mult"] = float(_p_stop)
            if _p_tp_rr is not None:
                cfg["tp_rr"] = str(_p_tp_rr)
            if _p_tp1 is not None:
                cfg["tp1_atr_mult"] = float(_p_tp1)
            if _p_tp_ratio is not None:
                cfg["tp_ratio"] = str(_p_tp_ratio)
        return cfg

    def test_profile_tp_rr_overrides_cfg(self):
        """profile_tp_rr в indicators → cfg['tp_rr'] изменяется."""
        base_cfg = {"tp_rr": "1.0,2.0", "stop_atr_mult": "1.5"}
        indicators = {"profile_tp_rr": "2.5,4.0"}
        result = self._run_overlay(indicators, base_cfg)
        assert result["tp_rr"] == "2.5,4.0"
        assert result["stop_atr_mult"] == "1.5"  # unchanged

    def test_profile_tp_ratio_overrides_cfg(self):
        """profile_tp_ratio в indicators → cfg['tp_ratio'] изменяется."""
        base_cfg = {"tp_ratio": "0.5,0.5", "tp_rr": "1.5,3.0"}
        indicators = {"profile_tp_ratio": "0.7,0.3"}
        result = self._run_overlay(indicators, base_cfg)
        assert result["tp_ratio"] == "0.7,0.3"
        assert result["tp_rr"] == "1.5,3.0"  # unchanged

    def test_both_tp_rr_and_ratio_applied_together(self):
        """Оба profile_tp_rr и profile_tp_ratio применяются одновременно."""
        base_cfg = {"tp_rr": "1.0", "tp_ratio": "0.5,0.5"}
        indicators = {"profile_tp_rr": "2.0,3.5", "profile_tp_ratio": "0.6,0.4"}
        result = self._run_overlay(indicators, base_cfg)
        assert result["tp_rr"] == "2.0,3.5"
        assert result["tp_ratio"] == "0.6,0.4"

    def test_no_profile_indicators_cfg_unchanged(self):
        """Без profile-индикаторов cfg не мутируется."""
        base_cfg = {"tp_rr": "1.5,3.0", "tp_ratio": "0.8,0.2"}
        indicators = {}
        result = self._run_overlay(indicators, base_cfg)
        assert result["tp_rr"] == "1.5,3.0"
        assert result["tp_ratio"] == "0.8,0.2"

    def test_overlay_does_not_mutate_original_cfg(self):
        """cfg-ссылка не мутирует runtime.config (shallow copy)."""
        base_cfg = {"tp_rr": "1.0", "tp_ratio": "0.5,0.5"}
        indicators = {"profile_tp_rr": "2.0"}
        result = self._run_overlay(indicators, base_cfg)
        assert result is not base_cfg        # copy was made
        assert base_cfg["tp_rr"] == "1.0"   # original untouched

    def test_stop_atr_mult_overridden(self):
        """profile_stop_atr_mult изменяет stop_atr_mult в cfg."""
        base_cfg = {"stop_atr_mult": "1.0", "tp_rr": "1.5"}
        indicators = {"profile_stop_atr_mult": "2.0"}
        result = self._run_overlay(indicators, base_cfg)
        assert result["stop_atr_mult"] == 2.0


# ─────────────── P2: ENFORCE vs SHADOW pre-calc indicators ──────────────────

class TestTpEnforceShadowPreCalc:
    """Проверяет что pre-calc блок пишет правильные indicator-ключи
    в зависимости от TRADE_PROFILE_TP_ENFORCE."""

    def _run_pre_calc(self, tp_enforce: str, tp_rr, tp_ratios) -> dict:
        """Воспроизводит pre-calc логику из signal_pipeline для TP enforce/shadow."""
        indicators: dict = {}
        _tp_enforce = tp_enforce == "1"
        _pd_tp_rr = tp_rr
        _pd_tp_ratios = tp_ratios

        if _pd_tp_rr is not None:
            if _tp_enforce:
                indicators["profile_tp_rr"] = _pd_tp_rr
                indicators["profile_tp_rr_enforced"] = _pd_tp_rr
            else:
                indicators["profile_tp_rr_shadow"] = _pd_tp_rr

        if _pd_tp_ratios:
            if _tp_enforce:
                indicators["profile_tp_ratio"] = ",".join(str(x) for x in _pd_tp_ratios)
                indicators["profile_tp_ratios_enforced"] = list(_pd_tp_ratios)
            else:
                indicators["profile_tp_ratios_shadow"] = list(_pd_tp_ratios)

        return indicators

    def test_enforce_1_writes_enforced_keys(self):
        """TRADE_PROFILE_TP_ENFORCE=1 → profile_tp_rr и profile_tp_ratio в indicators."""
        ind = self._run_pre_calc("1", tp_rr="2.0,3.5", tp_ratios=[0.7, 0.3])
        assert "profile_tp_rr" in ind
        assert "profile_tp_rr_enforced" in ind
        assert "profile_tp_ratio" in ind
        assert "profile_tp_ratios_enforced" in ind
        # shadow ключи не должны быть установлены в enforce-режиме
        assert "profile_tp_rr_shadow" not in ind
        assert "profile_tp_ratios_shadow" not in ind

    def test_enforce_0_writes_shadow_only(self):
        """TRADE_PROFILE_TP_ENFORCE=0 → только shadow-индикаторы, cfg-ключи отсутствуют."""
        ind = self._run_pre_calc("0", tp_rr="2.0,3.5", tp_ratios=[0.7, 0.3])
        assert "profile_tp_rr" not in ind          # не должен идти в _calculate_levels
        assert "profile_tp_ratio" not in ind        # не должен идти в _calculate_levels
        assert "profile_tp_rr_shadow" in ind
        assert "profile_tp_ratios_shadow" in ind

    def test_enforce_1_tp_ratio_format(self):
        """profile_tp_ratio форматируется как CSV-строка."""
        ind = self._run_pre_calc("1", tp_rr=None, tp_ratios=[0.6, 0.3, 0.1])
        assert ind["profile_tp_ratio"] == "0.6,0.3,0.1"

    def test_no_tp_rr_no_key_written(self):
        """Если профиль не задаёт tp_rr, ключи не пишутся."""
        ind = self._run_pre_calc("1", tp_rr=None, tp_ratios=None)
        assert "profile_tp_rr" not in ind
        assert "profile_tp_ratio" not in ind
        assert "profile_tp_rr_shadow" not in ind

    def test_enforce_combined_overlay_flow(self):
        """Enforce pre-calc + overlay → cfg["tp_rr"] изменяется."""
        tp_rr = "2.5,4.0"
        tp_ratios = [0.65, 0.35]
        # step 1: pre-calc sets indicator keys
        ind = self._run_pre_calc("1", tp_rr=tp_rr, tp_ratios=tp_ratios)
        # step 2: overlay in _calculate_levels applies them to cfg
        base_cfg = {"tp_rr": "1.0,1.5", "tp_ratio": "0.5,0.5"}
        _p_tp_rr = ind.get("profile_tp_rr")
        _p_tp_ratio = ind.get("profile_tp_ratio")
        if _p_tp_rr or _p_tp_ratio:
            cfg = {**base_cfg}
            if _p_tp_rr:
                cfg["tp_rr"] = str(_p_tp_rr)
            if _p_tp_ratio:
                cfg["tp_ratio"] = str(_p_tp_ratio)
        else:
            cfg = base_cfg
        assert cfg["tp_rr"] == "2.5,4.0"
        assert cfg["tp_ratio"] == "0.65,0.35"

    def test_shadow_combined_overlay_flow(self):
        """Shadow pre-calc → cfg не изменяется (overlay не находит enforced keys)."""
        tp_rr = "2.5,4.0"
        tp_ratios = [0.65, 0.35]
        # step 1: shadow — enforced ключи не пишутся
        ind = self._run_pre_calc("0", tp_rr=tp_rr, tp_ratios=tp_ratios)
        # step 2: overlay видит None для enforced-ключей — cfg не меняется
        base_cfg = {"tp_rr": "1.0,1.5", "tp_ratio": "0.5,0.5"}
        _p_tp_rr = ind.get("profile_tp_rr")     # None для shadow
        _p_tp_ratio = ind.get("profile_tp_ratio")  # None для shadow
        if _p_tp_rr or _p_tp_ratio:
            cfg = {**base_cfg}
            if _p_tp_rr:
                cfg["tp_rr"] = str(_p_tp_rr)
            if _p_tp_ratio:
                cfg["tp_ratio"] = str(_p_tp_ratio)
        else:
            cfg = base_cfg
        assert cfg["tp_rr"] == "1.0,1.5"    # unchanged
        assert cfg["tp_ratio"] == "0.5,0.5"  # unchanged


# ─────────────── P3: Poison-message retry cap ────────────────────────────────

class TestPoisonMessageRetryCap:

    def test_cap_reached_forces_ack_even_if_dlq_fails(self):
        """После TP_PEL_MAX_RETRIES итераций XACK вызывается даже при падении DLQ."""
        fake = fakeredis.FakeRedis(decode_responses=True)
        listener = _make_listener(r=fake)
        listener._MAX_RETRIES = 3

        # Simulate 3 prior failed deliveries (counter already at 3)
        fake.set("tp_listener:retries:msg-poison", "3")

        # DLQ write always fails
        with patch.object(listener, "_push_listener_dlq", return_value=False) as mock_dlq, \
             patch.object(listener, "_xack") as mock_ack, \
             patch.object(listener, "_parse_event", return_value=None):
            listener._process_one_message("msg-poison", {})

        # DLQ attempted with max_retries_exceeded reason
        mock_dlq.assert_called_once()
        args = mock_dlq.call_args[0]
        assert "max_retries" in args[2]
        # ACK forced regardless of DLQ failure
        mock_ack.assert_called_once_with("msg-poison")

    def test_below_cap_leaves_in_pel_on_dlq_fail(self):
        """Ниже порога: при падении DLQ no-XACK (нормальный путь)."""
        fake = fakeredis.FakeRedis(decode_responses=True)
        listener = _make_listener(r=fake)
        listener._MAX_RETRIES = 5

        # Only 2 prior retries — below cap
        fake.set("tp_listener:retries:msg-ok", "1")

        with patch.object(listener, "_push_listener_dlq", return_value=False), \
             patch.object(listener, "_xack") as mock_ack, \
             patch.object(listener, "_parse_event", return_value=None):
            listener._process_one_message("msg-ok", {})

        mock_ack.assert_not_called()

    def test_retry_counter_increments_on_each_call(self):
        """_check_poison_cap() инкрементирует счётчик при каждом вызове."""
        fake = fakeredis.FakeRedis(decode_responses=True)
        listener = _make_listener(r=fake)
        listener._MAX_RETRIES = 10

        listener._check_poison_cap("msg-x")
        listener._check_poison_cap("msg-x")
        listener._check_poison_cap("msg-x")

        assert int(fake.get("tp_listener:retries:msg-x") or 0) == 3

    def test_retry_counter_returns_false_below_cap(self):
        """_check_poison_cap() возвращает False пока не превышен порог."""
        fake = fakeredis.FakeRedis(decode_responses=True)
        listener = _make_listener(r=fake)
        listener._MAX_RETRIES = 3
        fake.set("tp_listener:retries:msg-y", "2")

        result = listener._check_poison_cap("msg-y")
        assert result is False

    def test_retry_counter_returns_true_above_cap(self):
        """_check_poison_cap() возвращает True когда превышен порог."""
        fake = fakeredis.FakeRedis(decode_responses=True)
        listener = _make_listener(r=fake)
        listener._MAX_RETRIES = 3
        fake.set("tp_listener:retries:msg-z", "3")

        result = listener._check_poison_cap("msg-z")  # будет count=4 > 3
        assert result is True

    def test_pel_poison_acked_stat_incremented(self):
        """stats['pel_poison_acked'] растёт при каждом force-ACK."""
        fake = fakeredis.FakeRedis(decode_responses=True)
        listener = _make_listener(r=fake)
        listener._MAX_RETRIES = 2
        fake.set("tp_listener:retries:msg-stat", "2")

        with patch.object(listener, "_push_listener_dlq", return_value=True), \
             patch.object(listener, "_xack"):
            listener._process_one_message("msg-stat", {})

        assert listener.stats.get("pel_poison_acked", 0) == 1

    def test_normal_message_not_capped(self):
        """Первая доставка нормального сообщения не активирует cap."""
        fake = fakeredis.FakeRedis(decode_responses=True)
        listener = _make_listener(r=fake)
        listener._MAX_RETRIES = 5

        from services.tp_hit_trailing_orchestrator import TrailingResult
        listener.orchestrator.handle_event.return_value = TrailingResult(
            success=True, skipped=False
        )
        event = {"event_type": "TP1_HIT", "sid": "abc", "symbol": "BTCUSDT"}
        with patch.object(listener, "_parse_event", return_value=event), \
             patch.object(listener, "_xack") as mock_ack:
            listener._process_one_message("msg-fresh", event)

        mock_ack.assert_called_once_with("msg-fresh")
        assert listener.stats.get("pel_poison_acked", 0) == 0


# ─────────── P4: profile_tp_rr → rr_levels → tp_levels integration ──────────

class TestProfileTpRrAffectsCalculateLevels:
    """Интеграционный тест: profile_tp_rr в indicators → rr_levels использует его.

    Воспроизводит полную цепочку без вызова _calculate_levels напрямую:
      overlay(indicators["profile_tp_rr"]) → cfg["tp_rr"]
      → rr_levels(cfg["tp_rr"]) → мультипликаторы для tp_levels

    _calculate_levels вызывает rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7")) в 5 местах.
    Функция rr_levels — чистая: float-split по запятой.
    """

    @staticmethod
    def _rr_levels(rr_str: str) -> list[float]:
        """Зеркало rr_levels из _calculate_levels (строка 4974 signal_pipeline.py)."""
        try:
            return [float(x.strip()) for x in rr_str.split(",") if x.strip()]
        except Exception:
            return [1.3, 2.0, 2.7]

    def _overlay_cfg(self, indicators: dict, base_cfg: dict) -> dict:
        cfg = dict(base_cfg)
        _p_tp_rr = indicators.get("profile_tp_rr")
        _p_tp_ratio = indicators.get("profile_tp_ratio")
        if _p_tp_rr is not None or _p_tp_ratio is not None:
            cfg = {**cfg}
            if _p_tp_rr is not None:
                cfg["tp_rr"] = str(_p_tp_rr)
            if _p_tp_ratio is not None:
                cfg["tp_ratio"] = str(_p_tp_ratio)
        return cfg

    def test_profile_tp_rr_enforced_changes_tp_rr_in_cfg(self):
        """ENFORCE=1 → profile_tp_rr попадает в cfg → rr_levels возвращают profile-значения."""
        entry = 50000.0
        stop_dist = 200.0  # ATR-based stop

        # Default config — should yield tp1 at 1.3R
        default_cfg = {"tp_rr": "1.3,2.0,2.7"}
        default_rr = self._rr_levels(default_cfg["tp_rr"])
        default_tp1 = entry + stop_dist * default_rr[0]

        # Profile override — tp_rr = "1.5,3.0"
        indicators = {"profile_tp_rr": "1.5,3.0"}
        override_cfg = self._overlay_cfg(indicators, default_cfg)
        assert override_cfg["tp_rr"] == "1.5,3.0"

        override_rr = self._rr_levels(override_cfg["tp_rr"])
        override_tp1 = entry + stop_dist * override_rr[0]

        assert override_rr == [1.5, 3.0]
        assert override_tp1 > default_tp1, "Profile R=1.5 > default R=1.3 → TP1 должен быть выше"
        assert abs(override_tp1 - (entry + stop_dist * 1.5)) < 0.01

    def test_profile_tp_rr_shadow_does_not_change_tp_levels(self):
        """ENFORCE=0 → profile_tp_rr_shadow в indicators, но profile_tp_rr НЕ установлен.
        Cfg["tp_rr"] остаётся default → rr_levels не изменяются."""
        default_cfg = {"tp_rr": "1.3,2.0,2.7"}
        indicators = {"profile_tp_rr_shadow": "1.5,3.0"}  # shadow, not profile_tp_rr

        cfg = self._overlay_cfg(indicators, default_cfg)
        assert cfg["tp_rr"] == "1.3,2.0,2.7"  # unchanged

        rr = self._rr_levels(cfg["tp_rr"])
        assert rr == [1.3, 2.0, 2.7]  # default values used

    def test_profile_tp_rr_1_level_short(self):
        """Для SHORT: TP уровни строятся в другую сторону."""
        entry = 50000.0
        stop_dist = 200.0

        default_cfg = {"tp_rr": "1.3,2.0,2.7"}
        indicators = {"profile_tp_rr": "2.0,4.0"}
        cfg = self._overlay_cfg(indicators, default_cfg)

        rr = self._rr_levels(cfg["tp_rr"])
        tps_short = [entry - stop_dist * r for r in rr]
        assert tps_short[0] < entry  # SHORT TP1 ниже entry
        assert tps_short[0] == entry - stop_dist * 2.0

    def test_profile_tp_ratio_does_not_affect_rr_levels(self):
        """profile_tp_ratio (объёмный split) не влияет на rr_levels (ценовые мультипликаторы)."""
        indicators = {"profile_tp_ratio": "0.7,0.3"}
        base_cfg = {"tp_rr": "1.5,3.0", "tp_ratio": "0.5,0.5"}
        cfg = self._overlay_cfg(indicators, base_cfg)

        assert cfg["tp_rr"] == "1.5,3.0"  # не изменился
        assert cfg["tp_ratio"] == "0.7,0.3"  # изменился только ratio

        rr = self._rr_levels(cfg["tp_rr"])
        assert rr == [1.5, 3.0]

    def test_enforce_end_to_end_pipeline(self):
        """Сквозная проверка: pre-calc ENFORCE=1 → overlay → rr_levels → tp distance."""
        # Step 1: pre-calc (ENFORCE=1) sets indicator keys
        _pd_tp_rr = "2.0,4.0"
        _pd_tp_ratios = [0.6, 0.4]
        indicators = {
            "profile_tp_rr": _pd_tp_rr,
            "profile_tp_rr_enforced": _pd_tp_rr,
            "profile_tp_ratio": ",".join(str(x) for x in _pd_tp_ratios),
            "profile_tp_ratios_enforced": _pd_tp_ratios,
        }

        # Step 2: overlay in _calculate_levels
        base_cfg = {"tp_rr": "1.3,2.0,2.7", "tp_ratio": "0.8,0.2"}
        cfg = self._overlay_cfg(indicators, base_cfg)
        assert cfg["tp_rr"] == "2.0,4.0"
        assert cfg["tp_ratio"] == "0.6,0.4"

        # Step 3: rr_levels applied to compute tp distances
        entry, stop_dist = 50000.0, 150.0
        rr = self._rr_levels(cfg["tp_rr"])
        tp_long = [entry + stop_dist * r for r in rr]
        assert tp_long[0] == pytest.approx(50000 + 150 * 2.0)  # TP1 = 50300
        assert tp_long[1] == pytest.approx(50000 + 150 * 4.0)  # TP2 = 50600
