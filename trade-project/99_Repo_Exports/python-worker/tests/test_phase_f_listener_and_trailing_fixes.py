"""Phase F regression tests: listener DLQ + trailing_decisions mapping.

Покрытие:
  - listener parse_error → DLQ push + ACK
  - listener orchestrator hard-failure → DLQ push + ACK
  - listener orchestrator skipped → no DLQ
  - listener exception during handle → DLQ push + ACK
  - trailing_decisions INSERT uses correct columns (trail_distance_price, atr_value, atr_mult)
  - idempotency_key включает position_id
  - handle_event возвращает TrailingResult, а не bool
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from services.tp_hit_trailing_orchestrator import TrailingResult


# ───────────────────────── helpers ──────────────────────────────────────────

def _make_listener(r=None):
    """Создаёт TPEventListener с замоканным Redis и оркестратором.

    Использует object.__new__ + ручную инициализацию, чтобы обойти Redis-connect
    в __init__ (не нужен fakeredis — тестируем только _process_one_message).
    """
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


# ───────────────────── P1: listener DLQ tests ───────────────────────────────

class TestListenerDLQ:
    def test_parse_error_pushes_dlq_and_acks(self):
        """Непарсируемое сообщение → DLQ + ACK."""
        r = MagicMock()
        listener = _make_listener(r)
        # _parse_event вернёт None
        with patch.object(listener, "_parse_event", return_value=None):
            listener._process_one_message("msg-1", {"bad": "data"})

        # DLQ xadd вызван
        assert r.xadd.called
        xadd_args = r.xadd.call_args
        stream_name = xadd_args[0][0]
        assert "dlq" in stream_name
        entry = xadd_args[0][1]
        assert entry["reason"].startswith("parse_error")

        # ACK вызван
        r.xack.assert_called_once_with("events:trades", "test-group", "msg-1")

    def test_orchestrator_hard_failure_pushes_dlq(self):
        """handle_event → TrailingResult(success=False, skipped=False, error=X) → DLQ."""
        r = MagicMock()
        listener = _make_listener(r)
        listener.orchestrator.handle_event.return_value = TrailingResult(
            success=False, skipped=False, error="signal_not_found"
        )
        event = {"event_type": "TP1_HIT", "sid": "abc", "symbol": "BTCUSDT", "price": "50000"}
        with patch.object(listener, "_parse_event", return_value=event):
            listener._process_one_message("msg-2", {"data": "{}"})

        assert r.xadd.called
        entry = r.xadd.call_args[0][1]
        assert "orchestrator_error" in entry["reason"]
        assert "signal_not_found" in entry["reason"]
        r.xack.assert_called_once_with("events:trades", "test-group", "msg-2")

    def test_orchestrator_skipped_no_dlq(self):
        """Skipped (dedup_hit, symbol_filtered) → не пишем в DLQ."""
        r = MagicMock()
        listener = _make_listener(r)
        listener.orchestrator.handle_event.return_value = TrailingResult(
            success=False, skipped=True, error="dedup_hit"
        )
        event = {"event_type": "TP1_HIT", "sid": "abc", "symbol": "BTCUSDT", "price": "50000"}
        with patch.object(listener, "_parse_event", return_value=event):
            listener._process_one_message("msg-3", {"data": "{}"})

        r.xadd.assert_not_called()
        r.xack.assert_called_once()

    def test_orchestrator_success_no_dlq(self):
        """Успешный trailing → не DLQ."""
        r = MagicMock()
        listener = _make_listener(r)
        listener.orchestrator.handle_event.return_value = TrailingResult(
            success=True, skipped=False, new_sl=59000.0
        )
        event = {"event_type": "TP1_HIT", "sid": "abc", "symbol": "BTCUSDT", "price": "50000"}
        with patch.object(listener, "_parse_event", return_value=event):
            listener._process_one_message("msg-4", {"data": "{}"})

        r.xadd.assert_not_called()
        r.xack.assert_called_once()

    def test_exception_during_handle_pushes_dlq(self):
        """Необработанное исключение → DLQ с reason exception:<Type>."""
        r = MagicMock()
        listener = _make_listener(r)
        listener.orchestrator.handle_event.side_effect = RuntimeError("boom")
        event = {"event_type": "TP1_HIT", "sid": "abc"}
        with patch.object(listener, "_parse_event", return_value=event):
            listener._process_one_message("msg-5", {"data": "{}"})

        assert r.xadd.called
        entry = r.xadd.call_args[0][1]
        assert "exception" in entry["reason"]
        assert "RuntimeError" in entry["reason"]
        r.xack.assert_called_once()

    def test_dlq_failure_leaves_in_pel(self):
        """Если xadd в DLQ падает, XACK НЕ вызывается — сообщение остаётся в PEL для reclaimer."""
        r = MagicMock()
        r.xadd.side_effect = Exception("redis down")
        listener = _make_listener(r)
        with patch.object(listener, "_parse_event", return_value=None):
            listener._process_one_message("msg-6", {})

        r.xack.assert_not_called()


# ───────────────────── P2: trailing_decisions mapping ───────────────────────

class TestTrailingDecisionsMapping:
    """Проверяет корректный маппинг полей при INSERT в trailing_decisions."""

    def _make_orchestrator(self):
        from unittest.mock import patch as p, MagicMock as MM
        with (
            p("services.tp_hit_trailing_orchestrator.redis.from_url"),
            p("services.tp_hit_trailing_orchestrator.TrailingProfilesRegistry"),
            p("services.tp_hit_trailing_orchestrator.OrderTrailingDispatcher"),
        ):
            from services.tp_hit_trailing_orchestrator import TpHitTrailingOrchestrator
            orch = object.__new__(TpHitTrailingOrchestrator)
            orch.r = MM()
            orch.profiles = MM()
            orch.dispatcher = MM()
            orch.events_logger = None
            return orch

    def test_trail_distance_maps_to_trail_distance_price_not_atr_mult(self):
        """trail_distance в INSERT = trail_distance_price (не trail_atr_mult)."""
        captured: list[tuple] = []

        class FakeCur:
            def execute(self, _sql, params):
                captured.append(params)
            def close(self): pass

        class FakeConn:
            def cursor(self): return FakeCur()
            def commit(self): pass
            def close(self): pass

        orch = self._make_orchestrator()

        with patch("psycopg2.connect", return_value=FakeConn()), \
             patch("psycopg2.extras.Json", side_effect=lambda x: x), \
             patch.dict("os.environ", {"TRADES_DB_DSN": "postgresql://fake"}):
            orch._write_trailing_decision_pg(
                sid="of:BTCUSDT:123:LONG",
                symbol="BTCUSDT",
                profile_name="rocket_v1",
                metadata={
                    "tp_level": 1,
                    "position_id": "POS-42",
                    "new_sl": "59000.0",
                    "trail_distance_price": "500.0",   # price units = ATR × mult
                    "atr_value": "333.33",
                    "trail_atr_mult": "1.5",           # legacy key
                    "atr_mult": "1.5",                 # preferred key
                    "policy_hash": "abc",
                    "profile_hash": "def",
                    "schema_ver": 2,
                },
            )

        assert len(captured) == 1
        params = captured[0]
        # positions in INSERT: sid, symbol, position_id, event_type, profile, side,
        # tp_level, old_sl, new_sl, trail_distance, atr_value, atr_mult, idempotency_key, ...
        trail_distance = params[9]
        atr_value      = params[10]
        atr_mult       = params[11]

        assert trail_distance == 500.0, f"trail_distance should be 500.0, got {trail_distance}"
        assert atr_value == 333.33,     f"atr_value should be 333.33, got {atr_value}"
        assert atr_mult == 1.5,         f"atr_mult should be 1.5, got {atr_mult}"

    def test_idempotency_key_includes_position_id(self):
        """idempotency_key = {sid}:{position_id}:{tp_level}:TRAILING_STARTED."""
        captured: list[tuple] = []

        class FakeCur:
            def execute(self, _sql, params):
                captured.append(params)
            def close(self): pass

        class FakeConn:
            def cursor(self): return FakeCur()
            def commit(self): pass
            def close(self): pass

        orch = self._make_orchestrator()

        with patch("psycopg2.connect", return_value=FakeConn()), \
             patch("psycopg2.extras.Json", side_effect=lambda x: x), \
             patch.dict("os.environ", {"TRADES_DB_DSN": "postgresql://fake"}):
            orch._write_trailing_decision_pg(
                sid="of:BTCUSDT:123:LONG",
                symbol="BTCUSDT",
                profile_name="rocket_v1",
                metadata={
                    "tp_level": 1,
                    "position_id": "POS-99",
                    "new_sl": "59000.0",
                    "trail_distance_price": "500.0",
                    "atr_value": "333.33",
                    "atr_mult": "1.5",
                },
            )

        params = captured[0]
        idempotency_key = params[12]
        assert idempotency_key == "of:BTCUSDT:123:LONG:POS-99:1:TRAILING_STARTED", (
            f"unexpected idempotency_key: {idempotency_key}"
        )

    def test_no_dsn_is_noop(self):
        """Без TRADES_DB_DSN INSERT не выбрасывает исключений (fail-open)."""
        orch = self._make_orchestrator()
        import os
        env = {k: v for k, v in os.environ.items() if k not in ("TRADES_DB_DSN", "ANALYTICS_DB_DSN")}
        with patch.dict("os.environ", env, clear=True):
            orch._write_trailing_decision_pg(
                sid="x", symbol="X", profile_name="p",
                metadata={"tp_level": 1, "new_sl": "1.0"},
            )


# ───────────────────── handle_event return type ─────────────────────────────

class TestHandleEventReturnsTrailingResult:
    def test_handle_event_returns_trailing_result_not_bool(self):
        """handle_event() должен возвращать TrailingResult, а не bool."""
        with (
            patch("services.tp_hit_trailing_orchestrator.redis.from_url"),
            patch("services.tp_hit_trailing_orchestrator.TrailingProfilesRegistry") as MockReg,
            patch("services.tp_hit_trailing_orchestrator.OrderTrailingDispatcher"),
        ):
            MockReg.return_value.validate_default.return_value = None
            from services.tp_hit_trailing_orchestrator import TpHitTrailingOrchestrator
            orch = object.__new__(TpHitTrailingOrchestrator)
            orch.r = MagicMock()
            orch.profiles = MagicMock()
            orch.dispatcher = MagicMock()
            orch.events_logger = None
            orch.stats = {k: 0 for k in ("events_processed", "tp1_hits", "trailing_started",
                                          "trailing_failed", "signals_not_found", "no_trail_flag")}
            orch.symbol_filter_enabled = False
            orch.trailing_symbols = set()
            orch.source_filter_enabled = False
            orch.trailing_sources = set()
            orch.signal_key_prefixes = ["signals:"]
            orch.default_profile = "protective_only"
            orch.trail_activate_tp_level = 1

            result = orch.handle_event({"event_type": "SL_HIT", "sid": "x"})

        assert isinstance(result, TrailingResult), (
            f"handle_event должен возвращать TrailingResult, получен: {type(result)}"
        )
        assert result.skipped is True


# ───────────────────── PEL reclaimer ────────────────────────────────────────

class TestPelReclaimer:
    def test_reclaim_calls_process_for_each_claimed_message(self):
        """_reclaim_pel() должен вызывать _process_one_message для каждого claimed сообщения."""
        r = MagicMock()
        # xautoclaim returns (next_id, [(msg_id, fields), ...], [])
        r.xautoclaim.return_value = ("0-0", [("msg-r1", {"k": "v"}), ("msg-r2", {"k": "v2"})], [])
        listener = _make_listener(r)
        processed = []
        with patch.object(listener, "_process_one_message", side_effect=lambda m, f: processed.append(m)):
            listener._reclaim_pel()
        assert processed == ["msg-r1", "msg-r2"]

    def test_reclaim_noop_on_empty_pel(self):
        """Пустой PEL — _process_one_message не вызывается."""
        r = MagicMock()
        r.xautoclaim.return_value = ("0-0", [], [])
        listener = _make_listener(r)
        with patch.object(listener, "_process_one_message") as mock_proc:
            listener._reclaim_pel()
        mock_proc.assert_not_called()

    def test_reclaim_ignores_xautoclaim_error(self):
        """Если xautoclaim упал, reclaimer не пробрасывает исключение."""
        r = MagicMock()
        r.xautoclaim.side_effect = Exception("NOGROUP")
        listener = _make_listener(r)
        listener._reclaim_pel()  # should not raise

    def test_reclaim_increments_pel_reclaimed_stat(self):
        """Счётчик pel_reclaimed растёт на каждое claimed сообщение."""
        r = MagicMock()
        r.xautoclaim.return_value = ("0-0", [("id1", {}), ("id2", {}), ("id3", {})], [])
        listener = _make_listener(r)
        with patch.object(listener, "_process_one_message"):
            listener._reclaim_pel()
        assert listener.stats.get("pel_reclaimed") == 3


# ───────────────────── trailing_decisions side column ───────────────────────

class TestTrailingDecisionsSideColumn:
    def _make_orchestrator(self):
        from unittest.mock import patch as p, MagicMock as MM
        with (
            p("services.tp_hit_trailing_orchestrator.redis.from_url"),
            p("services.tp_hit_trailing_orchestrator.TrailingProfilesRegistry"),
            p("services.tp_hit_trailing_orchestrator.OrderTrailingDispatcher"),
        ):
            from services.tp_hit_trailing_orchestrator import TpHitTrailingOrchestrator
            orch = object.__new__(TpHitTrailingOrchestrator)
            orch.r = MM()
            orch.profiles = MM()
            orch.dispatcher = MM()
            orch.events_logger = None
            return orch

    def test_side_column_written_from_metadata(self):
        """side в trailing_decisions INSERT должен браться из metadata["side"]."""
        captured: list[tuple] = []

        class FakeCur:
            def execute(self, _sql, params): captured.append(params)
            def close(self): pass

        class FakeConn:
            def cursor(self): return FakeCur()
            def commit(self): pass
            def close(self): pass

        orch = self._make_orchestrator()

        with patch("psycopg2.connect", return_value=FakeConn()), \
             patch("psycopg2.extras.Json", side_effect=lambda x: x), \
             patch.dict("os.environ", {"TRADES_DB_DSN": "postgresql://fake"}):
            orch._write_trailing_decision_pg(
                sid="of:BTCUSDT:123:LONG",
                symbol="BTCUSDT",
                profile_name="rocket_v1",
                metadata={
                    "tp_level": 1,
                    "position_id": "POS-7",
                    "side": "LONG",
                    "new_sl": "59000.0",
                    "trail_distance_price": "500.0",
                    "atr_value": "333.33",
                    "atr_mult": "1.5",
                },
            )

        params = captured[0]
        # INSERT column order: sid, symbol, position_id, event_type, profile, side, ...
        side_col = params[5]
        assert side_col == "LONG", f"expected side=LONG, got {side_col!r}"

    def test_side_column_short(self):
        """side=SHORT тоже корректно пишется."""
        captured: list[tuple] = []

        class FakeCur:
            def execute(self, _sql, params): captured.append(params)
            def close(self): pass

        class FakeConn:
            def cursor(self): return FakeCur()
            def commit(self): pass
            def close(self): pass

        orch = self._make_orchestrator()

        with patch("psycopg2.connect", return_value=FakeConn()), \
             patch("psycopg2.extras.Json", side_effect=lambda x: x), \
             patch.dict("os.environ", {"TRADES_DB_DSN": "postgresql://fake"}):
            orch._write_trailing_decision_pg(
                sid="of:ETHUSDT:456:SHORT",
                symbol="ETHUSDT",
                profile_name="protective_only",
                metadata={
                    "tp_level": 2,
                    "position_id": "POS-8",
                    "side": "SHORT",
                    "new_sl": "2000.0",
                    "trail_distance_price": "80.0",
                    "atr_value": "40.0",
                    "atr_mult": "2.0",
                },
            )

        params = captured[0]
        assert params[5] == "SHORT"


# ───────────────────── Watermark FSM arm via orchestrator ───────────────────

class TestWatermarkFsmArm:
    """Проверяет, что handle_event армирует WatermarkFSM при WATERMARK_TRAILING_ENABLED=1."""

    def _make_orchestrator_with_fakeredis(self, monkeypatch):
        import fakeredis
        fake = fakeredis.FakeRedis(decode_responses=True)
        monkeypatch.setattr("services.trailing_profiles.redis.from_url", lambda *a, **kw: fake)
        monkeypatch.setattr("services.tp_hit_trailing_orchestrator.redis.from_url", lambda *a, **kw: fake)
        monkeypatch.setattr(
            "services.tp_hit_trailing_orchestrator.OrderTrailingDispatcher",
            lambda *a, **kw: MagicMock(
                send_trailing_command_from_atr=MagicMock(return_value=True),
                send_trailing_command=MagicMock(return_value=True),
                send_trailing_modify=MagicMock(return_value=True),
                get_symbol_point=MagicMock(return_value=0.01),
            ),
        )
        monkeypatch.setenv("DEFAULT_TRAIL_PROFILE", "protective_only")
        monkeypatch.setenv("TRAILING_SYMBOLS", "*")
        monkeypatch.setenv("TRAILING_SOURCES", "*")
        monkeypatch.setenv("WATERMARK_TRAILING_ENABLED", "1")
        from services.tp_hit_trailing_orchestrator import TpHitTrailingOrchestrator
        o = TpHitTrailingOrchestrator()
        o.r = fake
        return o, fake

    def _seed(self, r, sid, trail_profile="protective_only"):
        import json
        r.set(f"signals:{sid}", json.dumps({
            "sid": sid, "symbol": "BTCUSDT", "side": "LONG",
            "source": "cryptoorderflow", "trail_after_tp1": True,
            "trail_profile": trail_profile, "atr": 100.0,
            "entry": 50000.0, "sl": 49500.0, "tp_levels": [50500.0, 51000.0],
        }))

    def test_watermark_fsm_armed_after_tp1_hit(self, monkeypatch):
        """После успешного TP1_HIT с WATERMARK_TRAILING_ENABLED=1 → trail:wm:{sid} создан."""
        o, fake = self._make_orchestrator_with_fakeredis(monkeypatch)
        self._seed(fake, "sid-wm-1")

        result = o.handle_event({
            "event_type": "TP1_HIT", "sid": "sid-wm-1", "symbol": "BTCUSDT",
            "price": "50500.0", "ts": "1", "source": "test",
        })
        assert result.success is True
        assert fake.exists("trail:wm:sid-wm-1"), "WatermarkSnapshot should be saved in Redis"
        snap_data: dict[str, str] = fake.hgetall("trail:wm:sid-wm-1")  # type: ignore[assignment]
        assert snap_data["state"] == "active"
        assert snap_data["side"] == "LONG"
        assert snap_data["symbol"] == "BTCUSDT"

    def test_watermark_fsm_not_armed_when_disabled(self, monkeypatch):
        """При WATERMARK_TRAILING_ENABLED=0 trail:wm:{sid} не создаётся."""
        import fakeredis
        fake = fakeredis.FakeRedis(decode_responses=True)
        monkeypatch.setattr("services.trailing_profiles.redis.from_url", lambda *a, **kw: fake)
        monkeypatch.setattr("services.tp_hit_trailing_orchestrator.redis.from_url", lambda *a, **kw: fake)
        monkeypatch.setattr(
            "services.tp_hit_trailing_orchestrator.OrderTrailingDispatcher",
            lambda *a, **kw: MagicMock(
                send_trailing_command_from_atr=MagicMock(return_value=True),
                send_trailing_command=MagicMock(return_value=True),
                send_trailing_modify=MagicMock(return_value=True),
                get_symbol_point=MagicMock(return_value=0.01),
            ),
        )
        monkeypatch.setenv("DEFAULT_TRAIL_PROFILE", "protective_only")
        monkeypatch.setenv("TRAILING_SYMBOLS", "*")
        monkeypatch.setenv("TRAILING_SOURCES", "*")
        monkeypatch.setenv("WATERMARK_TRAILING_ENABLED", "0")
        from services.tp_hit_trailing_orchestrator import TpHitTrailingOrchestrator
        o = TpHitTrailingOrchestrator()
        o.r = fake
        self._seed(fake, "sid-wm-2")

        result = o.handle_event({
            "event_type": "TP1_HIT", "sid": "sid-wm-2", "symbol": "BTCUSDT",
            "price": "50500.0", "ts": "1", "source": "test",
        })
        assert result.success is True
        assert not fake.exists("trail:wm:sid-wm-2"), "WatermarkSnapshot must NOT be saved when disabled"


# ───────────────────── TP enforce shadow indicators ──────────────────────────

class TestTpEnforceShadowIndicators:
    """Проверяет что profile_tp_rr_shadow / profile_tp_ratios_shadow пишутся корректно."""

    def test_watermark_fsm_unit_arm_sets_state_active(self):
        """FSM.arm() устанавливает state=TRAILING_ACTIVE и high_wm для LONG."""
        from services.watermark_trailing import fsm_from_signal, WMState
        fsm = fsm_from_signal(
            sid="s1", side="LONG", entry_price=50000.0,
            original_sl=49500.0, atr=100.0, atr_mult=1.5,
            profile_name="protective_only", symbol="BTCUSDT",
        )
        assert fsm.snap.state == WMState.PENDING
        dec = fsm.arm(price=50500.0, now_ms=1000)
        assert fsm.snap.state == WMState.TRAILING_ACTIVE
        assert fsm.snap.high_wm == 50500.0
        assert dec.moved is True

    def test_watermark_fsm_on_tick_ratchets_sl_higher(self):
        """on_tick на более высокой цене двигает SL вверх для LONG."""
        from services.watermark_trailing import fsm_from_signal
        fsm = fsm_from_signal(
            sid="s2", side="LONG", entry_price=50000.0,
            original_sl=49500.0, atr=100.0, atr_mult=1.5,
            profile_name="protective_only", symbol="BTCUSDT",
        )
        fsm.arm(price=50500.0, now_ms=1000)
        sl_after_arm = fsm.snap.current_sl

        dec = fsm.on_tick(price=51000.0, now_ms=2000)
        assert dec.moved is True
        assert fsm.snap.current_sl is not None and sl_after_arm is not None
        assert fsm.snap.current_sl > sl_after_arm, "SL должен рэтчетиться вверх при росте цены"

    def test_watermark_fsm_sl_never_retreats_long(self):
        """Для LONG SL никогда не опускается ниже предыдущего уровня."""
        from services.watermark_trailing import fsm_from_signal
        fsm = fsm_from_signal(
            sid="s3", side="LONG", entry_price=50000.0,
            original_sl=49500.0, atr=100.0, atr_mult=1.5,
            profile_name="protective_only", symbol="BTCUSDT",
        )
        fsm.arm(price=51000.0, now_ms=1000)
        high_sl = fsm.snap.current_sl

        # Цена откатывается — SL не должен отступить
        dec = fsm.on_tick(price=50600.0, now_ms=2000)
        assert not dec.moved
        assert fsm.snap.current_sl == high_sl

    def test_watermark_fsm_short_ratchets_sl_lower(self):
        """on_tick на более низкой цене двигает SL вниз для SHORT."""
        from services.watermark_trailing import fsm_from_signal
        fsm = fsm_from_signal(
            sid="s4", side="SHORT", entry_price=50000.0,
            original_sl=50500.0, atr=100.0, atr_mult=1.5,
            profile_name="protective_only", symbol="BTCUSDT",
        )
        fsm.arm(price=49500.0, now_ms=1000)
        sl_after_arm = fsm.snap.current_sl

        dec = fsm.on_tick(price=49000.0, now_ms=2000)
        assert dec.moved is True
        assert fsm.snap.current_sl is not None and sl_after_arm is not None
        assert fsm.snap.current_sl < sl_after_arm, "SL для SHORT должен рэтчетиться вниз при падении цены"
