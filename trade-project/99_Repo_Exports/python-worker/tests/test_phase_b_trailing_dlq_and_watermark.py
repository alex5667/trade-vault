"""Phase B regression tests:
  - DLQ push при gateway failure (5xx все 3 ретрая, 4xx сразу, timeout, conn err);
  - retry backoff срабатывает только до max_retries;
  - WatermarkTrailingFSM: arm/on_tick/exit; LONG ratchet up; SHORT ratchet down;
  - WatermarkStore: load/save/delete round-trip через fakeredis.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import fakeredis
import pytest
import requests

from core.redis_keys import RedisStreams as RS


# ─────────────────────────────── DLQ / dispatcher ──────────────────────────────
@pytest.fixture
def dispatcher(monkeypatch):
    """OrderTrailingDispatcher c fakeredis (для DLQ-стрима) и нулевыми sleep."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(
        "services.order_trailing_dispatcher.time.sleep",
        lambda *_: None,
    )
    from services.order_trailing_dispatcher import OrderTrailingDispatcher
    d = OrderTrailingDispatcher(
        gateway_url="http://test-gw:8090",
        max_retries=3,
        redis_client=fake,
    )
    return d, fake


def _mock_resp(status_code: int, text: str = ""):
    r = MagicMock()
    r.status_code = status_code
    r.text = text
    return r


def test_dlq_pushed_on_5xx_after_all_retries(dispatcher):
    d, fake = dispatcher
    payload = {"sid": "s1", "symbol": "BTCUSDT", "sl": 49000.0}
    with patch("services.order_trailing_dispatcher.requests.post", return_value=_mock_resp(503, "service down")):
        ok = d._post_to_gateway(payload, "trailing modify")
    assert ok is False
    entries = fake.xrange(RS.EVENTS_TRAILING_DLQ)
    assert len(entries) == 1
    _, fields = entries[0]
    assert fields["sid"] == "s1"
    assert fields["label"] == "trailing modify"
    assert "http_503" in fields["last_error"]
    assert fields["attempts"] == "3"


def test_dlq_pushed_on_4xx_immediately(dispatcher):
    """4xx — не ретраим, сразу DLQ."""
    d, fake = dispatcher
    post_mock = MagicMock(return_value=_mock_resp(400, "bad req"))
    with patch("services.order_trailing_dispatcher.requests.post", post_mock):
        ok = d._post_to_gateway({"sid": "s2", "symbol": "X"}, "modify_sl")
    assert ok is False
    # ровно одна попытка.
    assert post_mock.call_count == 1
    entries = fake.xrange(RS.EVENTS_TRAILING_DLQ)
    assert len(entries) == 1
    assert entries[0][1]["attempts"] == "1"


def test_dlq_pushed_on_timeout_after_all_retries(dispatcher):
    d, fake = dispatcher
    with patch(
        "services.order_trailing_dispatcher.requests.post",
        side_effect=requests.exceptions.Timeout("slow"),
    ):
        ok = d._post_to_gateway({"sid": "s3", "symbol": "Y"}, "trail")
    assert ok is False
    entries = fake.xrange(RS.EVENTS_TRAILING_DLQ)
    assert len(entries) == 1
    assert entries[0][1]["last_error"] == "timeout"


def test_dlq_pushed_on_conn_error(dispatcher):
    d, fake = dispatcher
    with patch(
        "services.order_trailing_dispatcher.requests.post",
        side_effect=requests.exceptions.ConnectionError("refused"),
    ):
        ok = d._post_to_gateway({"sid": "s4", "symbol": "Z"}, "trail")
    assert ok is False
    entries = fake.xrange(RS.EVENTS_TRAILING_DLQ)
    assert len(entries) == 1
    assert "connection_error" in entries[0][1]["last_error"]


def test_no_dlq_when_2xx_success(dispatcher):
    d, fake = dispatcher
    with patch(
        "services.order_trailing_dispatcher.requests.post",
        return_value=_mock_resp(200, "ok"),
    ):
        ok = d._post_to_gateway({"sid": "s5", "symbol": "W"}, "modify")
    assert ok is True
    entries = fake.xrange(RS.EVENTS_TRAILING_DLQ)
    assert entries == []


def test_retry_succeeds_after_first_5xx(dispatcher):
    d, fake = dispatcher
    responses = [_mock_resp(500), _mock_resp(200, "ok")]
    with patch(
        "services.order_trailing_dispatcher.requests.post",
        side_effect=responses,
    ):
        ok = d._post_to_gateway({"sid": "s6", "symbol": "Q"}, "modify")
    assert ok is True
    assert fake.xrange(RS.EVENTS_TRAILING_DLQ) == []


# ──────────────────────────── Watermark FSM behaviour ──────────────────────────
def test_watermark_long_ratchets_up_only():
    from services.watermark_trailing import WMState, fsm_from_signal

    fsm = fsm_from_signal(
        sid="t1", side="LONG", entry_price=100.0, original_sl=95.0,
        atr=2.0, atr_mult=1.0, profile_name="rocket_v1", point_size=0.01,
    )

    dec_arm = fsm.arm(price=105.0, now_ms=1)
    assert fsm.snap.state == WMState.TRAILING_ACTIVE
    assert dec_arm.moved is True
    assert dec_arm.new_sl is not None
    # SL = 105 - 2.0 = 103.00 (с округлением)
    assert pytest.approx(dec_arm.new_sl, abs=0.01) == 103.0
    sl_after_arm = dec_arm.new_sl

    # Тик вверх — SL должен подняться.
    dec_up = fsm.on_tick(price=110.0, now_ms=2)
    assert dec_up.moved is True
    assert dec_up.new_sl is not None and dec_up.new_sl > sl_after_arm
    assert pytest.approx(dec_up.new_sl, abs=0.01) == 108.0

    # Тик вниз (price < high_wm) — SL НЕ должен опуститься.
    sl_peak = dec_up.new_sl
    dec_down = fsm.on_tick(price=109.0, now_ms=3)
    # high_wm не изменился (110 > 109), SL остаётся.
    assert dec_down.moved is False
    assert fsm.snap.current_sl == sl_peak


def test_watermark_short_ratchets_down_only():
    from services.watermark_trailing import fsm_from_signal

    fsm = fsm_from_signal(
        sid="t2", side="SHORT", entry_price=100.0, original_sl=105.0,
        atr=2.0, atr_mult=1.0, profile_name="rocket_v1_bear", point_size=0.01,
    )

    dec_arm = fsm.arm(price=95.0, now_ms=1)
    assert dec_arm.moved is True
    # SL = 95 + 2.0 = 97.0
    assert pytest.approx(dec_arm.new_sl, abs=0.01) == 97.0

    dec_down = fsm.on_tick(price=90.0, now_ms=2)
    assert dec_down.moved is True
    assert pytest.approx(dec_down.new_sl, abs=0.01) == 92.0

    # Тик вверх (хуже для SHORT) — SL не должен подняться.
    dec_up = fsm.on_tick(price=91.0, now_ms=3)
    assert dec_up.moved is False
    assert fsm.snap.current_sl == 92.0


def test_watermark_pending_state_no_op_on_tick():
    """До вызова arm() — on_tick() ничего не делает."""
    from services.watermark_trailing import fsm_from_signal

    fsm = fsm_from_signal(
        sid="t3", side="LONG", entry_price=100.0, original_sl=95.0,
        atr=1.0, atr_mult=1.0, profile_name="x", point_size=0.01,
    )
    dec = fsm.on_tick(price=110.0, now_ms=1)
    assert dec.moved is False
    assert "pending" in dec.reason


def test_watermark_exit_locks_state():
    from services.watermark_trailing import WMState, fsm_from_signal

    fsm = fsm_from_signal(
        sid="t4", side="LONG", entry_price=100.0, original_sl=95.0,
        atr=1.0, atr_mult=1.0, profile_name="x", point_size=0.01,
    )
    fsm.arm(price=105.0, now_ms=1)
    fsm.exit()
    assert fsm.snap.state == WMState.EXITED
    # Дополнительный тик после exit — no-op.
    dec = fsm.on_tick(price=120.0, now_ms=2)
    assert dec.moved is False
    # Повторный arm после exit — должен отклоняться.
    dec_re = fsm.arm(price=130.0, now_ms=3)
    assert dec_re.moved is False
    assert dec_re.reason == "exited"


def test_watermark_round_trip_persist(monkeypatch):
    from services.watermark_trailing import fsm_from_signal
    from services.watermark_trailing_store import WatermarkStore

    fake = fakeredis.FakeRedis(decode_responses=True)
    store = WatermarkStore(redis_client=fake, ttl_sec=60)

    fsm = fsm_from_signal(
        sid="persist1", side="LONG", entry_price=100.0, original_sl=95.0,
        atr=2.0, atr_mult=1.0, profile_name="rocket_v1", point_size=0.01,
    )
    fsm.arm(price=110.0, now_ms=100)
    fsm.on_tick(price=120.0, now_ms=200)
    store.save(fsm.snap)

    loaded = store.load("persist1")
    assert loaded is not None
    assert loaded.side == "LONG"
    assert loaded.high_wm == 120.0
    assert loaded.current_sl is not None
    assert pytest.approx(loaded.current_sl, abs=0.01) == 118.0
    assert loaded.state.value == "active"
    assert loaded.updates_total == 2

    # TTL стоит.
    assert fake.ttl("trail:wm:persist1") > 0

    store.delete("persist1")
    assert store.load("persist1") is None


def test_watermark_fee_buffer_long_pushes_sl_lower():
    """fee_buffer_bps > 0 → SL ниже на доп. offset, чтобы пережить fees/spread."""
    from services.watermark_trailing import fsm_from_signal

    fsm = fsm_from_signal(
        sid="fee", side="LONG", entry_price=100.0, original_sl=95.0,
        atr=2.0, atr_mult=1.0, profile_name="rocket_v1", point_size=0.01,
        fee_buffer_bps=10.0,  # 0.1% от цены ≈ 0.105 при price=105
    )
    dec = fsm.arm(price=105.0, now_ms=1)
    # base SL = 103.0; fee_offset ≈ 0.105 → ~102.89 после округления вниз
    assert dec.new_sl is not None
    assert dec.new_sl < 103.0
