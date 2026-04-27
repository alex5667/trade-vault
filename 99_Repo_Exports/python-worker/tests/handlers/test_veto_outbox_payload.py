from types import SimpleNamespace


def test_build_veto_outbox_payload_contains_reason_codes():
    # импортируем класс, но не поднимаем тяжёлые зависимости:
    # вызываем метод напрямую на "псевдо-инстансе" с минимальными полями.
    from handlers.crypto_orderflow_handler import CryptoOrderFlowHandler

    h = CryptoOrderFlowHandler.__new__(CryptoOrderFlowHandler)  # bypass __init__

    # минимально необходимые поля/методы
    h._make_veto_event_id = CryptoOrderFlowHandler._make_veto_event_id.__get__(h)  # bind
    h._build_veto_outbox_payload = CryptoOrderFlowHandler._build_veto_outbox_payload.__get__(h)

    cand = SimpleNamespace(kind="breakout", side=1, level_price=100.0, level_key="L100", raw_score=2.5)
    ctx = SimpleNamespace(symbol="BTCUSDT", ts=1234567890000)
    res = SimpleNamespace(
        veto=True,
        conf_factor01=0.0,
        reason_code="VETO_WALL_NEAR",
        reason_u16=103,
        decision_code="VETO_WALL_NEAR",
        decision_u16=103,
        flags=[1, 2, 3],
        parts={"wall_dist_bps": 3.0},
    )

    p = h._build_veto_outbox_payload(cand=cand, ctx=ctx, res=res)
    assert p["kind"] == "signal_veto"
    assert p["symbol"] == "BTCUSDT"
    assert p["veto_reason_code"] == "VETO_WALL_NEAR"
    assert int(p["veto_reason_u16"]) == 103
    assert p["decision_code"] == "VETO_WALL_NEAR"
    assert int(p["decision_u16"]) == 103
    assert isinstance(p["signal_id"], str) and len(p["signal_id"]) > 0
    assert p["parts"]["wall_dist_bps"] == 3.0
