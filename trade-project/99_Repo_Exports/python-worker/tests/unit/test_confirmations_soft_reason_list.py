
from handlers.confirmations.engine import ConfirmationsEngine


class Ctx:
    # dataclass не нужен: setattr/read достаточно
    def __init__(self):
        self.symbol = "BTCUSDT"
        self.ts_ms = 1_700_000_000_000
        self.price = 42_000.0
        # geometry missing -> SOFT_HTF_MISSING
        self.geometry_score = None
        # emulate l3 missing -> SOFT_L3_MISSING
        self.l3_missing = True

def test_soft_reason_list_and_packed_u16(monkeypatch):
    monkeypatch.setenv("SOFT_REASON_MAX", "4")
    monkeypatch.setenv("PACK_SOFT_U16", "1")
    monkeypatch.setenv("DEBUG_SOFT_CODES", "1")  # to make soft_codes visible in Validation

    eng = ConfirmationsEngine()
    ctx = Ctx()

    # Use a kind where we do NOT fail-closed on L2 by default path
    # (we pass l2/l3 as None; l3 missing is penalized, not vetoed)
    res = eng.validate(kind="extreme", ctx=ctx, l2=None, l3=None, level_price=None)

    assert res.veto is False
    assert 0.0 <= res.conf_factor01 <= 1.0

    # Two independent soft reasons should be present (order by weight desc).
    assert "SOFT_L3_MISSING" in res.soft_codes
    assert "SOFT_HTF_MISSING" in res.soft_codes
    assert isinstance(res.soft_u16s, list) and all(isinstance(x, int) for x in res.soft_u16s)
    # packed present when u16 list present
    assert isinstance(res.soft16, str)
    if res.soft_u16s:
        assert len(res.soft16) > 0
