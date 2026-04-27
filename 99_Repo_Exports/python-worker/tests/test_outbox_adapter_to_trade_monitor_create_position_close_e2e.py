import json
from types import SimpleNamespace

from handlers.emitter.outbox_publisher_adapter import OutboxPublisherAdapter
from services.trade_monitor import TradeMonitorService

# create_position is used in your existing e2e tests
from domain.handlers import create_position


class _SpecStub:
    trailing_profile_default = "rocket_v1"
    def risk_money(self, entry, sl, lot, direction):
        return abs(float(entry) - float(sl)) * float(lot)


def _mk_trade_monitor_like() -> TradeMonitorService:
    mon = TradeMonitorService.__new__(TradeMonitorService)
    mon._get_spec = lambda symbol: _SpecStub()
    mon.logger = SimpleNamespace(
        debug=lambda *a, **k: None,
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    return mon


class _PubCapture:
    """
    Publisher stub that captures what adapter sends.
    This still tests the real adapter mapping + TradeMonitor normalization + create_position.
    """
    def __init__(self):
        self.last = None

    def publish(self, **kwargs):
        self.last = dict(kwargs)
        return "1-0"


def test_outbox_adapter_to_trade_monitor_to_create_position_close():
    pub = _PubCapture()
    adapter = OutboxPublisherAdapter(outbox_publisher=pub)

    payload = {
        "strategy_name": "orderflow",
        "strategy_source": "crypto_orderflow",
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "kind": "breakout",
        "level_key": "L1",
        "tf": "1m",
        "ts_ms": 1700000000000,
        # minimal levels expected by normalize/create_position in your tests
        "entry": 100.0,
        "sl": 99.0,
        "tp1": 101.0,
        "confidence": 55.0,
    }

    msg_id = adapter.publish(payload)
    assert msg_id is not None
    assert pub.last is not None

    # Envelope goes into TradeMonitor normalize path
    env = pub.last.get("envelope") or {}
    mon = _mk_trade_monitor_like()
    sig = mon._normalize_signal(env)
    assert sig is not None

    pos = create_position(sig, _SpecStub())
    assert pos is not None
    assert str(getattr(pos, "symbol", "")) == "BTCUSDT"
    assert str(getattr(pos, "tf", "")) == "1m"
    assert str(getattr(pos, "direction", "")) in ("LONG", "SHORT")
