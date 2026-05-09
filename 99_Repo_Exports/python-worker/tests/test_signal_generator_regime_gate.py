import types

import pytest

# Импортируйте реальные классы из вашего проекта
from handlers.signal_generator import SignalGenerator
from signals.outbox_utils import PublishResult


class DummyOutbox:
    def __init__(self):
        self.called = False
        self.envelope = None

    def publish(self, envelope):
        self.called = True
        self.envelope = envelope
        return PublishResult(sent=True, dedup=False, msg_id="msg")


class DummyCfg:
    # базовые настройки генератора
    signal_z_enter = 1.5
    signal_z_breakout = 2.0
    min_trades_breakout = 20
    burst_ratio_min = 1.6
    fano_min = 1.5
    flip_ratio_max = 0.70
    imbalance_min = 0.20

    # режимы
    regime_breakout_min_score = 0.0
    regime_extreme_min_score = 0.0
    regime_obi_spike_min_score = 0.0
    regime_absorption_max_score = 0.0
    regime_allow_sweep_any = True
    regime_require_score = False


def make_ctx(**kw):
    ctx = types.SimpleNamespace()
    ctx.symbol = kw.get("symbol", "BTCUSDT")
    ctx.ts = kw.get("ts", 1700000000000)
    ctx.price = kw.get("price", 100.0)

    ctx.z_delta = kw.get("z_delta", 2.5)
    ctx.extreme_z_threshold = kw.get("extreme_z_threshold", 3.0)

    ctx.obi = kw.get("obi", 0.0)
    ctx.obi_spike_threshold = kw.get("obi_spike_threshold", 0.8)

    ctx.weak_progress = kw.get("weak_progress", False)
    ctx.absorption_score = kw.get("absorption_score", 0.0)

    # regime
    if "regime_score" in kw:
        ctx.regime_score = kw["regime_score"]
    ctx.regime_label = kw.get("regime_label", "mixed")

    # BREAKOUT level fields (чтобы не отвалиться на level_key)
    ctx.level_key = kw.get("level_key", "R1")
    ctx.level_price = kw.get("level_price", 101.0)
    return ctx


@pytest.fixture
def gen(monkeypatch):
    outbox = DummyOutbox()
    cfg = DummyCfg()
    g = SignalGenerator(symbol="BTCUSDT", config=cfg, outbox=outbox)

    # Упростим зависимости:
    # _nums -> возвращаем z из ctx.z_delta
    monkeypatch.setattr(g, "_nums", lambda ctx: (float(getattr(ctx, "z_delta", 0.0)), 0.0, 0.0))
    # качество исполнения пусть всегда ok
    monkeypatch.setattr(g, "_exec_quality_ok", lambda ctx, side: True)
    # confidence пусть стабилен
    monkeypatch.setattr(g, "_compute_confidence", lambda ctx: (0.5, {"base": 0.5}))
    # cooldown отключаем
    g.cooldown = None

    # thresholds генератора
    g.z_enter = 1.0
    g.z_breakout = 2.0  # чтобы z=2.5 стал BREAKOUT

    return g, outbox


def test_breakout_rejected_when_regime_negative(gen):
    g, outbox = gen
    ctx = make_ctx(z_delta=2.5, regime_score=-0.2, regime_label="range")

    res = g.generate(ctx)

    assert isinstance(res, PublishResult)
    assert res.sent is False and res.dedup is False and res.msg_id is None
    assert outbox.called is False


def test_breakout_allowed_when_regime_nonnegative(gen):
    g, outbox = gen
    ctx = make_ctx(z_delta=2.5, regime_score=+0.1, regime_label="trend")

    res = g.generate(ctx)

    assert res.sent is True and res.dedup is False
    assert outbox.called is True
    assert outbox.envelope["kind"] == "BREAKOUT"
    assert outbox.envelope["regime_label"] == "trend"
    assert outbox.envelope["regime_score"] == pytest.approx(0.1)
    assert outbox.envelope["context"]["regime_score"] == pytest.approx(0.1)


def test_absorption_rejected_when_regime_positive(gen):
    g, outbox = gen

    # Сделаем ABSORPTION: отключим breakout и включим weak_progress
    g.z_breakout = 100.0
    ctx = make_ctx(z_delta=1.2, weak_progress=True, regime_score=+0.2, regime_label="trend")

    res = g.generate(ctx)

    assert res.sent is False
    assert outbox.called is False
