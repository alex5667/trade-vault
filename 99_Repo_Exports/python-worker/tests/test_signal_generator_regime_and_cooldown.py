import types
import pytest

# Подстройте импорты под ваш проект
from handlers.signal_generator import SignalGenerator
from signals.outbox_utils import PublishResult


class DummyOutbox:
    def __init__(self, result: PublishResult):
        self._result = result
        self.called = False
        self.envelope = None

    def publish(self, envelope):
        self.called = True
        self.envelope = envelope
        return self._result


class DummyCooldown:
    def __init__(self, allowed=True):
        self.allowed = allowed
        self.acquire_calls = []

    def acquire(self, kind, level_key, ts_ms, family="crypto", timeframe_s=60):
        self.acquire_calls.append((kind, level_key, ts_ms, family, timeframe_s))
        return self.allowed


class DummyCfg:
    # regime gate defaults
    regime_breakout_min_score = 0.0
    regime_extreme_min_score = 0.0
    regime_obi_spike_min_score = 0.0
    regime_absorption_max_score = 0.0
    regime_allow_sweep_any = True
    regime_require_score = False


def make_ctx(**kw):
    ctx = types.SimpleNamespace()
    ctx.symbol = kw.get("symbol", "BTCUSDT")
    ctx.ts = kw.get("ts", 1700000000123)
    ctx.price = kw.get("price", 100.0)

    # z
    ctx.z_delta = kw.get("z_delta", 2.5)
    ctx.extreme_z_threshold = kw.get("extreme_z_threshold", 3.0)

    # absorption / obi spike helpers
    ctx.weak_progress = kw.get("weak_progress", False)
    ctx.absorption_score = kw.get("absorption_score", 0.0)
    ctx.obi = kw.get("obi", 0.0)
    ctx.obi_spike_threshold = kw.get("obi_spike_threshold", 0.8)

    # family and timeframe
    ctx.family = kw.get("family", "crypto")
    ctx.timeframe_s = kw.get("timeframe_s", 60)

    # regime
    if "regime_score" in kw:
        ctx.regime_score = kw["regime_score"]
    ctx.regime_label = kw.get("regime_label", "mixed")

    # BREAKOUT level fields (чтобы не отвалиться)
    ctx.level_key = kw.get("level_key", "R1")
    ctx.level_price = kw.get("level_price", 101.0)

    # pivots (может быть None)
    ctx.pivots = kw.get("pivots", None)
    return ctx


@pytest.fixture
def gen(monkeypatch):
    # outbox по умолчанию "успешно отправил"
    outbox = DummyOutbox(PublishResult(sent=True, dedup=False, msg_id="msg"))
    cfg = DummyCfg()
    g = SignalGenerator(symbol="BTCUSDT", outbox=outbox, config=cfg)

    # Упростим зависимости генератора:
    monkeypatch.setattr(g, "_nums", lambda ctx: (float(getattr(ctx, "z_delta", 0.0)), 0.0, 0.0))
    monkeypatch.setattr(g, "_exec_quality_ok", lambda ctx, side, signal_type="bar": True)
    monkeypatch.setattr(g, "_compute_confidence", lambda ctx: (0.5, {"base": 0.5}))

    # Пороговые
    g.z_enter = 1.0
    g.z_breakout = 2.0  # чтобы z=2.5 стал BREAKOUT

    # cooldown отключим в тестах явно где нужно
    g.cooldown = None

    # Если у вас в generate используются утилиты build_level_key_* / normalize_to_bucket
    # и они импортированы в модуль, можно подменить их здесь:
    import handlers.signal_generator as sg_mod
    monkeypatch.setattr(sg_mod, "build_level_key_breakout", lambda lvl: f"BRK:{lvl}")
    monkeypatch.setattr(sg_mod, "build_level_key_sweep", lambda price, pivots=None: "SWP:PIVOT")
    monkeypatch.setattr(sg_mod, "build_level_key_extreme", lambda **kwargs: "EXT:PIVOT")
    monkeypatch.setattr(sg_mod, "price_bin_key", lambda price, step: f"BIN:{round(price/step)*step}")
    monkeypatch.setattr(sg_mod, "normalize_to_bucket", lambda ts_ms, bucket: (ts_ms // bucket) * bucket)

    # dedup bucket
    g.dedup_bucket_ms = 1000

    return g, outbox


def test_regime_gate_rejects_breakout_when_score_negative(gen):
    g, outbox = gen
    ctx = make_ctx(z_delta=2.5, regime_score=-0.2, regime_label="range")

    res = g.generate(ctx)

    assert res.sent is False and res.dedup is False
    assert outbox.called is False


def test_envelope_contains_regime_fields(gen):
    g, outbox = gen
    ctx = make_ctx(z_delta=2.5, regime_score=0.1, regime_label="trend")

    res = g.generate(ctx)

    assert res.sent is True
    assert outbox.called is True

    env = outbox.envelope
    assert env["kind"] == "BREAKOUT"
    assert env["regime_label"] == "trend"
    assert env["regime_score"] == pytest.approx(0.1)

    assert env["context"]["regime_label"] == "trend"
    assert env["context"]["regime_score"] == pytest.approx(0.1)


def test_cooldown_not_marked_on_dedup(monkeypatch, gen):
    g, outbox = gen

    # outbox теперь "dedup"
    outbox._result = PublishResult(sent=True, dedup=True, msg_id=None)

    cd = DummyCooldown(allowed=True)
    g.cooldown = cd

    ctx = make_ctx(z_delta=2.5, regime_score=0.1, regime_label="trend")
    res = g.generate(ctx)

    assert res.sent is True and res.dedup is True
    # acquire был вызван (даже для dedup), но это нормально - acquire резервирует слот
    assert len(cd.acquire_calls) == 1


def test_cooldown_acquired_on_sent_non_dedup_with_real_ts(monkeypatch, gen):
    g, outbox = gen

    outbox._result = PublishResult(sent=True, dedup=False, msg_id="msg")

    cd = DummyCooldown(allowed=True)
    g.cooldown = cd

    ctx = make_ctx(ts=1700000000123, z_delta=2.5, regime_score=0.1, regime_label="trend")
    res = g.generate(ctx)

    assert res.sent is True and res.dedup is False
    assert len(cd.acquire_calls) == 1

    kind, level_key, acquired_ts, family, timeframe_s = cd.acquire_calls[0]
    assert kind == "breakout"
    assert level_key.startswith("BRK:")
    assert family == "crypto"  # default
    assert timeframe_s == 60   # default

    # должен быть реальный ts, не normalized
    assert acquired_ts == 1700000000123


def test_cooldown_reject_returns_dedup_false(monkeypatch, gen):
    g, outbox = gen

    # cooldown не разрешает
    cd = DummyCooldown(allowed=False)
    g.cooldown = cd

    ctx = make_ctx(z_delta=2.5, regime_score=0.1, regime_label="trend")
    res = g.generate(ctx)

    # cooldown reject должен возвращать dedup=True (rate-limit / dedup)
    assert res.sent is False and res.dedup is True and res.msg_id is None
    assert outbox.called is False
    # acquire был вызван, но вернул False - это нормально
    assert len(cd.acquire_calls) == 1
    kind, level_key, acquired_ts, family, timeframe_s = cd.acquire_calls[0]
    assert kind == "breakout"
    assert family == "crypto"
    assert timeframe_s == 60


def test_different_family_timeframe_no_conflict(gen):
    """Разные family/timeframe не должны конфликтовать в cooldown"""
    g, outbox = gen

    cd = DummyCooldown(allowed=True)
    g.cooldown = cd

    # Разные контексты с разными family/timeframe
    ctx1 = make_ctx(z_delta=2.5, family="crypto", timeframe_s=60, regime_score=0.1)
    ctx2 = make_ctx(z_delta=2.5, family="xau", timeframe_s=60, regime_score=0.1)
    ctx3 = make_ctx(z_delta=2.5, family="crypto", timeframe_s=300, regime_score=0.1)

    # Все должны успешно опубликоваться
    res1 = g.generate(ctx1)
    res2 = g.generate(ctx2)
    res3 = g.generate(ctx3)

    assert res1.sent is True
    assert res2.sent is True
    assert res3.sent is True

    # Должно быть 3 разных acquire вызова
    assert len(cd.acquire_calls) == 3

    # Проверим, что family/timeframe передаются правильно
    families = [call[3] for call in cd.acquire_calls]
    timeframes = [call[4] for call in cd.acquire_calls]

    assert "crypto" in families
    assert "xau" in families
    assert 60 in timeframes
    assert 300 in timeframes
