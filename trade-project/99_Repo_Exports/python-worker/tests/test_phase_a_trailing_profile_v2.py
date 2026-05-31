"""Phase A regression tests:
  1) TrailingProfileV2 dataclass — frozen, hashable, stable profile_hash.
  2) TrailingProfilesRegistry — validate_default, profile_hash, policy_hash.
  3) Bug-1: per-profile activate_after_tp gates the orchestrator
     (expansion_v1 принимает TP2_HIT, rocket_v1 принимает TP1_HIT,
      и наоборот — отвергается).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import fakeredis
import pytest


# ───────────────────────── TrailingProfileV2 dataclass ─────────────────────────
def test_trailing_profile_v2_is_frozen():
    from services.trailing_profiles import TrailingProfileV2

    p = TrailingProfileV2(
        schema_ver=2,
        name="x",
        mode="ATR",
        activate_after_tp=1,
        atr_mult=1.0,
        arm_threshold_r=None,
        hard_lock_r=None,
        clear_tp_policy="never",
    )
    with pytest.raises(Exception):
        p.atr_mult = 2.0  # type: ignore[misc]


def test_trailing_profile_v2_round_trip_dict():
    from services.trailing_profiles import TrailingProfileV2

    p = TrailingProfileV2(
        schema_ver=2,
        name="rocket_v1",
        mode="ATR",
        activate_after_tp=1,
        atr_mult=1.2,
        arm_threshold_r=0.5,
        hard_lock_r=0.1,
        clear_tp_policy="rocket_only",
        allowed_regimes=("trending", "shock"),
        allowed_symbols=("BTCUSDT",),
        reason="bull trend",
    )
    d = p.to_dict()
    p2 = TrailingProfileV2.from_dict(d)
    assert p2 == p
    assert p2.profile_hash() == p.profile_hash()


def test_trailing_profile_v2_hash_stable():
    """profile_hash должен быть детерминирован между запусками."""
    from services.trailing_profiles import TrailingProfileV2

    p1 = TrailingProfileV2(
        schema_ver=2, name="a", mode="ATR", activate_after_tp=1,
        atr_mult=1.0, arm_threshold_r=None, hard_lock_r=None,
        clear_tp_policy="never",
    )
    p2 = TrailingProfileV2(
        schema_ver=2, name="a", mode="ATR", activate_after_tp=1,
        atr_mult=1.0, arm_threshold_r=None, hard_lock_r=None,
        clear_tp_policy="never",
    )
    assert p1.profile_hash() == p2.profile_hash()


def test_v1_to_v2_round_trip_expansion_keeps_activate_after_tp_2():
    """TrailingProfile.to_v2() обязан сохранить activate_after_tp."""
    from services.trailing_profiles import TrailingProfile

    p = TrailingProfile(
        name="expansion_v1", mode="ATR", atr_mult=1.5,
        activate_after_tp=2, comment="expansion",
    )
    v2 = p.to_v2()
    assert v2.activate_after_tp == 2
    assert v2.mode == "ATR"
    assert v2.atr_mult == 1.5


# ─────────────────────────── Registry validation/hash ──────────────────────────
@pytest.fixture
def fake_registry(monkeypatch):
    """Подменяем redis.from_url на fakeredis для изолированного юнита."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(
        "services.trailing_profiles.redis.from_url",
        lambda *a, **kw: fake,
    )
    from services.trailing_profiles import TrailingProfilesRegistry
    return TrailingProfilesRegistry(redis_url="redis://fake:6379/0")


def test_registry_validate_default_ok(fake_registry):
    fake_registry.validate_default("protective_only")  # should not raise


def test_registry_validate_default_raises_on_missing(fake_registry):
    with pytest.raises(ValueError) as exc:
        fake_registry.validate_default("nonexistent_profile")
    assert "nonexistent_profile" in str(exc.value)
    # Список доступных профилей должен быть в ошибке.
    assert "protective_only" in str(exc.value)


def test_registry_profile_hash_present_for_all_defaults(fake_registry):
    for name in fake_registry.list_names():
        h = fake_registry.profile_hash(name)
        assert isinstance(h, str) and len(h) == 12


def test_registry_policy_hash_changes_on_mutation(fake_registry):
    from services.trailing_profiles import TrailingProfile

    h_before = fake_registry.policy_hash()
    fake_registry.add(
        TrailingProfile(name="z_new", mode="ATR", atr_mult=0.9, activate_after_tp=1),
        save_to_redis=False,
    )
    h_after = fake_registry.policy_hash()
    assert h_before != h_after


def test_registry_policy_hash_stable_for_identical_state(monkeypatch):
    """Два экземпляра реестра без Redis-overrides → одинаковый policy_hash."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(
        "services.trailing_profiles.redis.from_url",
        lambda *a, **kw: fake,
    )
    from services.trailing_profiles import TrailingProfilesRegistry
    r1 = TrailingProfilesRegistry()
    r2 = TrailingProfilesRegistry()
    assert r1.policy_hash() == r2.policy_hash()


# ─────────────────────────────── _parse_tp_level ───────────────────────────────
@pytest.mark.parametrize(
    "ev,expected",
    [
        ("TP1_HIT", 1),
        ("TP2_HIT", 2),
        ("TP3_HIT", 3),
        ("tp1_hit", 1),  # case-insensitive
        ("SL_HIT", None),
        ("POSITION_OPENED", None),
        ("TP_HIT", None),  # без цифры
        ("TPX_HIT", None),
        ("", None),
        (None, None),
        (123, None),
        ("TP10_HIT", None),  # вне диапазона 1..9
    ],
)
def test_parse_tp_level(ev, expected):
    from services.tp_hit_trailing_orchestrator import _parse_tp_level
    assert _parse_tp_level(ev) == expected


# ─────────────────────────── Bug-1: per-profile gate ───────────────────────────
@pytest.fixture
def orchestrator(monkeypatch):
    """Соберём orchestrator на fakeredis + замоканном dispatcher.

    Дефолт `protective_only` (activate_after_tp=1) присутствует в registry,
    поэтому validate_default() не упадёт на старте.
    """
    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(
        "services.trailing_profiles.redis.from_url",
        lambda *a, **kw: fake,
    )
    monkeypatch.setattr(
        "services.tp_hit_trailing_orchestrator.redis.from_url",
        lambda *a, **kw: fake,
    )
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

    from services.tp_hit_trailing_orchestrator import TpHitTrailingOrchestrator
    o = TpHitTrailingOrchestrator()
    o.r = fake
    return o, fake


def _seed_signal(fake_redis, sid: str, *, trail_profile: str, side: str = "LONG"):
    payload = {
        "sid": sid,
        "symbol": "BTCUSDT",
        "side": side,
        "source": "cryptoorderflow",
        "trail_after_tp1": True,
        "trail_profile": trail_profile,
        "atr": 100.0,
        "entry": 50000.0,
        "sl": 49500.0,
        "tp_levels": [50500.0, 51000.0],
    }
    fake_redis.set(f"signals:{sid}", json.dumps(payload))


def test_bug1_expansion_v1_accepts_tp2_rejects_tp1(orchestrator):
    """expansion_v1.activate_after_tp = 2 → TP2_HIT работает, TP1_HIT отбрасывается."""
    o, fake = orchestrator
    _seed_signal(fake, "sid-exp-1", trail_profile="expansion_v1")

    res_tp1 = o.handle_event({
        "event_type": "TP1_HIT", "sid": "sid-exp-1", "symbol": "BTCUSDT",
        "price": "50500.0", "ts": "1", "source": "test",
    })
    # handle_event теперь возвращает TrailingResult; TP1 для expansion_v1 — skipped.
    assert res_tp1.skipped is True
    # А ключ dedup был установлен на tp1, не на tp2:
    assert fake.exists("dedup:tp1_trailing:sid-exp-1")
    assert not fake.exists("dedup:tp2_trailing:sid-exp-1")
    assert o.stats["trailing_started"] == 0

    # TP2_HIT должен пройти.
    res_tp2 = o.handle_event({
        "event_type": "TP2_HIT", "sid": "sid-exp-1", "symbol": "BTCUSDT",
        "price": "51000.0", "ts": "1", "source": "test",
    })
    assert res_tp2.success is True
    assert o.stats["trailing_started"] == 1


def test_bug1_rocket_v1_accepts_tp1_rejects_tp2(orchestrator):
    """rocket_v1.activate_after_tp = 1 → TP1_HIT работает, TP2_HIT отбрасывается."""
    o, fake = orchestrator
    _seed_signal(fake, "sid-rocket-1", trail_profile="rocket_v1")

    res_tp2 = o.handle_event({
        "event_type": "TP2_HIT", "sid": "sid-rocket-1", "symbol": "BTCUSDT",
        "price": "51000.0", "ts": "1", "source": "test",
    })
    assert res_tp2.skipped is True
    assert o.stats["trailing_started"] == 0

    res_tp1 = o.handle_event({
        "event_type": "TP1_HIT", "sid": "sid-rocket-1", "symbol": "BTCUSDT",
        "price": "50500.0", "ts": "1", "source": "test",
    })
    assert res_tp1.success is True
    assert o.stats["trailing_started"] == 1


def test_bug1_unsupported_event_returns_skipped(orchestrator):
    o, _ = orchestrator
    res = o.handle_event({
        "event_type": "SL_HIT", "sid": "sid-x", "symbol": "BTCUSDT",
        "price": "100", "ts": "1", "source": "test",
    })
    assert res.skipped is True
    assert o.stats["trailing_started"] == 0


def test_bug1_dedup_per_tp_level(orchestrator):
    """Один и тот же sid с TP1_HIT и TP2_HIT не должен схлопывать dedup."""
    o, fake = orchestrator
    _seed_signal(fake, "sid-dedup", trail_profile="expansion_v1")

    o.handle_event({
        "event_type": "TP1_HIT", "sid": "sid-dedup", "symbol": "BTCUSDT",
        "price": "50500.0", "ts": "1", "source": "test",
    })
    o.handle_event({
        "event_type": "TP2_HIT", "sid": "sid-dedup", "symbol": "BTCUSDT",
        "price": "51000.0", "ts": "1", "source": "test",
    })
    # Должны быть оба независимых dedup-ключа.
    assert fake.exists("dedup:tp1_trailing:sid-dedup")
    assert fake.exists("dedup:tp2_trailing:sid-dedup")
    assert o.stats["trailing_started"] == 1  # активирован только TP2 (expansion_v1)


def test_orchestrator_init_fails_on_unknown_default(monkeypatch):
    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(
        "services.trailing_profiles.redis.from_url",
        lambda *a, **kw: fake,
    )
    monkeypatch.setattr(
        "services.tp_hit_trailing_orchestrator.redis.from_url",
        lambda *a, **kw: fake,
    )
    monkeypatch.setattr(
        "services.tp_hit_trailing_orchestrator.OrderTrailingDispatcher",
        lambda *a, **kw: MagicMock(),
    )
    monkeypatch.setenv("DEFAULT_TRAIL_PROFILE", "totally_unknown_xxx")
    from services.tp_hit_trailing_orchestrator import TpHitTrailingOrchestrator
    with pytest.raises(ValueError):
        TpHitTrailingOrchestrator()


# ─────────────────── autocal atr_mult override tests ────────────────────────

def _make_autocal_state(knob: str, value: float, enforce: int) -> str:
    return json.dumps({
        "ts_ms": 9_999_999_999_000,
        "knobs": {knob: {"value": value, "enforce": enforce, "lift_r": 0.1, "n": 300}},
    })


def test_registry_autocal_applies_atr_mult_when_enforce_one(monkeypatch):
    """get() applies atr_mult override immediately when enforce=1 in autocal state."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    fake.set("autocal:tp_sl_trailing:state",
             _make_autocal_state("atr_mult_rocket_v1", 1.0, enforce=1))
    monkeypatch.setattr("services.trailing_profiles.redis.from_url", lambda *a, **kw: fake)
    from services.trailing_profiles import TrailingProfilesRegistry
    reg = TrailingProfilesRegistry(redis_url="redis://fake:6379/0")
    reg._autocal_ttl_sec = 0.0  # disable TTL so first get() triggers refresh

    p = reg.get("rocket_v1")
    assert p is not None
    assert p.atr_mult == pytest.approx(1.0), "autocal should have overridden default 1.2 → 1.0"


def test_registry_autocal_skips_when_enforce_zero(monkeypatch):
    """enforce=0 must NOT change the default atr_mult."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    fake.set("autocal:tp_sl_trailing:state",
             _make_autocal_state("atr_mult_rocket_v1", 1.0, enforce=0))
    monkeypatch.setattr("services.trailing_profiles.redis.from_url", lambda *a, **kw: fake)
    from services.trailing_profiles import TrailingProfilesRegistry
    reg = TrailingProfilesRegistry(redis_url="redis://fake:6379/0")
    reg._autocal_ttl_sec = 0.0

    p = reg.get("rocket_v1")
    assert p is not None
    assert p.atr_mult == pytest.approx(1.2), "enforce=0 must not change the default"


def test_registry_autocal_ttl_prevents_double_read(monkeypatch):
    """Second get() within TTL window must not re-read Redis."""
    import time
    reads: list[int] = []

    class _Counting:
        def get(self, _key):
            reads.append(1)
            return None
        def set(self, *a, **kw): pass

    monkeypatch.setattr("services.trailing_profiles.redis.from_url",
                        lambda *a, **kw: _Counting())
    from services.trailing_profiles import TrailingProfilesRegistry
    reg = TrailingProfilesRegistry(redis_url="redis://fake:6379/0")
    reg._autocal_ttl_sec = 60.0  # long TTL

    reads.clear()
    reg.get("rocket_v1")  # triggers autocal refresh → 1 read
    reg.get("rocket_v1")  # within TTL → no extra read
    assert len(reads) == 1, "TTL cache must suppress second Redis GET"


def test_registry_autocal_all_three_atr_knobs(monkeypatch):
    """All three atr_mult knobs are applied when each has enforce=1."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    state = json.dumps({
        "ts_ms": 9_999_999_999_000,
        "knobs": {
            "atr_mult_rocket_v1":      {"value": 1.0, "enforce": 1, "n": 300},
            "atr_mult_expansion_v1":   {"value": 1.0, "enforce": 1, "n": 300},
            "atr_mult_rocket_v1_bear": {"value": 0.8, "enforce": 1, "n": 300},
        },
    })
    fake.set("autocal:tp_sl_trailing:state", state)
    monkeypatch.setattr("services.trailing_profiles.redis.from_url", lambda *a, **kw: fake)
    from services.trailing_profiles import TrailingProfilesRegistry
    reg = TrailingProfilesRegistry(redis_url="redis://fake:6379/0")
    reg._autocal_ttl_sec = 0.0

    p_rocket = reg.get("rocket_v1")
    p_exp    = reg.get("expansion_v1")
    p_bear   = reg.get("rocket_v1_bear")
    assert p_rocket is not None and p_exp is not None and p_bear is not None
    assert p_rocket.atr_mult      == pytest.approx(1.0)
    assert p_exp.atr_mult         == pytest.approx(1.0)
    assert p_bear.atr_mult        == pytest.approx(0.8)


def test_registry_autocal_fail_open_on_bad_redis(monkeypatch):
    """Redis failure must be swallowed; defaults intact."""
    class _FailRedis:
        def get(self, _key): raise ConnectionError("boom")
        def set(self, *a, **kw): pass

    monkeypatch.setattr("services.trailing_profiles.redis.from_url",
                        lambda *a, **kw: _FailRedis())
    from services.trailing_profiles import TrailingProfilesRegistry
    reg = TrailingProfilesRegistry(redis_url="redis://fake:6379/0")
    reg._autocal_ttl_sec = 0.0

    p = reg.get("rocket_v1")
    assert p is not None
    assert p.atr_mult == pytest.approx(1.2), "defaults must survive Redis failure"
