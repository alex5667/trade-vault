"""
Тесты для services/tp_hit_trailing_orchestrator.py.

Покрывает:
- TestActivateAfterTp: per-profile gate по tp_level
- TestDedupTp1Tp2Independent: dedup ключи независимы по уровню TP
- TestTrailingDecisionsAudit: маппинг метаданных → поля trailing_decisions INSERT
"""

import sys
import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import fakeredis

_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from services.tp_hit_trailing_orchestrator import (
    TpHitTrailingOrchestrator,
    _parse_tp_level,
)
from services.trailing_profiles import TrailingProfilesRegistry


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные фабрики
# ─────────────────────────────────────────────────────────────────────────────

def _make_profiles(fake_r: fakeredis.FakeRedis) -> TrailingProfilesRegistry:
    """Реестр профилей, работающий с fakeredis."""
    with patch("services.trailing_profiles.redis.from_url", return_value=fake_r):
        registry = TrailingProfilesRegistry.__new__(TrailingProfilesRegistry)
        registry.r = fake_r
        registry._redis_key = TrailingProfilesRegistry.REDIS_KEY
        registry._profiles = {}
        registry._init_default()
        # Добавляем rocket_v1 с activate_after_tp=1 (уже есть в дефолтах)
        # expansion_v1 с activate_after_tp=2 тоже есть в дефолтах
    return registry


def _make_orchestrator(
    fake_r: fakeredis.FakeRedis,
    profiles: TrailingProfilesRegistry | None = None,
    default_profile: str = "rocket_v1",
) -> TpHitTrailingOrchestrator:
    """Создаёт оркестратор с fakeredis и заглушенными gateway/dispatcher."""
    if profiles is None:
        profiles = _make_profiles(fake_r)

    with (
        patch("services.tp_hit_trailing_orchestrator.redis.from_url", return_value=fake_r),
        patch("services.tp_hit_trailing_orchestrator.OrderTrailingDispatcher") as mock_disp_cls,
        patch.dict(os.environ, {
            "DEFAULT_TRAIL_PROFILE": default_profile,
            "TRAILING_SYMBOLS": "*",      # всё пропускаем
            "TRAILING_SOURCES": "*",
            "GATEWAY_URL": "http://fake-gateway:8090",
        }),
    ):
        mock_disp_cls.return_value = MagicMock()
        orc = TpHitTrailingOrchestrator(
            redis_client=fake_r,
            profiles=profiles,
        )
    return orc


def _store_signal(fake_r: fakeredis.FakeRedis, sid: str, signal: dict) -> str:
    """Кладёт сигнал в fakeredis под ключ signals:{sid}."""
    key = f"signals:{sid}"
    fake_r.set(key, json.dumps(signal))
    return key


# ─────────────────────────────────────────────────────────────────────────────
# _parse_tp_level — прямые тесты (unit)
# ─────────────────────────────────────────────────────────────────────────────

class TestParseTpLevel:
    def test_tp1_hit_returns_1(self):
        assert _parse_tp_level("TP1_HIT") == 1

    def test_tp2_hit_returns_2(self):
        assert _parse_tp_level("TP2_HIT") == 2

    def test_bare_tp_hit_returns_none(self):
        assert _parse_tp_level("TP_HIT") is None

    def test_sl_hit_returns_none(self):
        assert _parse_tp_level("SL_HIT") is None

    def test_none_returns_none(self):
        assert _parse_tp_level(None) is None


# ─────────────────────────────────────────────────────────────────────────────
# TestActivateAfterTp
# ─────────────────────────────────────────────────────────────────────────────

class TestActivateAfterTp:
    """Per-profile gate: profile.activate_after_tp должен совпадать с tp_level."""

    def setup_method(self):
        self.fake_r = fakeredis.FakeRedis(decode_responses=True)

    def _make_signal(self, profile_name: str = "expansion_v1") -> dict:
        return {
            "sid": "sig-test",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "trail_after_tp1": "true",
            "trail_profile": profile_name,
            "atr": "50.0",
            "entry": "30000.0",
            "sl": "29500.0",
            "source": "orderflow",
        }

    def test_expansion_v1_tp1_skipped(self):
        """expansion_v1 имеет activate_after_tp=2 → TP1_HIT должен быть skipped."""
        orc = _make_orchestrator(self.fake_r, default_profile="protective_only")
        sid = "sig-expansion-tp1"
        _store_signal(self.fake_r, sid, self._make_signal("expansion_v1"))

        event = {
            "event_type": "TP1_HIT",
            "sid": sid,
            "symbol": "BTCUSDT",
            "price": "31000.0",
            "source": "test",
        }
        result = orc.handle_event(event)
        assert result.skipped is True

    def test_expansion_v1_tp2_activates(self):
        """expansion_v1 + TP2_HIT → должен пройти per-profile gate.

        Мы мокаем dispatcher, чтобы команда "отправилась" успешно,
        и проверяем что result.skipped is False.
        """
        orc = _make_orchestrator(self.fake_r, default_profile="protective_only")
        sid = "sig-expansion-tp2"
        _store_signal(self.fake_r, sid, self._make_signal("expansion_v1"))

        # Dispatcher возвращает success
        orc.dispatcher.send_trailing_command_from_atr = MagicMock(return_value=True)
        orc.dispatcher.send_trailing_modify = MagicMock(return_value=True)
        orc.dispatcher.get_symbol_point = MagicMock(return_value=0.01)

        event = {
            "event_type": "TP2_HIT",
            "sid": sid,
            "symbol": "BTCUSDT",
            "price": "31500.0",
            "source": "test",
        }
        result = orc.handle_event(event)
        # Прошёл gate → не должен быть skipped из-за tp_level_mismatch
        assert result.skipped is False or result.success is True

    def test_rocket_v1_tp1_activates(self):
        """rocket_v1 имеет activate_after_tp=1 → TP1_HIT проходит per-profile gate."""
        orc = _make_orchestrator(self.fake_r, default_profile="rocket_v1")
        sid = "sig-rocket-tp1"
        _store_signal(self.fake_r, sid, self._make_signal("rocket_v1"))

        orc.dispatcher.send_trailing_command_from_atr = MagicMock(return_value=True)
        orc.dispatcher.send_trailing_modify = MagicMock(return_value=True)
        orc.dispatcher.get_symbol_point = MagicMock(return_value=0.01)

        event = {
            "event_type": "TP1_HIT",
            "sid": sid,
            "symbol": "BTCUSDT",
            "price": "31000.0",
            "source": "test",
        }
        result = orc.handle_event(event)
        # Не должен вернуть tp_level_mismatch skipped
        error = result.error or ""
        assert "tp_level_mismatch" not in error, f"Unexpected tp_level_mismatch: {result}"


# ─────────────────────────────────────────────────────────────────────────────
# TestDedupTp1Tp2Independent
# ─────────────────────────────────────────────────────────────────────────────

class TestDedupTp1Tp2Independent:
    """dedup ключи: dedup:tp{n}_trailing:{sid} — TP1 и TP2 независимы."""

    def setup_method(self):
        self.fake_r = fakeredis.FakeRedis(decode_responses=True)

    def test_dedup_tp1_and_tp2_use_different_keys(self):
        """TP1_HIT деdup → ключ tp1_trailing; TP2_HIT для того же sid → другой ключ tp2_trailing."""
        orc = _make_orchestrator(self.fake_r, default_profile="protective_only")
        sid = "sig-dedup-sep"

        # Вручную ставим dedup для TP1 — имитируем уже обработанный TP1
        self.fake_r.set(f"dedup:tp1_trailing:{sid}", "1", nx=True, ex=86400 * 3)

        # Ещё один TP1_HIT → dedup hit → skipped
        tp1_event = {
            "event_type": "TP1_HIT",
            "sid": sid,
            "symbol": "BTCUSDT",
            "price": "31000.0",
            "source": "test",
        }
        result_tp1_dup = orc.handle_event(tp1_event)
        assert result_tp1_dup.skipped is True
        assert result_tp1_dup.reason == "dedup_hit"

        # TP2_HIT для того же sid → другой ключ → dedup НЕ срабатывает
        # (результат будет skipped только из-за profile mismatch, НЕ из-за dedup)
        tp2_event = {
            "event_type": "TP2_HIT",
            "sid": sid,
            "symbol": "BTCUSDT",
            "price": "32000.0",
            "source": "test",
        }
        result_tp2 = orc.handle_event(tp2_event)
        # TP2 не должен иметь reason="dedup_hit"
        assert result_tp2.reason != "dedup_hit", (
            f"TP2 unexpectedly got dedup_hit, result={result_tp2}"
        )
        # Убеждаемся, что ключ tp2 теперь существует в Redis
        assert self.fake_r.exists(f"dedup:tp2_trailing:{sid}") == 1

    def test_dedup_same_level_blocks_duplicate(self):
        """Два TP1_HIT подряд для одного sid: второй → result.skipped=True, reason=dedup_hit."""
        orc = _make_orchestrator(self.fake_r, default_profile="protective_only")
        sid = "sig-dedup-same"

        # Ставим dedup-ключ для TP1 вручную
        self.fake_r.set(f"dedup:tp1_trailing:{sid}", "1", nx=True, ex=86400 * 3)

        event = {
            "event_type": "TP1_HIT",
            "sid": sid,
            "symbol": "BTCUSDT",
            "price": "31000.0",
            "source": "test",
        }
        result = orc.handle_event(event)
        assert result.skipped is True
        assert result.reason == "dedup_hit"


# ─────────────────────────────────────────────────────────────────────────────
# TestTrailingDecisionsAudit
# ─────────────────────────────────────────────────────────────────────────────

class TestTrailingDecisionsAudit:
    """Проверка маппинга metadata → параметры INSERT trailing_decisions."""

    def _call_write_pg(
        self,
        metadata: dict,
        sid: str = "sig-audit-1",
        symbol: str = "BTCUSDT",
        profile_name: str = "rocket_v1",
    ) -> list:
        """Вызывает _write_trailing_decision_pg с мокнутым psycopg2 и возвращает
        параметры cur.execute().
        """
        from services.tp_hit_trailing_orchestrator import TpHitTrailingOrchestrator

        fake_r = fakeredis.FakeRedis(decode_responses=True)
        orc = _make_orchestrator(fake_r, default_profile="rocket_v1")

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        import sys
        import types

        # psycopg2 импортируется внутри _write_trailing_decision_pg через локальный import.
        # Подменяем модуль в sys.modules перед вызовом.
        mock_psycopg2 = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_psycopg2.extras.Json = lambda x: x  # Json() просто возвращает переданный dict

        with (
            patch.dict(os.environ, {"TRADES_DB_DSN": "postgresql://fake/db"}),
            patch.dict(sys.modules, {"psycopg2": mock_psycopg2, "psycopg2.extras": mock_psycopg2.extras}),
        ):
            orc._write_trailing_decision_pg(
                sid=sid,
                symbol=symbol,
                profile_name=profile_name,
                metadata=metadata,
            )

        # Получаем параметры из cur.execute(sql, params)
        assert mock_cur.execute.called, "cur.execute was not called"
        _, params = mock_cur.execute.call_args[0]
        return list(params)

    def test_trailing_decisions_side_written(self):
        """metadata['side']='BUY' попадает в params на правильную позицию (idx=5)."""
        metadata = {
            "side": "BUY",
            "tp_level": 1,
            "trail_distance_price": 75.0,
            "atr_value": 50.0,
            "atr_mult": 1.5,
            "new_sl": "29500.0",
            "previous_sl": 29000.0,
            "position_id": "pos-1",
            "policy_hash": "abc",
            "profile_hash": "def",
            "schema_ver": 2,
        }
        params = self._call_write_pg(metadata, sid="sig-side-test")

        # INSERT column order:
        # sid(0), symbol(1), position_id(2), event_type(3), profile(4), side(5), tp_level(6),
        # old_sl(7), new_sl(8), trail_distance(9), atr_value(10), atr_mult(11),
        # idempotency_key(12), policy_hash(13), profile_hash(14), schema_ver(15), payload(16)
        assert params[5] == "BUY", f"Expected 'BUY' at index 5, got {params[5]!r} (full params={params})"

    def test_trailing_decisions_distance_atr_mapping(self):
        """trail_distance_price → trail_distance (idx=9), atr_value → atr_value (idx=10),
        atr_mult → atr_mult (idx=11).
        """
        metadata = {
            "side": "LONG",
            "tp_level": 1,
            "trail_distance_price": 123.45,
            "atr_value": 82.3,
            "atr_mult": 1.2,
            "new_sl": "29100.0",
            "previous_sl": 28900.0,
            "position_id": "pos-2",
            "policy_hash": "ph",
            "profile_hash": "prh",
            "schema_ver": 2,
        }
        params = self._call_write_pg(metadata, sid="sig-dist-test")

        trail_distance = params[9]
        atr_value = params[10]
        atr_mult = params[11]

        assert abs(trail_distance - 123.45) < 1e-6, (
            f"trail_distance expected 123.45 got {trail_distance}"
        )
        assert abs(atr_value - 82.3) < 1e-6, (
            f"atr_value expected 82.3 got {atr_value}"
        )
        assert abs(atr_mult - 1.2) < 1e-6, (
            f"atr_mult expected 1.2 got {atr_mult}"
        )
