"""G0 · Tick Validation (Time Normalization) — полный аудит.

Покрывает:
  1. normalize_epoch_ms — корректность нормализации единиц времени
  2. TickTimePolicy / TickTimeGuard (canonical common.tick_time)
  3. apply_tick_time_policy (functional API, backward-compat)
  4. BadTimeQuarantine — state machine карантина
  5. Интеграция: pipeline flow (guard + quarantine вместе)
  6. Граничные случаи / edge cases
"""

from __future__ import annotations

import pytest
from common.tick_time import TickTimeGuard, TickTimePolicy, SanitizeResult, apply_tick_time_policy
from common.time_quarantine import BadTimeQuarantine, BadTimeQuarantinePolicy
from common.time_norm import normalize_epoch_ms

# ─────────────────────────────────────────────
# Константы
# ─────────────────────────────────────────────
NOW_MS = 1_717_000_000_000   # реалистичный epoch ms (2024)
NOW_S  = NOW_MS // 1000       # в секундах
NOW_US = NOW_MS * 1_000       # в микросекундах


def pol(**kw) -> TickTimePolicy:
    defaults = dict(
        max_future_ms=500,
        max_past_ms=5_000,
        max_reorder_ms=1_500,
        clamp_soft_future=True,
        allow_soft_reorder=True,
        enforce_monotonic_watermark=True,
    )
    defaults.update(kw)
    return TickTimePolicy(**defaults)


def guard(**kw) -> TickTimeGuard:
    return TickTimeGuard(pol(**kw), now_provider=lambda: NOW_MS)


# ─────────────────────────────────────────────
# 1. normalize_epoch_ms
# ─────────────────────────────────────────────

class TestNormalizeEpochMs:
    def test_ms_passthrough(self):
        assert normalize_epoch_ms(NOW_MS) == NOW_MS

    def test_seconds_to_ms(self):
        assert normalize_epoch_ms(NOW_S) == NOW_S * 1000

    def test_microseconds_to_ms(self):
        assert normalize_epoch_ms(NOW_US) == NOW_US // 1000

    def test_float_ms(self):
        assert normalize_epoch_ms(float(NOW_MS)) == NOW_MS

    def test_iso_string(self):
        r = normalize_epoch_ms("2024-01-01T00:00:00Z")
        assert r > 1_700_000_000_000

    def test_numeric_string(self):
        assert normalize_epoch_ms(str(NOW_MS)) == NOW_MS

    def test_negative_or_zero_passthrough(self):
        # zero and negative are returned as-is (caller decides)
        assert normalize_epoch_ms(0) == 0

    def test_nan_raises(self):
        import math
        with pytest.raises(ValueError):
            normalize_epoch_ms(float("nan"))

    def test_inf_raises(self):
        import math
        with pytest.raises(ValueError):
            normalize_epoch_ms(float("inf"))


# ─────────────────────────────────────────────
# 2. TickTimeGuard — unit
# ─────────────────────────────────────────────

class TestTickTimeGuard:

    # --- ts_ms валидный ---

    def test_ok_ms_accepted(self):
        g = guard()
        r = g.sanitize_ts_ms(NOW_MS - 100, now_ms=NOW_MS)
        assert r is not None
        assert r.drop_reason is None
        assert r.ts_ms == NOW_MS - 100

    def test_watermark_advances_on_accept(self):
        g = guard()
        g.sanitize_ts_ms(NOW_MS - 100, now_ms=NOW_MS)
        assert g.watermark_ms == NOW_MS - 100

    # --- нормализация единиц ---

    def test_seconds_normalized_to_ms(self):
        g = guard()
        r = g.sanitize_ts_ms(NOW_S, now_ms=NOW_MS)
        assert r is not None and r.drop_reason is None
        assert r.ts_ms == NOW_MS
        assert r.flags and "normalized_seconds" in r.flags

    def test_microseconds_normalized_to_ms(self):
        g = guard()
        r = g.sanitize_ts_ms(NOW_US, now_ms=NOW_MS)
        assert r is not None and r.drop_reason is None
        assert r.ts_ms == NOW_MS
        assert r.flags and "normalized_micros" in r.flags

    # --- Future ---

    def test_soft_future_clamped_to_now(self):
        g = guard(max_future_ms=500, clamp_soft_future=True)
        r = g.sanitize_ts_ms(NOW_MS + 200, now_ms=NOW_MS)
        assert r is not None and r.drop_reason is None
        assert r.ts_ms == NOW_MS
        assert r.flags and "clamped_soft_future" in r.flags

    def test_hard_future_dropped(self):
        g = guard(max_future_ms=500, clamp_soft_future=True)
        r = g.sanitize_ts_ms(NOW_MS + 1_000, now_ms=NOW_MS)
        assert r is not None
        assert r.drop_reason == "future_hard"

    def test_hard_future_no_clamp_dropped(self):
        g = guard(max_future_ms=500, clamp_soft_future=False)
        r = g.sanitize_ts_ms(NOW_MS + 200, now_ms=NOW_MS)
        assert r is not None
        assert r.drop_reason == "future_hard"

    def test_exact_future_boundary_clamped(self):
        """Тик ровно на max_future_ms — должен клампиться (не дропаться)."""
        g = guard(max_future_ms=500, clamp_soft_future=True)
        r = g.sanitize_ts_ms(NOW_MS + 500, now_ms=NOW_MS)
        assert r is not None and r.drop_reason is None
        assert r.ts_ms == NOW_MS

    def test_future_plus_one_dropped(self):
        """Тик на max_future_ms+1 — должен дропаться."""
        g = guard(max_future_ms=500, clamp_soft_future=True)
        r = g.sanitize_ts_ms(NOW_MS + 501, now_ms=NOW_MS)
        assert r is not None
        assert r.drop_reason == "future_hard"

    # --- Past ---

    def test_past_hard_dropped(self):
        g = guard(max_past_ms=5_000)
        r = g.sanitize_ts_ms(NOW_MS - 6_000, now_ms=NOW_MS)
        assert r is not None
        assert r.drop_reason == "past_hard"

    def test_past_exactly_at_boundary_accepted(self):
        g = guard(max_past_ms=5_000)
        r = g.sanitize_ts_ms(NOW_MS - 5_000, now_ms=NOW_MS)
        assert r is not None and r.drop_reason is None

    def test_past_one_ms_over_boundary_dropped(self):
        g = guard(max_past_ms=5_000)
        r = g.sanitize_ts_ms(NOW_MS - 5_001, now_ms=NOW_MS)
        assert r is not None
        assert r.drop_reason == "past_hard"

    # --- Reorder ---

    def test_soft_reorder_clamped_to_watermark(self):
        g = guard(max_reorder_ms=1_500, allow_soft_reorder=True)
        g.sanitize_ts_ms(NOW_MS - 10, now_ms=NOW_MS)
        wm = g.watermark_ms
        r = g.sanitize_ts_ms(wm - 500, now_ms=NOW_MS)
        assert r is not None and r.drop_reason is None
        assert r.ts_ms == wm
        assert r.flags and "reorder_soft" in r.flags

    def test_soft_reorder_watermark_not_updated(self):
        g = guard(max_reorder_ms=1_500, allow_soft_reorder=True)
        g.sanitize_ts_ms(NOW_MS - 10, now_ms=NOW_MS)
        wm_before = g.watermark_ms
        g.sanitize_ts_ms(wm_before - 500, now_ms=NOW_MS)
        assert g.watermark_ms == wm_before

    def test_hard_reorder_dropped(self):
        g = guard(max_reorder_ms=1_500, allow_soft_reorder=True, max_past_ms=60_000)
        g.sanitize_ts_ms(NOW_MS, now_ms=NOW_MS)
        r = g.sanitize_ts_ms(NOW_MS - 10_000, now_ms=NOW_MS)
        assert r is not None
        assert r.drop_reason == "reorder_hard"

    def test_no_reorder_without_watermark(self):
        """Первый тик без watermark — reorder не проверяется."""
        g = guard(max_reorder_ms=1_500)
        r = g.sanitize_ts_ms(NOW_MS - 100, now_ms=NOW_MS)
        assert r is not None and r.drop_reason is None

    # --- Плохие входные данные ---

    def test_none_returns_none(self):
        g = guard()
        assert g.sanitize_ts_ms(None, now_ms=NOW_MS) is None

    def test_empty_string_returns_none(self):
        g = guard()
        assert g.sanitize_ts_ms("", now_ms=NOW_MS) is None

    def test_garbage_string_returns_none(self):
        g = guard()
        assert g.sanitize_ts_ms("not_a_ts", now_ms=NOW_MS) is None

    def test_zero_drops_bad_ts(self):
        g = guard()
        r = g.sanitize_ts_ms(0, now_ms=NOW_MS)
        assert r is not None
        assert r.drop_reason == "bad_ts"

    def test_negative_drops_bad_ts(self):
        g = guard()
        r = g.sanitize_ts_ms(-1000, now_ms=NOW_MS)
        assert r is not None
        assert r.drop_reason == "bad_ts"

    def test_bytes_input_parsed(self):
        g = guard()
        r = g.sanitize_ts_ms(str(NOW_MS - 100).encode(), now_ms=NOW_MS)
        assert r is not None and r.drop_reason is None

    # --- Монотонность watermark ---

    def test_watermark_never_decreases(self):
        g = guard(allow_soft_reorder=True, max_reorder_ms=1_500)
        for delta in [0, -100, 50, -200, 100]:
            ts = NOW_MS + delta
            if ts > 0:
                g.sanitize_ts_ms(ts, now_ms=NOW_MS)
        # watermark должен быть >= самого первого принятого тика
        assert g.watermark_ms >= NOW_MS - 200

    def test_watermark_never_goes_future(self):
        g = guard(max_future_ms=500, clamp_soft_future=True)
        g.sanitize_ts_ms(NOW_MS + 300, now_ms=NOW_MS)
        assert g.watermark_ms <= NOW_MS


# ─────────────────────────────────────────────
# 3. apply_tick_time_policy (functional API)
# ─────────────────────────────────────────────

class TestApplyTickTimePolicy:
    """Backward-compat functional API — common.tick_time.apply_tick_time_policy"""

    def _pol(self, **kw) -> TickTimePolicy:
        return pol(**kw)

    def test_ok(self):
        ts, dec, meta = apply_tick_time_policy(
            tick_ts_ms=NOW_MS - 100,
            ingest_now_ms=NOW_MS,
            prev_ts_ms=NOW_MS - 200,
            policy=self._pol(),
        )
        assert dec == "ok"
        assert ts == NOW_MS - 100

    def test_drop_missing_zero(self):
        ts, dec, meta = apply_tick_time_policy(
            tick_ts_ms=0,
            ingest_now_ms=NOW_MS,
            prev_ts_ms=0,
            policy=self._pol(),
        )
        assert dec == "drop_missing"
        assert ts == 0

    def test_clamp_future(self):
        ts, dec, meta = apply_tick_time_policy(
            tick_ts_ms=NOW_MS + 200,
            ingest_now_ms=NOW_MS,
            prev_ts_ms=NOW_MS - 100,
            policy=self._pol(max_future_ms=500, clamp_soft_future=True),
        )
        assert dec == "clamp_future"
        assert ts == NOW_MS   # clamped to now (no prev conflict)

    def test_drop_future(self):
        ts, dec, meta = apply_tick_time_policy(
            tick_ts_ms=NOW_MS + 10_000,
            ingest_now_ms=NOW_MS,
            prev_ts_ms=0,
            policy=self._pol(max_future_ms=500),
        )
        assert dec == "drop_future"

    def test_drop_past(self):
        ts, dec, meta = apply_tick_time_policy(
            tick_ts_ms=NOW_MS - 100_000,
            ingest_now_ms=NOW_MS,
            prev_ts_ms=0,
            policy=self._pol(max_past_ms=5_000),
        )
        assert dec == "drop_past"

    def test_reorder_soft(self):
        ts, dec, meta = apply_tick_time_policy(
            tick_ts_ms=NOW_MS - 200,
            ingest_now_ms=NOW_MS,
            prev_ts_ms=NOW_MS - 100,  # prev newer
            policy=self._pol(max_reorder_ms=1_500, allow_soft_reorder=True),
        )
        assert dec == "reorder_soft"
        assert ts == NOW_MS - 100 + 1   # prev + 1

    def test_reorder_hard(self):
        ts, dec, meta = apply_tick_time_policy(
            tick_ts_ms=NOW_MS - 10_000,
            ingest_now_ms=NOW_MS,
            prev_ts_ms=NOW_MS - 100,
            policy=self._pol(max_reorder_ms=1_500, max_past_ms=60_000),
        )
        assert dec == "reorder_hard"
        assert ts == 0

    def test_meta_contains_keys(self):
        _, _, meta = apply_tick_time_policy(
            tick_ts_ms=NOW_MS - 100,
            ingest_now_ms=NOW_MS,
            prev_ts_ms=NOW_MS - 200,
        )
        assert "orig_ts_ms" in meta
        assert "now_ms" in meta
        assert "prev_ts_ms" in meta


# ─────────────────────────────────────────────
# 4. BadTimeQuarantine — state machine
# ─────────────────────────────────────────────

class TestBadTimeQuarantine:

    def _qpol(self, **kw) -> BadTimeQuarantinePolicy:
        defaults = dict(
            trigger_streak=3,
            trigger_score=3.0,
            hard_penalty=1.0,
            soft_penalty=0.2,
            decay_per_ok=0.1,
            quarantine_ms=60_000,
            state_freeze_ms=15_000,
        )
        defaults.update(kw)
        return BadTimeQuarantinePolicy(**defaults)

    def test_not_quarantined_initially(self):
        q = BadTimeQuarantine(policy=self._qpol())
        assert not q.is_quarantined(NOW_MS)
        assert not q.should_suppress_processing(NOW_MS)

    def test_quarantine_triggers_on_streak(self):
        q = BadTimeQuarantine(policy=self._qpol(trigger_streak=3))
        for _ in range(3):
            q.on_hard_drop("future_hard", NOW_MS)
        assert q.is_quarantined(NOW_MS)

    def test_quarantine_triggers_on_score(self):
        q = BadTimeQuarantine(policy=self._qpol(trigger_score=2.0, hard_penalty=1.0, trigger_streak=100))
        q.on_hard_drop("past_hard", NOW_MS)
        q.on_hard_drop("past_hard", NOW_MS)
        assert q.is_quarantined(NOW_MS)

    def test_quarantine_expires(self):
        q = BadTimeQuarantine(policy=self._qpol(quarantine_ms=1_000, state_freeze_ms=500))
        for _ in range(3):
            q.on_hard_drop("x", NOW_MS)
        assert q.is_quarantined(NOW_MS)
        assert not q.is_quarantined(NOW_MS + 2_000)

    def test_state_freeze_shorter_than_quarantine(self):
        """quarantine < state_freeze trigger: 3 drops активируют карантин,
        но state_freeze требует state_trigger_streak (default=10)."""
        q = BadTimeQuarantine(policy=self._qpol(
            quarantine_ms=5_000,
            state_freeze_ms=1_000,
            trigger_streak=3,
            state_trigger_streak=3,  # тоже 3, чтобы проверить freeze
        ))
        for _ in range(3):
            q.on_hard_drop("x", NOW_MS)
        # карантин активен
        assert q.is_quarantined(NOW_MS)
        # state_freeze тоже активен (streak достиг state_trigger_streak=3)
        assert q.is_state_frozen(NOW_MS)
        # после истечения state_freeze_ms=1000 — freeze ушёл, quarantine ещё жив
        assert not q.is_state_frozen(NOW_MS + 1_500)
        assert q.is_quarantined(NOW_MS + 1_500)
        # после истечения quarantine_ms=5000 — оба истекли
        assert not q.is_quarantined(NOW_MS + 6_000)

    def test_soft_event_raises_score_not_streak(self):
        q = BadTimeQuarantine(policy=self._qpol(soft_penalty=0.2, trigger_streak=3))
        for _ in range(10):
            q.on_soft_event("reorder_soft")
        assert q.hard_streak == 0
        assert q.score > 0

    def test_ok_tick_decays_score_resets_streak(self):
        q = BadTimeQuarantine(policy=self._qpol())
        q.on_hard_drop("x", NOW_MS)
        q.on_hard_drop("x", NOW_MS)
        assert q.hard_streak == 2
        q.on_ok_tick()
        assert q.hard_streak == 0
        assert q.score < 2.0   # decayed

    def test_metric_callback_called_on_quarantine(self):
        calls: list[str] = []
        def inc(name: str, delta: int) -> None:
            calls.append(name)
        q = BadTimeQuarantine(policy=self._qpol(trigger_streak=3), inc=inc)
        for _ in range(3):
            q.on_hard_drop("future_hard", NOW_MS)
        # quarantine.enabled должен был вызваться
        assert any("quarantine.enabled" in c for c in calls)
        # hard_drop метрика тоже должна быть
        assert any("hard_drop" in c for c in calls)

    def test_no_double_quarantine_metric(self):
        """После Bug 2 fix: quarantine.enabled стреляет ровно 1 раз за цикл
        (transition guard inactive→active), не при каждом hard_drop."""
        enabled_count = 0
        def inc(name: str, delta: int) -> None:
            nonlocal enabled_count
            if "quarantine.enabled" in name:
                enabled_count += 1
        q = BadTimeQuarantine(policy=self._qpol(trigger_streak=3), inc=inc)
        for _ in range(6):   # 6 дропов — порог пройден на 3-м
            q.on_hard_drop("x", NOW_MS)
        # ровно 1 вызов (transition guard)
        assert enabled_count == 1
        # карантин активен
        assert q.is_quarantined(NOW_MS)


# ─────────────────────────────────────────────
# 5. Интеграция: Guard + Quarantine pipeline
# ─────────────────────────────────────────────

class TestG0Integration:
    """Симулирует реальный поток: guard.sanitize_ts_ms -> quarantine -> pipeline."""

    def _setup(self):
        p = pol(max_future_ms=500, max_past_ms=5_000, max_reorder_ms=1_500)
        qp = BadTimeQuarantinePolicy(
            trigger_streak=3,
            quarantine_ms=60_000,
            state_freeze_ms=15_000,
        )
        g = TickTimeGuard(p, now_provider=lambda: NOW_MS)
        q = BadTimeQuarantine(policy=qp)
        return g, q

    def _process_tick(self, g: TickTimeGuard, q: BadTimeQuarantine, ts_raw, drop_bad_time=True):
        now_ms = NOW_MS
        res = g.sanitize_ts_ms(ts_raw, now_ms=now_ms)
        if res is None:
            q.on_hard_drop("bad_ts", now_ms)
            return "hard_drop:bad_ts"
        if res.drop_reason:
            q.on_hard_drop(res.drop_reason, now_ms)
            if drop_bad_time:
                return f"hard_drop:{res.drop_reason}"
        else:
            if res.flags:
                for f in res.flags:
                    q.on_soft_event(f)
            else:
                q.on_ok_tick()
        if q.is_quarantined(now_ms):
            return "quarantined"
        if q.should_suppress_processing(now_ms):
            return "state_freeze"
        return "ok"

    def test_normal_stream(self):
        g, q = self._setup()
        for i in range(10):
            outcome = self._process_tick(g, q, NOW_MS - i * 10)
            assert outcome == "ok", f"Tick {i} не прошёл: {outcome}"

    def test_3_future_hard_triggers_quarantine(self):
        g, q = self._setup()
        for _ in range(3):
            self._process_tick(g, q, NOW_MS + 10_000)  # hard future
        assert q.is_quarantined(NOW_MS)

    def test_pipeline_blocked_during_quarantine(self):
        g, q = self._setup()
        for _ in range(3):
            self._process_tick(g, q, NOW_MS + 10_000)
        # нормальный тик — должен быть заблокирован карантином
        outcome = self._process_tick(g, q, NOW_MS - 100)
        assert outcome == "quarantined"

    def test_recovery_after_quarantine_expires(self):
        g2, q2 = self._setup()
        for _ in range(3):
            self._process_tick(g2, q2, NOW_MS + 10_000)

        # Создаём новый guard с now_provider > quarantine_ttl
        g_late = TickTimeGuard(
            pol(max_future_ms=500, max_past_ms=120_000),
            now_provider=lambda: NOW_MS + 70_000,
        )
        now_late = NOW_MS + 70_000
        res = g_late.sanitize_ts_ms(NOW_MS + 70_000 - 100, now_ms=now_late)
        assert res is not None and res.drop_reason is None
        # quarantine истёк
        assert not q2.is_quarantined(now_late)

    def test_soft_reorder_accumulates_score_not_streak(self):
        g, q = self._setup()
        # установим watermark
        self._process_tick(g, q, NOW_MS)
        streak_before = q.hard_streak
        # мягкий reorder
        self._process_tick(g, q, NOW_MS - 500)
        assert q.hard_streak == streak_before  # streak не растёт

    def test_none_ts_causes_hard_drop(self):
        g, q = self._setup()
        outcome = self._process_tick(g, q, None)
        assert outcome == "hard_drop:bad_ts"

    def test_zero_ts_causes_hard_drop(self):
        g, q = self._setup()
        outcome = self._process_tick(g, q, 0)
        assert outcome.startswith("hard_drop")

    def test_seconds_input_normalised_and_accepted(self):
        g, q = self._setup()
        outcome = self._process_tick(g, q, NOW_S)
        # normalized_seconds is soft event, pipeline не блокируется
        assert outcome in ("ok",)


# ─────────────────────────────────────────────
# 6. Выходные инварианты G0
# ─────────────────────────────────────────────

class TestG0OutputInvariants:
    """После G0 downstream получает только корректные ts_ms."""

    def test_output_ts_never_gt_now(self):
        g = guard(max_future_ms=500, clamp_soft_future=True)
        for delta in [-1000, -100, 0, 100, 300, 499, 500]:
            r = g.sanitize_ts_ms(NOW_MS + delta, now_ms=NOW_MS)
            if r and not r.drop_reason:
                assert r.ts_ms <= NOW_MS, f"delta={delta} => ts_ms={r.ts_ms} > now={NOW_MS}"

    def test_output_ts_always_positive_on_accept(self):
        g = guard()
        for ts in [NOW_MS, NOW_MS - 100, NOW_MS - 4999]:
            r = g.sanitize_ts_ms(ts, now_ms=NOW_MS)
            if r and not r.drop_reason:
                assert r.ts_ms > 0

    def test_output_ts_always_ms_range(self):
        """Принятые тики должны быть в диапазоне ms (>1e12)."""
        g = guard()
        for ts in [NOW_MS - 100, NOW_S, NOW_US]:
            r = g.sanitize_ts_ms(ts, now_ms=NOW_MS)
            if r and not r.drop_reason:
                assert r.ts_ms > 1_000_000_000_000, f"ts_ms={r.ts_ms} is not ms range"

    def test_drop_always_returns_drop_reason(self):
        g = guard(max_future_ms=500, max_past_ms=5_000)
        drops = [
            NOW_MS + 10_000,   # future_hard
            NOW_MS - 50_000,   # past_hard
            0,                 # bad_ts
        ]
        for ts in drops:
            r = g.sanitize_ts_ms(ts, now_ms=NOW_MS)
            assert r is not None
            assert r.drop_reason is not None, f"ts={ts} должен быть dropped"

    def test_accepted_always_has_no_drop_reason(self):
        g = guard()
        r = g.sanitize_ts_ms(NOW_MS - 100, now_ms=NOW_MS)
        assert r is not None
        assert r.drop_reason is None
