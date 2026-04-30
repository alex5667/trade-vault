from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
from dataclasses import dataclass
from typing import Callable, Optional
from common.token_bucket import TokenBucket


@dataclass
class BadTimeQuarantinePolicy:
    """
    Политика "аварийного режима" при плохом времени.

    trigger_streak:
      сколько подряд hard-drop'ов (future/past/reorder_hard) нужно, чтобы включить карантин.
    quarantine_ms:
      на сколько ms выключаем генерацию сигналов (но продолжаем обновлять состояние).

    soft_penalty:
      сколько "весит" soft-ивент (clamp/reorder_soft) относительно hard.
    hard_penalty:
      сколько "весит" hard-drop.
    trigger_score:
      порог по score (с учетом penalty), чтобы включить карантин.
    decay_per_ok:
      насколько быстро "отпускает" score при нормальных тиках.
    """

    trigger_streak: int = int(os.getenv("BAD_TIME_TRIGGER_STREAK", "5"))
    quarantine_ms: int = int(os.getenv("BAD_TIME_QUARANTINE_MS", "5000"))

    # "совсем жёстко": отдельный контур freeze state (не кормим EMA/окна), когда время реально сломано
    state_trigger_streak: int = int(os.getenv("BAD_TIME_STATE_TRIGGER_STREAK", "10"))
    state_freeze_ms: int = int(os.getenv("BAD_TIME_STATE_FREEZE_MS", "3000"))

    soft_penalty: float = float(os.getenv("BAD_TIME_SOFT_PENALTY", "0.25"))
    hard_penalty: float = float(os.getenv("BAD_TIME_HARD_PENALTY", "1.0"))
    trigger_score: float = float(os.getenv("BAD_TIME_TRIGGER_SCORE", "4.0"))
    state_trigger_score: float = float(os.getenv("BAD_TIME_STATE_TRIGGER_SCORE", "8.0"))

    decay_per_ok: float = float(os.getenv("BAD_TIME_DECAY_PER_OK", "0.5"))

    # "ещё сильнее": rate-limit по аномалиям времени.
    # если лимит превышен -> агрессивно расширяем state_freeze/quarantine (защита от flood/сломанных часов).
    hard_drops_rate_per_sec: float = float(os.getenv("BAD_TIME_HARD_DROPS_RPS", "20"))
    hard_drops_burst: float = float(os.getenv("BAD_TIME_HARD_DROPS_BURST", "40"))
    soft_events_rate_per_sec: float = float(os.getenv("BAD_TIME_SOFT_EVENTS_RPS", "50"))
    soft_events_burst: float = float(os.getenv("BAD_TIME_SOFT_EVENTS_BURST", "100"))
    ratelimit_penalty_freeze_ms: int = int(os.getenv("BAD_TIME_RATELIMIT_FREEZE_MS", "8000"))
    ratelimit_penalty_quarantine_ms: int = int(os.getenv("BAD_TIME_RATELIMIT_QUARANTINE_MS", "15000"))

    # "совсем жёстко": если подряд слишком много reorder_soft -> считаем время "дрожащим" и режем state
    reorder_soft_streak_trigger: int = int(os.getenv("BAD_TIME_REORDER_SOFT_STREAK", "25"))

    # "следующая гайка": time-recovery gate после state_freeze.
    # После выхода из state_freeze НЕ обновляем state/НЕ эмитим, пока не увидим N подряд "OK" тиков
    # (без soft flags и без hard drop).
    recovery_ok_streak: int = int(os.getenv("BAD_TIME_RECOVERY_OK_STREAK", "5"))
    recovery_max_ms: int = int(os.getenv("BAD_TIME_RECOVERY_MAX_MS", "8000"))
    recovery_fail_quarantine_ms: int = int(os.getenv("BAD_TIME_RECOVERY_FAIL_QUARANTINE_MS", "20000"))
    recovery_fail_state_freeze_ms: int = int(os.getenv("BAD_TIME_RECOVERY_FAIL_STATE_FREEZE_MS", "5000"))


class BadTimeQuarantine:
    """
    Состояние карантина.
    Дизайн: максимально простой, без таймеров/потоков.

    - hard-drop (future_hard/past_hard/reorder_hard) увеличивает streak и score.
    - soft-ивенты (clamped_soft_future/reorder_soft) увеличивают score слабее и не увеличивают streak.
    - нормальный тик уменьшает score (decay) и сбрасывает streak.
    """

    def __init__(
        self
        policy: Optional[BadTimeQuarantinePolicy] = None
        *
        inc: Optional[Callable[[str, int], None]] = None
    ) -> None:
        self.policy = policy or BadTimeQuarantinePolicy()
        self._inc = inc
        self._score: float = 0.0
        self._hard_streak: int = 0
        self._until_ms: int = 0
        self._state_until_ms: int = 0
        self._hard_bucket = TokenBucket(
            rate_per_sec=float(self.policy.hard_drops_rate_per_sec)
            burst=float(self.policy.hard_drops_burst)
        )
        self._soft_bucket = TokenBucket(
            rate_per_sec=float(self.policy.soft_events_rate_per_sec)
            burst=float(self.policy.soft_events_burst)
        )
        self._reorder_soft_streak: int = 0

        # recovery gate state
        self._recovery_active: bool = False
        self._recovery_ok: int = 0
        self._recovery_deadline_ms: int = 0
        self._last_seen_state_until_ms: int = 0

    @property
    def until_ms(self) -> int:
        return int(self._until_ms)

    @property
    def score(self) -> float:
        return float(self._score)

    @property
    def hard_streak(self) -> int:
        return int(self._hard_streak)

    def _m(self, name: str, delta: int = 1) -> None:
        if not self._inc:
            return
        try:
            self._inc(name, int(delta))
        except Exception:
            pass

    def is_quarantined(self, now_ms: int) -> bool:
        return int(now_ms) < int(self._until_ms)

    def is_state_frozen(self, now_ms: int) -> bool:
        return int(now_ms) < int(self._state_until_ms)

    def is_in_recovery(self, now_ms: int) -> bool:
        # recovery gate действует только ПОСЛЕ выхода из state_freeze
        _ = now_ms
        return bool(self._recovery_active)

    def should_suppress_processing(self, now_ms: int) -> bool:
        """
        Главный "затвор":
        - во время state_freeze: suppress
        - после state_freeze: suppress пока не пройдём recovery_ok_streak подряд "OK" тиков
        """
        now_ms = int(now_ms)

        # track latest freeze end
        if self._state_until_ms > 0:
            self._last_seen_state_until_ms = max(int(self._last_seen_state_until_ms), int(self._state_until_ms))

        # still frozen -> suppress
        if self.is_state_frozen(now_ms):
            return True

        # freeze ended, but we haven't started recovery yet -> start it
        if (not self._recovery_active) and self._last_seen_state_until_ms > 0 and now_ms >= int(self._last_seen_state_until_ms) and not hasattr(self, '_recovery_started'):
            if int(self.policy.recovery_ok_streak) > 0:
                self._recovery_active = True
                self._recovery_ok = 0
                self._recovery_deadline_ms = now_ms + int(self.policy.recovery_max_ms)
                self._recovery_started = True
                self._m("tick.time.recovery.started", 1)

        if not self._recovery_active:
            return False

        # recovery timeout -> escalate quarantine + short state_freeze, reset recovery
        if now_ms > int(self._recovery_deadline_ms):
            self._until_ms = max(int(self._until_ms), now_ms + int(self.policy.recovery_fail_quarantine_ms))
            self._state_until_ms = max(int(self._state_until_ms), now_ms + int(self.policy.recovery_fail_state_freeze_ms))
            self._recovery_active = False
            self._recovery_ok = 0
            self._recovery_deadline_ms = 0
            self._m("tick.time.recovery.timeout", 1)
            return True

        # active recovery -> suppress until enough OK ticks
        return True

    def on_hard_drop(self, reason: str, now_ms: int) -> None:
        # hard-drop = токсично: увеличиваем streak и score
        self._hard_streak += 1
        self._score += float(self.policy.hard_penalty)
        self._m("tick.time.hard_drop", 1)
        if reason:
            self._m(f"tick.time.hard_drop.{reason}", 1)
        # rate-limit hard drops: если overflow — значит время/фид реально сломано (или flood)
        # делаем расширенный freeze/quarantine.
        if not self._hard_bucket.allow(int(now_ms), cost=1.0):
            self._until_ms = max(int(self._until_ms), int(now_ms) + int(self.policy.ratelimit_penalty_quarantine_ms))
            self._state_until_ms = max(int(self._state_until_ms), int(now_ms) + int(self.policy.ratelimit_penalty_freeze_ms))
            self._m("tick.time.ratelimit.hard_drop_overflow", 1)

        # hard drop breaks recovery immediately
        if self._recovery_active:
            self._recovery_ok = 0
            # keep deadline, but note reset
            self._m("tick.time.recovery.reset.hard_drop", 1)

        if self._hard_streak >= int(self.policy.trigger_streak) or self._score >= float(self.policy.trigger_score):
            # включаем карантин
            self._until_ms = max(int(self._until_ms), int(now_ms) + int(self.policy.quarantine_ms))
            self._m("tick.time.quarantine.enabled", 1)

        # state-freeze (ещё жестче): включаем, когда уже явно "сломано время"
        if self._hard_streak >= int(self.policy.state_trigger_streak) or self._score >= float(self.policy.state_trigger_score):
            self._state_until_ms = max(int(self._state_until_ms), int(now_ms) + int(self.policy.state_freeze_ms))
            self._m("tick.time.state_freeze.enabled", 1)

    def on_soft_event(self, flag: str) -> None:
        # мягкие искажения времени: penalize score, но не дергаем streak
        self._score += float(self.policy.soft_penalty)
        self._m("tick.time.soft_event", 1)
        if flag:
            self._m(f"tick.time.soft_event.{flag}", 1)

        # rate-limit soft events: если overflow, эскалируем quarantine (не state_freeze по умолчанию)
        # чтобы не "убить" state из-за одних только нормализаций.
        if not self._soft_bucket.allow(self._now_ms_fallback(), cost=1.0):
            self._until_ms = max(int(self._until_ms), self._now_ms_fallback() + int(self.policy.ratelimit_penalty_quarantine_ms))
            self._m("tick.time.ratelimit.soft_event_overflow", 1)

        # Любой soft flag сбрасывает recovery streak (но recovery остаётся активным)
        if self._recovery_active:
            self._recovery_ok = 0
            self._m("tick.time.recovery.reset.soft_event", 1)

        if flag == "reorder_soft":
            self._reorder_soft_streak += 1
            if self._reorder_soft_streak >= int(self.policy.reorder_soft_streak_trigger):
                now_ms = self._now_ms_fallback()
                self._state_until_ms = max(int(self._state_until_ms), int(now_ms) + int(self.policy.ratelimit_penalty_freeze_ms))
                self._m("tick.time.reorder_soft.streak_freeze", 1)
        else:
            self._reorder_soft_streak = 0

    def _now_ms_fallback(self) -> int:
        # quarantine может жить отдельно от handler, поэтому "now" нужен здесь тоже.
        import time
        return int(get_ny_time_millis())

    def on_ok_tick(self) -> None:
        # нормальный тик: сбрасываем streak, уменьшаем score (не ниже 0)
        self._hard_streak = 0
        self._score = max(0.0, float(self._score) - float(self.policy.decay_per_ok))
        self._reorder_soft_streak = 0
        self._m("tick.time.ok", 1)

        # In recovery: count OK streak
        if self._recovery_active:
            self._recovery_ok += 1
            self._m("tick.time.recovery.ok", 1)
            if self._recovery_ok >= int(self.policy.recovery_ok_streak):
                self._recovery_active = False
                self._recovery_ok = 0
                self._recovery_deadline_ms = 0
                self._m("tick.time.recovery.passed", 1)
