from __future__ import annotations

import time


def decide_should_train(
    *,
    min_new: int,
    force_after_sec: int,
    last_trained_at: int,
    new_eligible: int,
) -> tuple[bool, str]:
    """
    Триггер по приросту данных + safety-триггер по времени.
    Используется внутри time-based запуска (systemd timer).
    """
    now = int(time.time())
    if new_eligible >= int(min_new):
        return True, f"new_eligible={new_eligible} >= min_new={min_new}"
    if int(force_after_sec) > 0 and int(last_trained_at) > 0 and (now - int(last_trained_at)) >= int(force_after_sec):
        return True, f"force_after_sec reached: {now-last_trained_at}s >= {force_after_sec}s"
    return False, f"skip: new_eligible={new_eligible} < min_new={min_new}"
