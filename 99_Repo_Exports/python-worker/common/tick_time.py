from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
from dataclasses import dataclass
from typing import Callable, Optional, List, Any, Dict

# ---------------------------------------------------------------------
# Prometheus metrics for replay timestamp verification
# ---------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram
    tick_time_replay_mismatch_total = Counter(
        "tick_time_replay_mismatch_total",
        "Total count of replay timestamp mismatches",
        ["severity", "reason"]
    )
    tick_time_replay_diff_ms = Histogram(
        "tick_time_replay_diff_ms",
        "Histogram of replay timestamp differences in ms",
        buckets=(1, 5, 10, 20, 50, 100, 250, 500, 1000, 5000)
    )
except ImportError:
    Counter = Histogram = None
    tick_time_replay_mismatch_total = None
    tick_time_replay_diff_ms = None


@dataclass
class TickTimePolicy:
    """
    Жёсткая нормализация/защита времени для тиков.

    max_future_ms:
      hard-drop если тик > now + max_future_ms
      (если clamp_soft_future=True, то в окне [now .. now+max_future_ms] тик клампится к now)
    max_past_ms:
      hard-drop если now - тик > max_past_ms
    max_reorder_ms:
      если тик < watermark, но (watermark - тик) <= max_reorder_ms:
        reorder_soft (кламп к watermark) если allow_soft_reorder=True,
        иначе hard-drop.
      если (watermark - тик) > max_reorder_ms => reorder_hard hard-drop.
    """

    max_future_ms: int = int(os.getenv("TICK_TIME_MAX_FUTURE_MS", "500"))
    max_past_ms: int = int(os.getenv("TICK_TIME_MAX_PAST_MS", "5000"))
    max_reorder_ms: int = int(os.getenv("TICK_TIME_MAX_REORDER_MS", "1500"))

    clamp_soft_future: bool = os.getenv("TICK_TIME_CLAMP_SOFT_FUTURE", "1").lower() in {"1", "true", "yes"}
    allow_soft_reorder: bool = os.getenv("TICK_TIME_ALLOW_SOFT_REORDER", "1").lower() in {"1", "true", "yes"}

    # unit heuristics
    # < 1e12 => seconds (epoch seconds ~ 1.7e9), normalize *1000
    # > 1e15 => microseconds, normalize //1000
    seconds_threshold: int = int(float(os.getenv("TICK_TIME_SECONDS_THRESHOLD", "1e12")))
    micros_threshold: int = int(float(os.getenv("TICK_TIME_MICROS_THRESHOLD", "1e15")))

    # if enabled: enforce monotonic watermark (never decreases)
    enforce_monotonic_watermark: bool = os.getenv("TICK_TIME_MONOTONIC_WATERMARK", "1").lower() in {"1", "true", "yes"}


@dataclass
class SanitizeResult:
    ts_ms: int
    drop_reason: Optional[str] = None   # future_hard / past_hard / reorder_hard / bad_ts
    flags: Optional[List[str]] = None   # normalized_seconds / normalized_micros / clamped_soft_future / reorder_soft

    def to_meta(self) -> Dict[str, Any]:
        return {
            "ts_ms": self.ts_ms,
            "drop_reason": self.drop_reason,
            "flags": self.flags or []
        }


@dataclass
class TsVerifyResult:
    ok: bool
    severity: str  # "ok" | "warn" | "severe"
    reason: str
    meta: Dict[str, Any]


def verify_bucketed_ts(
    actual_ts_ms: int,
    expected_ts_ms: int,
    bucket_ms: int,
    tol_ms: Optional[int] = None,
    hard_ms: Optional[int] = None,
) -> TsVerifyResult:
    """
    Верификация меток времени с учетом квантования (бакетизации).
    Используется в replay для проверки того, что события приходят в ожидаемые окна.
    """
    if bucket_ms <= 0:
        return TsVerifyResult(True, "ok", "bucket_disabled", {})

    a0 = (int(actual_ts_ms) // bucket_ms) * bucket_ms
    e0 = (int(expected_ts_ms) // bucket_ms) * bucket_ms
    diff = abs(a0 - e0)

    # default tolerances: half-bucket and 3x
    tol = int(tol_ms) if tol_ms is not None else max(1, bucket_ms // 2)
    hard = int(hard_ms) if hard_ms is not None else 3 * tol

    meta = {
        "actual_ts_ms": int(actual_ts_ms),
        "expected_ts_ms": int(expected_ts_ms),
        "actual_bucket_ms": int(a0),
        "expected_bucket_ms": int(e0),
        "bucket_ms": int(bucket_ms),
        "diff_ms": int(diff),
        "tol_ms": int(tol),
        "hard_ms": int(hard),
    }

    # IMPORTANT: use strict ">" to avoid boundary false positives
    if diff > hard:
        if tick_time_replay_mismatch_total:
            tick_time_replay_mismatch_total.labels(severity="severe", reason="replay_ts_mismatch_severe").inc()
        if tick_time_replay_diff_ms:
            tick_time_replay_diff_ms.observe(float(diff))
        return TsVerifyResult(False, "severe", "replay_ts_mismatch_severe", meta)

    if diff > tol:
        if tick_time_replay_mismatch_total:
            tick_time_replay_mismatch_total.labels(severity="warn", reason="replay_ts_mismatch").inc()
        if tick_time_replay_diff_ms:
            tick_time_replay_diff_ms.observe(float(diff))
        return TsVerifyResult(False, "warn", "replay_ts_mismatch", meta)

    return TsVerifyResult(True, "ok", "replay_ts_ok", meta)


class TickTimeGuard:
    """
    Внутренний контракт:
      - tick.ts на входе может быть seconds/ms/micros
      - на выходе всегда ms
      - watermark_ms обновляется только на accept (drop_reason is None)
      - watermark_ms не может быть > now (если soft-future clamp включён)
    """

    def __init__(self, policy: Optional[TickTimePolicy] = None, now_provider: Optional[Callable[[], int]] = None) -> None:
        self.policy = policy or TickTimePolicy()
        self._now = now_provider
        self._watermark_ms: int = 0  # monotonic accepted time watermark (ms)

    @property
    def watermark_ms(self) -> int:
        return int(self._watermark_ms)

    def _now_ms(self) -> int:
        if self._now:
            try:
                return int(self._now())
            except Exception:
                pass
        # fallback: time.time() is seconds
        import time
        return int(get_ny_time_millis())

    def _to_int(self, ts: Any) -> Optional[int]:
        try:
            if ts is None:
                return None
            # allow strings/bytes
            if isinstance(ts, (bytes, bytearray)):
                ts = ts.decode("utf-8", "ignore")
            return int(ts)
        except Exception:
            return None

    def sanitize_ts_ms(self, ts: Any, *, now_ms: Optional[int] = None) -> Optional[SanitizeResult]:
        """
        Возвращает SanitizeResult или None (если ts вообще не парсится).
        """
        t = self._to_int(ts)
        if t is None:
            return None
        if t <= 0:
            return SanitizeResult(ts_ms=int(t), drop_reason="bad_ts", flags=None)

        flags: List[str] = []

        # 1) normalize units
        if t > int(self.policy.micros_threshold):
            # likely microseconds
            t = int(t // 1000)
            flags.append("normalized_micros")
        elif t < int(self.policy.seconds_threshold):
            # likely seconds
            t = int(t * 1000)
            flags.append("normalized_seconds")

        # 2) watermark + future/past checks
        nm = int(now_ms) if now_ms is not None else self._now_ms()

        # future: either clamp (soft) or drop (hard)
        if t > nm:
            fut = int(t - nm)
            if fut <= int(self.policy.max_future_ms) and bool(self.policy.clamp_soft_future):
                t = nm
                flags.append("clamped_soft_future")
            else:
                return SanitizeResult(ts_ms=int(t), drop_reason="future_hard", flags=flags or None)

        # past hard
        if (nm - t) > int(self.policy.max_past_ms):
            return SanitizeResult(ts_ms=int(t), drop_reason="past_hard", flags=flags or None)

        # reorder relative to watermark
        wm = int(self._watermark_ms)
        if wm > 0 and t < wm:
            lag = int(wm - t)
            if lag <= int(self.policy.max_reorder_ms):
                if bool(self.policy.allow_soft_reorder):
                    t = wm
                    flags.append("reorder_soft")
                else:
                    return SanitizeResult(ts_ms=int(t), drop_reason="reorder_hard", flags=flags or None)
            else:
                return SanitizeResult(ts_ms=int(t), drop_reason="reorder_hard", flags=flags or None)

        # 3) accept => update watermark
        if bool(self.policy.enforce_monotonic_watermark):
            self._watermark_ms = max(int(self._watermark_ms), int(t))
        else:
            self._watermark_ms = int(t)

        return SanitizeResult(ts_ms=int(t), drop_reason=None, flags=flags or None)


# ---------------------------------------------------------------------------
# Functional API (backward-compat with core.tick_time consumers)
# Numbers are treated as-is (no seconds/micros normalisation) to preserve
# the original core.tick_time.apply_tick_time_policy contract.
# ---------------------------------------------------------------------------

def apply_tick_time_policy(
    *,
    tick_ts_ms: int,
    ingest_now_ms: int,
    prev_ts_ms: int,
    policy: Optional[TickTimePolicy] = None,
) -> "tuple[int, str, Dict[str, Any]]":
    """Apply time policy and return (normalized_ts_ms, decision, meta).

    decision values:
      ok | clamp_future | drop_future | drop_past | reorder_soft | reorder_hard | drop_missing

    Numbers are accepted as-is (no seconds/micros normalisation).
    Use TickTimeGuard.sanitize_ts_ms when normalisation is needed.
    """
    pol = policy or TickTimePolicy()
    ts = int(tick_ts_ms or 0)
    now = int(ingest_now_ms or 0)
    prev = int(prev_ts_ms or 0)

    meta: Dict[str, Any] = {
        "orig_ts_ms": ts,
        "now_ms": now,
        "prev_ts_ms": prev,
    }

    if ts <= 0:
        return 0, "drop_missing", meta

    if now <= 0:
        now = prev if prev > 0 else ts
        meta["now_ms"] = now

    if ts > now:
        skew = ts - now
        meta["skew_ms"] = int(skew)
        if skew > int(pol.max_future_ms):
            return 0, "drop_future", meta
        if pol.clamp_soft_future:
            ts2 = now
            if prev > 0 and ts2 <= prev:
                ts2 = prev + 1
            meta["norm_ts_ms"] = int(ts2)
            return int(ts2), "clamp_future", meta

    age = now - ts
    if age > int(pol.max_past_ms):
        meta["age_ms"] = int(age)
        return 0, "drop_past", meta

    if prev > 0 and ts < prev:
        back = prev - ts
        meta["back_ms"] = int(back)
        if back > int(pol.max_reorder_ms):
            return 0, "reorder_hard", meta
        if pol.allow_soft_reorder:
            ts2 = prev + 1
            meta["norm_ts_ms"] = int(ts2)
            return int(ts2), "reorder_soft", meta
        return 0, "reorder_hard", meta

    return int(ts), "ok", meta
