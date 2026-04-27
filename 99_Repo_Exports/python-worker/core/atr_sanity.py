from __future__ import annotations
import os
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional

def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d

def _median(xs) -> float:
    n = len(xs)
    if n == 0:
        return 0.0
    ys = sorted(xs)
    mid = n // 2
    if n % 2 == 1:
        return float(ys[mid])
    return 0.5 * (float(ys[mid - 1]) + float(ys[mid]))

def get_max_age_ms_for_tf(tf: str, default_ms: int = 900_000) -> int:
    """Helper to scale allowed staleness based on timeframe."""
    t = (tf or "").lower().strip()
    # Normalize tf
    tf_normalized = t
    if t in ("1m", "m1"): tf_normalized = "1m"
    elif t in ("3m", "m3"): tf_normalized = "3m"
    elif t in ("5m", "m5"): tf_normalized = "5m"
    elif t in ("15m", "m15"): tf_normalized = "15m"
    elif t in ("30m", "m30"): tf_normalized = "30m"
    elif t in ("1h", "h1"): tf_normalized = "1h"
    elif t in ("2h", "h2"): tf_normalized = "2h"
    elif t in ("4h", "h4"): tf_normalized = "4h"
    elif t in ("1d", "d1"): tf_normalized = "1d"

    mapping = {
        "1m": 120_000,
        "3m": 300_000,
        "5m": 600_000,
        "15m": 1_200_000,
        "30m": 1_800_000,
        "1h": 7_200_000,
        "2h": 14_400_000,
        "4h": 28_800_000,
        "1d": 86_400_000,
    }
    return mapping.get(tf_normalized, default_ms)

def tf_to_ms(tf: str, default_ms: int = 60_000) -> int:
    """Best-effort timeframe -> milliseconds mapping (used for jump de-duplication)."""
    t = (tf or "").lower().strip()
    mapping = {
        "1m": 60_000, "m1": 60_000,
        "3m": 180_000, "m3": 180_000,
        "5m": 300_000, "m5": 300_000,
        "15m": 900_000, "m15": 900_000,
        "30m": 1_800_000, "m30": 1_800_000,
        "1h": 3_600_000, "h1": 3_600_000,
        "2h": 7_200_000, "h2": 7_200_000,
        "4h": 14_400_000, "h4": 14_400_000,
        "1d": 86_400_000, "d1": 86_400_000,
    }
    return int(mapping.get(t, default_ms))

@dataclass
class ATRSanityResult:
    atr_used: float
    bad: int
    reason: Optional[str]
    atr_bps: float
    used_last_good: int
    jump_event: int
    jump_count_window: int
    # Jump diagnostics (optional; kept with defaults for backward compatibility)
    jump_rel: float = 0.0
    jump_streak: int = 0
    jump_accept: int = 0
    med_bps: float = 0.0
    cand_med_bps: float = 0.0
    jump_bucket_id: int = 0

class ATRSanity:
    def __init__(self, window: int = 60):
        self.window = int(os.getenv("ATR_SANITY_WINDOW", str(window)))
        self.max_age_ms = int(os.getenv("ATR_MAX_AGE_MS", "900000"))
        self.bps_min = float(os.getenv("ATR_BPS_MIN_SANITY", "2.0"))
        self.bps_max = float(os.getenv("ATR_BPS_MAX_SANITY", "800.0"))
        self.last_good_ttl_ms = int(os.getenv("ATR_LAST_GOOD_TTL_MS", "1800000"))
        self.jump_max_rel = float(os.getenv("ATR_JUMP_MAX_REL", "1.2"))
        
        # Internal state
        # Keys are "symbol:tf" to support multi-timeframe streams without cross-contamination
        self._hist: Dict[str, Deque[float]] = {}
        self._last_good: Dict[str, float] = {}
        self._last_good_ts: Dict[str, int] = {}
        # per-key jump timestamps (for window count)
        self._jump_ts: Dict[str, Deque[int]] = {}
        self._jump_window_ms = int(os.getenv("ATR_JUMP_WINDOW_SEC", "3600")) * 1000
        # Jump step-change acceptance (prevents 30m lockout when volatility regime shifts)
        self.jump_accept_k = int(os.getenv("ATR_JUMP_ACCEPT_K", "3") or 3)
        self.jump_accept_max_rel = float(os.getenv("ATR_JUMP_ACCEPT_MAX_REL", "10.0") or 10.0)
        self.jump_accept_rebase = os.getenv("ATR_JUMP_ACCEPT_REBASE", "1").strip().lower() in ("1", "true", "yes", "on")
        # per-key jump acceptance state (deduped per tf bucket to avoid counting per-tick)
        self._jump_candidate: Dict[str, Deque[float]] = {}
        self._jump_streak: Dict[str, int] = {}
        self._jump_last_bucket: Dict[str, int] = {}

    def update(self, *, atr: float, px: float, age_ms: int, now_ms: int, symbol: str = "na", tf: str = "1m") -> ATRSanityResult:
        """
        Main entry point for ATR validation.
        Returns ATRSanityResult with bad=1 if check fails.
        """
        # Composite key for isolation: e.g. "BTCUSDT:1m", "BTCUSDT:15m"
        key = f"{symbol}:{tf}"
        atr_bps = (atr / px) * 10000 if px > 0 else 0.0

        bad = 0
        reason = ""
        jump_event = 0
        jump_rel = 0.0
        med_bps = 0.0
        cand_med_bps = 0.0
        jump_streak = int(self._jump_streak.get(key, 0) or 0)
        jump_accept = 0
        jump_bucket_id = 0
        history_preseeded = False

        if atr <= 0:
            bad, reason = 1, "atr<=0"
        elif age_ms > get_max_age_ms_for_tf(tf, self.max_age_ms):
            max_age_for_tf = get_max_age_ms_for_tf(tf, self.max_age_ms)
            bad, reason = 1, f"stale>{max_age_for_tf}:tf={tf}"
        elif atr_bps <= 0 or atr_bps < self.bps_min or atr_bps > self.bps_max:
            bad, reason = 1, f"atr_bps_oob:{atr_bps:.2f}"
        else:
            # jump check vs robust center (median) with step-change acceptance (K buckets)
            buf = self._hist.setdefault(key, deque(maxlen=self.window))
            min_hist = max(10, self.window // 4)
            if len(buf) >= min_hist:
                med_bps = _median(list(buf))
                if med_bps > 0:
                    jump_rel = abs(atr_bps - med_bps) / max(1e-9, med_bps)
                    # De-dupe by TF bucket to avoid counting per-tick within the same bar
                    tf_ms = tf_to_ms(tf, 60_000)
                    jump_bucket_id = int(int(now_ms) // max(1, int(tf_ms)))
                    last_bucket = int(self._jump_last_bucket.get(key, -1) or -1)
                    
                    if jump_rel > self.jump_max_rel:
                        bad, reason = 1, f"jump_rel>{self.jump_max_rel:.3f}:{jump_rel:.3f}:tf={tf}"
                        jump_event = 1
                        # Count only once per bucket; otherwise many ticks would reach K instantly
                        if jump_bucket_id != last_bucket:
                            self._jump_last_bucket[key] = int(jump_bucket_id)
                            jump_streak = int(jump_streak) + 1
                            self._jump_streak[key] = int(jump_streak)
                            cand = self._jump_candidate.setdefault(key, deque(maxlen=max(1, int(self.jump_accept_k))))
                            cand.append(float(atr_bps))
                        else:
                            cand = self._jump_candidate.setdefault(key, deque(maxlen=max(1, int(self.jump_accept_k))))

                        # Step-change acceptance: if jump persists across K buckets, accept the new regime
                        if (int(self.jump_accept_k) > 0
                            and int(jump_streak) >= int(self.jump_accept_k)
                            and len(cand) >= int(self.jump_accept_k)
                            and float(jump_rel) <= float(self.jump_accept_max_rel)):
                            cand_med_bps = _median(list(cand))
                            # Extra safety: candidate median must still be in bps bounds
                            if cand_med_bps >= float(self.bps_min) and cand_med_bps <= float(self.bps_max):
                                jump_accept = 1
                                bad = 0
                                reason = f"jump_step_accept:k={int(self.jump_accept_k)}:{jump_rel:.3f}:tf={tf}"
                                # Rebase history to the new level so future rel is stable
                                if self.jump_accept_rebase:
                                    buf.clear()
                                    seed_n = int(min(self.window, max(10, self.window // 4)))
                                    # Seed with candidate median, then append current observation
                                    for _ in range(max(0, seed_n - 1)):
                                        buf.append(float(cand_med_bps))
                                    buf.append(float(atr_bps))
                                    history_preseeded = True
                                else:
                                    # Non-rebase: just append candidate median to let center move gradually
                                    buf.append(float(cand_med_bps))
                                # reset streak/candidate after acceptance
                                self._jump_streak[key] = 0
                                jump_streak = 0
                                cand.clear()
                    else:
                        # Jump cleared: reset candidate/streak
                        self._jump_streak[key] = 0
                        jump_streak = 0
                        if key in self._jump_candidate:
                            self._jump_candidate[key].clear()

            else:
                # Not enough history for jump check yet: reset candidate/streak
                self._jump_streak[key] = 0
                jump_streak = 0
                if key in self._jump_candidate:
                    self._jump_candidate[key].clear()

        used_last_good = 0
        atr_used = atr
        if bad:
            # last-good fallback
            last_val = self._last_good.get(key)
            last_ts = self._last_good_ts.get(key, 0)
            if last_val is not None and (now_ms - last_ts) < self.last_good_ttl_ms:
                atr_used = last_val
                used_last_good = 1
        else:
            # success -> update last good
            self._last_good[key] = float(atr_used)
            self._last_good_ts[key] = int(now_ms)
            # update history buffer
            buf = self._hist.setdefault(key, deque(maxlen=self.window))
            if not history_preseeded:
                buf.append(float(atr_bps))

        # If ATR is bad for non-jump reasons, reset jump candidates to avoid accidental acceptance later
        if int(bad) == 1 and not str(reason or "").startswith("jump_rel"):
            self._jump_streak[key] = 0
            jump_streak = 0
            if key in self._jump_candidate:
                self._jump_candidate[key].clear()

        # maintain jump window count
        jq = self._jump_ts.setdefault(key, deque())
        if jump_event:
            jq.append(now_ms)
        while jq and (now_ms - jq[0]) > self._jump_window_ms:
            jq.popleft()
        jump_count_window = len(jq)

        return ATRSanityResult(
            atr_used=float(atr_used),
            bad=int(bad),
            reason=reason,
            atr_bps=float(atr_bps),
            used_last_good=int(used_last_good),
            jump_event=int(jump_event),
            jump_count_window=int(jump_count_window),
            jump_rel=float(jump_rel),
            jump_streak=int(jump_streak),
            jump_accept=int(jump_accept),
            med_bps=float(med_bps),
            cand_med_bps=float(cand_med_bps),
            jump_bucket_id=int(jump_bucket_id),
        )
