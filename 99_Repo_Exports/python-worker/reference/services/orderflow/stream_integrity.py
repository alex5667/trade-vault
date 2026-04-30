from __future__ import annotations

"""Stream integrity telemetry (P5).

This module provides *measurable*, non-binary signals about stream health:
  - sequence gaps (missing messages)
  - duplicates / out-of-order delivery
  - schema drift (unexpected field-set change)

Why a dedicated module?
----------------------
The codebase already has several continuity checks (tick_id gaps, book U/u, etc.).
However, those checks were historically used as ad-hoc flags/counters.
For SRE-grade observability and for deterministic gating we need:
  - stable rates (EMA)
  - bounded burst indicators (robust z)
  - explicit max-gap magnitude
  - schema-hash monitoring to detect silent producer changes.

Design constraints
------------------
* Hot-path safe: O(1) per message, no allocations proportional to payload size.
* Fail-open: never raises, callers can ignore results.
* Deterministic: all state evolves only from (seq, ts_ms) observed.

NOTE:
- This tracker is **not** a validator. It does not veto.
- Veto logic should be applied by a separate gate using these metrics.
"""

import hashlib
import math
from dataclasses import dataclass
from typing import Any, Iterable, Tuple

from core.seq_gap_tracker_v1 import GapEmaTracker
from core.robust_stats import RollingRobustZ


def _finite_f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return float(d)
    if not math.isfinite(v):
        return float(d)
    return float(v)


def schema_hash(keys: Iterable[str]) -> str:
    """Stable short hash for a set of keys (schema drift detector)."""
    try:
        s = ",".join(sorted(str(k) for k in keys))
    except Exception:
        s = ""
    h = hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()
    return h[:10]


@dataclass
class IntegritySnapshot:
    last_seq: int = 0
    gap_last: int = 0
    gap_max_window: int = 0
    gap_rate_ema: float = 0.0
    dup_rate_ema: float = 0.0
    dup_burst_z: float = 0.0
    schema_hash: str = ""
    schema_changed: int = 0


class StreamIntegrityTracker:
    """Bounded integrity tracker for a single logical stream."""

    def __init__(
        self
        *
        tau_ms: int = 10_000
        z_window: int = 120
        max_gap_window_ms: int = 60_000
    ) -> None:
        self.gap_ema = GapEmaTracker(tau_ms=int(max(500, tau_ms)))
        # Reuse GapEmaTracker for dup-rate EMA (same math: event {0,1}).
        self.dup_ema = GapEmaTracker(tau_ms=int(max(500, tau_ms)))
        self.dup_rate_stats = RollingRobustZ(window=int(max(32, z_window)))

        self.max_gap_window_ms = int(max(5_000, max_gap_window_ms))

        self.last_seq: int = 0
        self.last_ts_ms: int = 0

        # Gap magnitudes
        self.gap_last: int = 0
        self.gap_max_window: int = 0
        self._gap_window_start_ms: int = 0

        # Duplicate burst aggregation (per-second bucket)
        self._bucket_sec: int = 0
        self._bucket_total: int = 0
        self._bucket_dup: int = 0
        self.dup_burst_z: float = 0.0

        # Schema drift
        self.schema_hash_last: str = ""
        self.schema_changed_last: int = 0

    def update_schema(self, keys: Iterable[str]) -> Tuple[str, int]:
        """Update schema hash. Returns (hash, changed_flag)."""
        h = schema_hash(keys)
        changed = 0
        if self.schema_hash_last and h and h != self.schema_hash_last:
            changed = 1
        if h:
            self.schema_hash_last = h
        self.schema_changed_last = int(changed)
        return self.schema_hash_last, int(changed)

    def update_seq(self, *, seq: int, ts_ms: int) -> IntegritySnapshot:
        """Update with a new observed sequence number."""
        try:
            seq_i = int(seq)
            ts_i = int(ts_ms)
        except Exception:
            return IntegritySnapshot(
                last_seq=int(self.last_seq)
                gap_last=int(self.gap_last)
                gap_max_window=int(self.gap_max_window)
                gap_rate_ema=float(_finite_f(getattr(self.gap_ema, "ema", 0.0)))
                dup_rate_ema=float(_finite_f(getattr(self.dup_ema, "ema", 0.0)))
                dup_burst_z=float(self.dup_burst_z)
                schema_hash=str(self.schema_hash_last)
                schema_changed=int(self.schema_changed_last)
            )

        # Enforce monotone time inside the tracker to keep EMA stable.
        if ts_i <= int(self.last_ts_ms or 0):
            ts_i = int(self.last_ts_ms or 0) + 1
        self.last_ts_ms = int(ts_i)

        # Window reset for max-gap
        if self._gap_window_start_ms <= 0:
            self._gap_window_start_ms = int(ts_i)
        if (ts_i - self._gap_window_start_ms) >= self.max_gap_window_ms:
            self._gap_window_start_ms = int(ts_i)
            self.gap_max_window = 0

        gap = 0
        is_gap = False
        is_dup = False

        if self.last_seq > 0:
            delta = int(seq_i - self.last_seq)
            if delta > 1:
                gap = int(delta - 1)
                is_gap = True
            elif delta == 0:
                is_dup = True
            else:
                # reorder/reset: do not count as gap/dup, but do not regress last_seq
                pass

        self.gap_last = int(gap)
        if gap > self.gap_max_window:
            self.gap_max_window = int(gap)

        # EMA updates
        gap_rate = float(self.gap_ema.update(is_gap=bool(is_gap), ts_ms=int(ts_i)))
        dup_rate = float(self.dup_ema.update(is_gap=bool(is_dup), ts_ms=int(ts_i)))

        # Duplicate burst z (per-second dup ratio)
        sec = int(ts_i // 1000)
        if self._bucket_sec == 0:
            self._bucket_sec = sec
        if sec != self._bucket_sec:
            r = float(self._bucket_dup) / float(max(1, self._bucket_total))
            self.dup_burst_z = float(self.dup_rate_stats.update(r))
            self._bucket_sec = sec
            self._bucket_total = 0
            self._bucket_dup = 0

        self._bucket_total += 1
        if is_dup:
            self._bucket_dup += 1

        # Advance last_seq only on monotone progression.
        if seq_i > self.last_seq:
            self.last_seq = int(seq_i)

        return IntegritySnapshot(
            last_seq=int(self.last_seq)
            gap_last=int(self.gap_last)
            gap_max_window=int(self.gap_max_window)
            gap_rate_ema=float(gap_rate)
            dup_rate_ema=float(dup_rate)
            dup_burst_z=float(self.dup_burst_z)
            schema_hash=str(self.schema_hash_last)
            schema_changed=int(self.schema_changed_last)
        )
