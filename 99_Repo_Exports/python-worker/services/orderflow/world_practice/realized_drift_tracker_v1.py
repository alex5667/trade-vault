from __future__ import annotations

"""World-practice: Adverse Selection via Realized Drift (v1).

Tracks post-signal realized drift (bps) at a fixed horizon. Provides:
- EW mean drift (bps)
- EW sigma drift (bps)
- EW adverse share
- VETO bit (derived)

Design goals:
- deterministic time: uses tick ts_ms only
- cheap: update() O(k) for due evaluations, k typically small
- low cardinality output: sym × exec_regime_bucket
"""

import math
from dataclasses import dataclass
from typing import Deque, Dict, Tuple
from collections import deque


def _dir_sign(direction: str) -> float:
    d = (direction or "").strip().upper()
    return 1.0 if d == "LONG" else -1.0


@dataclass
class _BucketState:
    n: int = 0
    mean_bps: float = 0.0
    var_bps2: float = 0.0
    bad_share: float = 0.0


class RealizedDriftTrackerV1:
    def __init__(
        self
        *
        horizon_ms: int = 120_000
        alpha: float = 0.03
        min_n: int = 40
        mean_th_bps: float = 0.8
        bad_share_th: float = 0.60
        z_th: float = 1.5
        sigma_floor_bps: float = 0.05
        max_pending: int = 4096
    ) -> None:
        self.horizon_ms = int(max(1, horizon_ms))
        self.alpha = float(min(max(alpha, 1e-6), 1.0))

        self.min_n = int(max(1, min_n))
        self.mean_th_bps = float(max(0.0, mean_th_bps))
        self.bad_share_th = float(min(max(bad_share_th, 0.0), 1.0))
        self.z_th = float(max(0.0, z_th))
        self.sigma_floor_bps = float(max(1e-9, sigma_floor_bps))
        self.max_pending = int(max(8, max_pending))

        # Each pending item: (due_ts_ms, direction, px0, bucket)
        self._pending: Deque[Tuple[int, str, float, str]] = deque()
        self._st: Dict[str, _BucketState] = {}

    def on_signal(self, *, ts_ms: int, direction: str, px0: float, bucket: str) -> None:
        """Arm a realized drift evaluation at ts_ms + horizon."""
        try:
            due = int(ts_ms) + int(self.horizon_ms)
            px0f = float(px0)
            if not math.isfinite(px0f) or px0f <= 0:
                return
            b = str(bucket or "NORMAL")
            d = str(direction or "").upper()
            if d not in ("LONG", "SHORT"):
                return

            # backpressure: cap pending queue
            if len(self._pending) >= self.max_pending:
                self._pending.popleft()

            self._pending.append((due, d, px0f, b))
        except Exception:
            return

    def update(self, *, ts_ms: int, px_now: float) -> Dict[str, int]:
        """Process due evaluations. Returns dict bucket->n_processed."""
        out: Dict[str, int] = {}

        try:
            now = int(ts_ms)
            px = float(px_now)
            if (not math.isfinite(px)) or px <= 0:
                return out

            while self._pending and self._pending[0][0] <= now:
                due, direction, px0, bucket = self._pending.popleft()

                sign = _dir_sign(direction)
                r_bps = sign * ((px - px0) / px0) * 10_000.0
                if not math.isfinite(r_bps):
                    continue

                st = self._st.get(bucket)
                if st is None:
                    st = _BucketState()
                    self._st[bucket] = st

                a = self.alpha
                old_mean = st.mean_bps
                new_mean = (1.0 - a) * old_mean + a * r_bps

                # EW variance update (stable form)
                dv = (r_bps - old_mean) * (r_bps - new_mean)
                new_var = (1.0 - a) * st.var_bps2 + a * dv
                if not math.isfinite(new_var) or new_var < 0.0:
                    new_var = 0.0

                # Bad if drift is adverse beyond mean threshold
                is_bad = 1.0 if (r_bps < -self.mean_th_bps) else 0.0
                new_bad = (1.0 - a) * st.bad_share + a * is_bad

                st.n += 1
                st.mean_bps = float(new_mean)
                st.var_bps2 = float(new_var)
                st.bad_share = float(new_bad)

                out[bucket] = int(out.get(bucket, 0) + 1)

        except Exception:
            return out

        return out

    def snapshot(self, bucket: str) -> Dict[str, float]:
        """Return current stats for bucket (safe defaults if none)."""
        b = str(bucket or "NORMAL")
        st = self._st.get(b)
        if st is None:
            return {
                "adverse_rd_mean_bps": 0.0
                "adverse_rd_sigma_bps": float(self.sigma_floor_bps)
                "adverse_rd_z": 0.0
                "adverse_rd_bad_share": 0.0
                "adverse_rd_n": 0.0
                "adverse_rd_veto": 0.0
            }

        sigma = math.sqrt(max(float(st.var_bps2), 0.0))
        if (not math.isfinite(sigma)) or sigma < float(self.sigma_floor_bps):
            sigma = float(self.sigma_floor_bps)

        mean = float(st.mean_bps)
        z = mean / sigma if sigma > 0 else 0.0
        if not math.isfinite(z):
            z = 0.0

        bad = float(st.bad_share)
        n = float(st.n)

        veto = 0.0
        if n >= float(self.min_n):
            if (mean < -float(self.mean_th_bps)) and (bad >= float(self.bad_share_th)) and (z < -float(self.z_th)):
                veto = 1.0

        return {
            "adverse_rd_mean_bps": mean
            "adverse_rd_sigma_bps": float(sigma)
            "adverse_rd_z": float(z)
            "adverse_rd_bad_share": bad
            "adverse_rd_n": n
            "adverse_rd_veto": veto
        }
