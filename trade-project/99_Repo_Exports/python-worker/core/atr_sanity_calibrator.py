"""
ATR Sanity Calibrator (Source Selector)

Problem we solve:
  - Multiple ATR producers / legacy keys exist: ATR:{sym}:{TF}, atr:{sym}:{tf}, atr:json..., ta:last:atr:{sym}
  - Sources can disagree (double workers, different baselines, stale keys).
  - Execution needs a deterministic choice per symbol and requested TF.

Solution:
  - On bar_close (deterministic ts_ms), we sample all ATR candidates.
  - Rank them by:
      1) freshness (age_ms, prefer timestamped sources)
      2) TF match (requested normalized TF, e.g. M1)
      3) consistency (closest to median candidates)
  - Persist chosen preference to Redis (calib:atrbps:src:{SYM}:{TF})
  - Expose ready flag after min_samples.

Fail-open:
  - if no candidates -> no preference, execution falls back to existing behavior.
"""

import math
from dataclasses import dataclass
from typing import Any


@dataclass
class ATRCandidate:
    atr: float
    src: str
    key: str
    tf: str
    ts_ms: int
    age_ms: int
    has_ts: int


@dataclass
class ATRSanityDecision:
    ok: bool
    src_pref: str
    reason: str
    n: int
    mismatch: int
    median: float
    picked: float


class ATRSanityCalibrator:
    def __init__(self, *, min_samples: int = 50, max_age_ms: int = 120_000) -> None:
        self.min_samples = int(min_samples)
        self.max_age_ms = int(max_age_ms)
        # counters
        self._n: dict[str, int] = {}          # key: tf_norm
        self._src_pref: dict[str, str] = {}   # key: tf_norm -> src
        self._mismatch: dict[str, int] = {}   # key: tf_norm -> 0/1

    @staticmethod
    def _median(xs: list[float]) -> float:
        ys = [float(x) for x in xs if math.isfinite(float(x)) and float(x) > 0]
        if not ys:
            return 0.0
        ys.sort()
        return float(ys[len(ys) // 2])

    def decide(self, *, tf_norm: str, candidates: list[dict[str, Any]]) -> ATRSanityDecision:
        """
        candidates: list of dicts from ATRCache.get_candidates()
        Required keys per candidate:
          - atr, src, key, tf, ts_ms, age_ms, has_ts
        """
        tfk = (tf_norm or "M1").upper()
        cs: list[ATRCandidate] = []
        for c in candidates or []:
            try:
                atr = float(c.get("atr", 0.0) or 0.0)
                if not (math.isfinite(atr) and atr > 0):
                    continue
                cs.append(
                    ATRCandidate(
                        atr=atr,
                        src=(c.get("src", "na") or "na"),
                        key=(c.get("key", "") or ""),
                        tf=(c.get("tf", "") or ""),
                        ts_ms=int(c.get("ts_ms", 0) or 0),
                        age_ms=int(c.get("age_ms", 0) or 0),
                        has_ts=int(c.get("has_ts", 0) or 0),
                    )
                )
            except Exception:
                continue

        if not cs:
            return ATRSanityDecision(ok=False, src_pref="", reason="no_candidates", n=int(self._n.get(tfk, 0)), mismatch=0, median=0.0, picked=0.0)

        # Filter by age if timestamped.
        fresh: list[ATRCandidate] = []
        for c in cs:
            if c.has_ts == 1 and c.age_ms > 0 and c.age_ms <= self.max_age_ms:
                fresh.append(c)
        # If no fresh timestamped candidates, keep all (fail-open, but lower confidence).
        pool = fresh if fresh else cs

        med = self._median([c.atr for c in pool])
        mismatch = 0
        try:
            # mismatch if spread is too wide vs median (relative)
            if med > 0:
                mx = max([c.atr for c in pool])
                mn = min([c.atr for c in pool])
                rel = (mx - mn) / max(1e-12, med)
                mismatch = 1 if rel >= 0.35 else 0
        except Exception:
            mismatch = 0

        # Score:
        #   +2.0 if has_ts
        #   +2.0 if tf matches requested tf_norm
        #   +freshness: exp(-age/scale) in [0..1]
        #   +consistency: 1 - |atr-med|/med (clamped)
        best = None
        best_s = -1e9
        for c in pool:
            s = 0.0
            s += 2.0 if c.has_ts == 1 else 0.0
            s += 2.0 if str(c.tf or "").upper() == tfk else 0.0
            if c.has_ts == 1 and c.age_ms > 0:
                scale = float(max(10_000, self.max_age_ms // 3))
                s += float(math.exp(-float(c.age_ms) / scale))
            if med > 0:
                s += max(0.0, 1.0 - abs(c.atr - med) / max(1e-12, med))
            if s > best_s:
                best_s = s
                best = c

        if best is None:
            return ATRSanityDecision(ok=False, src_pref="", reason="no_pick", n=int(self._n.get(tfk, 0)), mismatch=mismatch, median=med, picked=0.0)

        # Update Counters
        self._n[tfk] = int(self._n.get(tfk, 0) + 1)
        self._src_pref[tfk] = str(best.src)
        self._mismatch[tfk] = int(mismatch)

        n = int(self._n.get(tfk, 0))
        ok = n >= self.min_samples
        reason = "picked_fresh" if fresh else "picked_best_effort"
        return ATRSanityDecision(ok=ok, src_pref=str(best.src), reason=reason, n=n, mismatch=mismatch, median=med, picked=float(best.atr))

    # ---------------- Persistence ----------------
    def dump_state(self, *, symbol: str, tf_norm: str, updated_ts_ms: int) -> dict[str, Any]:
        tfk = (tf_norm or "M1").upper()
        return {
            "v": 1,
            "symbol": symbol,
            "tf": tfk,
            "updated_ts_ms": int(updated_ts_ms),
            "n": int(self._n.get(tfk, 0)),
            "src_pref": str(self._src_pref.get(tfk, "")),
            "mismatch": int(self._mismatch.get(tfk, 0)),
        }

    def load_state(self, st: dict[str, Any]) -> None:
        try:
            tfk = (st.get("tf") or "M1").upper()
            self._n[tfk] = int(st.get("n", 0) or 0)
            self._src_pref[tfk] = (st.get("src_pref", "") or "")
            self._mismatch[tfk] = int(st.get("mismatch", 0) or 0)
        except Exception:
            return
