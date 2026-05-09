import math
from dataclasses import dataclass
from typing import Any


@dataclass
class ATRTfChoice:
    """
    Deterministic ATR TF + source selection snapshot.
    - tf: timeframe string (e.g. '1m','5m','15m') as requested by caller
    - src/key/ts_ms/age_ms: from ATRCache.get_with_meta
    - atr: absolute ATR value (price units)
    - atr_bps: atr/price*10000 (requires price at decision time)
    - score: higher is better
    """
    tf: str
    src: str
    key: str
    ts_ms: int
    age_ms: int
    atr: float
    atr_bps: float
    score: float
    reason: str


class ATRTfCalibrator:
    """
    Selects best ATR timeframe/source by:
      - freshness (age_ms)
      - consistency (penalize huge jumps vs previous)
      - sanity range in atr_bps
      - tf-tag match (ta:last may be M1 while candidate is 15m)

    Persistence is handled by runtime/service (dump/load helpers).
    """

    def __init__(
        self,
        *,
        candidates: list[str],
        max_age_ms_by_tf: dict[str, int] | None = None,
        min_atr_bps: float = 0.10,
        max_atr_bps: float = 500.0,
        max_jump_mult: float = 4.0,
    ) -> None:
        self.candidates = [str(x).strip() for x in (candidates or []) if str(x).strip()]
        if not self.candidates:
            self.candidates = ["1m", "5m", "15m"]
        self.max_age_ms_by_tf = dict(max_age_ms_by_tf or {})
        self.min_atr_bps = float(min_atr_bps)
        self.max_atr_bps = float(max_atr_bps)
        self.max_jump_mult = float(max_jump_mult)
        # previous chosen atr_bps per symbol to detect nonsense jumps
        self._prev_bps: dict[str, float] = {}
        self._prev_tf: dict[str, str] = {}

    @staticmethod
    def _freshness_score(age_ms: int, max_age_ms: int) -> float:
        if age_ms <= 0:
            # no timestamp => treat as weak source
            return 0.15
        if max_age_ms <= 0:
            max_age_ms = 120_000
        # linear decay to 0 at max_age_ms
        x = 1.0 - (float(age_ms) / float(max_age_ms))
        return max(0.0, min(1.0, x))

    def choose(
        self,
        *,
        symbol: str,
        price: float,
        now_ms: int,
        atr_cache: Any,
    ) -> ATRTfChoice | None:
        sym = (symbol or "").upper()
        px = float(price or 0.0)
        if px <= 0:
            return None

        best: ATRTfChoice | None = None
        prev_bps = float(self._prev_bps.get(sym, 0.0) or 0.0)
        prev_tf = str(self._prev_tf.get(sym, "") or "")

        for tf in self.candidates:
            try:
                atr_v, meta = atr_cache.get_with_meta(symbol=sym, timeframe=tf, now_ms=int(now_ms))
            except Exception:
                continue
            if atr_v is None:
                continue
            try:
                atr = float(atr_v or 0.0)
            except Exception:
                continue
            if not (math.isfinite(atr) and atr > 0):
                continue

            src = str((meta or {}).get("src", "none") or "none")
            key = str((meta or {}).get("key", "") or "")
            ts_ms = int((meta or {}).get("ts_ms", 0) or 0)
            age_ms = int((meta or {}).get("age_ms", 0) or 0)
            tf_tag = str((meta or {}).get("tf_tag", "") or "").strip().upper()

            atr_bps = 10000.0 * (atr / px) if px > 0 else 0.0
            if not (math.isfinite(atr_bps) and atr_bps > 0):
                continue

            # Sanity range (hard)
            if atr_bps < self.min_atr_bps or atr_bps > self.max_atr_bps:
                continue

            # Freshness per TF (default heuristics)
            max_age = int(self.max_age_ms_by_tf.get(tf, 0) or 0)
            if max_age <= 0:
                # typical expectations: 1m <= 2m, 5m <= 10m, 15m <= 30m
                tfl = tf.lower()
                if tfl in ("1m", "m1"):
                    max_age = 120_000
                elif tfl in ("3m", "m3"):
                    max_age = 240_000
                elif tfl in ("5m", "m5"):
                    max_age = 600_000
                elif tfl in ("15m", "m15"):
                    max_age = 1_800_000
                else:
                    max_age = 600_000

            fresh = self._freshness_score(age_ms, max_age)

            # tf-tag mismatch penalty for ta:last
            tf_pen = 1.0
            if src == "ta_last" and tf_tag:
                # tf_tag examples: M1, M5...
                want = tf.strip().upper()
                # accept "1m" vs "M1"
                want_norm = want.replace("1M", "M1").replace("5M", "M5").replace("15M", "M15")
                if (tf_tag != want_norm) and (tf_tag != want):
                    tf_pen = 0.35

            # jump penalty vs previous chosen bps
            jump_pen = 1.0
            if prev_bps > 0:
                r = max(atr_bps, prev_bps) / max(1e-9, min(atr_bps, prev_bps))
                if r >= self.max_jump_mult:
                    jump_pen = 0.25

            # mild stickiness: keep previous tf unless clearly worse
            sticky = 1.0
            if prev_tf and tf == prev_tf:
                sticky = 1.05

            score = float(fresh * tf_pen * jump_pen * sticky)
            reason = f"fresh={fresh:.2f} tf_pen={tf_pen:.2f} jump_pen={jump_pen:.2f} sticky={sticky:.2f}"

            choice = ATRTfChoice(
                tf=tf,
                src=src,
                key=key,
                ts_ms=ts_ms,
                age_ms=age_ms,
                atr=atr,
                atr_bps=float(atr_bps),
                score=float(score),
                reason=reason,
            )
            if best is None or choice.score > best.score:
                best = choice

        if best is not None:
            self._prev_bps[sym] = float(best.atr_bps)
            self._prev_tf[sym] = str(best.tf)
        return best

    # ---------------- persistence helpers ----------------
    @staticmethod
    def dump_choice(*, symbol: str, choice: ATRTfChoice, updated_ts_ms: int) -> dict[str, Any]:
        return {
            "v": 1,
            "symbol": symbol.upper(),
            "updated_ts_ms": int(updated_ts_ms),
            "tf": str(choice.tf),
            "src": str(choice.src),
            "key": str(choice.key),
            "ts_ms": int(choice.ts_ms),
            "age_ms": int(choice.age_ms),
            "atr": float(choice.atr),
            "atr_bps": float(choice.atr_bps),
            "score": float(choice.score),
        }

    @staticmethod
    def load_choice(state: dict[str, Any]) -> ATRTfChoice | None:
        try:
            if not isinstance(state, dict):
                return None
            tf = (state.get("tf") or "").strip()
            if not tf:
                return None
            return ATRTfChoice(
                tf=tf,
                src=(state.get("src") or "none"),
                key=(state.get("key") or ""),
                ts_ms=int(state.get("ts_ms", 0) or 0),
                age_ms=int(state.get("age_ms", 0) or 0),
                atr=float(state.get("atr", 0.0) or 0.0),
                atr_bps=float(state.get("atr_bps", 0.0) or 0.0),
                score=float(state.get("score", 0.0) or 0.0),
                reason="loaded",
            )
        except Exception:
            return None
