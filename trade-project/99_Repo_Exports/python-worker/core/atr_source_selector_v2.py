from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        if x is None:
            return d
        return int(float(x))
    except Exception:
        return d


@dataclass
class ATRCandidate:
    tf: str
    src: str
    key: str
    atr: float
    ts_ms: int
    age_ms: int
    atr_bps: float
    score: float
    reason: str


class ATRSourceSelector:
    """
    Periodically selects best ATR source/TF per symbol and writes:
      cfg:atr_tf:{sym}
      cfg:atr_src:{sym}
      cfg:atr_sel_meta:{sym}

    Selection: freshness + sanity + stability + hysteresis (hold-down).
    """

    def __init__(self, r: redis.Redis) -> None:
        self.r = r

        self.enable = os.getenv("ATR_SELECTOR_ENABLE", "0") == "1"
        self.max_age_ms = int(os.getenv("ATR_SELECTOR_MAX_AGE_MS", "900000"))
        self.hold_down_ms = int(os.getenv("ATR_SELECTOR_HOLD_DOWN_MS", "1800000"))
        self.switch_margin = float(os.getenv("ATR_SELECTOR_SWITCH_MARGIN", "0.05"))

        self.atr_bps_min = float(os.getenv("ATR_BPS_MIN_SANITY", "2"))
        self.atr_bps_max = float(os.getenv("ATR_BPS_MAX_SANITY", "800"))
        self.jump_max_rel = float(os.getenv("ATR_JUMP_MAX_REL", "1.2"))

        # Candidate TFs you support in Redis
        self.tfs = [x.strip() for x in os.getenv("ATR_SELECTOR_TFS", "1m,5m,15m").split(",") if x.strip()]

    def _read_hash_candidate(self, sym: str, tf: str, px: float) -> ATRCandidate | None:
        key = f"ATR:{sym}:{tf}"
        h = self.r.hgetall(key) or {}
        if not h:
            return None
        h_dec = {k.decode("utf-8") if isinstance(k, bytes) else k: v for k, v in h.items()}
        atr = _f(h_dec.get("atr"), 0.0)
        ts_ms = _i(h_dec.get("ts_ms"), 0)
        if atr <= 0 or ts_ms <= 0:
            return None
        age = max(0, _now_ms() - ts_ms)
        atr_bps = (atr / px * 10000.0) if px > 0 else 0.0
        return self._score(sym, tf=tf, src="ATR_HASH", key=key, atr=atr, ts_ms=ts_ms, age_ms=age, atr_bps=atr_bps)

    def _read_json_candidate(self, sym: str, tf: str, px: float) -> ATRCandidate | None:
        key = f"atr:json:{sym}:{tf}"
        raw = self.r.get(key)
        if not raw:
            return None
        try:
            d = json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode("utf-8", "ignore"))
        except Exception:
            return None
        atr = _f(d.get("atr", None), 0.0)
        ts_ms = _i(d.get("ts_ms", None), 0)
        if atr <= 0 or ts_ms <= 0:
            return None
        age = max(0, _now_ms() - ts_ms)
        atr_bps = (atr / px * 10000.0) if px > 0 else 0.0
        return self._score(sym, tf=tf, src="atr_json", key=key, atr=atr, ts_ms=ts_ms, age_ms=age, atr_bps=atr_bps)

    def _read_string_candidate(self, sym: str, tf: str, px: float) -> ATRCandidate | None:
        # Example keys: atr:{sym}:{tf} or atr:val:{sym}:{tf}
        for key in (f"atr:{sym}:{tf}", f"atr:val:{sym}:{tf}"):
            raw = self.r.get(key)
            if not raw:
                continue
            atr = _f(raw, 0.0)
            if atr <= 0:
                continue
            # try timestamp side key (optional)
            ts_ms = _i(self.r.get(f"{key}:ts_ms"), 0)
            if ts_ms <= 0:
                # no ts => heavy penalty (still usable as last resort)
                ts_ms = _now_ms()
            age = max(0, _now_ms() - ts_ms)
            atr_bps = (atr / px * 10000.0) if px > 0 else 0.0
            c = self._score(sym, tf=tf, src="atr_string", key=key, atr=atr, ts_ms=ts_ms, age_ms=age, atr_bps=atr_bps)
            c.score -= 0.2  # penalty for weak metadata
            c.reason += "|no_ts_penalty"
            return c
        return None

    def _read_fallback_candidate(self, sym: str, px: float) -> ATRCandidate | None:
        # last known TA output
        key = f"ta:last:atr:{sym}"
        raw = self.r.get(key)
        if not raw:
            return None
        atr = _f(raw, 0.0)
        if atr <= 0:
            return None
        ts_ms = _i(self.r.get(f"ta:last:atr_ts_ms:{sym}"), 0)
        if ts_ms <= 0:
            ts_ms = _now_ms()
        age = max(0, _now_ms() - ts_ms)
        atr_bps = (atr / px * 10000.0) if px > 0 else 0.0
        c = self._score(sym, tf="na", src="ta_last", key=key, atr=atr, ts_ms=ts_ms, age_ms=age, atr_bps=atr_bps)
        c.score -= 0.3
        c.reason += "|fallback_penalty"
        return c

    def _score(
        self,
        sym: str,
        *,
        tf: str,
        src: str,
        key: str,
        atr: float,
        ts_ms: int,
        age_ms: int,
        atr_bps: float,
    ) -> ATRCandidate:
        # freshness: [0..1]
        fresh = max(0.0, 1.0 - (age_ms / max(1.0, float(self.max_age_ms))))
        score = 0.7 * fresh
        reason = f"fresh={fresh:.3f}"

        # sanity
        if atr_bps <= 0.0 or atr_bps < self.atr_bps_min or atr_bps > self.atr_bps_max:
            score -= 10.0
            reason += f"|bps_bad={atr_bps:.2f}"
        else:
            score += 0.2
            reason += f"|bps_ok={atr_bps:.2f}"

        # stability vs last selected bps (stored in meta)
        prev_meta = self._read_sel_meta(sym)
        prev_bps = _f(prev_meta.get("atr_bps", None), 0.0)
        if prev_bps > 0 and atr_bps > 0:
            rel = abs(atr_bps - prev_bps) / max(1e-9, prev_bps)
            if rel > self.jump_max_rel:
                score -= 1.0
                reason += f"|jump_rel={rel:.2f}"
            else:
                score += 0.1
                reason += f"|stable_rel={rel:.2f}"

        return ATRCandidate(tf=tf, src=src, key=key, atr=atr, ts_ms=ts_ms, age_ms=age_ms, atr_bps=atr_bps, score=score, reason=reason)

    def _read_sel_meta(self, sym: str) -> dict[str, Any]:
        raw = self.r.get(f"cfg:atr_sel_meta:{sym}")
        if not raw:
            return {}
        try:
            return json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode("utf-8", "ignore"))
        except Exception:
            return {}

    def _persist_choice(self, sym: str, c: ATRCandidate) -> None:
        prev_meta = self._read_sel_meta(sym)
        meta = {
            "v": 1,
            "symbol": (sym or "").upper(),
            "picked_tf": c.tf,
            "picked_src": c.src,
            "picked_key": c.key,
            "ts_ms": _now_ms(),
            "age_ms": int(c.age_ms),
            "atr": float(c.atr),
            "atr_bps": float(c.atr_bps),
            "score": float(c.score),
            "reason": str(c.reason),
        }
        pipe = self.r.pipeline()
        pipe.set(f"cfg:atr_tf:{sym}", c.tf, ex=6 * 3600)
        pipe.set(f"cfg:atr_src:{sym}", c.src, ex=6 * 3600)
        pipe.set(f"cfg:atr_sel_meta:{sym}", json.dumps(meta, ensure_ascii=False), ex=6 * 3600)
        # count switches in a rolling window (for reporting)
        try:
            prev_tf = (prev_meta.get("picked_tf", "") or "")
            prev_src = (prev_meta.get("picked_src", "") or "")
            if (prev_tf and prev_src) and ((prev_tf != c.tf) or (prev_src != c.src)):
                win = int(os.getenv("ATR_SWITCH_WINDOW_SEC", "3600"))
                pipe.incr(f"cfg:atr_switch_count:{sym}")
                pipe.expire(f"cfg:atr_switch_count:{sym}", win)
                pipe.sadd("cfg:atr_switch:symbols", sym)
                pipe.expire("cfg:atr_switch:symbols", int(os.getenv("ATR_SWITCH_SYMBOLS_SET_TTL_SEC", "86400")))
        except Exception:
            pass
        pipe.execute()

    def select(self, sym: str, *, px: float) -> ATRCandidate | None:
        if not self.enable:
            return None
        if px <= 0:
            return None

        now = _now_ms()
        prev = self._read_sel_meta(sym)
        prev_tf = (prev.get("picked_tf", "") or "")
        prev_src = (prev.get("picked_src", "") or "")
        prev_sw_ms = _i(prev.get("ts_ms", None), 0)

        cands: list[ATRCandidate] = []
        for tf in self.tfs:
            c = self._read_hash_candidate(sym, tf, px)
            if c:
                cands.append(c)
            c = self._read_json_candidate(sym, tf, px)
            if c:
                cands.append(c)
            c = self._read_string_candidate(sym, tf, px)
            if c:
                cands.append(c)

        fb = self._read_fallback_candidate(sym, px)
        if fb:
            cands.append(fb)

        if not cands:
            return None

        # best by score
        cands.sort(key=lambda x: x.score, reverse=True)
        best = cands[0]

        # hysteresis: if prev is close and hold-down active -> keep prev if still acceptable
        if prev_tf and prev_src and prev_sw_ms > 0 and (now - prev_sw_ms) < self.hold_down_ms:
            # find prev candidate if present
            prev_c = next((x for x in cands if x.tf == prev_tf and x.src == prev_src), None)
            if prev_c and prev_c.score >= (best.score - self.switch_margin):
                best = prev_c

        self._persist_choice(sym, best)
        return best

