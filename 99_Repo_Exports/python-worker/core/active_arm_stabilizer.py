from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ActiveArmStabilizer:
    """
    Stabilizes active_arm reads to prevent flapping:
      - hold_down_ms: raw must be stable for this long before switching effective
      - min_switch_gap_ms: after effective switch, forbid switching too soon

    Key is arbitrary (we use f"{sym}:{regime}:{group}:{scenario}").
    """
    hold_down_ms: int = 10 * 60_000
    min_switch_gap_ms: int = 30 * 60_000

    _eff: dict[str, str] = None
    _eff_ts: dict[str, int] = None
    _cand: dict[str, str] = None
    _cand_ts: dict[str, int] = None

    def __post_init__(self) -> None:
        if self._eff is None:
            self._eff = {}
        if self._eff_ts is None:
            self._eff_ts = {}
        if self._cand is None:
            self._cand = {}
        if self._cand_ts is None:
            self._cand_ts = {}

    def update(self, *, key: str, raw: str, now_ms: int) -> str:
        k = (key or "").strip()
        rv = (raw or "").strip()
        if not k:
            return rv

        eff = str(self._eff.get(k, "") or "")
        if not eff:
            # first-time accept raw immediately (fail-open)
            self._eff[k] = rv
            self._eff_ts[k] = now_ms
            self._cand[k] = rv
            self._cand_ts[k] = now_ms
            return rv

        if rv == eff:
            # refresh candidate to current value
            self._cand[k] = rv
            self._cand_ts[k] = now_ms
            return eff

        # min switch gap
        last_sw = int(self._eff_ts.get(k, 0) or 0)
        if last_sw > 0 and (now_ms - last_sw) < int(self.min_switch_gap_ms):
            # stage candidate, but do not switch
            if str(self._cand.get(k, "") or "") != rv:
                self._cand[k] = rv
                self._cand_ts[k] = now_ms
            return eff

        cand = str(self._cand.get(k, "") or "")
        cand_ts = int(self._cand_ts.get(k, 0) or 0)
        if cand != rv:
            self._cand[k] = rv
            self._cand_ts[k] = now_ms
            return eff

        # cand == rv: check hold-down
        if cand_ts > 0 and (now_ms - cand_ts) >= int(self.hold_down_ms):
            self._eff[k] = rv
            self._eff_ts[k] = now_ms
            return rv

        return eff

    def snapshot(self, key: str) -> dict:
        k = (key or "").strip()
        return {
            "eff": str(self._eff.get(k, "") or ""),
            "eff_ts": int(self._eff_ts.get(k, 0) or 0),
            "cand": str(self._cand.get(k, "") or ""),
            "cand_ts": int(self._cand_ts.get(k, 0) or 0),
            "hold_down_ms": int(self.hold_down_ms),
            "min_switch_gap_ms": int(self.min_switch_gap_ms),
        }
