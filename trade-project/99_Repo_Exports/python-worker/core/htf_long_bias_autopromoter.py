from __future__ import annotations

"""htf_long_bias_autopromoter.py

Auto-promote the HTF LONG bias gate from SHADOW → ENFORCE once enough
evidence has accumulated. Pairs with `EntryPolicyGate._eval_htf_long_bias()`
in `handlers/crypto_orderflow/utils/entry_policy_gate.py`.

Promotion criteria (ALL required, fail-safe by construction):
  - ``min_hours``    elapsed since first observation
  - ``min_hits``     bear-LONG events observed in shadow
  - ``min_evals``    total LONG evaluations seen (sanity: rule actually saw traffic)

Scope: GLOBAL by default (single switch for all symbols); set
``per_symbol=True`` to track and promote per-symbol. Once promoted, the
state is sticky — manual rollback is done by deleting the Redis key.

State persistence:
  ``RK.AUTOCAL_HTF_LONG_BIAS`` (HASH) — field ``"global"`` (per-symbol mode
  uses uppercase symbol as field). JSON value via :func:`HtfLongBiasState.to_json` /
  :func:`HtfLongBiasState.from_json`.

Override semantics:
  ENV ``HTF_LONG_BIAS_MODE`` is the **floor** — explicit ``enforce`` skips the
  auto-promoter entirely (force-enforce). Any other value (``shadow``/empty)
  defers to the auto-promoter.
"""

import json
import math
import time
from dataclasses import dataclass, asdict
from typing import Any


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class HtfLongBiasState:
    """Per-scope (global or per-symbol) auto-promoter state."""
    first_eval_ms: int = 0
    last_eval_ms: int = 0
    n_evals: int = 0            # total LONG evaluations the gate saw
    n_hits: int = 0             # bear-LONG events (would-veto in enforce)
    promoted: bool = False
    promoted_ms: int = 0

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))

    @classmethod
    def from_json(cls, raw: Any) -> "HtfLongBiasState | None":
        if raw is None:
            return None
        try:
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", "ignore")
            d = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(d, dict):
                return None
            return cls(
                first_eval_ms=int(d.get("first_eval_ms", 0) or 0),
                last_eval_ms=int(d.get("last_eval_ms", 0) or 0),
                n_evals=int(d.get("n_evals", 0) or 0),
                n_hits=int(d.get("n_hits", 0) or 0),
                promoted=bool(d.get("promoted", False)),
                promoted_ms=int(d.get("promoted_ms", 0) or 0),
            )
        except Exception:
            return None


class HtfLongBiasAutoPromoter:
    """Tracks LONG-evaluation traffic and promotes the gate mode after warmup.

    Stateful in-process (single instance per EntryPolicyGate). Persistence is
    handled by the owning gate (HSET to ``RK.AUTOCAL_HTF_LONG_BIAS`` on
    snapshot, HGETALL on first ``evaluate()``).
    """

    SCOPE_GLOBAL = "global"

    def __init__(
        self,
        *,
        enabled: bool = True,
        min_hours: float = 6.0,
        min_hits: int = 50,
        min_evals: int = 200,
        per_symbol: bool = False,
    ) -> None:
        self.enabled = bool(enabled)
        self.min_hours = float(max(0.0, min_hours))
        self.min_hits = int(max(0, min_hits))
        self.min_evals = int(max(0, min_evals))
        self.per_symbol = bool(per_symbol)
        self._states: dict[str, HtfLongBiasState] = {}

    # ── scope helper ─────────────────────────────────────────────────────────
    def _scope(self, symbol: str | None) -> str:
        if not self.per_symbol:
            return self.SCOPE_GLOBAL
        s = (symbol or "").strip().upper()
        return s or self.SCOPE_GLOBAL

    def _get(self, scope: str) -> HtfLongBiasState:
        st = self._states.get(scope)
        if st is None:
            st = HtfLongBiasState()
            self._states[scope] = st
        return st

    # ── observe ──────────────────────────────────────────────────────────────
    def observe(self, *, symbol: str | None, hit: bool, now_ms: int | None = None) -> None:
        """Record one LONG evaluation. ``hit=True`` if the bear-LONG rule fired."""
        if not self.enabled:
            return
        ts = int(now_ms if now_ms is not None else _now_ms())
        scope = self._scope(symbol)
        st = self._get(scope)
        if st.first_eval_ms == 0:
            st.first_eval_ms = ts
        st.last_eval_ms = ts
        st.n_evals += 1
        if hit:
            st.n_hits += 1
        if not st.promoted and self._ready(st, ts):
            st.promoted = True
            st.promoted_ms = ts

    # ── mode resolution ──────────────────────────────────────────────────────
    def is_promoted(self, *, symbol: str | None, now_ms: int | None = None) -> bool:
        """Return True once promotion criteria are met for the given scope."""
        if not self.enabled:
            return False
        scope = self._scope(symbol)
        st = self._states.get(scope)
        if st is None:
            return False
        if st.promoted:
            return True
        return self._ready(st, int(now_ms if now_ms is not None else _now_ms()))

    def _ready(self, st: HtfLongBiasState, now_ms: int) -> bool:
        if st.first_eval_ms <= 0:
            return False
        elapsed_h = (now_ms - st.first_eval_ms) / (3600.0 * 1000.0)
        if not math.isfinite(elapsed_h):
            return False
        return (
            elapsed_h >= self.min_hours
            and st.n_evals >= self.min_evals
            and st.n_hits >= self.min_hits
        )

    # ── persistence ──────────────────────────────────────────────────────────
    def dump_all(self) -> dict[str, str]:
        """Return a mapping {scope: state_json} suitable for HSET mapping=."""
        return {scope: st.to_json() for scope, st in self._states.items()}

    def load_mapping(self, raw_map: dict[Any, Any]) -> None:
        """Restore states from a HGETALL result (bytes keys/values tolerated)."""
        if not isinstance(raw_map, dict):
            return
        for k, v in raw_map.items():
            try:
                key = k.decode("utf-8", "ignore") if isinstance(k, (bytes, bytearray)) else str(k)
            except Exception:
                continue
            st = HtfLongBiasState.from_json(v)
            if st is not None:
                self._states[key] = st

    # ── introspection (tests + metrics) ──────────────────────────────────────
    def snapshot(self, scope: str | None = None) -> HtfLongBiasState | None:
        return self._states.get(scope or self.SCOPE_GLOBAL)
