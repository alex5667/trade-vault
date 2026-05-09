from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class P2Quantile:
    """
    Streaming P² quantile estimator (Jain & Chlamtac).
    Stores only 5 markers -> small persistent state.
    Matches expert specification with to_state/from_state.
    """
    p: float
    _count: int = 0
    _init: list[float] = field(default_factory=list)
    _n: list[int] = field(default_factory=lambda: [0]*5)
    _np: list[float] = field(default_factory=lambda: [0.0]*5)
    _dn: list[float] = field(default_factory=lambda: [0.0]*5)
    _q: list[float] = field(default_factory=lambda: [0.0]*5)

    def ready(self) -> bool:
        return self._count >= 5

    def update(self, x: float) -> None:
        try:
            val = float(x)
        except (ValueError, TypeError):
            return

        if self._count < 5:
            self._init.append(val)
            self._count += 1
            if self._count == 5:
                # Initialization
                self._init.sort()
                self._q = list(self._init)  # q0..q4 (heights)
                self._n = [0, 1, 2, 3, 4]   # positions (0-based in Logic?)
                # Actually, standard algorithm uses 1-based or 0-based.
                # Let's infer from standard implementation or user snippet.
                # User snippet didn't provide update(), only value/to_state/from_state.
                # I must provide a CORRECT update() that matches the state structure.

                # Standard P2 initialization (0-based indexing for arrays):
                # Markers 0..4
                self._n = [0, 1, 2, 3, 4]

                # Desired positions
                # n'_0 = 0
                # n'_1 = 2p
                # n'_2 = 4p
                # n'_3 = 2 + 2p
                # n'_4 = 4
                # (Scaled to N-1=4)

                # Let's use the logic I had before but adapted to these variable names.
                # My previous implementation used 1-based NPOS.
                # Let's stick to my robust implementation but RENAME fields to match to_state expectation.

                # MAPPING:
                # q -> _q
                # npos -> _n (integers?) User's to_state says "n": list(self._n).
                # Standard P2 uses integers for actual positions.
                # np -> _np (floats, desired positions)
                # dn -> _dn (floats, increments)

                self._n = [0, 1, 2, 3, 4]
                p = self.p
                self._np = [0.0, 2.0*p, 4.0*p, 2.0 + 2.0*p, 4.0]
                self._dn = [0.0, p/2.0, p, (1.0+p)/2.0, 1.0]
            return

        self._count += 1

        # 1. Find cell k
        if val < self._q[0]:
            self._q[0] = val
            k = 0
        elif val >= self._q[4]:
            self._q[4] = val
            k = 3
        else:
            k = 3 # default
            for i in range(4):
                if self._q[i] <= val < self._q[i+1]:
                    k = i
                    break

        # 2. Increment positions
        # For actual positions _n[i], all i > k increment by 1
        for i in range(5):
            if i > k: # logic: if strictly greater? Standard says "for i=k+1 to 5"
                 self._n[i] += 1
            self._np[i] += self._dn[i]

        # 3. Adjust markers
        for i in range(1, 4):
            d = self._np[i] - self._n[i]
            if (d >= 1.0 and (self._n[i+1] - self._n[i] > 1)) or \
               (d <= -1.0 and (self._n[i] - self._n[i-1] > 1)):
                d_int = 1 if d > 0 else -1

                # Parabolic
                qp = self._parabolic(i, d_int)
                if self._q[i-1] < qp < self._q[i+1]:
                    self._q[i] = qp
                else:
                    self._q[i] = self._linear(i, d_int)

                self._n[i] += d_int

    def _parabolic(self, i: int, d: int) -> float:
        # q[i] + d / (n[i+1]-n[i-1]) * ...
        q = self._q
        n = self._n
        return q[i] + d / (n[i+1] - n[i-1]) * (
            (n[i] - n[i-1] + d) * (q[i+1] - q[i]) / (n[i+1] - n[i]) +
            (n[i+1] - n[i] - d) * (q[i] - q[i-1]) / (n[i] - n[i-1])
        )

    def _linear(self, i: int, d: int) -> float:
        q = self._q
        n = self._n
        return q[i] + d * (q[i+d] - q[i]) / (n[i+d] - n[i])

    def value(self) -> float | None:
        if self._count < 5:
            if not self._init:
                return None
            s = sorted(self._init)
            k = int(round((len(s) - 1) * self.p))
            return float(s[max(0, min(len(s) - 1, k))])
        return float(self._q[2])

    def to_state(self) -> dict:
        """
        JSON-serializable internal state for persistence.
        """
        return {
            "p": float(self.p),
            "count": int(self._count),
            "init": list(self._init) if self._init is not None else [],
            "n": list(self._n) if self._n is not None else [0, 0, 0, 0, 0],
            "np": list(self._np) if self._np is not None else [0.0] * 5,
            "dn": list(self._dn) if self._dn is not None else [0.0] * 5,
            "q": list(self._q) if self._q is not None else [0.0] * 5,
        }

    @staticmethod
    def from_state(state: dict) -> P2Quantile:
        """
        Restore P2Quantile from to_state().
        Fail-open: if anything looks wrong, returns a fresh estimator.
        """
        try:
            p = float(state.get("p", 0.5))
            obj = P2Quantile(p=p)
            obj._count = int(state.get("count", 0))
            obj._init = list(state.get("init", []))[:5]
            obj._n = [int(x) for x in list(state.get("n", [0, 0, 0, 0, 0]))][:5]
            obj._np = [float(x) for x in list(state.get("np", [0.0] * 5))][:5]
            obj._dn = [float(x) for x in list(state.get("dn", [0.0] * 5))][:5]
            obj._q = [float(x) for x in list(state.get("q", [0.0] * 5))][:5]
            return obj
        except Exception:
            return P2Quantile(p=float(state.get("p", 0.5) or 0.5))
