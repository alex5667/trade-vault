from __future__ import annotations

import hashlib
from dataclasses import dataclass


def _stable_u01(s: str) -> float:
    """Deterministic u in [0,1). Use first 8 bytes of sha1.
    
    Args:
        s: Input string
        
    Returns:
        Float in [0, 1) range
    """
    h = hashlib.sha1(s.encode("utf-8")).digest()
    x = int.from_bytes(h[:8], "big", signed=False)
    return (x % (10**12)) / float(10**12)

@dataclass(frozen=True)
class CanaryPolicy:
    """Deterministic canary routing policy.
    
    enforce_share: fraction in [0,1] for hash-based routing
    enforce_symbols: explicit symbol allowlist (always ENFORCE)
    sample_key_mode: "sid" (default) or "symbol_ts" for timebucket-based routing
    timebucket_sec: bucket size for symbol_ts mode (default 60)
    """
    enforce_share: float = 1.0
    enforce_symbols: set[str] | None = None
    sample_key_mode: str = "sid"  # sid|symbol_ts
    timebucket_sec: int = 60

    def should_enforce(self, *, sid: str, symbol: str, ts_ms: int) -> bool:
        """Deterministic enforce decision.
        
        Priority:
        1. If symbol in enforce_symbols -> always True
        2. If enforce_share >= 1.0 -> always True
        3. If enforce_share <= 0.0 -> always False
        4. Otherwise: hash-based routing on sample_key
        
        Args:
            sid: Signal ID (used for sid mode)
            symbol: Symbol name (used for symbol_ts mode)
            ts_ms: Timestamp in milliseconds
            
        Returns:
            True if should enforce, False for shadow
        """
        sym = (symbol or "").upper()
        if self.enforce_symbols and sym in self.enforce_symbols:
            return True

        p = max(0.0, min(1.0, float(self.enforce_share)))
        if p <= 0.0:
            return False
        if p >= 1.0:
            return True

        if self.sample_key_mode == "symbol_ts":
            tb = (int(ts_ms) // 1000) // max(1, int(self.timebucket_sec))
            key = f"{sym}|{tb}"
        else:
            key = sid or f"{sym}|{ts_ms}"

        return _stable_u01(key) < p

def parse_symbol_set(csv: str) -> set[str]:
    """Parse comma-separated symbol list into set.
    
    Args:
        csv: Comma-separated string like "BTCUSDT,ETHUSDT"
        
    Returns:
        Set of uppercase symbols
    """
    out: set[str] = set()
    for x in (csv or "").split(","):
        x = x.strip().upper()
        if x:
            out.add(x)
    return out

