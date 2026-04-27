from __future__ import annotations

import json
import os
import time
from typing import Dict, Any, Tuple

# Process-level shared cache for guard states to prevent redundant I/O across engine instances.
_SHARED_GUARD_CACHE: Dict[str, Dict[str, Any]] = {}
_SHARED_GUARD_STATS: Dict[str, Tuple[float, float]] = {} # path -> (mtime, last_check_ts)

class MetaFreezeFile:
    """Reliable parser for META_FREEZE_FILE with TTL caching and fail-open logic."""
    
    def __init__(self, path: str, ttl_sec: int = 5) -> None:
        self.path = str(path or "").strip()
        self.ttl_sec = int(ttl_sec)
        self._default = {
            "freeze": 0,
            "ab_share_cap": 1.0,
            "enforce_share_cap": 1.0,
            "comment": "fallback_default"
        }

    def get_guard_state(self) -> Dict[str, Any]:
        """Retrieve guard state from file or cache (fail-open)."""
        if not self.path:
            return self._default
            
        now = time.time()
        
        # 1. Check process-level cache
        stats = _SHARED_GUARD_STATS.get(self.path)
        if stats and (now - stats[1] < self.ttl_sec):
            return _SHARED_GUARD_CACHE.get(self.path, self._default)
            
        # 2. Try to load from disk
        try:
            if not os.path.exists(self.path):
                # If file disappeared, we keep using old cache for stability until TTL
                return _SHARED_GUARD_CACHE.get(self.path, self._default)
                
            mtime = os.path.getmtime(self.path)
            
            # If mtime hasn't changed, just update last_check_ts and return cache
            if stats and mtime == stats[0] and self.path in _SHARED_GUARD_CACHE:
                _SHARED_GUARD_STATS[self.path] = (mtime, now)
                return _SHARED_GUARD_CACHE[self.path]
                
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    raise ValueError("Invalid guard file format: must be a JSON object")
                
                # Normalize and ensure essential keys exist
                state = {
                    "freeze": int(data.get("freeze", 0)),
                    "ab_share_cap": float(data.get("ab_share_cap", 1.0)),
                    "enforce_share_cap": float(data.get("enforce_share_cap", 1.0)),
                    "comment": str(data.get("comment", "ok"))
                }
                
                _SHARED_GUARD_CACHE[self.path] = state
                _SHARED_GUARD_STATS[self.path] = (mtime, now)
                return state
                
        except Exception:
            # Fail-open: return last known good state from cache or default
            return _SHARED_GUARD_CACHE.get(self.path, self._default)
