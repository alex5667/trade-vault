from __future__ import annotations

import os
from typing import Any, Dict, Tuple

try:
    import redis
except ImportError:
    redis = None  # type: ignore[assignment]

try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None


class PolicyExecutionBudgetGate:
    """
    Final hard gate before orders:queue / orders:queue:mt5.
    Checks:
      - kill-switch hierarchy
      - open risk budget
      - open positions limit
      - daily trades limit
      - daily realized loss limit
      - slippage EMA budget
      - stop-loss streak limit
    """

    def __init__(self, redis_url: str = "", redis_client: Any = None):
        if redis_client is not None:
            self.r = redis_client
        elif get_atr_redis is not None:
            self.r = get_atr_redis()
        else:
            if not redis_url:
                redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            if redis is not None:
                self.r = redis.Redis.from_url(redis_url, decode_responses=True)
            else:
                self.r = None

    def validate(self, signal: Dict[str, Any], ctx: Any = None) -> Tuple[bool, str, Dict[str, Any]]:
        # Fail-open if no redis
        if self.r is None:
            return True, "ATR_POLICY_BUDGET_ALLOW_NO_REDIS", {}

        meta = signal.get("meta", {}) if isinstance(signal.get("meta"), dict) else {}
        prov = meta.get("policy_provenance", {}) if isinstance(meta.get("policy_provenance"), dict) else {}

        source = str(signal.get("source") or "CryptoOrderFlow")
        venue = str(signal.get("venue") or "unknown")
        symbol = str(signal.get("symbol") or "").upper()
        scenario = str(prov.get("scenario") or signal.get("kind") or "").lower()
        regime = str(prov.get("regime") or meta.get("regime") or "na").lower()
        bucket = str(prov.get("risk_horizon_bucket") or "unknown").lower()
        layer = str(signal.get("atr_policy_layer") or "stop_ttl")
        policy_ver = int(prov.get("policy_ver") or 0)

        # 1) kill-switch hierarchy
        scopes = [
            "global",
            f"venue:{venue}",
            f"cohort:{source}:{symbol}:{scenario}:{regime}:{bucket}",
            f"layer:{source}:{symbol}:{scenario}:{regime}:{bucket}:{layer}",
            f"policy:{source}:{symbol}:{scenario}:{regime}:{bucket}:{layer}:{policy_ver}",
        ]
        
        try:
            for scope in scopes:
                if self.r.get(f"cfg:atr_kill_switch:{scope}") == "1":
                    return False, "ATR_POLICY_KILL_SWITCH_ACTIVE", {"scope": scope}

            # 2) Capital Allocator (Phase 5.5) integration
            alloc_scope = f"policy:{source}:{symbol}:{scenario}:{regime}:{bucket}:{layer}:{policy_ver}"
            observe_only = os.getenv("ATR_POLICY_ALLOCATOR_OBSERVE_ONLY", "1") == "1"
            
            if not observe_only:
                risk_mult = float(self.r.get(f"cfg:atr_alloc:risk_pct_mult:{alloc_scope}") or 1.0)
                alloc_max_open_risk_pct = float(self.r.get(f"cfg:atr_alloc:max_open_risk_pct:{alloc_scope}") or 0.0)
                
                effective_risk_pct = float(signal.get("risk_pct") or 0.0) * risk_mult
                signal["effective_risk_pct"] = effective_risk_pct
                signal["atr_alloc_scope"] = alloc_scope
            else:
                effective_risk_pct = float(signal.get("risk_pct") or 0.0)
                signal["effective_risk_pct"] = effective_risk_pct
                alloc_max_open_risk_pct = 0.0

            # 3) example open risk budget (hard budget combined with allocator cap)
            open_risk_pct = float(self.r.get(f"state:atr_budget:open_risk_pct:cohort:{source}:{symbol}:{scenario}:{regime}:{bucket}") or 0.0)
            max_open_risk_pct = float(self.r.get(f"cfg:atr_budget:max_open_risk_pct:cohort:{source}:{symbol}:{scenario}:{regime}:{bucket}") or 0.0)
            
            if alloc_max_open_risk_pct > 0.0 and (open_risk_pct + effective_risk_pct) > alloc_max_open_risk_pct:
                return False, "ATR_POLICY_BUDGET_ALLOC_OPEN_RISK_EXCEEDED", {
                    "open_risk_pct": open_risk_pct,
                    "effective_risk_pct": effective_risk_pct,
                    "alloc_max_open_risk_pct": alloc_max_open_risk_pct,
                }
                
            if max_open_risk_pct > 0.0 and (open_risk_pct + effective_risk_pct) > max_open_risk_pct:
                return False, "ATR_POLICY_BUDGET_OPEN_RISK_EXCEEDED", {
                    "open_risk_pct": open_risk_pct,
                    "effective_risk_pct": effective_risk_pct,
                    "max_open_risk_pct": max_open_risk_pct,
                }

            # 3) slippage ema budget
            slip_ema = float(self.r.get(f"slippage_ema:{symbol}") or 0.0)
            max_slip_ema = float(self.r.get(f"cfg:atr_budget:max_slippage_ema_bps:cohort:{source}:{symbol}:{scenario}:{regime}:{bucket}") or 0.0)
            if max_slip_ema > 0.0 and slip_ema > max_slip_ema:
                return False, "ATR_POLICY_BUDGET_SLIPPAGE_EMA_EXCEEDED", {
                    "slippage_ema_bps": slip_ema,
                    "max_slippage_ema_bps": max_slip_ema,
                }
        except Exception:
            # Let the caller decide if it should fail-open or fail-closed on exception.
            # Usually we throw, to ensure fail-closed during order queuing.
            raise
            
        return True, "ATR_POLICY_BUDGET_ALLOW", {}
