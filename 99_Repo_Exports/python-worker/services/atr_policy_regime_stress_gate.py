import os
import redis
import logging
from typing import Any, Tuple

logger = logging.getLogger("atr_policy_regime_stress_gate")

class PolicyRegimeStressGate:
    def __init__(self, redis_url: str = None, redis_client=None):
        if redis_client:
            self.r = redis_client
        else:
            self.r = redis.Redis.from_url(redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

    def validate(self, signal: dict, ctx: Any) -> Tuple[bool, str, dict]:
        if os.getenv("ATR_POLICY_REGIME_STRESS_ENABLE", "1") != "1":
            return True, "ATR_POLICY_REGIME_STRESS_DISABLED", {}

        advisory_only = os.getenv("ATR_POLICY_REGIME_STRESS_ADVISORY_ONLY", "1") == "1"

        meta = signal.get("meta", {}) if isinstance(signal.get("meta"), dict) else {}
        prov = meta.get("policy_provenance", {}) if isinstance(meta.get("policy_provenance"), dict) else {}

        symbol = str(signal.get("symbol") or "").upper()
        layer = str(signal.get("atr_policy_layer") or prov.get("layer") or "stop_ttl")
        
        # Determine rollout stage
        scenario = prov.get('scenario', '')
        risk_horizon_bucket = prov.get('risk_horizon_bucket', '')
        policy_ver = int(prov.get('policy_ver') or 0)
        
        # From allocator/rollout states
        rollout_stage = str(self.r.get(
            f"cfg:atr_rollout_stage:{symbol}:{scenario}:{prov.get('regime','')}:{risk_horizon_bucket}:{layer}:{policy_ver}"
        ) or "shadow")

        regime = str(self.r.get(f"state:atr_regime:{symbol}") or meta.get("regime") or "unknown")
        stress = str(self.r.get(f"state:atr_stress:{symbol}") or "normal")

        # cfg:atr_regime_action is what determines the action
        action = str(self.r.get(f"cfg:atr_regime_action:{regime}:{stress}:{layer}:{rollout_stage}") or "allow").lower()
        
        details = {"regime": regime, "stress_state": stress, "action": action}

        if action == "deny":
            if advisory_only:
                logger.info(f"[REGIME_STRESS_ADVISORY] Would DENY {symbol} {layer}. {details}")
                return True, "ATR_POLICY_REGIME_STRESS_ADVISORY_DENY", details
            return False, "ATR_POLICY_REGIME_STRESS_DENY", details
            
        if action == "freeze":
            if advisory_only:
                logger.info(f"[REGIME_STRESS_ADVISORY] Would FREEZE {symbol} {layer}. {details}")
                return True, "ATR_POLICY_REGIME_STRESS_ADVISORY_FREEZE", details
            return False, "ATR_POLICY_REGIME_STRESS_FREEZE", details
            
        if action == "clip":
            mult = float(self.r.get(f"cfg:atr_regime_risk_mult:{regime}:{stress}:{layer}:{rollout_stage}") or 1.0)
            details["risk_mult"] = mult
            if advisory_only:
                logger.info(f"[REGIME_STRESS_ADVISORY] Would CLIP {symbol} {layer} with mult={mult}. {details}")
                return True, "ATR_POLICY_REGIME_STRESS_ADVISORY_CLIP", details
            
            # Apply clip
            current_risk = float(signal.get("effective_risk_pct") or signal.get("risk_pct") or 0.0)
            signal["effective_risk_pct"] = current_risk * mult
            return True, "ATR_POLICY_REGIME_STRESS_CLIP", details
            
        return True, "ATR_POLICY_REGIME_STRESS_ALLOW", details
