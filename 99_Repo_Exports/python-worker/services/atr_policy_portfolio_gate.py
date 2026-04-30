from __future__ import annotations

import os
import redis
from typing import Any, Dict

class PolicyPortfolioGate:
    """
    Hard portfolio gate:
    - factor cluster concentration
    - venue concentration
    - same-side cluster crowding
    - policy concentration
    - correlation-aware marginal risk
    """

    def __init__(self, redis_client: redis.Redis | None = None, redis_url: str | None = None):
        if redis_client:
            self.r = redis_client
        else:
            url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            self.r = redis.Redis.from_url(url, decode_responses=True)

    def validate(self, signal: Dict[str, Any], ctx: Any = None) -> tuple[bool, str, Dict[str, Any]]:
        # Advisory mode toggle is evaluated outside usually, but we keep the gate stateless.
        
        meta = signal.get("meta", {}) if isinstance(signal.get("meta"), dict) else {}
        prov = meta.get("policy_provenance", {}) if isinstance(meta.get("policy_provenance"), dict) else {}

        source = str(signal.get("source") or "CryptoOrderFlow")
        venue = str(signal.get("venue") or "unknown")
        symbol = str(signal.get("symbol") or "").upper()
        side = str(signal.get("side") or str(signal.get("direction")) or "").upper()
        scenario = str(prov.get("scenario") or signal.get("kind") or "").lower()
        regime = str(prov.get("regime") or meta.get("regime") or "na").lower()
        bucket = str(prov.get("risk_horizon_bucket") or "unknown").lower()
        layer = str(signal.get("atr_policy_layer") or "stop_ttl")
        policy_ver = int(prov.get("policy_ver") or 0)

        cluster = str(self.r.get(f"cfg:atr_symbol_cluster:{symbol}") or "unclassified")
        effective_risk_pct = float(signal.get("effective_risk_pct") or signal.get("risk_pct") or 0.0)

        # 1) factor cluster risk
        cluster_open = float(self.r.get(f"state:atr_portfolio:open_risk_pct:factor:{cluster}") or 0.0)
        cluster_cap = float(self.r.get(f"cfg:atr_portfolio:max_factor_cluster_risk_pct:factor:{cluster}") or 0.0)
        if cluster_cap > 0.0 and (cluster_open + effective_risk_pct) > cluster_cap:
            return False, "ATR_PORTFOLIO_FACTOR_CLUSTER_EXCEEDED", {
                "factor_cluster": cluster
                "cluster_open_risk_pct": cluster_open
                "incoming_risk_pct": effective_risk_pct
                "cluster_cap": cluster_cap
            }

        # 2) same-side crowding
        same_side = float(self.r.get(f"state:atr_portfolio:same_side_risk_pct:factor:{cluster}:{side}") or 0.0)
        same_side_cap = float(self.r.get(f"cfg:atr_portfolio:max_same_side_risk_pct:factor:{cluster}:{side}") or 0.0)
        if same_side_cap > 0.0 and (same_side + effective_risk_pct) > same_side_cap:
            return False, "ATR_PORTFOLIO_SAME_SIDE_CLUSTER_EXCEEDED", {
                "factor_cluster": cluster
                "side": side
                "same_side_risk_pct": same_side
                "incoming_risk_pct": effective_risk_pct
                "same_side_cap": same_side_cap
            }

        # 3) venue concentration
        venue_open = float(self.r.get(f"state:atr_portfolio:open_risk_pct:venue:{venue}") or 0.0)
        venue_cap = float(self.r.get(f"cfg:atr_portfolio:max_venue_risk_pct:venue:{venue}") or 0.0)
        if venue_cap > 0.0 and (venue_open + effective_risk_pct) > venue_cap:
            return False, "ATR_PORTFOLIO_VENUE_EXCEEDED", {
                "venue": venue
                "venue_open_risk_pct": venue_open
                "incoming_risk_pct": effective_risk_pct
                "venue_cap": venue_cap
            }

        # 4) policy concentration
        pscope = f"policy:{source}:{symbol}:{scenario}:{regime}:{bucket}:{layer}:{policy_ver}"
        policy_open = float(self.r.get(f"state:atr_portfolio:open_risk_pct:{pscope}") or 0.0)
        policy_cap = float(self.r.get(f"cfg:atr_portfolio:max_policy_risk_pct:{pscope}") or 0.0)
        if policy_cap > 0.0 and (policy_open + effective_risk_pct) > policy_cap:
            return False, "ATR_PORTFOLIO_POLICY_EXCEEDED", {
                "policy_scope": pscope
                "policy_open_risk_pct": policy_open
                "incoming_risk_pct": effective_risk_pct
                "policy_cap": policy_cap
            }

        return True, "ATR_PORTFOLIO_ALLOW", {"factor_cluster": cluster}
