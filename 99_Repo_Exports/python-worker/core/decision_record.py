
import json
import time
from dataclasses import dataclass, asdict, field
from typing import Dict, Any, Optional, List, Union

@dataclass
class DecisionRecord:
    """
    Unified record of a signal decision.
    Persisted to Redis hash `decision:{sid}` and stream `decisions:final`.
    """
    sid: str
    symbol: str
    ts: int  # Decision timestamp (ms)
    
    # Input Snapshot
    features: Dict[str, Any] = field(default_factory=dict)
    
    # Rule Output
    rule_score: float = 0.0
    rule_ok: bool = False
    rule_soft: bool = False
    rule_reasons: List[str] = field(default_factory=list)
    
    # ML Output
    ml_allow: bool = False
    ml_abstain: bool = False
    ml_deny: bool = False
    ml_prob: float = 0.0
    ml_version: str = ""
    ml_calibrated_prob: Optional[float] = None
    
    # Final Decision
    final_permit: bool = False
    final_reason: str = ""
    
    # System States
    dq_state: Dict[str, Any] = field(default_factory=dict)  # tick_age, book_stale
    drift_state: Dict[str, Any] = field(default_factory=dict) # z-scores
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DecisionRecord':
        # Handle potential distinct types or sets from JSON/Redis
        return cls(**data)
        
    def serialize_for_redis(self) -> Dict[str, str]:
        """Flattens and serializes for Redis Hash (HSET)."""
        d = self.to_dict()
        out = {}
        for k, v in d.items():
            if isinstance(v, (dict, list, bool, type(None))):
                out[k] = json.dumps(v)
            else:
                out[k] = str(v)
        return out

    @classmethod
    def parse_from_redis(cls, data: Dict[str, str]) -> 'DecisionRecord':
        """Parses from Redis Hash (HGETALL)."""
        kwargs = {}
        # We need to know which fields are JSON/complex types
        # A simple heuristic or explicit mapping is needed.
        # For now, let's try to JSON decode commonly complex fields
        complex_fields = {
            'features', 'rule_reasons', 'dq_state', 'drift_state'
        }
        bool_fields = {
            'rule_ok', 'rule_soft', 'ml_allow', 'ml_abstain', 'ml_deny', 'final_permit'
        }
        float_fields = {
            'rule_score', 'ml_prob', 'ml_calibrated_prob'
        }
        int_fields = {'ts'}
        
        for k, v in data.items():
            if k in complex_fields:
                try:
                    kwargs[k] = json.loads(v)
                except:
                    kwargs[k] = v # Fallback
            elif k in bool_fields:
                 # Check 'True'/'False' string or '1'/'0' or json 'true'/'false'
                 if v.lower() in ('true', '1'):
                     kwargs[k] = True
                 elif v.lower() in ('false', '0'):
                     kwargs[k] = False
                 else:
                     # try json
                     try:
                        kwargs[k] = json.loads(v)
                     except:
                        kwargs[k] = False
            elif k in float_fields:
                try:
                    kwargs[k] = float(v) if v != 'None' else None
                except:
                     kwargs[k] = 0.0
            elif k in int_fields:
                try:
                    kwargs[k] = int(v)
                except:
                    kwargs[k] = 0
            else:
                kwargs[k] = v
                
        return cls(**kwargs)

