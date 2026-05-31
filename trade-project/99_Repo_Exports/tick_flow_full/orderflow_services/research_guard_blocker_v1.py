import os
import json
from typing import Tuple, Dict, Any
import redis

def check_research_guard_blocker(redis_url: str, blocker_key: str) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Checks the P5 Strategy Research Guard blocker state in Redis.
    Fail-open if report-only=1 or if state is missing/invalid.
    
    Returns:
        blocked (bool): True if the guard is active and enforces blocking.
        reason (str): Description of the blocking reason.
        details (dict): The full state dictionary.
    """
    try:
        r = redis.from_url(redis_url)
        data = r.get(blocker_key)
        
        if not data:
            return False, "no_data", {}
            
        state = json.loads(data)
        report_only = int(state.get("report_only", 1))
        
        if report_only == 1:
            return False, "report_only", state
            
        is_blocked = bool(state.get("blocker_active", False))
        reason = str(state.get("reason", ""))
        
        return is_blocked, reason, state
        
    except Exception as e:
        # Fail-open on error for safety during rollout
        return False, f"error_reading_state:{e}", {}
