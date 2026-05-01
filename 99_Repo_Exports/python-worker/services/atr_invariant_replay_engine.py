import json
import logging
import uuid
import time
from typing import Dict, Any, List

from services.analytics_db import get_conn
from services.atr_invariants_registry import get_active_invariants

logger = logging.getLogger("atr_invariant_replay_engine")

class InvariantReplayEngine:
    """
    Validates signal_id stability and governance invariants during Replay certification.
    Integrated into `atr_replay_runner.py`.
    """
    def __init__(self):
        self.invariants = get_active_invariants()
    
    def validate_change(self, baseline: Dict[str, Any], candidate: Dict[str, Any], replay_id: str) -> List[Dict[str, Any]]:
        """
        Validates change across baseline vs candidate.
        Returns a list of violations.
        """
        violations = []
        active_codes = {inv["reason_code"]: inv for inv in self.invariants}
        
        baseline_signal_id = baseline.get("signal_id", "")
        candidate_signal_id = candidate.get("signal_id", "")
        
        # Stability Check
        if "INV_SIGNAL_ID_STABLE_IN_REPLAY" in active_codes:
            if baseline_signal_id and baseline_signal_id != candidate_signal_id:
                violations.append({
                    "invariant_id": active_codes["INV_SIGNAL_ID_STABLE_IN_REPLAY"]["invariant_id"],
                    "reason_code": "INV_SIGNAL_ID_STABLE_IN_REPLAY",
                    "severity": active_codes["INV_SIGNAL_ID_STABLE_IN_REPLAY"]["severity"],
                    "enforcement_mode": active_codes["INV_SIGNAL_ID_STABLE_IN_REPLAY"]["enforcement_mode"],
                    "details": f"signal_id drifted: {baseline_signal_id} -> {candidate_signal_id}"
                })
        
        # We can also call the runtime engine checks on the candidate
        from services.atr_invariant_runtime_engine import get_runtime_engine
        runtime_engine = get_runtime_engine()
        runtime_allow, runtime_violations = runtime_engine.validate_signal(candidate)
        
        violations.extend(runtime_violations)
        
        # Persist replay violations via snapshots
        self._persist_snapshot(replay_id, violations, candidate)
        
        return violations
        
    def _persist_snapshot(self, replay_id: str, violations: List[Dict[str, Any]], candidate: Dict[str, Any]) -> None:
        if not violations:
            return
            
        snapshot_id = f"snap_replay_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"
        try:
            with get_conn() as conn, conn.cursor() as cur:
                 cur.execute("""
                    INSERT INTO atr_invariant_snapshots (snapshot_id, snapshot_kind, snapshot_json)
                    VALUES (%s, %s, %s)
                 """, (snapshot_id, "replay_check", json.dumps({
                     "replay_id": replay_id,
                     "candidate_signal": candidate.get("signal_id"),
                     "violations": violations
                 })))
                 conn.commit()
        except Exception as e:
            logger.error(f"Failed to persist replay invariant snapshot: {e}")

_engine = None
def get_replay_engine() -> InvariantReplayEngine:
    global _engine
    if _engine is None:
        _engine = InvariantReplayEngine()
    return _engine
