import json
import logging
from typing import Dict, Any, Optional

from services.analytics_db import get_conn

logger = logging.getLogger("atr_failure_signatures")

def detect_and_record_signature(signature_kind: str, signature_hash: str, payload_json: Dict[str, Any]) -> int:
    """
    Records a failure signature and returns its current hit count.
    Used for detecting recurring patterns in incidents, errors, or slippage.
    """
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Check if signature exists
            cur.execute(
                "SELECT signature_id, hit_count FROM atr_failure_signatures WHERE signature_kind = %s AND signature_hash = %s",
                (signature_kind, signature_hash)
            )
            row = cur.fetchone()
            
            if row:
                sig_id = row[0]
                new_hit_count = row[1] + 1
                cur.execute("""
                    UPDATE atr_failure_signatures 
                    SET hit_count = hit_count + 1, last_seen_at = now(), signature_json = %s
                    WHERE signature_id = %s
                """, (json.dumps(payload_json), sig_id))
            else:
                import uuid
                sig_id = f"sig_{uuid.uuid4().hex[:8]}"
                new_hit_count = 1
                cur.execute("""
                    INSERT INTO atr_failure_signatures (signature_id, signature_kind, signature_hash, signature_json, hit_count)
                    VALUES (%s, %s, %s, %s, %s)
                """, (sig_id, signature_kind, signature_hash, json.dumps(payload_json), new_hit_count))
            
            # Reopen related postmortems if threshold crossed
            # For simplicity, let's say every 3 hits we raise a flag if it's recent
            # Real implementation would call postmortem_control_service
            if new_hit_count > 1 and new_hit_count % 3 == 0:
                logger.warning(f"Recurring failure threshold reached for {signature_kind}:{signature_hash} (Hits: {new_hit_count})")
            
            try:
                from prometheus_client import Counter
                Counter("atr_failure_signature_recurring_total", "Recurring failures", ["signature_kind"]).labels(signature_kind=signature_kind).inc()
            except Exception:
                pass
                
            conn.commit()
            return new_hit_count
    except Exception as e:
        logger.error(f"Failed to record failure signature: {e}")
        return 0

def detect_venue_error_signature(venue: str, error_code: str, reason: str):
    """Specific detector for exchange (e.g. Binance/MT5) errors."""
    sig_hash = f"{venue}_{error_code}"
    payload = {"venue": venue, "error_code": error_code, "last_reason": reason}
    return detect_and_record_signature("venue_error_pattern", sig_hash, payload)

def detect_replay_mismatch_signature(target: str, reason: str):
    """Specific detector for replay mismatches that occur repeatedly on the same logic line."""
    sig_hash = f"{target}_{reason}"
    payload = {"target": target, "reason": reason}
    return detect_and_record_signature("replay_mismatch_pattern", sig_hash, payload)
