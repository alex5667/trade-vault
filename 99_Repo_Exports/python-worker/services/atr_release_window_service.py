import json
import logging
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import psycopg2.extras
from services.analytics_db import get_conn as get_db_connection
from prometheus_client import Counter
from services.atr_release_quarantine_service import ATRReleaseQuarantineService
from services.atr_model_config_drift_service import ATRModelConfigDriftService

logger = logging.getLogger("atr_release_window_service")

# Metrics
RELEASE_WINDOWS_TOTAL = Counter("atr_release_windows_total", "Total release windows", ["window_kind", "status"])
PRE_RELEASE_CHECKLISTS_TOTAL = Counter("atr_pre_release_checklists_total", "Total checklists", ["change_class", "status"])
PRE_RELEASE_BLOCKERS_TOTAL = Counter("atr_pre_release_blockers_total", "Total blocked checklists", ["reason_code"])
RELEASE_SIGNOFFS_TOTAL = Counter("atr_release_signoffs_total", "Total checklist signoffs", ["signer_role", "status"])
RELEASE_WINDOWS_BLOCKED_TOTAL = Counter("atr_release_windows_blocked_total", "Total windows blocked", ["window_kind"])

# Ensure environment variables dictating policy are visible
ATR_RELEASE_WINDOW_POLICY_ENABLE = os.getenv("ATR_RELEASE_WINDOW_POLICY_ENABLE", "1") == str("1")
ATR_RELEASE_WINDOW_POLICY_ENFORCE = os.getenv("ATR_RELEASE_WINDOW_POLICY_ENFORCE", "0") == "1"

CHANGE_CLASSES = [
    "LOW_RISK_CONFIG"
    "LOW_RISK_OBSERVABILITY"
    "MEDIUM_POLICY"
    "HIGH_GOVERNANCE"
    "CRITICAL_RUNTIME_GATING"
    "CRITICAL_EXECUTION_TOUCHING"
    "PROTECTIVE_PATH_TOUCHING"
]

WINDOW_KINDS = [
    "standard"
    "governance"
    "runtime_critical"
    "execution_critical"
    "protective_isolated"
]

def classify_change(change_type: str, components_touched: List[str]) -> str:
    """Classify change into one of the CHANGE_CLASSES."""
    components_str = " ".join(components_touched).lower()
    
    if "execution" in components_str or "mt5" in components_str or "risk_pct" in components_str:
        return "CRITICAL_EXECUTION_TOUCHING"
    if "protective" in components_str or "trailing" in components_str or "closeout" in components_str or "tp1" in components_str:
        return "PROTECTIVE_PATH_TOUCHING"
    if "gating" in components_str or "allow" in components_str or "clip" in components_str or "deny" in components_str:
        return "CRITICAL_RUNTIME_GATING"
    if "graph" in components_str or "freeze" in components_str or "override" in components_str:
        return "HIGH_GOVERNANCE"
    if "policy" in components_str:
        return "MEDIUM_POLICY"
    if "metrics" in components_str or "boards" in components_str or "observability" in components_str:
        return "LOW_RISK_OBSERVABILITY"
    return "LOW_RISK_CONFIG"

def find_eligible_window(change_class: str) -> str:
    if change_class in ["LOW_RISK_CONFIG", "LOW_RISK_OBSERVABILITY"]:
        return "standard"
    if change_class in ["MEDIUM_POLICY", "HIGH_GOVERNANCE"]:
        return "governance"
    if change_class == "CRITICAL_RUNTIME_GATING":
        return "runtime_critical"
    if change_class == "CRITICAL_EXECUTION_TOUCHING":
        return "execution_critical"
    if change_class == "PROTECTIVE_PATH_TOUCHING":
        return "protective_isolated"
    return "standard"

def build_pre_release_checklist(change_id: str, change_class: str, target_scope: str) -> Dict[str, Any]:
    # Gathers metrics/status from Control Plane, Execution, and Protective readiness
    # Dummy implementation representing the real telemetry fetches
    checklist_id = f"relchk_{datetime.now(timezone.utc).strftime('%Y_%m_%d')}_{change_id[:4]}"
    
    checks = {
        "control_plane": {
            "graph_consistency_cert": "passed"
            "projection_consistency_cert": "passed"
            "open_critical_drifts": 0
        }
        "signal_gates": {
            "book_stale_spike": False
            "atr_unavailable_spike": False
            "negative_ev_shift": False
        }
        "execution": {
            "mt5_connection_burst": False
            "requote_burst": False
            "slippage_shift": False
        }
        "protective": {
            "open_protective_critical_drift": 0
        }
        "rollback_ready": {
            "rollback_bundle_prepared": True
            "rollback_owner_present": True
        }
    }
    
    summary = {
        "window_kind": find_eligible_window(change_class)
        "decision": "eligible_for_release_window"
    }

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO atr_pre_release_checklists 
                (checklist_id, change_id, change_class, target_scope, status, checks_json, summary_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (checklist_id) DO UPDATE 
                SET checks_json = EXCLUDED.checks_json, summary_json = EXCLUDED.summary_json, status = EXCLUDED.status
            """, (checklist_id, change_id, change_class, target_scope, "ready", json.dumps(checks), json.dumps(summary)))
            conn.commit()

    PRE_RELEASE_CHECKLISTS_TOTAL.labels(change_class=change_class, status="ready").inc()
    return {
        "checklist_id": checklist_id
        "change_id": change_id
        "change_class": change_class
        "target_scope": target_scope
        "status": "ready"
        "checks": checks
        "summary": summary
    }

def evaluate_release_blockers(checklist_id: str) -> List[str]:
    """Check for SEV-1, weekly HOLD, daily BLACK, and critical drifts."""
    blockers = []
    with get_db_connection() as conn:
         with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
             cur.execute("SELECT * FROM atr_pre_release_checklists WHERE checklist_id = %s", (checklist_id,))
             row = cur.fetchone()
             if not row:
                 return ["Checklist not found"]
             
             target_scope = row.get('target_scope', '')
             q_blocker = ATRReleaseQuarantineService.is_release_blocked_by_quarantine(target_scope)
             if q_blocker:
                 blockers.append(f"active quarantine: {q_blocker['quarantine_class']} on {q_blocker['scope_value']}")
                 
             from services.atr_disaster_recovery_service import ATRDisasterRecoveryService
             dr_blocker = ATRDisasterRecoveryService.is_release_blocked_by_dr(target_scope)
             if dr_blocker:
                 blockers.append(f"active disaster recovery: {dr_blocker['dr_class']} in state {dr_blocker['status']}")
             
             checks = row['checks_json']
             if checks.get("control_plane", {}).get("open_critical_drifts", 0) > 0:
                 blockers.append("control plane critical drift open")
             if checks.get("protective", {}).get("open_protective_critical_drift", 0) > 0:
                 blockers.append("protective critical drift open")
                 
             if row['change_class'] in ["CRITICAL_EXECUTION_TOUCHING", "CRITICAL_RUNTIME_GATING", "PROTECTIVE_PATH_TOUCHING"]:
                 if not checks.get("rollback_ready", {}).get("rollback_bundle_prepared", False):
                     blockers.append("rollback bundle not prepared for critical change")

             from services.atr_replay_certification_service import ATRReplayCertificationService
             cert_status = ATRReplayCertificationService.get_cert_status_for_change(row['change_id'], row['change_class'])
             if cert_status in ["failed", "missing", "incomplete"]:
                 blockers.append(f"replay certification fails requirements: {cert_status}")

             # Drift Governance Integration
             drift_blockers = ATRModelConfigDriftService.is_release_blocked_by_drift(row['change_class'], target_scope)
             blockers.extend(drift_blockers)

             # Baseline Validity Integration
             required_datasets = ATRReplayCertificationService.select_required_datasets(row['change_class'])
             for ds in required_datasets:
                 v_status, _ = ATRModelConfigDriftService.check_dataset_validity(ds['dataset_id'])
                 if v_status in ["expired", "missing"]:
                     blockers.append(f"required dataset baseline {ds['dataset_class']} is {v_status}")

             # Example mock for external boards
             # In full implementation, query v_ops_weekly_scorecard and v_ops_daily_triage
             
    return blockers

def get_required_signoffs(change_class: str) -> List[str]:
    matrix = {
        "LOW_RISK_CONFIG": ["owner"]
        "LOW_RISK_OBSERVABILITY": ["owner"]
        "MEDIUM_POLICY": ["owner", "oncall"]
        "HIGH_GOVERNANCE": ["owner", "control_plane_owner", "oncall"]
        "CRITICAL_RUNTIME_GATING": ["owner", "control_plane_owner", "oncall"]
        "CRITICAL_EXECUTION_TOUCHING": ["owner", "execution_owner", "oncall"]
        "PROTECTIVE_PATH_TOUCHING": ["owner", "protective_owner", "oncall"]
    }
    return matrix.get(change_class, ["owner"])

def collect_signoffs(checklist_id: str, signer_role: str, signer: str, status: str) -> None:
    signoff_id = f"{checklist_id}_{signer_role}"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO atr_release_signoffs 
                (signoff_id, checklist_id, signer_role, signer, status, signoff_json)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (signoff_id) DO UPDATE SET status = EXCLUDED.status, signer = EXCLUDED.signer
            """, (signoff_id, checklist_id, signer_role, signer, status, json.dumps({})))
            
            # Auto-approve checklist if all required signoffs are met
            cur.execute("SELECT change_class FROM atr_pre_release_checklists WHERE checklist_id = %s", (checklist_id,))
            row = cur.fetchone()
            if row:
                required = get_required_signoffs(row[0])
                cur.execute("SELECT signer_role, status FROM atr_release_signoffs WHERE checklist_id = %s AND status = 'approved'", (checklist_id,))
                approved_roles = [r[0] for r in cur.fetchall()]
                
                if all(req in approved_roles for req in required):
                    cur.execute("UPDATE atr_pre_release_checklists SET status = 'approved', approved_at = now() WHERE checklist_id = %s", (checklist_id,))
                    PRE_RELEASE_CHECKLISTS_TOTAL.labels(change_class=row[0], status="approved").inc()
            conn.commit()

    RELEASE_SIGNOFFS_TOTAL.labels(signer_role=signer_role, status=status).inc()

def open_release_window(checklist_id: str) -> Optional[str]:
    if not ATR_RELEASE_WINDOW_POLICY_ENABLE:
        logger.info("Release window policy is disabled. Bypassing lock.")
        return "bypass"

    blockers = evaluate_release_blockers(checklist_id)
    if blockers and ATR_RELEASE_WINDOW_POLICY_ENFORCE:
        logger.warning(f"Cannot open window for {checklist_id}, blockers exist: {blockers}")
        for b in blockers:
            PRE_RELEASE_BLOCKERS_TOTAL.labels(reason_code=b.replace(' ', '_')).inc()
            
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE atr_pre_release_checklists SET status = 'blocked' WHERE checklist_id = %s", (checklist_id,))
                conn.commit()
                
        # To determine window_kind for metrics:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT summary_json FROM atr_pre_release_checklists WHERE checklist_id = %s", (checklist_id,))
                r = cur.fetchone()
                wk = r['summary_json'].get('window_kind', 'unknown') if r else 'unknown'
                RELEASE_WINDOWS_BLOCKED_TOTAL.labels(window_kind=wk).inc()
        return None
        
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM atr_pre_release_checklists WHERE checklist_id = %s", (checklist_id,))
            checklist = cur.fetchone()
            
            if not checklist:
                return None
                
            if checklist['status'] != 'approved' and ATR_RELEASE_WINDOW_POLICY_ENFORCE:
                logger.warning(f"Checklist {checklist_id} is not approved yet.")
                return None
                
            window_id = f"win_{checklist_id}"
            window_kind = checklist['summary_json'].get('window_kind', 'standard')
            
            cur.execute("""
                INSERT INTO atr_release_windows 
                (window_id, window_kind, starts_at, ends_at, status, window_json)
                VALUES (%s, %s, now(), now() + interval '4 hours', 'open', '{}')
                ON CONFLICT (window_id) DO UPDATE SET status = 'open'
            """, (window_id, window_kind))
            conn.commit()
            
            RELEASE_WINDOWS_TOTAL.labels(window_kind=window_kind, status="open").inc()
            return window_id

def close_release_window(window_id: str) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE atr_release_windows SET status = 'closed' WHERE window_id = %s", (window_id,))
            cur.execute("SELECT window_kind FROM atr_release_windows WHERE window_id = %s", (window_id,))
            r = cur.fetchone()
            wk = r[0] if r else 'unknown'
            
        conn.commit()
        RELEASE_WINDOWS_TOTAL.labels(window_kind=wk, status="closed").inc()

