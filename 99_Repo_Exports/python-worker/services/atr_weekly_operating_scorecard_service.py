#!/usr/bin/env python3
"""
ATR Weekly Operating Scorecard Service (Phase 9.1)
Forms the canonical weekly scorecard, review ceremony data, and action loop.
"""

import os
import time
import json
import uuid
import logging
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import redis

from common.log import setup_logger
from services.analytics_db import get_conn
from services.atr_program_closure_service import ATRProgramClosureService

logger = setup_logger("atr_weekly_scorecard_service")

DOMAINS = [
    "signal_gates",
    "dispatch_runtime",
    "execution",
    "protective_lifecycle",
    "control_plane_graph",
    "audit_hygiene"
]

class ATRWeeklyScorecardService:
    def __init__(self, enable: bool = True, enforce: bool = False):
        self.enable = enable
        self.enforce = enforce
        self.closure_svc = ATRProgramClosureService()

    def generate_scorecard_id(self, week_start: datetime) -> str:
        week_num = week_start.isocalendar()[1]
        year = week_start.year
        return f"wk_{year}_{week_num:02d}_{uuid.uuid4().hex[:6]}"

    def generate_id(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:10]}"

    def _get_metrics_for_domain(self, domain: str, conn, r) -> dict:
        """
        Gathers raw metrics per domain. For demonstration, outputs mock/synthetic values
        that can be injected or matched to actual metrics logic.
        """
        metrics = {}
        if domain == "signal_gates":
            metrics = {
                "signals_total": 1284,
                "tradeable_total": 213,
                "veto_total": 1071,
                "top_veto_reasons": {"book_stale": 241, "negative_ev": 198, "spread_too_wide": 114},
                "veto_drift_detected": False
            }
        elif domain == "dispatch_runtime":
            metrics = {
                "raw_publish_ok_rate": 0.999,
                "order_queue_publish_ok_rate": 0.998,
                "runtime_critical_drifts": 0
            }
        elif domain == "execution":
            metrics = {
                "slippage_ema_shift_symbols": 0,
                "mt5_requotes_total": 5
            }
        elif domain == "protective_lifecycle":
            metrics = {
                "be_before_tp1_violations": 0,
                "sl_ratchet_backwards_violations": 0,
                "protective_critical_drifts": 0
            }
        elif domain == "control_plane_graph":
            metrics = {
                "graph_consistency_cert": "passed",
                "projection_drifts_open": 0,
                "authority_violations": 0
            }
        elif domain == "audit_hygiene":
            metrics = {
                "overdue_actions_p1": 0,
                "expired_overrides_active": 0,
                "hidden_dependency_findings": 0
            }
        return metrics

    def derive_domain_status(self, domain: str, metrics: dict) -> str:
        status = "GREEN"
        
        if domain == "signal_gates":
            if metrics.get("top_veto_reasons", {}).get("book_stale", 0) > 1000 or metrics.get("veto_drift_detected"):
                status = "RED"
        elif domain == "dispatch_runtime":
            if metrics.get("runtime_critical_drifts", 0) > 0 or metrics.get("raw_publish_ok_rate", 1.0) < 0.95:
                status = "RED"
        elif domain == "execution":
            if metrics.get("slippage_ema_shift_symbols", 0) > 1 or metrics.get("mt5_requotes_total", 0) > 15:
                status = "YELLOW"
            if metrics.get("slippage_ema_shift_symbols", 0) > 3:
                status = "RED"
        elif domain == "protective_lifecycle":
            if metrics.get("be_before_tp1_violations", 0) > 0 or metrics.get("sl_ratchet_backwards_violations", 0) > 0 or metrics.get("protective_critical_drifts", 0) > 0:
                status = "RED"
        elif domain == "control_plane_graph":
            if metrics.get("graph_consistency_cert") == "failed" or metrics.get("authority_violations", 0) > 0 or metrics.get("projection_drifts_open", 0) > 0:
                status = "RED"
        elif domain == "audit_hygiene":
            if metrics.get("overdue_actions_p1", 0) > 0:
                status = "YELLOW"
            if metrics.get("expired_overrides_active", 0) > 0 or metrics.get("hidden_dependency_findings", 0) > 0:
                status = "RED"
                
        return status

    def propose_weekly_decision(self, domain_statuses: dict, domain_metrics: dict) -> str:
        # Check immediate HOLD rules
        cpg_metrics = domain_metrics.get("control_plane_graph", {})
        dr_metrics = domain_metrics.get("dispatch_runtime", {})
        if cpg_metrics.get("graph_consistency_cert") == "failed":
            return "HOLD"
        if dr_metrics.get("runtime_critical_drifts", 0) > 0:
            return "HOLD"
            
        # Check FREEZE_ESCALATION & ROLLBACK_REVIEW_REQUIRED rules
        pl_metrics = domain_metrics.get("protective_lifecycle", {})
        if pl_metrics.get("protective_critical_drifts", 0) > 0:
            return "FREEZE_ESCALATION" # Or ROLLBACK_REVIEW_REQUIRED depending on exact policy state

        # Aggregate statuses
        counts = {"GREEN": 0, "YELLOW": 0, "RED": 0}
        for s in domain_statuses.values():
            counts[s] += 1
            
        if counts["RED"] > 0:
            return "HOLD"
        elif counts["YELLOW"] > 0:
            return "GO_WITH_CONSTRAINTS"
        
        return "GO"

    def suggest_action_items(self, scorecard_id: str, domain_statuses: dict, domain_metrics: dict) -> list:
        actions = []
        for domain, status in domain_statuses.items():
            if status in ("RED", "YELLOW"):
                # Propose P0/P1 actions
                mets = domain_metrics.get(domain, {})
                priority = "P1" if status == "RED" else "P2"
                due_delta = timedelta(days=3) if priority == "P1" else timedelta(days=7)
                
                reason_code = f"{domain}_degraded"
                if domain == "control_plane_graph" and mets.get("graph_consistency_cert") == "failed":
                    reason_code = "graph_cert_failed"
                    priority = "P0"
                    due_delta = timedelta(days=1)
                elif domain == "execution" and mets.get("mt5_requotes_total", 0) > 10:
                    reason_code = "mt5_requote_spike"
                    
                actions.append({
                    "action_id": self.generate_id("act"),
                    "scorecard_id": scorecard_id,
                    "domain": domain,
                    "owner": "trade_bot", # default assignment
                    "priority": priority,
                    "status": "open",
                    "title": f"Investigate {domain} degradation",
                    "reason_code": reason_code,
                    "due_at": datetime.now(timezone.utc) + due_delta,
                    "action_json": json.dumps({"source_metrics": mets})
                })
        return actions

    def build_weekly_scorecard(self, conn, r, week_start: datetime, week_end: datetime, custom_metrics: dict = None) -> str:
        if not self.enable:
            logger.info("Weekly Scorecard skipped, enable=False")
            return None
            
        scorecard_id = self.generate_scorecard_id(week_start)
        all_metrics = {}
        domain_statuses = {}
        
        for domain in DOMAINS:
            # allow passing custom metrics for testing
            if custom_metrics and domain in custom_metrics:
                mets = custom_metrics[domain]
            else:
                mets = self._get_metrics_for_domain(domain, conn, r)
            all_metrics[domain] = mets
            domain_statuses[domain] = self.derive_domain_status(domain, mets)
            
        decision = self.propose_weekly_decision(domain_statuses, all_metrics)
        actions = self.suggest_action_items(scorecard_id, domain_statuses, all_metrics)
        
        # Build JSON
        domains_json = {}
        for domain in DOMAINS:
            domains_json[domain] = {
                "status": domain_statuses[domain],
                "metrics": all_metrics[domain]
            }
            
        summary_json = {
            "constraints": []
        }
        if decision == "GO_WITH_CONSTRAINTS":
            summary_json["constraints"].append("System operable, but with limits. Handle YELLOW alerts.")
            
        with conn.cursor() as cur:
            # Insert scorecard
            cur.execute("""
                INSERT INTO atr_weekly_operating_scorecards (
                    scorecard_id, week_start, week_end, overall_status, domains_json, summary_json
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (scorecard_id, week_start, week_end, decision, json.dumps(domains_json), json.dumps(summary_json)))
            
            # Insert decision
            decision_id = self.generate_id("dec")
            cur.execute("""
                INSERT INTO atr_weekly_review_decisions (
                    decision_id, scorecard_id, decision_type, actor, reason_code, decision_json
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (decision_id, scorecard_id, decision, "system", "weekly_auto_build", json.dumps(summary_json)))
            
            # Insert Action items
            for act in actions:
                cur.execute("""
                    INSERT INTO atr_weekly_action_items (
                        action_id, scorecard_id, domain, owner, priority, status, title, reason_code, due_at, action_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    act["action_id"], act["scorecard_id"], act["domain"], act["owner"],
                    act["priority"], act["status"], act["title"], act["reason_code"],
                    act["due_at"], act["action_json"]
                ))
                
            conn.commit()

        # Phase 10.6: Closure Readiness
        closure_info = self.closure_svc.check_stabilization_window(conn)
        summary_json["closure_readiness"] = closure_info

        self.emit_telegram_digest(scorecard_id, week_start, week_end, decision, domains_json, actions, closure_info)
        return scorecard_id

    def emit_telegram_digest(self, scorecard_id, week_start, week_end, decision, domains_json, actions, closure_info=None):
        """Builds and emits the Telegram UX text."""
        msg = [
            "ATR Weekly Operating Scorecard",
            f"Week: {week_start.strftime('%Y-%m-%d')} → {week_end.strftime('%Y-%m-%d')}",
            f"Overall: {decision}",
            ""
        ]
        
        green_domains = [d for d, v in domains_json.items() if v["status"] == "GREEN"]
        yellow_domains = [d for d, v in domains_json.items() if v["status"] == "YELLOW"]
        red_domains = [d for d, v in domains_json.items() if v["status"] == "RED"]
        
        if green_domains:
            msg.append("GREEN:")
            for d in green_domains:
                msg.append(f"- {d}")
        if yellow_domains:
            msg.append("YELLOW:")
            for d in yellow_domains:
                msg.append(f"- {d}")
        if red_domains:
            msg.append("RED:")
            for d in red_domains:
                msg.append(f"- {d}")
                
        if actions:
            msg.append("\nATR Weekly Action Items (P0/P1 Open):")
            for act in actions:
                if act["priority"] in ("P0", "P1"):
                    msg.append(f"- {act['domain']} | owner={act['owner']} | {act['priority']} | {act['title']}")
                    
        if closure_info:
            msg.append("\nProgram Closure Readiness:")
            status_icon = "✅" if closure_info["passed"] else "⏳"
            msg.append(f"{status_icon} Stabilization: {closure_info['days_stable']}/{closure_info.get('required_days', 14)} days")
            msg.append(f"- Reason: {closure_info['reason']}")

        msg.append(f"\nDecision Artifact: {scorecard_id}")
        
        full_text = "\n".join(msg)
        logger.info(f"Telegram Digest emitted:\n{full_text}")
        # Real integration would push to Telegram topics using atr_policy_telegram_pack_service

def main():
    enable = str(os.getenv("ATR_WEEKLY_SCORECARD_ENABLE", "1")).lower() in ("1", "true", "yes")
    enforce = str(os.getenv("ATR_WEEKLY_SCORECARD_ENFORCE", "0")).lower() in ("1", "true", "yes")
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    check_interval = int(os.getenv("ATR_WEEKLY_SCORECARD_INTERVAL_SEC", "86400"))

    if not enable:
        logger.info("Weekly Scorecard bypassed via ENV: ATR_WEEKLY_SCORECARD_ENABLE=0")
        return

    logger.info("Starting ATR Weekly Operating Scorecard Service")
    service = ATRWeeklyScorecardService(enable=enable, enforce=enforce)
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    while True:
        try:
            with get_conn() as conn:
                # Mock week period for the cron
                now = datetime.now(timezone.utc)
                week_start = now - timedelta(days=now.weekday(), hours=now.hour, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
                week_end = week_start + timedelta(days=6)
                
                # Check if this week's scorecard was already built
                with conn.cursor() as cur:
                    try:
                        cur.execute("SELECT scorecard_id FROM atr_weekly_operating_scorecards WHERE week_start = %s", (week_start.date(),))
                        exists = cur.fetchone()
                        if exists:
                            logger.info(f"Scorecard for week {week_start.date()} already built. Skipping.")
                        else:
                            service.build_weekly_scorecard(conn, r, week_start, week_end)
                    except Exception as pg_err:
                        # might fail if migrations not run yet
                        logger.error(f"Waiting for migrations: {pg_err}")
                        conn.rollback()
                        
        except Exception as e:
            logger.error(f"Error in ATR Weekly Scorecard cycle: {e}")
            
        time.sleep(check_interval)

if __name__ == "__main__":
    main()
