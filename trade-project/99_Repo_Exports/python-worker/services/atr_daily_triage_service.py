#!/usr/bin/env python3
"""
ATR Daily Triage Service (Phase 9.2)
Forms the canonical daily triage board and operational oncall workflow.
"""

import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta

import redis

from common.log import setup_logger
from services.analytics_db import get_conn

logger = setup_logger("atr_daily_triage_service")

DOMAINS = [
    "signal_gates",
    "dispatch_runtime",
    "execution",
    "protective",
    "control_plane"
]

class ATRDailyTriageService:
    def __init__(self, enable: bool = True, enforce: bool = False):
        self.enable = enable
        self.enforce = enforce
        self.status_rank = {"GREEN": 0, "YELLOW": 1, "RED": 2, "BLACK": 3}

    def generate_board_id(self, day: datetime) -> str:
        return f"day_{day.strftime('%Y_%m_%d')}_{uuid.uuid4().hex[:6]}"

    def generate_id(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:10]}"

    def _get_metrics_for_section(self, section: str, conn, r) -> dict:
        """
        Mock telemetry for the daily triage workflow.
        In reality, fetches from PromQL or Redis streams.
        """
        metrics = {}
        if section == "signal_gates":
            metrics = {
                "signals_total": 450,
                "tradeable_total": 80,
                "veto_total": 370,
                "veto_top": {"book_stale": 31, "negative_ev": 52},
                "unusual_veto_drift": False
            }
        elif section == "dispatch_runtime":
            metrics = {
                "raw_publish_ok_rate": 0.999,
                "order_queue_publish_ok_rate": 0.999,
                "dedup_hit_rate": 0.05,
                "runtime_critical_drifts": 0
            }
        elif section == "execution":
            metrics = {
                "mt5_requotes_total": 14,
                "connection_bursts": 3,
                "venue_degradation": False
            }
        elif section == "protective":
            metrics = {
                "tp1_reached_count": 45,
                "be_activations": 42,
                "trailing_activations": 30,
                "be_before_tp1": 0,
                "sl_ratchet_backwards": 0,
                "unresolved_protective_drifts": 0
            }
        elif section == "control_plane":
            metrics = {
                "graph_cert_status": "passed",
                "open_overrides": 2,
                "expired_overrides_active": 0,
                "open_critical_drifts": 0,
                "authority_violations": 0,
                "overdue_actions_p0_p1": 0
            }
        # Add active quarantines count
        q_class = None
        if section == "signal_gates":
            q_class = "SIGNAL_GATE_QUARANTINE"
        elif section == "execution":
            q_class = "EXECUTION_VENUE_QUARANTINE"
        elif section == "protective":
            q_class = "PROTECTIVE_PATH_QUARANTINE"
        elif section == "control_plane":
            q_class = "CONTROL_PLANE_QUARANTINE"
        elif section == "dispatch_runtime":
            q_class = "POST_TRADE_FEEDBACK_QUARANTINE"

        active_q_count = 0
        if q_class:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM atr_release_quarantines WHERE quarantine_class = %s AND status NOT IN ('RELEASE_ELIGIBLE', 'WAIVED', 'NOT_QUARANTINED')", (q_class,))
                    res = cur.fetchone()
                    if res:
                        active_q_count = res[0]
            except Exception:
                conn.rollback()

        metrics["active_quarantines"] = active_q_count
        return metrics

    def derive_section_status(self, section: str, metrics: dict) -> str:
        status = "GREEN"

        if section == "signal_gates":
            veto_top = metrics.get("veto_top", {})
            if veto_top.get("book_stale", 0) > 50 or metrics.get("unusual_veto_drift"):
                status = "RED"
            elif veto_top.get("negative_ev", 0) > 40 or veto_top.get("book_stale", 0) > 20:
                status = "YELLOW"

        elif section == "dispatch_runtime":
            if metrics.get("runtime_critical_drifts", 0) > 0 or metrics.get("order_queue_publish_ok_rate", 1.0) < 0.99:
                status = "RED"

        elif section == "execution":
            requotes = metrics.get("mt5_requotes_total", 0)
            bursts = metrics.get("connection_bursts", 0)
            if bursts >= 5:
                status = "BLACK"
            elif bursts >= 2 or requotes > 10 or metrics.get("venue_degradation"):
                status = "RED"
            elif requotes > 5:
                status = "YELLOW"

        elif section == "protective":
            if metrics.get("be_before_tp1", 0) > 0 or metrics.get("sl_ratchet_backwards", 0) > 0 or metrics.get("unresolved_protective_drifts", 0) > 0:
                status = "BLACK"

        elif section == "control_plane":
            if metrics.get("graph_cert_status") == "failed" or metrics.get("authority_violations", 0) > 0:
                status = "BLACK"
            elif metrics.get("open_critical_drifts", 0) > 0 or metrics.get("expired_overrides_active", 0) > 0:
                status = "RED"
            elif metrics.get("open_overrides", 0) > 0 or metrics.get("overdue_actions_p0_p1", 0) > 0:
                status = "YELLOW"

        if metrics.get("active_quarantines", 0) > 0 and status == "GREEN":
            status = "YELLOW"

        return status

    def propose_daily_decision(self, section_statuses: dict, section_metrics: dict) -> list:
        counts = {"GREEN": 0, "YELLOW": 0, "RED": 0, "BLACK": 0}
        for s in section_statuses.values():
            counts[s] += 1

        decisions = []
        if counts["BLACK"] > 0:
            if section_statuses.get("control_plane") == "BLACK":
                decisions.append("FREEZE_RELEASES")
                decisions.append("ROLLBACK_REVIEW")
            elif section_statuses.get("protective") == "BLACK":
                decisions.append("INCIDENT_OPEN")
                decisions.append("FREEZE_SCOPE")
            elif section_statuses.get("execution") == "BLACK":
                decisions.append("INCIDENT_OPEN")
            else:
                decisions.append("INCIDENT_OPEN")

        elif counts["RED"] > 0:
            if section_statuses.get("control_plane") == "RED":
                # Repeated control-plane issues would ideally query past states
                decisions.append("FREEZE_RELEASES")
            if section_statuses.get("execution") == "RED" or section_statuses.get("dispatch_runtime") == "RED":
                decisions.append("SAME_DAY_FIX")
            if not decisions: # fallback if a red didn't trigger specific
                decisions.append("SAME_DAY_FIX")

        elif counts["YELLOW"] > 0:
            decisions.append("WATCH")

        if not decisions:
            decisions.append("NO_ACTION")

        return decisions

    def suggest_daily_actions(self, board_id: str, section_statuses: dict, section_metrics: dict) -> list:
        actions = []
        owner_mapping = {
            "signal_gates": "signal_owner",
            "dispatch_runtime": "execution_owner",
            "execution": "execution_owner",
            "protective": "protective_owner",
            "control_plane": "control_plane_owner"
        }

        for section, status in section_statuses.items():
            if status in ("RED", "BLACK"):
                priority = "P0" if status == "BLACK" else "P1"
                due_delta = timedelta(hours=1) if priority == "P0" else timedelta(hours=12) # same day ideally

                reason_code = f"{section}_{status.lower()}"
                mets = section_metrics.get(section, {})

                if section == "protective" and status == "BLACK":
                    reason_code = "protective_invariant_violation"
                elif section == "execution" and status in ("RED", "BLACK"):
                    reason_code = "execution_venue_degraded"
                elif section == "dispatch_runtime" and status == "RED":
                    reason_code = "publish_route_failure"

                actions.append({
                    "action_id": self.generate_id("act"),
                    "board_id": board_id,
                    "section": section,
                    "owner": owner_mapping.get(section, "oncall_operator"),
                    "priority": priority,
                    "status": "open",
                    "title": f"Resolve {status} condition in {section}",
                    "reason_code": reason_code,
                    "due_at": datetime.now(UTC) + due_delta,
                    "action_json": json.dumps({"metrics_snapshot": mets})
                })
        return actions

    def build_daily_triage_board(self, conn, r, day_start: datetime, custom_metrics: dict = None) -> dict:  # type: ignore
        if not self.enable:
            logger.info("Daily Triage Board skipped, enable=False")
            return None  # type: ignore

        board_id = self.generate_board_id(day_start)
        all_metrics = {}
        section_statuses = {}

        overall_status_val = 0

        for section in DOMAINS:
            if custom_metrics and section in custom_metrics:
                mets = custom_metrics[section]
            else:
                mets = self._get_metrics_for_section(section, conn, r)
            all_metrics[section] = mets

            # Allow custom statuses via custom_metrics if defined
            if custom_metrics and section in custom_metrics and custom_metrics.get(f"{section}_status"):
                 st = custom_metrics[f"{section}_status"]
            else:
                 st = self.derive_section_status(section, mets)

            section_statuses[section] = st

            if self.status_rank[st] > overall_status_val:
                overall_status_val = self.status_rank[st]

        reverse_status = {v: k for k, v in self.status_rank.items()}
        overall_status = reverse_status[overall_status_val]

        decisions = self.propose_daily_decision(section_statuses, all_metrics)
        primary_decision = decisions[0]
        actions = self.suggest_daily_actions(board_id, section_statuses, all_metrics)

        sections_json = {}
        for section in DOMAINS:
            sections_json[section] = {
                "status": section_statuses[section],
                "metrics": all_metrics[section]
            }

        constraints = []
        if overall_status in ("RED", "BLACK"):
            constraints.append("Review mandatory prior to release window.")
        if "execution" in [s for s,st in section_statuses.items() if st in ("RED", "BLACK")]:
             constraints.append("Investigate MT5 connection/requote bursts.")

        summary_json = {
            "primary_decision": primary_decision,
            "all_decisions": decisions,
            "constraints": constraints
        }

        # Format output
        board_data = {
            "board_id": board_id,
            "day": day_start.strftime("%Y-%m-%d"),
            "overall_status": overall_status,
            "sections": sections_json,
            "summary": summary_json
        }

        with conn.cursor() as cur:
            # Insert board
            cur.execute("""
                INSERT INTO atr_daily_triage_boards (
                    board_id, day, overall_status, sections_json, summary_json
                ) VALUES (%s, %s, %s, %s, %s)
            """, (board_id, day_start.date(), overall_status, json.dumps(sections_json), json.dumps(summary_json)))

            # Insert decisions generator
            for dec in decisions:
                decision_id = self.generate_id("dec")
                cur.execute("""
                    INSERT INTO atr_daily_triage_decisions (
                        decision_id, board_id, decision_type, actor, reason_code, decision_json
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (decision_id, board_id, dec, "auto_builder", "daily_computed", json.dumps({"components": list(section_statuses.keys())})))

            # Insert Actions
            for act in actions:
                cur.execute("""
                    INSERT INTO atr_daily_triage_actions (
                        action_id, board_id, section, owner, priority, status, title, reason_code, due_at, action_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    act["action_id"], act["board_id"], act["section"], act["owner"],
                    act["priority"], act["status"], act["title"], act["reason_code"],
                    act["due_at"], act["action_json"]
                ))

            conn.commit()

        self.emit_telegram_digest(board_data, actions)
        return board_data

    def emit_telegram_digest(self, board_data: dict, actions: list):
        summary = board_data["summary"]
        sections = board_data["sections"]

        msg = [
            "ATR Daily Triage Board",
            f"\nDay: {board_data['day']}",
            f"Overall: {board_data['overall_status']}\n"
        ]

        green_sec = [d for d, v in sections.items() if v["status"] == "GREEN"]
        yellow_sec = [d for d, v in sections.items() if v["status"] == "YELLOW"]
        red_sec = [d for d, v in sections.items() if v["status"] == "RED"]
        black_sec = [d for d, v in sections.items() if v["status"] == "BLACK"]

        if green_sec:
            msg.append("GREEN:")
            for d in green_sec:
                msg.append(f"- {d}")

        if yellow_sec:
            msg.append("\nYELLOW:")
            for d in yellow_sec:
                msg.append(f"- {d}")

        if red_sec:
            msg.append("\nRED:")
            for d in red_sec:
                msg.append(f"- {d}")

        if black_sec:
            msg.append("\nBLACK:")
            for d in black_sec:
                msg.append(f"- {d}")

        msg.append("\nDecision:")
        for dec in summary.get("all_decisions", [summary.get("primary_decision")]):
            msg.append(f"- {dec}")

        if summary.get("constraints"):
            msg.append("\nConstraints:")
            for c in summary.get("constraints"):
                msg.append(f"- {c}")

        if black_sec:
            msg.append("\nATR Daily Triage Escalation")
            for sect in black_sec:
                msg.append(f"\nSection: {sect}\nStatus: BLACK\nAction: escalated to INCIDENT_OPEN / FREEZE_SCOPE")

        full_text = "\n".join(msg)
        logger.info(f"Telegram Digest emitted:\n{full_text}")


def main():
    enable = os.getenv("ATR_DAILY_TRIAGE_ENABLE", "1").lower() in ("1", "true", "yes")
    enforce = os.getenv("ATR_DAILY_TRIAGE_ENFORCE", "0").lower() in ("1", "true", "yes")
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    check_interval = int(os.getenv("ATR_DAILY_TRIAGE_INTERVAL_SEC", "3600"))

    if not enable:
        logger.info("Daily Triage Board bypassed via ENV: ATR_DAILY_TRIAGE_ENABLE=0")
        return

    logger.info("Starting ATR Daily Triage Service")
    service = ATRDailyTriageService(enable=enable, enforce=enforce)
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    while True:
        try:
            with get_conn() as conn:
                day_start = datetime.now(UTC)
                with conn.cursor() as cur:
                    try:
                        cur.execute("SELECT board_id FROM atr_daily_triage_boards WHERE day = %s", (day_start.date(),))
                        exists = cur.fetchone()
                        if exists:
                            logger.info(f"Daily board for {day_start.date()} already built. Skipping.")
                        else:
                            service.build_daily_triage_board(conn, r, day_start)
                    except Exception as pg_err:
                        logger.error(f"Waiting for migrations: {pg_err}")
                        conn.rollback()

        except Exception as e:
            logger.error(f"Error in ATR Daily Triage cycle: {e}")

        time.sleep(check_interval)

if __name__ == "__main__":
    main()
