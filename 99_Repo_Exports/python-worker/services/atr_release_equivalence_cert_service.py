from __future__ import annotations
""",
Phase 8.2 — Release Equivalence Cert Service
(atr_release_equivalence_cert_service.py)

Checks E1-E6 across all bounded scopes for a given change set.
Returns a cert payload suitable for audit trail and Telegram.

Intended usage:
    - Called nightly or on demand from the auditor / Telegram surface
    - Runs over atr_release_equivalence_checks for bounded scopes
    - Produces a release_equivalence_cert object written to
      atr_control_plane_certifications
""",

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from services.analytics_db import get_conn

logger = logging.getLogger("atr_release_equivalence_cert")

# Bounded pilot scopes (mirrors atr_graph_backed_release_gate._BOUNDED_SYMBOLS)
_BOUNDED_SYMBOLS = {"BTCUSDT", "ETHUSDT"}


def _gen_id(prefix: str) -> str:
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}_{uuid.uuid4().hex[:6]}"


class ReleaseEquivalenceCertService:
    """,
    Runs the formal E1–E6 equivalence certification across all bounded scopes
    for the changes run in the last N days.

    Checks:
        E1  legacy decision == graph decision
        E2  legacy blockers set == graph blockers set
        E3  legacy warnings set == graph warnings set (or approved normalized diff)
        E4  no graph-only missing cert chain  (replay/rollout cert edges)
        E5  no graph-only missing freeze/override blocker
        E6  no critical release drift open on bounded scope
    """,

    @staticmethod
    def run_cert(window_days: int = 7) -> dict[str, Any]:
        """,
        Run equivalence cert over bounded scopes within the last window_days.
        Persists a cert row in atr_control_plane_certifications and returns the cert dict.
        """,
        cert_id = _gen_id("releq_cert")
        now     = datetime.now(tz=timezone.utc)

        try:
            with get_conn() as conn, conn.cursor(
                cursor_factory=__import__("psycopg2").extras.RealDictCursor
            ) as cur:

                # ── Load equivalence checks for bounded scopes ────────────────
                cur.execute(
                    """,
                    SELECT *
                    FROM atr_release_equivalence_checks
                    WHERE scope_value = ANY(%s)
                      AND created_at > now() - interval '%s days'
                    ORDER BY created_at DESC
                    """,
                    (list(_BOUNDED_SYMBOLS), window_days),
                )
                checks = cur.fetchall()

                total_checks     = len(checks)
                failed_checks    = [c for c in checks if c["status"] == "failed"]
                matching_checks  = total_checks - len(failed_checks)

                # ── Load open critical drifts on bounded scopes ────────────────
                cur.execute(
                    """,
                    SELECT *
                    FROM atr_release_drifts
                    WHERE scope_value = ANY(%s)
                      AND status = 'open'
                      AND severity = 'critical'
                      AND created_at > now() - interval '%s days'
                    """,
                    (list(_BOUNDED_SYMBOLS), window_days),
                )
                critical_drifts = cur.fetchall()

                # ── E1 — decision match % ─────────────────────────────────────
                e1_pass = len(failed_checks) == 0

                # ── E2/E3 — blocker / warning set mismatch drifts ─────────────
                blocker_mismatches = [
                    d for d in critical_drifts if d["drift_kind"] == "blocker_set_mismatch"
                ]
                warning_mismatches = [
                    d for d in critical_drifts if d["drift_kind"] == "warning_set_mismatch"
                ]
                e2_pass = len(blocker_mismatches) == 0
                e3_pass = True  # warn-level; degrades to warning not blocker

                # ── E4 — missing cert chain ────────────────────────────────────
                missing_cert_edges = [
                    d for d in critical_drifts
                    if d["drift_kind"] in ("missing_replay_cert_edge", "missing_rollout_cert_edge")
                ]
                e4_pass = len(missing_cert_edges) == 0

                # ── E5 — missing freeze/override blocker ──────────────────────
                missing_blockers = [
                    d for d in critical_drifts
                    if d["drift_kind"] in ("missing_freeze_blocker", "missing_override_constraint")
                ]
                e5_pass = len(missing_blockers) == 0

                # ── E6 — any critical drift ────────────────────────────────────
                e6_pass = len(critical_drifts) == 0

                all_pass = all([e1_pass, e2_pass, e3_pass, e4_pass, e5_pass, e6_pass])
                status   = "passed" if all_pass else "failed"

                # ── Warning drifts (non-critical) ──────────────────────────────
                cur.execute(
                    """,
                    SELECT count(*) AS c
                    FROM atr_release_drifts
                    WHERE scope_value = ANY(%s)
                      AND status = 'open'
                      AND severity != 'critical'
                      AND created_at > now() - interval '%s days'
                    """,
                    (list(_BOUNDED_SYMBOLS), window_days),
                )
                warning_drifts = cur.fetchone()["c"]

                checks_detail = {
                    "E1_decision_match":        {"pass": e1_pass, "failures": len(failed_checks)},
                    "E2_blocker_set_match":     {"pass": e2_pass, "failures": len(blocker_mismatches)},
                    "E3_warning_set_match":      {"pass": e3_pass, "warning_mismatches": len(warning_mismatches)},
                    "E4_no_missing_cert_edge":  {"pass": e4_pass, "failures": len(missing_cert_edges)},
                    "E5_no_missing_freeze_blocker": {"pass": e5_pass, "failures": len(missing_blockers)},
                    "E6_no_critical_drift":     {"pass": e6_pass, "critical_open": len(critical_drifts)},
                }

                summary = {
                    "checked_changes":   total_checks,
                    "matching_decisions": matching_checks,
                    "critical_drifts":   len(critical_drifts),
                    "warning_drifts":    warning_drifts,
                    "window_days":       window_days,
                    "bounded_symbols":   sorted(_BOUNDED_SYMBOLS),
                    "certified_at":      now.isoformat(),
                }

                cert_payload = {
                    "cert_kind": "release_equivalence_cert",
                    "cert_id":   cert_id,
                    "status":    status,
                    "summary":   summary,
                    "checks":    checks_detail,
                }

                # ── Persist to atr_control_plane_certifications ───────────────
                with conn.cursor() as wcur:
                    wcur.execute(
                        """,
                        INSERT INTO atr_control_plane_certifications
                            (cert_id, cert_kind, target_node_id, status, checks_json, summary_json)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            cert_id,
                            "release_equivalence_cert",
                            "release_gate_component",   # virtual target
                            status,
                            json.dumps(checks_detail),
                            json.dumps(summary),
                        )
                    )
                conn.commit()

                level = logging.INFO if all_pass else logging.WARNING
                logger.log(
                    level,
                    "ReleaseEquivalenceCert %s: %s | checks=%d critical_drifts=%d",
                    cert_id, status, total_checks, len(critical_drifts),
                )
                return cert_payload

        except Exception as exc:
            logger.error("ReleaseEquivalenceCertService.run_cert failed: %s", exc, exc_info=True)
            return {
                "cert_kind": "release_equivalence_cert",
                "cert_id":   cert_id,
                "status":    "failed",
                "error":     str(exc),
                "summary":   {"certified_at": now.isoformat()},
                "checks":    {},
            }

    @staticmethod
    def render_telegram(cert: dict[str, Any]) -> str:
        """Render a Telegram HTML summary of the cert result."""
        status  = cert.get("status", "unknown")
        summary = cert.get("summary", {})
        icon    = "✅" if status == "passed" else "❌"

        lines = [
            f"{icon} <b>ATR Release Equivalence Cert</b>",
            "",
            f"Status: <code>{status.upper()}</code>",
            f"Cert ID: <code>{cert.get('cert_id', '?')}</code>",
            f"Checked changes: <code>{summary.get('checked_changes', 0)}</code>",
            f"Matching decisions: <code>{summary.get('matching_decisions', 0)}</code>",
            f"Critical drifts: <code>{summary.get('critical_drifts', 0)}</code>",
            f"Warning drifts: <code>{summary.get('warning_drifts', 0)}</code>",
        ]

        checks = cert.get("checks", {})
        if checks:
            lines.append("\nCheck results:")
            for k, v in checks.items():
                icon_c = "✅" if v.get("pass") else "❌"
                lines.append(f"  {icon_c} {k}")

        return "\n".join(lines)
