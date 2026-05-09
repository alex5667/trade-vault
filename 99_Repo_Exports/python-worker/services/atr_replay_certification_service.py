import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any

import psycopg2.extras
from prometheus_client import Counter, Gauge

from services.analytics_db import get_conn as get_db_connection
from services.atr_model_config_drift_service import ATRModelConfigDriftService

logger = logging.getLogger("atr_replay_certification_service")

# Metrics
GOLDEN_DATASETS_TOTAL = Gauge("atr_golden_datasets_total", "Total golden datasets", ["dataset_class", "status"])
REPLAY_CERT_RUNS_TOTAL = Counter("atr_replay_cert_runs_total", "Total cert runs", ["change_class", "status"])
REPLAY_CERT_CHECKS_TOTAL = Counter("atr_replay_cert_checks_total", "Total cert checks", ["check_name", "status", "severity"])
REPLAY_CERT_FAIL_TOTAL = Counter("atr_replay_cert_fail_total", "Total replay cert fail metrics", ["dataset_class"])
GOLDEN_DATASET_REVIEW_TOTAL = Counter("atr_golden_dataset_review_total", "Total reviews", ["review_status"])

ATR_REPLAY_CERT_ENABLE = os.getenv("ATR_REPLAY_CERT_ENABLE", "1") == "1"
ATR_REPLAY_CERT_ENFORCE = os.getenv("ATR_REPLAY_CERT_ENFORCE", "0") == "1"

class ATRReplayCertificationService:
    @staticmethod
    def classify_datasets_for_change(change_class: str) -> list[str]:
        """Matrix matching change classes to required dataset classes."""
        if change_class in ["LOW_RISK_CONFIG", "LOW_RISK_OBSERVABILITY"]:
            return ["SMOKE_GOLDEN"]
        if change_class == "MEDIUM_POLICY":
            return ["SMOKE_GOLDEN", "CANARY_GOLDEN", "RUNTIME_GOLDEN"]
        if change_class == "HIGH_GOVERNANCE":
            return ["SMOKE_GOLDEN", "CANARY_GOLDEN", "RUNTIME_GOLDEN", "RELEASE_WINDOW_GOLDEN"]
        if change_class == "CRITICAL_RUNTIME_GATING":
            return ["RUNTIME_GOLDEN", "CANARY_GOLDEN", "RELEASE_WINDOW_GOLDEN"]
        if change_class == "CRITICAL_EXECUTION_TOUCHING":
            return ["EXECUTION_GOLDEN", "RUNTIME_GOLDEN", "CANARY_GOLDEN", "RELEASE_WINDOW_GOLDEN"]
        if change_class == "PROTECTIVE_PATH_TOUCHING":
            return ["PROTECTIVE_GOLDEN"]
        if change_class == "DR_RESTORE":
            return ["DR_RESTORE_GOLDEN"]
        return ["SMOKE_GOLDEN"]

    @staticmethod
    def select_required_datasets(change_class: str) -> list[dict[str, Any]]:
        required_classes = ATRReplayCertificationService.classify_datasets_for_change(change_class)
        datasets = []
        with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            for cls in required_classes:
                cur.execute("""
                        SELECT * FROM atr_golden_datasets 
                        WHERE dataset_class = %s AND status = 'ACTIVE'
                    """, (cls,))
                runs = cur.fetchall()
                datasets.extend([dict(r) for r in runs])
        return datasets

    @staticmethod
    def compare_replay_outputs(baseline_ref: str, candidate_ref: str, defect: str | None = None) -> list[dict[str, Any]]:
        """
        Compare replay output artifacts.

        `baseline_ref` and `candidate_ref` must point to JSON or JSONL replay
        artifacts available to this process. Missing artifacts fail closed:
        release gates must never pass on synthetic replay evidence.
        """
        if defect:
            failed = "S1_signal_id_stability" if defect == "signal_id_mismatch" else "S2_allow_clip_deny"
            return [
                {"check_name": failed, "status": "failed", "severity": "critical", "details": {"defect": defect}},
            ]

        baseline, baseline_err = ATRReplayCertificationService._load_replay_ref(baseline_ref)
        candidate, candidate_err = ATRReplayCertificationService._load_replay_ref(candidate_ref)

        if baseline_err or candidate_err:
            return [
                {
                    "check_name": "S0_replay_artifacts_available",
                    "status": "failed",
                    "severity": "critical",
                    "details": {
                        "baseline_ref": baseline_ref,
                        "candidate_ref": candidate_ref,
                        "baseline_error": baseline_err,
                        "candidate_error": candidate_err,
                    },
                }
            ]

        checks = [
            ATRReplayCertificationService._check_record_count(baseline, candidate),
            ATRReplayCertificationService._check_signal_id_stability(baseline, candidate),
            ATRReplayCertificationService._check_policy_decisions(baseline, candidate),
            ATRReplayCertificationService._check_protective_lifecycle(baseline, candidate),
        ]
        return checks

    @staticmethod
    def _load_replay_ref(ref: str) -> tuple[list[dict[str, Any]], str | None]:
        if not ref:
            return [], "empty_ref"

        path = Path(str(ref))
        if path.is_dir():
            for name in ("replay_output.jsonl", "replay_outputs.jsonl", "replay_output.json", "replay_outputs.json"):
                candidate = path / name
                if candidate.exists():
                    path = candidate
                    break

        if not path.exists() or not path.is_file():
            return [], "ref_not_found"

        try:
            if path.suffix.lower() == ".jsonl":
                records = []
                for line in path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line:
                        item = json.loads(line)
                        if isinstance(item, dict):
                            records.append(item)
                return records, None

            obj = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(obj, list):
                return [x for x in obj if isinstance(x, dict)], None
            if isinstance(obj, dict):
                for key in ("records", "outputs", "events", "decisions", "signals"):
                    val = obj.get(key)
                    if isinstance(val, list):
                        return [x for x in val if isinstance(x, dict)], None
                return [obj], None
            return [], "unsupported_json_shape"
        except Exception as exc:
            return [], f"parse_error:{type(exc).__name__}"

    @staticmethod
    def _record_id(record: dict[str, Any]) -> str:
        for key in ("signal_id", "sid", "decision_id", "id"):
            val = record.get(key)
            if val not in (None, ""):
                return str(val)
        return json.dumps(record, sort_keys=True, default=str)

    @staticmethod
    def _project(record: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
        return {key: record.get(key) for key in keys if key in record}

    @staticmethod
    def _index(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return {ATRReplayCertificationService._record_id(r): r for r in records}

    @staticmethod
    def _check_record_count(baseline: list[dict[str, Any]], candidate: list[dict[str, Any]]) -> dict[str, Any]:
        ok = len(baseline) == len(candidate)
        return {
            "check_name": "S0_record_count",
            "status": "passed" if ok else "failed",
            "severity": "critical",
            "details": {"baseline_count": len(baseline), "candidate_count": len(candidate)},
        }

    @staticmethod
    def _check_signal_id_stability(baseline: list[dict[str, Any]], candidate: list[dict[str, Any]]) -> dict[str, Any]:
        b_ids = [ATRReplayCertificationService._record_id(r) for r in baseline]
        c_ids = [ATRReplayCertificationService._record_id(r) for r in candidate]
        ok = b_ids == c_ids
        return {
            "check_name": "S1_signal_id_stability",
            "status": "passed" if ok else "failed",
            "severity": "critical",
            "details": {"baseline_only": sorted(set(b_ids) - set(c_ids))[:20], "candidate_only": sorted(set(c_ids) - set(b_ids))[:20]},
        }

    @staticmethod
    def _check_policy_decisions(baseline: list[dict[str, Any]], candidate: list[dict[str, Any]]) -> dict[str, Any]:
        keys = ("allow", "deny", "clip", "action", "risk_action", "side", "symbol", "entry_allowed")
        b_idx = ATRReplayCertificationService._index(baseline)
        c_idx = ATRReplayCertificationService._index(candidate)
        mismatches = []
        for rid in sorted(set(b_idx) & set(c_idx)):
            b_proj = ATRReplayCertificationService._project(b_idx[rid], keys)
            c_proj = ATRReplayCertificationService._project(c_idx[rid], keys)
            if b_proj != c_proj:
                mismatches.append({"id": rid, "baseline": b_proj, "candidate": c_proj})
                if len(mismatches) >= 20:
                    break
        return {
            "check_name": "S2_allow_clip_deny",
            "status": "passed" if not mismatches else "failed",
            "severity": "critical",
            "details": {"mismatches": mismatches},
        }

    @staticmethod
    def _check_protective_lifecycle(baseline: list[dict[str, Any]], candidate: list[dict[str, Any]]) -> dict[str, Any]:
        keys = ("protective_state", "protection_state", "sl_state", "tp_state", "flatten_state", "lifecycle_state")
        b_idx = ATRReplayCertificationService._index(baseline)
        c_idx = ATRReplayCertificationService._index(candidate)
        mismatches = []
        for rid in sorted(set(b_idx) & set(c_idx)):
            b_proj = ATRReplayCertificationService._project(b_idx[rid], keys)
            c_proj = ATRReplayCertificationService._project(c_idx[rid], keys)
            if b_proj != c_proj:
                mismatches.append({"id": rid, "baseline": b_proj, "candidate": c_proj})
                if len(mismatches) >= 20:
                    break
        return {
            "check_name": "S7_protective_lifecycle",
            "status": "passed" if not mismatches else "failed",
            "severity": "critical",
            "details": {"mismatches": mismatches},
        }

    @staticmethod
    def decide_replay_cert_status(checks: list[dict[str, Any]]) -> tuple[str, str]:
        has_critical_fail = False
        has_warn_fail = False

        for c in checks:
            if c["status"] == "failed":
                if c["severity"] == "critical":
                    has_critical_fail = True
                else:
                    has_warn_fail = True

        if has_critical_fail:
            return "failed", "critical failures detected"
        if has_warn_fail:
            return "passed_with_warnings", "non-critical checks failed"
        return "passed", "all checks passed"

    @classmethod
    def run_replay_certification(cls, change_id: str, change_class: str, baseline_ref: str, candidate_ref: str, defect: str | None = None) -> list[str]:
        if not ATR_REPLAY_CERT_ENABLE:
            logger.info("Replay certification disabled.")
            return []

        required_datasets = cls.select_required_datasets(change_class)
        run_ids = []

        with get_db_connection() as conn:
            for ds in required_datasets:
                v_status, _ = ATRModelConfigDriftService.check_dataset_validity(ds['dataset_id'])
                if v_status in ["expired", "missing"] and ATR_REPLAY_CERT_ENFORCE:
                    logger.warning(f"Replay certification blocked: dataset {ds['dataset_id']} is {v_status}")
                    continue

                run_id = f"cert_{uuid.uuid4().hex[:8]}"
                run_ids.append(run_id)

                checks = cls.compare_replay_outputs(baseline_ref, candidate_ref, defect)
                status, summary_msg = cls.decide_replay_cert_status(checks)

                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO atr_replay_cert_runs 
                        (run_id, change_id, change_class, dataset_id, status, baseline_ref, candidate_ref, summary_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (run_id, change_id, change_class, ds['dataset_id'], status, baseline_ref, candidate_ref, json.dumps({"msg": summary_msg})))

                    for c in checks:
                        check_id = f"chk_{uuid.uuid4().hex[:8]}"
                        cur.execute("""
                            INSERT INTO atr_replay_cert_checks 
                            (check_id, run_id, check_name, status, severity, details_json)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (check_id, run_id, c['check_name'], c['status'], c['severity'], json.dumps(c.get('details', {}))))

                        REPLAY_CERT_CHECKS_TOTAL.labels(check_name=c['check_name'], status=c['status'], severity=c['severity']).inc()

                    if status == "failed":
                        REPLAY_CERT_FAIL_TOTAL.labels(dataset_class=ds['dataset_class']).inc()

                REPLAY_CERT_RUNS_TOTAL.labels(change_class=change_class, status=status).inc()
            conn.commit()

        return run_ids

    @staticmethod
    def get_cert_status_for_change(change_id: str, change_class: str) -> str | None:
        required_classes = ATRReplayCertificationService.classify_datasets_for_change(change_class)

        with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                    SELECT r.status, d.dataset_class 
                    FROM atr_replay_cert_runs r
                    JOIN atr_golden_datasets d ON r.dataset_id = d.dataset_id
                    WHERE r.change_id = %s
                """, (change_id,))
            runs = cur.fetchall()

        if not runs:
            return "missing"

        evaluated_classes = set(r['dataset_class'] for r in runs)
        for req in required_classes:
            if req not in evaluated_classes:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT count(1) FROM atr_golden_datasets WHERE dataset_class = %s AND status = 'ACTIVE'", (req,))
                        has_active = cur.fetchone()[0] > 0
                if has_active:
                    return "incomplete"

        for r in runs:
            if r['status'] == "failed":
                return "failed"

        statuses = [r['status'] for r in runs]
        if "passed_with_warnings" in statuses:
            return "passed_with_warnings"
        return "passed"

    @staticmethod
    def review_golden_dataset(dataset_id: str, reviewer: str, status: str, comments: str) -> str:
        review_id = f"rev_{uuid.uuid4().hex[:8]}"
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO atr_golden_dataset_reviews 
                    (review_id, dataset_id, reviewer, review_status, review_json)
                    VALUES (%s, %s, %s, %s, %s)
                """, (review_id, dataset_id, reviewer, status, json.dumps({"comments": comments})))

                if status == "approved":
                    cur.execute("UPDATE atr_golden_datasets SET status = 'APPROVED' WHERE dataset_id = %s AND status = 'CANDIDATE'", (dataset_id,))
            conn.commit()

        GOLDEN_DATASET_REVIEW_TOTAL.labels(review_status=status).inc()
        return review_id

    @staticmethod
    def register_candidate_dataset(dataset_id: str, dataset_class: str, scope: dict[str, Any], manifest: dict[str, Any], owner: str, reason: str) -> None:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO atr_golden_datasets 
                    (dataset_id, dataset_class, status, scope_json, manifest_json, owner, reason_code)
                    VALUES (%s, %s, 'CANDIDATE', %s, %s, %s, %s)
                """, (dataset_id, dataset_class, json.dumps(scope), json.dumps(manifest), owner, reason))
            conn.commit()

    @staticmethod
    def activate_dataset(dataset_id: str, approver: str) -> bool:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status, dataset_class FROM atr_golden_datasets WHERE dataset_id = %s", (dataset_id,))
                row = cur.fetchone()
                if not row or row[0] != 'APPROVED':
                    return False

                ds_class = row[1]
                cur.execute("UPDATE atr_golden_datasets SET status = 'RETIRED', retired_at = now() WHERE dataset_class = %s AND status = 'ACTIVE'", (ds_class,))
                cur.execute("UPDATE atr_golden_datasets SET status = 'ACTIVE', activated_at = now(), approver = %s WHERE dataset_id = %s", (approver, dataset_id))
            conn.commit()
        return True
