#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
import sys

from ml_analysis.tools.build_ofc_contextual_bundle_v1 import build_bundle
from ml_analysis.tools.build_ofc_contextual_dataset_v1 import build_dataset
from ml_analysis.tools.train_ofc_exec_cost_v1 import train_exec_cost_model
from ml_analysis.tools.train_ofc_rule_success_v1 import train_rule_success_model

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("ofc_contextual_nightly")

def main():
    decisions_ndjson = os.environ.get('OFC_CTX_DECISIONS_NDJSON', '/var/lib/trade/training/latest_confirm_train_v7.ndjson')
    outcomes_ndjson = os.environ.get('OFC_CTX_OUTCOMES_NDJSON', '/var/lib/trade/training/latest_outcomes.ndjson')

    # Work dir for intermediate files
    work_dir = os.environ.get('OFC_CTX_WORK_DIR', '/var/lib/trade/ofc_ctx')
    os.makedirs(work_dir, exist_ok=True)

    # Registry & Promote
    registry_dir = os.environ.get('OFC_CTX_REGISTRY_DIR', '/var/lib/trade/models/ofc_contextual_registry')
    promote_dir = os.environ.get('OFC_CTX_PROMOTE_DIR', '/var/lib/trade/models/ofc_ctx_champion')

    # Output paths
    out_exec_cost_jsonl = os.path.join(work_dir, 'exec_cost.jsonl')
    out_rule_success_jsonl = os.path.join(work_dir, 'rule_success.jsonl')
    out_dataset_report = os.path.join(work_dir, 'dataset_report.json')

    exec_model_json = os.path.join(work_dir, 'exec_model.json')
    exec_report_json = os.path.join(work_dir, 'exec_report.json')

    rule_model_json = os.path.join(work_dir, 'rule_model.json')
    rule_report_json = os.path.join(work_dir, 'rule_report.json')

    logger.info("Building OFC Contextual Dataset...")
    if not os.path.exists(decisions_ndjson) or not os.path.exists(outcomes_ndjson):
        logger.warning(f"Missing decisions ({decisions_ndjson}) or outcomes ({outcomes_ndjson}) file. Skipping run.")
        return 0

    try:
        report = build_dataset(
            decisions_jsonl=decisions_ndjson,
            outcomes_jsonl=outcomes_ndjson,
            out_exec_cost_jsonl=out_exec_cost_jsonl,
            out_rule_success_jsonl=out_rule_success_jsonl,
            out_report_json=out_dataset_report,
            success_bps=0.0
        )
        logger.info(f"Dataset Built: {report}")

        logger.info("Training Exec Cost Model...")
        exec_cost_report = train_exec_cost_model(
            rows_jsonl=out_exec_cost_jsonl,
            out_model_json=exec_model_json,
            out_report_json=exec_report_json,
            min_group_rows=30
        )
        logger.info(f"Exec Cost Model Trained: Groups kept={exec_cost_report['report']['groups_kept']}")

        logger.info("Training Rule Success Model...")
        rule_success_report = train_rule_success_model(
            rows_jsonl=out_rule_success_jsonl,
            out_model_json=rule_model_json,
            out_report_json=rule_report_json,
            min_group_rows=50,
            beta_prior=5.0
        )
        logger.info(f"Rule Success Model Trained: Groups kept={rule_success_report['report']['groups_kept']}")

        logger.info("Building and Promoting Contextual Bundle...")
        bundle_out = build_bundle(
            exec_cost_model_path=exec_model_json,
            rule_success_model_path=rule_model_json,
            registry_dir=registry_dir,
            promote_dir=promote_dir,
            kind='ofc_ctx_bundle'
        )
        logger.info(f"Bundle Built and Promoted! Version: {bundle_out['version']}")
        logger.info("OFC Contextual Pipeline completed successfully.")
    except Exception:
        logger.exception("Error running OFC Contextual Pipeline")
        return 1

    return 0

if __name__ == '__main__':
    sys.exit(main())
