#!/usr/bin/env python3
"""sre_monitor_all_v3.py

Unified entrypoint for SRE checks:
  1) ml_sre_monitor.py (core ML metrics)
  2) cfg_suggestions_sre_monitor_v2.py (lifecycle & flapping)
  3) tb_sre_monitor_v2.py (Triple Barrier health)
  4) ml_confirm_stream_sre_monitor.py (Stream integrity)
  5) notify_slo_burn_monitor_v1.py (routing/notifications)

Optional (meta ENFORCE / meta coverage):
  - ENABLE_META_COV_OPS_BUNDLE=1 -> run nightly_meta_enforce_cov_ops_bundle_v1.py
  - ENABLE_META_COV_OUTCOME_AUTO_APPLY=1 -> run meta_cov_outcome_auto_apply_v1.py (legacy)
  - ENABLE_META_COV_OUTCOME_GUARD=1 -> run meta_cov_outcome_guard_v1.py
  - ENABLE_META_COV_QUARANTINE_MONITOR=1 -> run meta_cov_quarantine_monitor_v1.py (read-only by default)

Note:
  Not all tools accept the same CLI flags. We only pass common flags to tools that
  explicitly support them.

Usage:
  python3 tools/sre_monitor_all_v3.py --emit-metrics --notify
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("sre_monitor_v3")


@dataclass
class ToolDef:
    script: str
    extra: list[str]
    pass_common_flags: bool = True


def run_tool(cmd: list[str]) -> int:
    logger.info("Running: %s", " ".join(cmd))
    try:
        res = subprocess.run(cmd, check=False)
        return int(res.returncode)
    except Exception as e:
        logger.error("Failed to run tool: %s", e)
        return 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--emit-metrics", action="store_true")
    parser.add_argument("--notify", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--emit-suggestions", action="store_true", help="Pass to notify_slo_burn_monitor")
    args = parser.parse_args()

    py = sys.executable
    base = os.path.dirname(__file__)

    common_flags: list[str] = []
    if args.emit_metrics:
        common_flags.append("--emit-metrics")
    if args.notify:
        common_flags.append("--notify")
    if args.dry_run:
        common_flags.append("--dry-run")

    notify_flags: list[str] = []
    if args.emit_suggestions:
        notify_flags.append("--emit-suggestions")

    tools: list[ToolDef] = [
        ToolDef("ml_sre_monitor.py", []),
        ToolDef("cfg_suggestions_sre_monitor_v2.py", []),
        ToolDef("tb_sre_monitor_v2.py", []),
        ToolDef("ml_confirm_stream_sre_monitor.py", []),
    ]

    # Meta coverage ops bundle (preferred)
    if os.getenv("ENABLE_META_COV_OPS_BUNDLE", "0") == "1":
        # Does not accept common flags.
        tools.append(ToolDef("../orderflow_services/nightly_meta_enforce_cov_ops_bundle_v1.py", ["--emit-metrics"], pass_common_flags=False))

    # Legacy: direct outcome auto-apply
    if os.getenv("ENABLE_META_COV_OUTCOME_AUTO_APPLY", "0") == "1":
        tools.append(ToolDef("../orderflow_services/meta_cov_outcome_auto_apply_v1.py", ["--apply", "1"], pass_common_flags=False))

    # Quarantine monitor (read-only by default, can be enabled with APPLY=1 via env)
    if os.getenv("ENABLE_META_COV_QUARANTINE_MONITOR", "0") == "1":
        extra = []
        # If the operator wants it to apply inside SRE monitor, they can set META_COV_OPS_APPLY=1
        if os.getenv("META_COV_OPS_APPLY", "0") == "1":
            extra += ["--apply", "1"]
        tools.append(ToolDef("../orderflow_services/meta_cov_quarantine_monitor_v1.py", extra, pass_common_flags=False))

    # P71 Policy effectiveness report
    if os.getenv("ENABLE_POLICY_EFFECTIVENESS_REPORT", "0") == "1":
        tools.append(ToolDef("../ok_rate_logic/tools/policy_effectiveness_report_worker_v1.py", ["--once"], pass_common_flags=False))

    # notify_slo_burn_monitor supports common flags
    tools.append(ToolDef("notify_slo_burn_monitor_v1.py", ["--print_json"] + notify_flags))

    # Optional: guardrails by coverage buckets
    if os.getenv("ENABLE_META_COV_OUTCOME_GUARD", "0") == "1":
        tools.append(ToolDef("../orderflow_services/meta_cov_outcome_guard_v1.py", [], pass_common_flags=False))

    exit_codes: list[int] = []
    for td in tools:
        cmd = [py, os.path.join(base, td.script)]
        if td.pass_common_flags:
            cmd += common_flags
        cmd += td.extra
        exit_codes.append(run_tool(cmd))

    # 0 = OK, 1 = Error/Exception, 2 = Alert found
    max_rc = max(exit_codes) if exit_codes else 0
    logger.info("All monitors finished. Max exit code: %s", max_rc)
    raise SystemExit(max_rc)


if __name__ == "__main__":
    main()
