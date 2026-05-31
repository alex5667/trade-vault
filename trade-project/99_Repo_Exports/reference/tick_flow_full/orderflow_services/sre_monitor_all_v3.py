#!/usr/bin/env python3
"""
sre_monitor_all_v3.py

Unified entrypoint for SRE checks:
1. tools/ml_sre_monitor.py (core ML metrics)
2. tools/cfg_suggestions_sre_monitor_v2.py (lifecycle & flapping)
3. tools/tb_sre_monitor_v2.py (Triple Barrier health)
4. tools/ml_confirm_stream_sre_monitor.py (Stream integrity)

Usage:
  python3 -m tools.sre_monitor_all_v3 --emit-metrics --notify
"""
import os
import sys
import subprocess
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("sre_monitor_v3")

def run_tool(cmd: list[str]) -> int:
    logger.info(f"Running: {' '.join(cmd)}")
    try:
        res = subprocess.run(cmd, check=False)
        return res.returncode
    except Exception as e:
        logger.error(f"Failed to run tool: {e}")
        return 1

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--emit-metrics", action="store_true")
    parser.add_argument("--notify", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--emit-suggestions", action="store_true", help="Pass to notify_slo_burn_monitor") # P6.9
    args = parser.parse_args()

    py = sys.executable
    base = os.path.dirname(__file__)
    # Optional: auto-downgrade meta ENFORCE per coverage bucket based on outcomes (P33)
    enable_meta_cov_outcome = bool(int(os.getenv("ENABLE_META_COV_OUTCOME_AUTO_APPLY", "0") or 0))
    
    flags = []
    if args.emit_metrics: flags.append("--emit-metrics")
    if args.notify: flags.append("--notify")
    if args.dry_run: flags.append("--dry-run")
    
    # Specific flags for specific tools
    notify_flags = []
    if args.emit_suggestions:
        notify_flags.append("--emit-suggestions")

    tools = [
        ["ml_sre_monitor.py", []],
        ["cfg_suggestions_sre_monitor_v2.py", []],
        ["tb_sre_monitor_v2.py", []],
        ["ml_confirm_stream_sre_monitor.py", []],
    ]

    # Optional: P32 meta outcome auto-apply
    if os.getenv("ENABLE_META_COV_OUTCOME_AUTO_APPLY", "0") == "1":
        tools.append(["meta_cov_outcome_auto_apply_v1.py", ["--apply", "1"]])

    tools.append(["notify_slo_burn_monitor_v1.py", ["--print_json"] + notify_flags])

    # P47/P48: Signal Quality KPI (optional on-demand from SRE loop)
    if os.getenv("ENABLE_SIGNAL_QUALITY_KPI", "0") == "1":
        # Does not accept common flags, usually just --once
        # Note: The script path here is relative to the module, so it needs to be `tools.signal_quality_kpi_worker_v1`
        # if it's meant to be run as `python -m tools.signal_quality_kpi_worker_v1`.
        # If it's a direct file path, it should be `signal_quality_kpi_worker_v1.py`.
        # Assuming it's a direct file path for consistency with other tools.
        tools.append(["signal_quality_kpi_worker_v1.py", ["--once"]])

    # Optional: P32 meta outcome guardrails by coverage buckets
    if os.getenv("ENABLE_META_COV_OUTCOME_GUARD", "0") == "1":
        tools.append(["meta_cov_outcome_guard_v1.py", []])

    # Producer contract check (metrics:of_gate fields for meta coverage ops)
    if os.getenv("ENABLE_OF_GATE_CONTRACT_CHECK", "0") == "1":
        tools.append(["of_gate_metrics_contract_check_v1.py", []])

    # P94: Feature Registry contract check (schema_hash / feature_cols_hash pinning)
    if os.getenv("ENABLE_FEATURE_REGISTRY_CONTRACT_CHECK", "0") == "1":
        tools.append(["feature_registry_contract_check_v1.py", []])

    exit_codes = []
    for tool_def in tools:
        tool_script = tool_def[0]
        tool_extra_args = tool_def[1] if len(tool_def) > 1 else []
        
        full_cmd = [py, os.path.join(base, tool_script)] + flags + tool_extra_args
        rc = run_tool(full_cmd)
        exit_codes.append(rc)

    # 0 = OK, 1 = Error/Exception, 2 = Alert found
    # We return the worst status (2 > 1 > 0)
    max_rc = max(exit_codes) if exit_codes else 0
    
    logger.info(f"All monitors finished. Max exit code: {max_rc}")
    sys.exit(max_rc)

if __name__ == "__main__":
    main()
