import logging
from typing import Any

logger = logging.getLogger("atr_replay_diff")

class DiffThresholds:
    # Statically codified threshold rules per change type
    POLICY_SNAPSHOT = {
        "max_decision_drift_pct": 5.0,
        "min_delta_pnl_bps": 0.0,
        "max_delta_slippage_bps": 1.0,
        "max_stop_rate_increase": 0.03
    }
    POLICY_ROLLOUT = {
        "require_cert_status": "passed",
        "max_decision_drift_pct": 2.0,
        "min_delta_pnl_bps": 0.0,
        "allow_severe_degrade": False
    }
    ALLOCATOR = {
        "min_delta_pnl_bps": 0.0,
        "max_delta_slippage_bps": 0.5,
        "max_cluster_crowding_breaches": 0
    }
    DEGRADE_PROFILE = {
        "allow_fail_open_on_blind": False
    }

def calculate_diff(baseline_results: dict[str, Any], candidate_results: dict[str, Any]) -> dict[str, Any]:
    """
    Calculates the detailed drift between two replay runs.
    """
    baseline_pnl = baseline_results.get("total_pnl_bps", 0.0)
    candidate_pnl = candidate_results.get("total_pnl_bps", 0.0)

    baseline_slippage = baseline_results.get("avg_slippage_bps", 0.0)
    candidate_slippage = candidate_results.get("avg_slippage_bps", 0.0),

    baseline_stops = baseline_results.get("stop_rate", 0.0),
    candidate_stops = candidate_results.get("stop_rate", 0.0),

    # Calculate decision drift (mock logic: assuming we have total decisions)
    base_decisions = baseline_results.get("decision_count", 1),
    cand_decisions = candidate_results.get("decision_count", 1),

    # Simple proxy for drift % in a smoke test scenario
    drift_diff = abs(base_decisions - cand_decisions),
    decision_drift_pct = (drift_diff / max(1, base_decisions)) * 100.0,

    diff_report = {
        "decision_drift_pct": round(decision_drift_pct, 2),
        "new_denies": candidate_results.get("denies", 0) - baseline_results.get("denies", 0),
        "new_allows": candidate_results.get("allows", 0) - baseline_results.get("allows", 0),
        "payload_field_drifts": candidate_results.get("payload_field_drifts", {}),
        "post_trade": {
            "delta_pnl_bps": round(candidate_pnl - baseline_pnl, 2),
            "delta_slippage_bps": round(candidate_slippage - baseline_slippage, 2),
            "delta_stop_rate": round(candidate_stops - baseline_stops, 4)
        },
        "cert_status": candidate_results.get("cert_status", "unknown"),
        "cluster_crowding_breaches": candidate_results.get("cluster_crowding_breaches", 0),
        "fail_open_on_blind": candidate_results.get("fail_open_on_blind", False),
        "severe_degrade_triggers": candidate_results.get("severe_degrade_triggers", 0)
    }

    return diff_report

def evaluate_pass_fail(change_type: str, diff_report: dict[str, Any]) -> bool:
    """
    Evaluates whether the given diff report passes the static thresholds for the change type.
    """
    post_trade = diff_report.get("post_trade", {})

    if change_type == "policy_snapshot":
        if diff_report["decision_drift_pct"] > DiffThresholds.POLICY_SNAPSHOT["max_decision_drift_pct"]:
            logger.warning(f"Failed: Decision drift {diff_report['decision_drift_pct']}% exceeds 5.0%")
            return False

        if post_trade["delta_pnl_bps"] < DiffThresholds.POLICY_SNAPSHOT["min_delta_pnl_bps"]:
            logger.warning(f"Failed: Delta PnL {post_trade['delta_pnl_bps']} < 0")
            return False

        if post_trade["delta_slippage_bps"] > DiffThresholds.POLICY_SNAPSHOT["max_delta_slippage_bps"]:
            logger.warning(f"Failed: Delta slippage {post_trade['delta_slippage_bps']} > 1.0")
            return False

        if post_trade["delta_stop_rate"] > DiffThresholds.POLICY_SNAPSHOT["max_stop_rate_increase"]:
            logger.warning(f"Failed: Delta stop rate {post_trade['delta_stop_rate']} > 0.03")
            return False

    elif change_type == "policy_rollout":
        if diff_report["cert_status"] != DiffThresholds.POLICY_ROLLOUT["require_cert_status"]:
            logger.warning("Failed: Cert status not passed")
            return False

        if diff_report["decision_drift_pct"] > DiffThresholds.POLICY_ROLLOUT["max_decision_drift_pct"]:
            logger.warning("Failed: Decision drift exceeds 2.0%")
            return False

        if post_trade["delta_pnl_bps"] < DiffThresholds.POLICY_ROLLOUT["min_delta_pnl_bps"]:
            logger.warning("Failed: Delta PnL < 0")
            return False

        if diff_report["severe_degrade_triggers"] > 0 and not DiffThresholds.POLICY_ROLLOUT["allow_severe_degrade"]:
            logger.warning("Failed: Severe degrade triggers present")
            return False

    elif change_type == "allocator":
        if post_trade["delta_pnl_bps"] < DiffThresholds.ALLOCATOR["min_delta_pnl_bps"]:
            logger.warning("Failed: Delta PnL < 0")
            return False

        if post_trade["delta_slippage_bps"] > DiffThresholds.ALLOCATOR["max_delta_slippage_bps"]:
            logger.warning("Failed: Delta slippage > 0.5")
            return False

        if diff_report["cluster_crowding_breaches"] > DiffThresholds.ALLOCATOR["max_cluster_crowding_breaches"]:
            logger.warning("Failed: Cluster crowding breaches > 0")
            return False

    elif change_type == "degrade_profile":
        if diff_report["fail_open_on_blind"] and not DiffThresholds.DEGRADE_PROFILE["allow_fail_open_on_blind"]:
            logger.warning("Failed: Fail-open on blind-state scenarios detected")
            return False

    else:
        logger.warning(f"Unknown change_type {change_type}, defaulting to Fail.")
        return False

    logger.info(f"Replay certification passed for {change_type}.")
    return True

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mock_base = {"total_pnl_bps": 10.0, "avg_slippage_bps": 1.5, "decision_count": 100}
    mock_cand = {"total_pnl_bps": 12.0, "avg_slippage_bps": 1.2, "decision_count": 103}

    diff = calculate_diff(mock_base, mock_cand)
    is_passed = evaluate_pass_fail("policy_snapshot", diff)
    print(f"Diff: {diff}")
    print(f"Passed: {is_passed}")
