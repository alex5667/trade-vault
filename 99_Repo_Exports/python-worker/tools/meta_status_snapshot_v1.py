from utils.time_utils import get_ny_time_millis

#!/usr/bin/env python3
"""
Meta Status Snapshot V1.
Diagnostic tool to aggregate meta-loop state (model, report, ramp, redis)
into a single snapshot and Prometheus textfile.
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("meta_status")

def load_json(path: str) -> dict[str, Any]:
    if not path or not Path(path).exists():
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load JSON from {path}: {e}")
        return {}

def get_redis_cfg(redis_url: str) -> dict[str, Any]:
    try:
        import redis
        r = redis.from_url(redis_url, decode_responses=True)
        # settings:dynamic_cfg is a hash
        cfg = r.hgetall("settings:dynamic_cfg")
        return cfg if cfg else {}
    except ImportError:
        logger.error("redis-py not installed, skipping Redis config collection")
        return {"error": "redis_import_error"}
    except Exception as e:
        logger.warning(f"Failed to fetch Redis config: {e}")
        return {"error": str(e)}

def _action_code(action: str) -> float:
    a = (action or "").strip().upper()
    if a == "HOLD": return 0.0
    if a.startswith("HOLD_MIN_HOLD"): return 1.0
    if a.startswith("HOLD_COOLDOWN"): return 2.0
    if a.startswith("HOLD_TREND"): return 3.0
    if a.startswith("INCREASE"): return 4.0
    if a.startswith("DECREASE"): return 5.0
    if a.startswith("FREEZE"): return 6.0
    return -1.0

def _block_reason_code(reason: str) -> float:
    r = (reason or "").strip().lower()
    if not r: return 0.0
    if "trend" in r: return 1.0
    if "freeze" in r: return 2.0
    if "min_hold" in r: return 3.0
    if "cooldown" in r: return 4.0
    return 5.0

def _dyn_get_schema(dyn: dict[str, Any], schema: str, key: str, default: Any) -> Any:
    # Mirror meta_ramp_apply_v3._cfg_get() / _state_get() logic
    sk = f"{key}__{schema}"
    if sk in dyn: return dyn[sk]
    return dyn.get(key, default)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-json", help="Path to meta_model.json")
    ap.add_argument("--report-json", help="Path to meta_report.json")
    ap.add_argument("--ramp-json", help="Path to meta_ramp_state.json or ramp_decision.json")
    ap.add_argument("--schema", default="", help="Override schema_name for per-schema cfg2 keys")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--out-json", help="Path to save aggregate snapshot JSON")
    ap.add_argument("--prom-textfile", help="Path to save Prometheus metrics (node_exporter format)")
    args = ap.parse_args()

    snapshot = {
        "ts": get_ny_time_millis(),
        "ts_str": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "model": {},
        "report": {},
        "ramp": {},
        "redis_cfg": {},
        "schema_resolved": ""
    }

    # 1. Model Info
    model_data = load_json(args.model_json)
    if model_data:
        snapshot["model"] = {
            "schema_name": model_data.get("schema_name"),
            "version": model_data.get("schema_version") or model_data.get("version"),
            "hash": model_data.get("schema_hash") or model_data.get("model_signature") or model_data.get("model_hash"),
            "n_features": len(model_data.get("features", [])),
            "created_at": model_data.get("created_ms") or model_data.get("created_at")
        }

    # 2. Report Metrics
    report_data = load_json(args.report_json)
    if report_data:
        # Extract global metrics
        snapshot["report"] = {
            "schema": report_data.get("schema_name") or (report_data.get("schema") or {}).get("name"),
            "dq_present_n": report_data.get("dq_present_n"),
            "dq_health_mean": report_data.get("dq_health_mean") or (report_data.get("metrics") or {}).get("dq_health_mean"),
            "corr_meta_p_dq_health": report_data.get("corr_meta_p_dq_health"),
            "global_pr_auc": report_data.get("global_pr_auc") or (report_data.get("metrics") or {}).get("pr_auc"),
            "global_ece": report_data.get("global_ece") or (report_data.get("metrics") or {}).get("ece"),
        }
        # Worst bucket info (from v2/v3 reports)
        worst = report_data.get("worst_bucket") or report_data.get("worst") or {}
        if worst:
            snapshot["report"]["worst_bucket"] = {
                "name": worst.get("bucket_name") or worst.get("name"),
                "pr_auc": worst.get("pr_auc") or worst.get("worst_pr_auc"),
                "ece": worst.get("ece") or worst.get("worst_ece"),
                "n": worst.get("n")
            }

    # 3. Ramp Info
    ramp_data = load_json(args.ramp_json)
    if ramp_data:
        snapshot["ramp"] = {
            "current_share": ramp_data.get("current_share"),
            "target_share": ramp_data.get("target_share"),
            "ramp_id": ramp_data.get("ramp_id"),
            "status": ramp_data.get("status")
        }

    # 4. Redis Dyn Cfg
    snapshot["redis_cfg"] = get_redis_cfg(args.redis_url)

    # Resolve schema for per-schema cfg2 keys
    schema = (args.schema or "").strip()
    if not schema:
        schema = str(snapshot.get("model", {}).get("schema_name") or "").strip()
    if not schema:
        schema = str(snapshot.get("report", {}).get("schema") or "").strip()
    snapshot["schema_resolved"] = schema

    # Build ramp_state + baselines from cfg2
    ramp_state = {}
    dyn = snapshot["redis_cfg"]
    if isinstance(dyn, dict) and schema:
        def _fi(k: str) -> float | None:
            v = _dyn_get_schema(dyn, schema, k, "")
            try: return float(v)
            except Exception: return None

        ramp_state["share"] = _fi("meta_enforce_share") or _fi("meta_ramp_share") or 0.0
        ramp_state["mode"] = str(_dyn_get_schema(dyn, schema, "meta_model_mode", _dyn_get_schema(dyn, schema, "meta_ramp_mode", "SHADOW"))).strip().upper()
        ramp_state["last_eval_ts"] = int(_fi("meta_ramp_last_eval_ts") or 0)
        ramp_state["last_change_ts"] = int(_fi("meta_ramp_last_change_ts") or 0)
        ramp_state["last_action"] = _dyn_get_schema(dyn, schema, "meta_ramp_last_action", "")
        ramp_state["last_reason"] = _dyn_get_schema(dyn, schema, "meta_ramp_last_reason", "")
        ramp_state["block_reason"] = _dyn_get_schema(dyn, schema, "meta_status_ramp_block_reason", "")
        ramp_state["baseline_ts"] = int(_fi("meta_ramp_baseline_ts") or 0)

        ramp_state["baseline_pr_auc"] = _fi("meta_ramp_baseline_pr_auc")
        ramp_state["baseline_ece"] = _fi("meta_ramp_baseline_ece")
        ramp_state["baseline_dq_health_mean"] = _fi("meta_ramp_baseline_dq_health_mean")

        # Calculate deltas
        r = snapshot.get("report") or {}
        if r:
            cur_pr = r.get("global_pr_auc")
            cur_ece = r.get("global_ece")
            cur_dq = r.get("dq_health_mean")

            if cur_pr is not None and ramp_state.get("baseline_pr_auc") is not None:
                ramp_state["delta_pr_auc"] = float(cur_pr) - float(ramp_state["baseline_pr_auc"])
            if cur_ece is not None and ramp_state.get("baseline_ece") is not None:
                ramp_state["delta_ece"] = float(cur_ece) - float(ramp_state["baseline_ece"])
            if cur_dq is not None and ramp_state.get("baseline_dq_health_mean") is not None:
                ramp_state["delta_dq_health_mean"] = float(cur_dq) - float(ramp_state["baseline_dq_health_mean"])

    snapshot["ramp_state"] = ramp_state

    # Output JSON
    if args.out_json:
        try:
            with open(args.out_json, "w") as f:
                json.dump(snapshot, f, indent=2)
            logger.info(f"Snapshot saved to {args.out_json}")
        except Exception as e:
            logger.error(f"Failed to write snapshot JSON: {e}")
    else:
        print(json.dumps(snapshot, indent=2))

    # Output Prometheus
    if args.prom_textfile:
        try:
            lines = [
                "# HELP meta_status_ts_ms Current snapshot timestamp",
                "# TYPE meta_status_ts_ms gauge",
                f"meta_status_ts_ms {snapshot['ts']}",
            ]

            # Model info
            if snapshot["model"].get("n_features") is not None:
                lines.append(f"meta_status_model_features {snapshot['model']['n_features']}")

            # DQ Metrics
            rep = snapshot["report"]
            if rep.get("dq_present_n") is not None:
                lines.append(f"meta_status_dq_present_n {rep['dq_present_n']}")
            if rep.get("dq_health_mean") is not None:
                lines.append(f"meta_status_dq_health_mean {rep['dq_health_mean']}")
            if rep.get("corr_meta_p_dq_health") is not None:
                lines.append(f"meta_status_corr_meta_p_dq_health {rep['corr_meta_p_dq_health']}")

            # Worst bucket
            worst = rep.get("worst_bucket")
            if worst:
                lines.append(f"meta_status_worst_pr_auc {worst.get('pr_auc', 0)}")
                lines.append(f"meta_status_worst_ece {worst.get('ece', 0)}")

            # Redis Cfg / Latch
            rcfg = snapshot["redis_cfg"]
            latch = 1 if (rcfg.get("meta_guard_freeze", "0")) in ("1", "true", "True") else 0
            lines.append(f"meta_status_guard_freeze {latch}")

            # Ramp State Metrics
            rs = snapshot.get("ramp_state", {})
            if rs:
                lines.append(f"meta_status_ramp_share {rs.get('share', 0.0)}")
                lines.append(f"meta_status_ramp_mode_code {1.0 if rs.get('mode') == 'ENFORCE' else 0.0}")
                lines.append(f"meta_status_ramp_last_eval_ts {rs.get('last_eval_ts', 0)}")
                lines.append(f"meta_status_ramp_last_change_ts {rs.get('last_change_ts', 0)}")
                lines.append(f"meta_status_ramp_baseline_ts {rs.get('baseline_ts', 0)}")
                lines.append(f"meta_status_ramp_last_action_code {_action_code(rs.get('last_action', ''))}")
                lines.append(f"meta_status_ramp_block_reason_code {_block_reason_code(rs.get('block_reason', ''))}")

                if rs.get("delta_pr_auc") is not None:
                    lines.append(f"meta_status_ramp_delta_pr_auc {rs['delta_pr_auc']}")
                if rs.get("delta_ece") is not None:
                    lines.append(f"meta_status_ramp_delta_ece {rs['delta_ece']}")
                if rs.get("delta_dq_health_mean") is not None:
                    lines.append(f"meta_status_ramp_delta_dq_health_mean {rs['delta_dq_health_mean']}")

            with open(args.prom_textfile, "w") as f:
                f.write("\n".join(lines) + "\n")
            logger.info(f"Prometheus metrics saved to {args.prom_textfile}")
        except Exception as e:
            logger.error(f"Failed to write Prometheus textfile: {e}")

if __name__ == "__main__":
    main()
