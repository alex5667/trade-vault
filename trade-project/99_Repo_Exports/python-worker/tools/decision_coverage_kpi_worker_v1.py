import json
import logging
import os
import sys
import time
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
from core.redis_keys import RedisStreams as RS
logger = logging.getLogger(__name__)

class DecisionCoverageWorker:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=True)

        self.stream_key = os.getenv("DECISIONS_FINAL_STREAM", RS.DECISIONS_FINAL)
        self.out_key = os.getenv("DECISION_COVERAGE_OUT_HASH", "metrics:decision_coverage:24h")
        self.dyn_cfg_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")

        self.window_h = int(os.getenv("DECISION_COVERAGE_WINDOW_H", "24"))
        self.max_scan = int(os.getenv("DECISION_COVERAGE_MAX_SCAN", "200000"))

        self.window_ms = self.window_h * 3600 * 1000

    def get_decision_regime(self, decision: dict[str, Any]) -> str:
        """
        Determine regime: ok/warn/block based on policy_regime or dq_state and drift_state.
        P68: Prefer explicit policy_regime from decision record.
        """
        # P68: Prefer explicit policy regime recorded in indicators
        if "policy_effective_mode" in decision and decision["policy_effective_mode"]:
            reg = str(decision["policy_effective_mode"]).lower()
            if reg in ("ok", "warn", "block"):
                return reg

        # P68: Prefer explicit policy regime recorded in indicators
        if "policy_regime" in decision and decision["policy_regime"]:
            reg = str(decision["policy_regime"]).lower()
            if reg in ("ok", "warn", "block"):
                return reg

        # Fallback to old logic (P65/P67)
        dq = decision.get("dq_state", "ok")
        drift = decision.get("drift_state", "ok")

        if dq == "block" or drift == "block":
            return "block"
        if dq == "warn" or drift == "warn":
            return "warn"
        return "ok"

    def parse_entry(self, entry_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
        """Parse stream entry, preferring payload JSON."""
        try:
            # Try payload first
            if "payload" in fields:
                try:
                    data = json.loads(fields["payload"])
                    # Ensure timestamp from ID if not present
                    if "ts" not in data:
                        ts_ms = int(entry_id.split("-")[0])
                        data["ts"] = ts_ms
                    return data
                except json.JSONDecodeError:
                    pass

            # Fallback to fields
            # We expect at least 'decision', 'ts' (or from ID)
            data = fields.copy()
            ts_ms = int(entry_id.split("-")[0])
            if "ts" not in data:
                data["ts"] = ts_ms
            else:
                data["ts"] = int(data["ts"])

            return data
        except Exception as e:
            logger.debug(f"Failed to parse entry {entry_id}: {e}")
            return None

    def run_once(self):
        """Run single calculation pass."""
        logger.info(f"Starting Decision Coverage run over {self.window_h}h window...")

        now_ms = get_ny_time_millis()
        min_ts = now_ms - self.window_ms

        # Read stream in reverse or range?
        # Since we need last 24h, reading from min_ts to +inf is better,
        # but XREVRANGE is usually faster for "recent" items if stream is huge.
        # User spec says "rolling 24h".
        # Let's use XRANGE from min_ts.

        # Optimization: if stream is huge, we might cap logic.
        # But instructions say "reads ... Redis Stream".
        # We'll use XRANGE.

        start_id = f"{min_ts}-0"

        # We might need pagination if many events
        entries = self.r.xrange(self.stream_key, min=start_id, max="+", count=self.max_scan)

        if not entries:
            logger.info("No entries found in window.")
            # We should valid non-exist or empty metrics?
            # Better to keep old or set zero?
            # Let's set zero for rates to reflect reality.
            self.r.hset(self.out_key, mapping={
                "decision_allow_rate_24h": "0.0",
                "decision_veto_rate_24h": "0.0",
                "decision_n_24h": "0",
                "decision_last_ts_ms": str(now_ms),
                # P65: Zero out regime shares too
                # P65: Zero out regime shares too
                "decision_policy_mode_n_24h_ok": "0",
                "decision_policy_mode_share_24h_ok": "0.0",
                "decision_policy_mode_n_24h_warn": "0",
                "decision_policy_mode_share_24h_warn": "0.0",
                "decision_policy_mode_n_24h_block": "0",
                "decision_policy_mode_share_24h_block": "0.0",
                "decision_policy_mode_n_24h_unknown": "0",
                "decision_policy_mode_share_24h_unknown": "0.0"
            })
            return

        logger.info(f"Processing {len(entries)} entries...")

        counts = {
            "allow": 0,
            "veto": 0,
            "deny": 0, # Should be rare in final but possible
            "unknown": 0
        }

        regimes = {
            "ok": {"allow": 0, "veto": 0, "total": 0},
            "warn": {"allow": 0, "veto": 0, "total": 0},
            "block": {"allow": 0, "veto": 0, "total": 0},
            "unknown": {"allow": 0, "veto": 0, "total": 0}
        }

        # Breakdown by DQ/Drift reasons
        # key format: "dq={state}|drift={state}"
        breakdown = {}

        last_ts = 0

        for eid, fields in entries:
            data = self.parse_entry(eid, fields)
            if not data:
                continue

            decision = data.get("decision", "unknown").lower()
            last_ts = max(last_ts, data.get("ts", 0))

            # Global counts
            if decision in counts:
                counts[decision] += 1
            else:
                counts["unknown"] += 1

            # Regime analysis
            regime = self.get_decision_regime(data)
            if regime not in regimes:
                regime = "unknown"

            regimes[regime]["total"] += 1
            if decision == "allow":
                regimes[regime]["allow"] += 1
            elif decision == "veto":
                regimes[regime]["veto"] += 1

            # Breakdown
            dq_s = data.get("dq_state", "unknown")
            drift_s = data.get("drift_state", "unknown")
            bd_key = f"dq={dq_s}|drift={drift_s}"

            if bd_key not in breakdown:
                breakdown[bd_key] = {"allow": 0, "veto": 0, "total": 0}

            breakdown[bd_key]["total"] += 1
            if decision == "allow":
                breakdown[bd_key]["allow"] += 1
            elif decision == "veto":
                breakdown[bd_key]["veto"] += 1

        total = sum(counts.values())
        allow_rate = counts["allow"] / total if total > 0 else 0.0
        veto_rate = counts["veto"] / total if total > 0 else 0.0

        # Write to Redis
        pipe = self.r.pipeline()

        metrics = {
            "decision_allow_rate_24h": str(round(allow_rate, 4)),
            "decision_veto_rate_24h": str(round(veto_rate, 4)),
            "decision_n_24h": str(total),
            "decision_last_ts_ms": str(last_ts),
            "decision_regimes_24h_json": json.dumps(regimes),
            "decision_breakdown_drift_dq_24h_json": json.dumps(breakdown)
        }

        # Add flat keys for regimes (P65)
        for r_name, r_stats in regimes.items():
            r_total = r_stats["total"]
            r_share = r_total / total if total > 0 else 0.0
            metrics[f"decision_policy_mode_n_24h_{r_name}"] = str(r_total)
            metrics[f"decision_policy_mode_share_24h_{r_name}"] = str(round(r_share, 4))

        pipe.hset(self.out_key, mapping=metrics)

        # Also update dynamic cfg if needed (as per spec "Пишет: В DYN_CFG_KEY")
        # Assuming we just write rates there for other components to read cleanly
        # P65: Add regime shares to dynamic cfg as well
        dyn_cfg_update = {
            "decision_allow_rate_24h": metrics["decision_allow_rate_24h"],
            "decision_veto_rate_24h": metrics["decision_veto_rate_24h"]
        }
        for r_name in regimes:
             dyn_cfg_update[f"decision_policy_mode_share_24h_{r_name}"] = metrics[f"decision_policy_mode_share_24h_{r_name}"]
             dyn_cfg_update[f"decision_policy_mode_n_24h_{r_name}"] = metrics[f"decision_policy_mode_n_24h_{r_name}"]

        pipe.hset(self.dyn_cfg_key, mapping=dyn_cfg_update)

        pipe.execute()

        logger.info(f"Updated metrics. N={total}, Allow={allow_rate:.2%}, Veto={veto_rate:.2%}")

if __name__ == "__main__":
    worker = DecisionCoverageWorker()
    if "--once" in sys.argv:
        worker.run_once()
    else:
        # Loop mode not strictly requested but good practice for workers
        while True:
            try:
                worker.run_once()
                # Run hourly roughly? Or wait for schedule?
                # The user said "hourly at :17", so this script might be called via timer or cron.
                # But if run as service, we loop.
                # Given instructions imply it's called by OF Timers Worker, we probably just run once and exit.
                # "python -m tools.decision_coverage_kpi_worker_v1 --once" in quick run instructions supports this.
                # However, if main is called without --once, we should probably sleep.
                # Let's assume timer calls it with --once usually, but we default to sleep loop if not.
                time.sleep(3600)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Error in loop: {e}")
                time.sleep(60)
