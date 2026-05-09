from utils.time_utils import get_ny_time_millis

"""Feature Drift Monitor V1 (P49).

Calculates drift metrics (Robust Z-score, PSI) for features in `decisions:final` stream.
Updates `settings:dynamic_cfg` with drift state and metrics.

Config:
  DRIFT_CUR_H: hours for current window (default 24)
  DRIFT_REF_H: hours for reference window (default 72)
  DRIFT_MAX_SCAN: max items to scan in stream (default 200000)
  DRIFT_MIN_N_CUR: min samples in current window (default 200)
  DRIFT_MIN_N_REF: min samples in reference window (default 400)
  DRIFT_FEATURE_FIELDS: comma-separated list of feature keys (default: rule_score,ml_p_cal,ml_p)
  DRIFT_Z_WARN: robust Z threshold for warning (default 4.0)
  DRIFT_Z_BLOCK: robust Z threshold for blocking (default 6.0)
  DRIFT_PSI_WARN: PSI threshold for warning (default 0.25)
  DRIFT_PSI_BLOCK: PSI threshold for blocking (default 0.40)
  DYN_CFG_KEY: Redis key for dynamic config (default: settings:dynamic_cfg)

Output (in DYN_CFG_KEY):
  feature_drift_max_z_24h: float
  psi_max_24h: float
  drift_state_24h: int (0=ok, 1=warn, 2=block, 3=unknown)
  drift_top_feature_z: str
  drift_top_feature_psi: str
  drift_last_ts_ms: int
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from collections import defaultdict
from typing import Any

import numpy as np
import redis.asyncio as aioredis

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("feature_drift_v1")

# Env config
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DECISIONS_STREAM = os.getenv("DECISIONS_FINAL_STREAM", "decisions:final")
DYN_CFG_KEY = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")

DRIFT_CUR_H = int(os.getenv("DRIFT_CUR_H", "24"))
DRIFT_REF_H = int(os.getenv("DRIFT_REF_H", "72"))
DRIFT_MAX_SCAN = int(os.getenv("DRIFT_MAX_SCAN", "200000"))

DRIFT_MIN_N_CUR = int(os.getenv("DRIFT_MIN_N_CUR", "200"))
DRIFT_MIN_N_REF = int(os.getenv("DRIFT_MIN_N_REF", "400"))

DRIFT_FEATURE_FIELDS_STR = os.getenv("DRIFT_FEATURE_FIELDS", "rule_score,ml_p_cal,ml_p")
DRIFT_FEATURE_FIELDS = [f.strip() for f in DRIFT_FEATURE_FIELDS_STR.split(",") if f.strip()]

DRIFT_Z_WARN = float(os.getenv("DRIFT_Z_WARN", "4.0"))
DRIFT_Z_BLOCK = float(os.getenv("DRIFT_Z_BLOCK", "6.0"))

DRIFT_PSI_WARN = float(os.getenv("DRIFT_PSI_WARN", "0.25"))
DRIFT_PSI_BLOCK = float(os.getenv("DRIFT_PSI_BLOCK", "0.40"))

# Mapping from friendly name to payload path
FEATURE_MAP = {
    "rule_score": ["rule", "score"],
    "ml_p": ["ml", "score"],
    "ml_p_cal": ["meta", "meta_p"],  # calibrated p
}


def _safe_get_path(d: Any, path: list[str], default: Any = None) -> Any:
    cur = d
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def compute_robust_z(curr: np.ndarray, ref: np.ndarray) -> float:
    """Compute Robust Z-score: |Median_curr - Median_ref| / MAD_ref."""
    if len(curr) < 2 or len(ref) < 2:
        return 0.0

    med_ref = np.median(ref)
    mad_ref = np.median(np.abs(ref - med_ref))

    # Avoid div by zero, use a small epsilon or fallback
    if mad_ref < 1e-6:
        # If MAD is 0, check if medians differ. If so, infinite drift.
        med_curr = np.median(curr)
        if abs(med_curr - med_ref) > 1e-6:
            return 999.0  # Large value
        return 0.0

    med_curr = np.median(curr)
    z = abs(med_curr - med_ref) / (mad_ref * 1.4826) # 1.4826 scale factor for normal consistency
    return float(z)


def compute_psi(curr: np.ndarray, ref: np.ndarray, buckets: int = 10) -> float:
    """Compute Population Stability Index (PSI)."""
    if len(curr) < 2 or len(ref) < 2:
        return 0.0

    # Define bins based on reference distribution
    try:
        # Use quantiles for binning
        breakpoints = np.nanpercentile(ref, np.linspace(0, 100, buckets + 1))
        # Drop duplicates in breakpoints (e.g. sparse data)
        breakpoints = np.unique(breakpoints)
        if len(breakpoints) < 2:
            return 0.0 # Cannot bin

        # Avoid strict edges issue by extending infinity
        breakpoints[0] = -np.inf
        breakpoints[-1] = np.inf

        ref_counts, _ = np.histogram(ref, breakpoints)
        curr_counts, _ = np.histogram(curr, breakpoints)

        # Normalize to probability
        ref_pct = ref_counts / len(ref)
        curr_pct = curr_counts / len(curr)

        # Add epsilon to zero buckets
        epsilon = 1e-5
        ref_pct = np.maximum(ref_pct, epsilon)
        curr_pct = np.maximum(curr_pct, epsilon)

        psi_val = np.sum((curr_pct - ref_pct) * np.log(curr_pct / ref_pct))
        return float(psi_val)
    except Exception:
        return 0.0


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    # If not running in a loop (handled by caller/cron), we just run once here logic-wise
    # But adapting to the request, this script seems designed to be called periodically (cron/timer).

    try:
        r = aioredis.from_url(REDIS_URL, decode_responses=True)

        # 1. Read Stream
        # XREVRANGE to get latest items.
        # We need enough data for 72h.

        now_ms = get_ny_time_millis()
        cur_cutoff = now_ms - (DRIFT_CUR_H * 3600 * 1000)
        ref_cutoff = now_ms - (DRIFT_REF_H * 3600 * 1000)

        # We fetch items. Optimization: fetch in chunks if needed, but for 200k items,
        # it might fit in memory (200k * ~1KB = 200MB).
        # Redis XREVRANGE decisions:final + count DRIFT_MAX_SCAN

        logger.info(f"Fetching up to {DRIFT_MAX_SCAN} items from {DECISIONS_STREAM}...")

        items = await r.xrevrange(DECISIONS_STREAM, max="+", min="-", count=DRIFT_MAX_SCAN)

        logger.info(f"Fetched {len(items)} items.")

        if not items:
            logger.warning("No data found in stream.")
            # Write 'unknown' state
            await r.hset(DYN_CFG_KEY, mapping={
                "drift_state_24h": 3,
                "drift_last_ts_ms": now_ms,
                "feature_drift_max_z_24h": 0.0,
                "psi_max_24h": 0.0
            })
            await r.close()
            return

        # 2. Parse & Bucket
        # We need raw values for each feature

        # Structure: { feature_name: { "cur": [], "ref": [] } }
        data = defaultdict(lambda: {"cur": [], "ref": []})

        cnt_cur = 0
        cnt_ref = 0

        for item_id, fields in items:
            try:
                # fields contains 'payload'
                payload_str = fields.get("payload")
                if not payload_str:
                    continue

                # Optimization: simplistic parsing or strict json
                try:
                    record = json.loads(payload_str)
                except json.JSONDecodeError:
                    continue

                ts = record.get("ts_ms")
                if not ts:
                    # Fallback to stream id timestamp
                    ts = int(item_id.split("-")[0])

                # windows (inclusive of cur, exclusive of ref start?)
                # Definitions:
                # Current: [now - 24h, now]
                # Reference: [now - 72h, now - 24h] ??? Or [now - 72h, now]?
                # Usually reference is a stable baseline.
                # The requirement says: current 24h, reference 72h.
                # Let's assume Reference is the "past 72h" NOT including current?
                # Or Reference is "past 72h total"?
                # Usually drift checks Current (recent) vs Reference (older).
                # Let's implement: Current = [Now-24h, Now], Reference = [Now-72h, Now-24h].
                # If Ref window is "72h", it might mean "Last 72h" or "Window of size 72h".
                # Standard practice: Reference window is a larger trailing window BEFORE current.
                # Let's use:
                # Cur: [t - 24h, t]
                # Ref: [t - 72h, t - 24h] (Size 48h) or [t - 96h, t - 24h] (Size 72h).
                # Given "reference: 72h", I'll assume it's a 72h window PRIOR to current.

                if ts > now_ms:
                    continue # Future data?

                if ts >= cur_cutoff:
                    # Current window
                    bucket = "cur"
                    cnt_cur += 1
                elif ts >= ref_cutoff:
                    # Reference window
                    bucket = "ref"
                    cnt_ref += 1
                else:
                    # Too old, stop processing if stream is ordered (it is)
                    # items are rev ordered, so once we hit old, we can stop?
                    # Yes, XREVRANGE returns newest first.
                    break

                # Extract features
                for feat in DRIFT_FEATURE_FIELDS:
                    # Map to payload path
                    path = FEATURE_MAP.get(feat, [feat])
                    # Try to get value
                    val = _safe_get_path(record, path)

                    if val is not None:
                        try:
                            val_f = float(val)
                            data[feat][bucket].append(val_f)
                        except (ValueError, TypeError):
                            pass

            except Exception:
                pass

        logger.info(f"Analyzed samples: Current={cnt_cur} (min {DRIFT_MIN_N_CUR}), Ref={cnt_ref} (min {DRIFT_MIN_N_REF})")

        # 3. Compute Metrics
        max_z = 0.0
        max_psi = 0.0
        top_z_feat = ""
        top_psi_feat = ""

        # If not enough data, state = unknown
        if cnt_cur < DRIFT_MIN_N_CUR or cnt_ref < DRIFT_MIN_N_REF:
            state = 3 # Unknown
            logger.warning("Insufficient data. State -> UNKNOWN (3).")
        else:
            state = 0 # OK

            for feat, buckets in data.items():
                curr_arr = np.array(buckets["cur"])
                ref_arr = np.array(buckets["ref"])

                # Check feature-level counts
                if len(curr_arr) < (DRIFT_MIN_N_CUR * 0.5) or len(ref_arr) < (DRIFT_MIN_N_REF * 0.5):
                    continue

                # Robust Z
                z = compute_robust_z(curr_arr, ref_arr)
                if z > max_z:
                    max_z = z
                    top_z_feat = feat

                # PSI
                psi = compute_psi(curr_arr, ref_arr)
                if psi > max_psi:
                    max_psi = psi
                    top_psi_feat = feat

            logger.info(f"Max Z: {max_z:.2f} ({top_z_feat}), Max PSI: {max_psi:.2f} ({top_psi_feat})")

            # Determine State
            # Logic:
            # Block if any > BLOCK threshold
            # Warn if any > WARN threshold
            # Else OK

            if max_z >= DRIFT_Z_BLOCK or max_psi >= DRIFT_PSI_BLOCK:
                state = 2 # Block
            elif max_z >= DRIFT_Z_WARN or max_psi >= DRIFT_PSI_WARN:
                state = 1 # Warn
            else:
                state = 0 # OK

        logger.info(f"Final State: {state}")

        # 4. Write to Redis
        pl = {
            "drift_state_24h": state,
            "feature_drift_max_z_24h": max_z,
            "psi_max_24h": max_psi,
            "drift_top_feature_z": top_z_feat,
            "drift_top_feature_psi": top_psi_feat,
            "drift_n_cur_24h": cnt_cur,
            "drift_n_ref_24h": cnt_ref,
            "drift_last_ts_ms": now_ms
        }

        await r.hset(DYN_CFG_KEY, mapping=pl)
        logger.info(f"Updated {DYN_CFG_KEY} with {pl}")

        # Also publish debug metric to feature_drift:24h hash if needed, but requirements say cfg2.

        await r.close()

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
