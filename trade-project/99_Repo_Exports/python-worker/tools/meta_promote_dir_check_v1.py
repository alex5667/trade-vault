import argparse
import logging
import os
import shutil
import sys

from prometheus_client import CollectorRegistry, Gauge, write_to_textfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def check_promote_dir(promote_dir, min_free_pct=5.0):
    """
    Checks if promote_dir exists, is writable, and has free space.
    Returns a dictionary of metrics.
    """
    metrics = {
        "meta_promote_dir_exists": 0,
        "meta_promote_dir_writable": 0,
        "meta_promote_dir_ok": 0,
        "meta_promote_dir_free_bytes": 0,
        "meta_promote_dir_free_pct": 0.0
    }

    # 1. Check existence
    if os.path.exists(promote_dir) and os.path.isdir(promote_dir):
        metrics["meta_promote_dir_exists"] = 1
    else:
        logger.error(f"Promote dir not found or not a directory: {promote_dir}")
        # Return early with 0s if it doesn't exist (can't check writable/space)
        return metrics

    # 2. Check writability
    if os.access(promote_dir, os.W_OK):
        metrics["meta_promote_dir_writable"] = 1
    else:
        logger.error(f"Promote dir is not writable: {promote_dir}")

    # 3. Check disk space
    try:
        usage = shutil.disk_usage(promote_dir)
        metrics["meta_promote_dir_free_bytes"] = usage.free
        if usage.total > 0:
            metrics["meta_promote_dir_free_pct"] = (usage.free / usage.total) * 100.0

        logger.info(f"Disk usage for {promote_dir}: Total={usage.total}, Free={usage.free}, FreePct={metrics['meta_promote_dir_free_pct']:.2f}%")

    except Exception as e:
        logger.error(f"Error checking disk usage for {promote_dir}: {e}")
        # Writable might be 1, but if we can't check space, is it ok?
        # let's proceed with what we have

    # 4. Overall status
    # OK if exists AND writable AND free space > min_free_pct
    if (metrics["meta_promote_dir_exists"] == 1 and
        metrics["meta_promote_dir_writable"] == 1 and
        metrics["meta_promote_dir_free_pct"] >= min_free_pct):
        metrics["meta_promote_dir_ok"] = 1
    else:
        logger.warning(f"Promote dir check failed criteria. Metrics: {metrics}")

    return metrics

def write_metrics(metrics, out_file):
    registry = CollectorRegistry()

    # Define gauges
    g_ok = Gauge('meta_promote_dir_ok', '1 if promote dir is healthy and writable with space', registry=registry)
    g_exists = Gauge('meta_promote_dir_exists', '1 if promote dir exists', registry=registry)
    g_writable = Gauge('meta_promote_dir_writable', '1 if promote dir is writable', registry=registry)
    g_free_bytes = Gauge('meta_promote_dir_free_bytes', 'Free bytes in promote dir filesystem', registry=registry)
    g_free_pct = Gauge('meta_promote_dir_free_pct', 'Free percentage in promote dir filesystem', registry=registry)

    # Set values
    g_ok.set(metrics['meta_promote_dir_ok'])
    g_exists.set(metrics['meta_promote_dir_exists'])
    g_writable.set(metrics['meta_promote_dir_writable'])
    g_free_bytes.set(metrics['meta_promote_dir_free_bytes'])
    g_free_pct.set(metrics['meta_promote_dir_free_pct'])

    # Write to file
    # If out_file is a directory, append default filename, otherwise use as is
    # But usually textfile exporter expects a .prom file
    # We'll use write_to_textfile from prometheus_client

    # Ensure dir exists for out_file
    out_dir = os.path.dirname(out_file)
    if out_dir and not os.path.exists(out_dir):
        try:
            os.makedirs(out_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create output directory {out_dir}: {e}")
            return False

    try:
        write_to_textfile(out_file, registry)
        logger.info(f"Metrics written to {out_file}")
        return True
    except Exception as e:
        logger.error(f"Failed to write metrics to {out_file}: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check model promotion directory health.")

    default_dir = os.environ.get("META_PROMOTE_DIR", "/var/lib/trade/meta_promote")

    parser.add_argument("--promote-dir", default=default_dir, help="Directory containing promoted models")
    parser.add_argument("--out", required=True, help="Output path for Prometheus textfile (e.g. /var/lib/node_exporter/promote_dir.prom)")
    parser.add_argument("--min-free-pct", type=float, default=5.0, help="Minimum free percentage to be considered OK (default: 5.0)")

    args = parser.parse_args()

    logger.info(f"Starting check with: promote_dir={args.promote_dir}, out={args.out}")

    metrics = check_promote_dir(args.promote_dir, args.min_free_pct)

    if not write_metrics(metrics, args.out):
        sys.exit(1)

    # Exit code based on OK status? The user didn't specify, but usually check scripts might trigger alerts via exit code too.
    # However, since we are writing metrics for alerts, exit code 0 is fine as long as execution finished.
    # The alert `MetaPromoteDirNotOk` handles the logic.
