from utils.time_utils import get_ny_time_millis
"""Feature Drift Exporter V1.

Exposes drift metrics from Redis `settings:dynamic_cfg` to Prometheus.
Port: 9136

Metrics:
  feature_drift_max_z_24h (Gauge)
  psi_max_24h (Gauge)
  drift_state_24h (Gauge)
  drift_n_cur_24h (Gauge)
  drift_n_ref_24h (Gauge)
  drift_staleness_sec (Gauge)
"""

import os
import sys
import time
import logging
from prometheus_client import start_http_server, Gauge
import redis

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("drift_exporter")

# Env
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DYN_CFG_KEY = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")
PORT = int(os.getenv("DRIFT_EXPORTER_PORT", "9136"))
POLL_INTERVAL = int(os.getenv("DRIFT_EXPORTER_POLL_SEC", "15"))

# Metrics
G_MAX_Z = Gauge("feature_drift_max_z_24h", "Max Robust Z-score across features (24h)")
G_MAX_PSI = Gauge("psi_max_24h", "Max PSI across features (24h)")
G_STATE = Gauge("drift_state_24h", "Drift State: 0=OK, 1=WARN, 2=BLOCK, 3=UNKNOWN")
G_N_CUR = Gauge("drift_n_cur_24h", "Sample count in current window")
G_N_REF = Gauge("drift_n_ref_24h", "Sample count in reference window")
G_STALENESS = Gauge("drift_staleness_sec", "Seconds since last drift calculation")

def main():
    logger.info(f"Starting Drift Exporter on port {PORT}...")
    start_http_server(PORT)
    
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    
    while True:
        try:
            val = r.hgetall(DYN_CFG_KEY)
            
            if not val:
                logger.debug("No data in dynamic cfg")
                time.sleep(POLL_INTERVAL)
                continue
                
            # Parse values
            max_z = float(val.get("feature_drift_max_z_24h", 0.0))
            max_psi = float(val.get("psi_max_24h", 0.0))
            state = int(float(val.get("drift_state_24h", 3)))
            n_cur = int(float(val.get("drift_n_cur_24h", 0)))
            n_ref = int(float(val.get("drift_n_ref_24h", 0)))
            last_ts = int(float(val.get("drift_last_ts_ms", 0)))
            
            # Update gauges
            G_MAX_Z.set(max_z)
            G_MAX_PSI.set(max_psi)
            G_STATE.set(state)
            G_N_CUR.set(n_cur)
            G_N_REF.set(n_ref)
            
            # Staleness
            now_ms = get_ny_time_millis()
            if last_ts > 0:
                staleness = (now_ms - last_ts) / 1000.0
                G_STALENESS.set(staleness)
            else:
                G_STALENESS.set(999999)

        except redis.exceptions.ConnectionError as e:
            logger.warning(f"Waiting for Redis connection: {e}")
        except redis.exceptions.BusyLoadingError as e:
            logger.warning(f"Waiting for Redis to load dataset: {e}")
        except Exception as e:
            logger.error(f"Error updating metrics: {e}")
        
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
