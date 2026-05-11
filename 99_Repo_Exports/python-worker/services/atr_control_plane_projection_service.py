import json
import logging
import os
import time

import redis

try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None

from services.analytics_db import get_conn

logger = logging.getLogger("atr_control_plane_projection")

def _redis():
    if get_atr_redis is not None:
        return get_atr_redis()
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)

class ControlPlaneProjectionService:
    """
    Projects the SQL event-journal graph state into the Redis serving layer.
    """

    @staticmethod
    def project_node(node_id: str) -> bool:
        """Projects a single graph node into its appropriate Redis keys."""
        enforce = os.getenv("ATR_CONTROL_PLANE_PROJECTION_ENFORCE", "0") == "1"
        try:
            with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM atr_control_plane_nodes WHERE node_id = %s", (node_id,))
                node = cur.fetchone()
                if not node:
                    logger.warning(f"Node {node_id} not found for projection")
                    return False

                node_type = node["node_type"]  # type: ignore
                scope_value = node["scope_value"]  # type: ignore
                state = node["node_state_json"]  # type: ignore
                version = node["version"]  # type: ignore

                # Build keys based on node type
                r = _redis()
                pipeline = r.pipeline()  # type: ignore

                prefix = f"cfg:atr:{node_type.lower()}:{scope_value}"

                # We project the entire state as a JSON, and specific keys for O(1) runtime lookups
                pipeline.set(prefix, json.dumps(state))
                pipeline.set(f"{prefix}:_version", version)
                pipeline.set(f"{prefix}:_updated_ms", int(time.time() * 1000))

                if node_type == "RolloutState":
                    pipeline.set(f"cfg:atr_rollout_stage:{scope_value}", state.get("rollout_stage", "none"))
                elif node_type == "FreezeState":
                    pipeline.set(f"cfg:atr_freeze:{scope_value}", state.get("status", "none"))
                elif node_type == "OverrideState":
                    # For override, handle TTL
                    expires_at = state.get("expires_at_ms", 0)
                    now_ms = int(time.time() * 1000)
                    ttl = max(0, int((expires_at - now_ms) / 1000))
                    if ttl > 0:
                        pipeline.setex(f"cfg:atr_override:{scope_value}", ttl, state.get("status", "active"))
                    else:
                        logger.info(f"Override {node_id} expired, not projecting to Redis.")

                if enforce:
                    pipeline.execute()
                else:
                    logger.info(f"[SHADOW MODE] Would project {node_id} to Redis: {state}")

            return True
        except Exception as e:
            logger.error(f"Failed to project node {node_id}: {e}")
            return False

    @staticmethod
    def sync_all_active_nodes():
        """Full re-sync from SQL to Redis. Suitable for bootstrap."""
        try:
            with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
                cur.execute("SELECT node_id FROM v_control_plane_active_nodes")
                nodes = cur.fetchall()
                for node in nodes:
                    ControlPlaneProjectionService.project_node(node["node_id"])  # type: ignore
            logger.info(f"Successfully synced {len(nodes)} active nodes to projection.")
            return True
        except Exception as e:
            logger.error(f"Failed to sync all active nodes: {e}")
            return False
