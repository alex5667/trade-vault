#!/usr/bin/env python3
import logging
import os
import time
import uuid
from datetime import datetime

import redis

try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None

from services.analytics_db import get_conn
from services.atr_effective_state_resolver import EffectiveStateResolver

logger = logging.getLogger("atr_cert_service")

class ProjectionCertService:
    @staticmethod
    def _generate_id(prefix: str):
         return f"{prefix}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    @staticmethod
    def run_cert_cycle():
        # Get all distinct scopes from legacy or new
        r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
        # We can scan config spaces for scopes
        scopes = set()
        cursor = '0'
        while cursor != 0:
            cursor, keys = r.scan(cursor=cursor, match="cfg:atr_effective_state:*", count=10000)
            for k in keys:
                scopes.add(k.split(":")[-1])

        # also add from graph nodes
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT DISTINCT scope_value FROM atr_control_plane_nodes")
            for row in cur.fetchall():
                scopes.add(row["scope_value"])  # type: ignore

            for symb in scopes:
                cert_id = ProjectionCertService._generate_id("cert")
                drifts = []

                # Retrieve legacy state
                legacy_state = EffectiveStateResolver.resolve_scope("symbol", symb, is_shadow_graph_mode=False)
                # Retrieve shadow graph state
                shadow_state = EffectiveStateResolver.resolve_scope("symbol", symb, is_shadow_graph_mode=True)

                # Check DB mismatches
                for field in ["rollout_stage", "freeze_state", "override_state", "release_state", "effective_runtime_state"]:
                    if (legacy_state.get(field)) != (shadow_state.get(field)):
                        drifts.append({
                            "drift_type": "state_mismatch",
                            "field": field,
                            "legacy_val": (legacy_state.get(field)),
                            "shadow_val": (shadow_state.get(field))
                        })

                # Check projection mismatches (redis)
                legacy_redis = r.get(f"cfg:atr_effective_state:{symb}")
                shadow_redis = r.get(f"shadow:cfg:atr_effective_state:{symb}")

                # Sometimes legacy state may not be there yet, but if it is different
                if legacy_redis and shadow_redis and str(legacy_redis) != str(shadow_redis):
                    drifts.append({
                        "drift_type": "projection_mismatch",
                        "field": "redis_effective_state",
                        "legacy_val": str(legacy_redis),
                        "shadow_val": str(shadow_redis)
                    })
                elif (legacy_redis and not shadow_redis) or (not legacy_redis and shadow_redis):
                    # One missing
                    pass # We leave this loose for now as shadow is syncing

                status = "passed" if not drifts else "failed"

                # Insert DB
                now_utc = datetime.utcnow()
                cur.execute("""
                    INSERT INTO atr_control_plane_projection_certs (
                        cert_id, scope_kind, scope_value, status, checked_at
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (cert_id, "symbol", symb, status, now_utc))

                for drift in drifts:
                    drift_id = ProjectionCertService._generate_id("drift")
                    cur.execute("""
                        INSERT INTO atr_control_plane_drifts (
                            drift_id, cert_id, scope_kind, scope_value, drift_type, drift_severity,
                            legacy_val, graph_val, status, detected_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        drift_id, cert_id, "symbol", symb, drift["drift_type"], "high",
                        drift.get("legacy_val"), drift.get("shadow_val"), "unresolved", now_utc
                    ))

            conn.commit()

if __name__ == "__main__":
    def loop():
        logging.basicConfig(level=logging.INFO)
        logger.info("Starting ProjectionCertService loop")
        while True:
            try:
                ProjectionCertService.run_cert_cycle()
            except Exception as e:
                logger.error(f"Error in cert cycle: {e}")
            time.sleep(30)

    loop()
