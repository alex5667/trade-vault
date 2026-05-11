#!/usr/bin/env python3
"""
ATR Policy Enforcement Router (Phase 10.2)
Responsible for mapping charter rule violations to specific enforcement actions
across different system layers (L1-L9).
"""

import json
import os
import time
from datetime import datetime
from typing import Any

import redis
from psycopg2.extras import RealDictCursor

from common.log import setup_logger
from services.analytics_db import get_conn

try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None  # type: ignore

logger = setup_logger("atr_policy_enforcement_router")


# Enforcement Action Priority (Higher index = higher priority)
ENFORCEMENT_PRIORITY = [
    "DIAG_ONLY",
    "WARN",
    "CLIP_NEW_RISK",
    "BLOCK_PROMOTION",
    "BLOCK_RELEASE",
    "DENY_NEW_RISK",
    "FREEZE_RELEASES",
    "FREEZE_SCOPE",
    "OPEN_QUARANTINE",
    "REQUIRE_ROLLBACK_REVIEW",
    "REQUIRE_DR_RESTORE"
]

class EnforcementDecision:
    def __init__(self, overall_action: str, results: list[dict[str, Any]]):
        self.overall_action = overall_action
        self.results = results
        self.timestamp = datetime.now().isoformat()

class ATRPolicyEnforcementRouter:
    _instance = None

    # TTL for automatic cache refresh (seconds)
    _MAP_CACHE_TTL = 300

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, redis_url: str = None):  # type: ignore
        if self._initialized:
            return
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        # Use centralized ATR shared pool from core.redis_client
        if get_atr_redis is not None:
            self._r = get_atr_redis()
        else:
            self._r = redis.Redis.from_url(self.redis_url, decode_responses=True)
        self._map_cache: dict[str, list[dict[str, Any]]] = {}
        self._map_loaded_at: float = 0.0
        self._initialized = True
        # Pre-warm cache at startup (blocking, but only once)
        try:
            self._load_map()
        except Exception as e:
            logger.warning("ATRPolicyEnforcementRouter: pre-warm failed (will retry lazily): %s", e)

    def _load_map(self) -> dict[str, list[dict[str, Any]]]:
        """Load enforcement map from DB. Thread-safe, TTL-based refresh."""
        new_map: dict[str, list[dict[str, Any]]] = {}
        try:
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT * FROM atr_policy_enforcement_map WHERE activated_at IS NOT NULL OR created_at IS NOT NULL")
                    rows = cur.fetchall()
                    for row in rows:
                        ctx = row["context_kind"]
                        if ctx not in new_map:
                            new_map[ctx] = []
                        new_map[ctx].append(dict(row))
            self._map_cache = new_map
            self._map_loaded_at = time.monotonic()
            return new_map
        except Exception as e:
            logger.error(f"Failed to load enforcement map from DB: {e}")
            return self._map_cache

    def decide_enforcement(self, context_kind: str, context_ref: str, failed_rule_ids: list[str]) -> dict[str, Any]:
        """
        Map failed rules to enforcement actions and aggregate them.
        """
        # Refresh cache if stale (TTL-based, not only on first call)
        if not self._map_cache or (time.monotonic() - self._map_loaded_at) > self._MAP_CACHE_TTL:
            self._load_map()

        mappings = self._map_cache.get(context_kind, [])
        triggered_actions = []

        # Find mappings for failed rules
        for rule_id in failed_rule_ids:
            rule_mappings = [m for m in mappings if m["rule_id"] == rule_id]
            if not rule_mappings:
                # Default behavior if no mapping: WAARN?
                triggered_actions.append({
                    "rule_id": rule_id,
                    "action": "WARN",
                    "severity": "info",
                    "target_layer": "L9", # Audit Only
                    "reason_code": "NO_ENFORCEMENT_MAPPING"
                })
                continue

            for m in rule_mappings:
                action = m["default_action"]
                # In blocking mode, we use default_action.
                # If advisory, we downgrade to WARN/DIAG_ONLY
                if m["enforcement_mode"] == "advisory":
                    action = "WARN"

                triggered_actions.append({
                    "rule_id": rule_id,
                    "action": action,
                    "severity": m["severity"],
                    "target_layer": m["target_layer"],
                    "reason_code": m["map_json"].get("reason_codes", {}).get("fail", f"FAIL_{rule_id}"),
                    "map_json": m["map_json"]
                })

        if not triggered_actions:
            return {
                "overall_action": "allow",
                "triggered_actions": [],
                "context_kind": context_kind,
                "context_ref": context_ref,
            }

        # Aggregate overall action based on priority
        overall_action = self._aggregate_actions([a["action"] for a in triggered_actions])

        decision = {
            "overall_action": overall_action,
            "triggered_actions": triggered_actions,
            "context_kind": context_kind,
            "context_ref": context_ref,
            "timestamp": datetime.now().isoformat()
        }

        # Persist decision and events
        self._persist_decision(decision)

        # Update Redis cache for runtime context
        if context_kind == "runtime_context":
            self._update_runtime_cache(context_ref, decision)

        return decision

    def _update_runtime_cache(self, symbol: str, decision: dict[str, Any]):
        """Update Redis with runtime decision for fast access."""
        cache_key = f"cache:atr:enforcement:runtime:{symbol}"
        # Cache for 1 hour by default, or less if needed
        self._r.setex(cache_key, 3600, json.dumps(decision))  # type: ignore

    def _aggregate_actions(self, actions: list[str]) -> str:
        """Find the highest priority action."""
        highest_idx = -1
        best_action = "allow"

        for a in actions:
            try:
                idx = ENFORCEMENT_PRIORITY.index(a)
                if idx > highest_idx:
                    highest_idx = idx
                    best_action = a
            except ValueError:
                logger.warning(f"Unknown action: {a}")

        return best_action

    def _persist_decision(self, decision: dict[str, Any]):
        """Save decision and events to Postgres (Best Effort)."""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 1. Decisions
                    decision_id = f"dec_{datetime.now().strftime('%Y%m%d%H%M%S')}_{decision['context_ref'][:10]}"
                    cur.execute("""
                        INSERT INTO atr_policy_enforcement_decisions (
                            decision_id, context_kind, context_ref, overall_action, summary_json
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, (decision_id, decision["context_kind"], decision["context_ref"],
                          decision["overall_action"], json.dumps(decision)))

                    # 2. Events
                    for a in decision["triggered_actions"]:
                        event_id = f"evt_{uuid_short()}"
                        cur.execute("""
                            INSERT INTO atr_policy_enforcement_events (
                                event_id, context_kind, context_ref, rule_id, target_layer,
                                action, severity, reason_code, evidence_json
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (event_id, decision["context_kind"], decision["context_ref"],
                              a["rule_id"], a["target_layer"], a["action"], a["severity"],
                              a["reason_code"], json.dumps(a.get("map_json", {}))))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to persist enforcement decision: {e}")

    def get_runtime_decision(self, symbol: str) -> dict[str, Any]:
        """
        Specific helper for L2 Runtime Dispatch.
        Uses cached decision from Redis to avoid DB hits in hot paths.
        """
        cache_key = f"cache:atr:enforcement:runtime:{symbol}"
        cached = self._r.get(cache_key)  # type: ignore
        if cached:
            return json.loads(cached)

        # Global cache check (all symbols)
        global_cache = self._r.get("cache:atr:enforcement:runtime:global")  # type: ignore
        if global_cache:
            return json.loads(global_cache)

        return {"overall_action": "allow", "reason": "cache_miss_default_allow"}

def uuid_short():
    import uuid
    return uuid.uuid4().hex[:12]

def get_enforcement_router():
    return ATRPolicyEnforcementRouter()
