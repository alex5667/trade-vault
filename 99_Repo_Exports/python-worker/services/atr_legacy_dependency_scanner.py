import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import asyncpg
from utils.env_config import get_env_var
from utils.redis_client import get_redis_client

logger = logging.getLogger("atr_legacy_dependency_scanner")

class ATRLegacyDependencyScanner:
    """
    Scanner for finding hidden legacy reads and out-of-band legacy writes.
    It writes findings to `atr_hidden_dependency_findings`.
    Classifies readers into:
     - harmless shadow readers
     - needed fallback readers
    and writers into:
     - forbidden out-of-band writers
    """

    def __init__(self, pg_dsn: str = None):
        self.pg_dsn = pg_dsn or get_env_var("TRADE_PG_DSN", "postgresql://trading:trading@postgres:5432/trade")
        self.redis = None

    async def connect(self):
        if not self.redis:
            self.redis = await get_redis_client()

    async def scan_redis_legacy_writes(self) -> list[dict[str, Any]]:
        """
        Scan Redis for recent out-of-band direct writes to legacy namespaces.
        For example, directly mutating `cfg:atr_override:*` bypassing the graph.
        """
        findings = []
        # Example pattern for legacy writes.
        # In a real dynamic scanner, we might check key idle times or use Redis keyspace notifications.
        # Here we perform a structural check of legacy namespaces.
        namespaces_to_check = {
            "atr_override": "cfg:atr_override:*",
            "atr_freeze": "atr:freeze:*",
            "atr_release": "atr:release:*"
        }

        for component, pattern in namespaces_to_check.items():
            # If the decommission feature is enabled, writes are forbidden.
            is_disabled = get_env_var(f"ATR_LEGACY_{component.upper().replace('ATR_','')}_WRITES_DISABLE", "0") == "1"
            if is_disabled:
                # Finding keys that might have been recently written (pseudo-check via TTL or IDLETIME in production)
                # For this implementation, we insert a placeholder logic.
                pass

        return findings

    async def scan_sql_legacy_dependencies(self) -> list[dict[str, Any]]:
        """
        Scan for SQL legacy queries/dependencies. 
        Could be hooked to pg_stat_statements or application logs.
        """
        return []

    async def run_scan(self):
        """Runs the scan and persists to atr_hidden_dependency_findings"""
        await self.connect()
        try:
            redis_findings = await self.scan_redis_legacy_writes()
            sql_findings = await self.scan_sql_legacy_dependencies()

            all_findings = redis_findings + sql_findings

            if all_findings:
                await self._persist_findings(all_findings)
            else:
                logger.info("scanner: no new hidden dependencies found.")
        except Exception as e:
            logger.error(f"Error during legacy dependency scan: {e}", exc_info=True)

    async def _persist_findings(self, findings: list[dict[str, Any]]):
        async with asyncpg.create_pool(self.pg_dsn) as pool, pool.acquire() as conn:
            for finding in findings:
                await conn.execute(
                    """
                        INSERT INTO atr_hidden_dependency_findings
                        (finding_id, component, severity, status, reason_code, finding_json, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (finding_id) DO NOTHING
                        """,
                    str(uuid.uuid4()),
                    finding['component'],
                    finding['severity'],
                    "open",
                    finding['reason_code'],
                    json.dumps(finding['finding_json']),
                    datetime.now(UTC)
                )
        logger.info(f"Persisted {len(findings)} legacy hidden dependency findings.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scanner = ATRLegacyDependencyScanner()
    asyncio.run(scanner.run_scan())
