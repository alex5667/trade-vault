import os
from typing import Any

from utils.time_utils import get_ny_time_millis

DRY_RUN = os.getenv("VERTEX_ROUTING_INCIDENT_ROUTE_RCA_DRY_RUN", "1") == "1"

class VertexRcaProvider:
    def __init__(self) -> None:
        pass

    async def generate_rca(self, payload: dict[str, Any]) -> dict[str, Any]:
        severity = payload.get("severity", "info")
        reason = payload.get("primary_reason_codes", "UNKNOWN")

        # Mock Vertex generation
        await asyncio.sleep(0.5)

        advisory_action = "DEGRADE" if severity == "critical" else "MONITOR"
        return {
            "analysis": f"Vertex Governance RCA completed for {reason}. System flagged as {severity}.",
            "advisory_action": advisory_action,
            "confidence": 0.85,
            "provider_ts": get_ny_time_millis()
        }

# For test purposes
import asyncio
