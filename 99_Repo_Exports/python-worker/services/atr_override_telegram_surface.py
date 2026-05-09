import json
import logging
from typing import Any

from core.redis_keys import STREAM_RETENTION
from core.redis_keys import RedisStreams as RS
from services.atr_change_control_telegram_surface import _ops_chat_id, _redis
from services.atr_override_governance_service import ATROverrideGovernanceService

logger = logging.getLogger("atr_override_telegram")

class ATROverrideTelegramSurface:
    def __init__(self, override_service: ATROverrideGovernanceService):
        self.override_service = override_service

    def publish_override_request(self, req: dict[str, Any]) -> bool:
        override_id = req.get("override_id")
        scope = req.get("request_json", {}).get("scope", {})

        txt = (
            f"ATR Safe-State Override\n"
            f"Override: {override_id}\n"
            f"Class: {req.get('override_class')}\n"
            f"Scope: {scope.get('symbol','*')} | {scope.get('scenario','*')} | {scope.get('layer','*')} | v{scope.get('policy_ver','0')}\n"
            f"Current: {req.get('current_state')}\n"
            f"Target: {req.get('requested_target_state')}\n"
            f"TTL: {req.get('ttl_sec', 0) // 60}m\n\n"
        )

        # In a real impl, constraints & rollback conditions would be dynamically formatted
        txt += (
            "Allowed:\n"
            "- new entries (if constraints permit)\n"
            "- protective exits\n\n"
            "Blocked:\n"
            "- overriding hard invariants\n\n"
            "Rollback conditions:\n"
            "- any critical invariant breach\n"
            "- burn exhausted again\n"
            "- post-override cert fail\n"
        )

        buttons = [
            [
                {"text": "✅ Approve", "callback": f"atrovr:approve:{override_id}"},
                {"text": "❌ Reject", "callback": f"atrovr:reject:{override_id}"},
            ],
            [
                {"text": "⏹ Revoke", "callback": f"atrovr:revoke:{override_id}"},
            ]
        ]

        payload = {
            "text": txt,
            "buttons": json.dumps(buttons, ensure_ascii=False)
        }

        chat_id = _ops_chat_id()
        if chat_id:
            payload["chat_id"] = chat_id

        try:
            _redis().xadd(RS.NOTIFY_TELEGRAM, payload, maxlen=STREAM_RETENTION[RS.NOTIFY_TELEGRAM], approximate=True)
            return True
        except Exception as e:
            logger.error(f"Failed to publish TG: {e}")
            return False

    def handle_callback(self, action: str, override_id: str, actor: str):
        if action == "approve":
            res = self.override_service.approve_override(override_id, actor)
            status = "Approved" if res.get("status") == "success" else f"Failed: {res.get('message')}"
            self._publish_ack(override_id, status, actor)
        elif action == "reject":
            self.override_service.revoke_override(override_id, "REJECTED_BY_OPERATOR")
            self._publish_ack(override_id, "Rejected", actor)
        elif action == "revoke":
            self.override_service.revoke_override(override_id, "REVOKED_BY_OPERATOR")
            self._publish_ack(override_id, "Revoked", actor)

    def _publish_ack(self, override_id: str, action: str, actor: str) -> bool:
        text = (
            f"ATR Override Action\n"
            f"Override ID: {override_id}\n"
            f"Action: {action}\n"
            f"Actor: {actor}\n"
        )
        payload = {"text": text}
        chat_id = _ops_chat_id()
        if chat_id:
            payload["chat_id"] = chat_id
        try:
            _redis().xadd(RS.NOTIFY_TELEGRAM, payload, maxlen=STREAM_RETENTION[RS.NOTIFY_TELEGRAM], approximate=True)
            return True
        except Exception:
            return False
