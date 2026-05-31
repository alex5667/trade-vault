import json
from typing import Any

from common.json_safe import to_json_safe
from common.payload_fingerprint import fingerprint_tradeable_payload
from utils.time_utils import get_ny_time_millis


def parse_envelope_fields(fields: dict[str, Any]) -> dict[str, Any] | None:
    try:
        raw = fields.get("data") or fields.get("payload") or fields.get("payload_json")
        if raw is None:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        if isinstance(raw, str):
            return json.loads(raw)
        if isinstance(raw, dict):
            return raw
        return None
    except Exception:
        return None


class EnvelopeParser:
    def __init__(self, redis_client: Any, dlq_stream: str, logger: Any):
        self.redis = redis_client
        self.dlq_stream = dlq_stream
        self.logger = logger

    def strict_validate_env(self, env: dict[str, Any], outbox_strict_validate: str) -> None:
        if outbox_strict_validate.lower() in {"0", "false", "no"}:
            return
        # 1) trace/events не должны быть в targets
        t = env.get("targets") or {}

        def _scan(x: Any) -> None:
            if isinstance(x, dict):
                if isinstance(x.get("trace"), (dict, list)) or isinstance(x.get("decision_trace"), (dict, list)):
                    raise ValueError("trace leaked into tradeable targets")
                for v in x.values():
                    _scan(v)
            elif isinstance(x, list):
                for v in x:
                    _scan(v)

        _scan(t)

    def parse_envelope(self, fields: dict[str, Any]) -> dict[str, Any] | None:
        """
        Backward compatibility:
          - OutboxWriter may write both: data + payload
          - Some legacy producers wrote only: payload
        """
        raw = fields.get("data")
        if not raw:
            raw = fields.get("payload")
        if not raw:
            raw = fields.get("payload_json")
        if not raw:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "errors='ignore'")
        if isinstance(raw, str):
            try:
                env = json.loads(raw)
            except Exception:
                return None
        elif isinstance(raw, dict):
            env = raw
        else:
            return None

        if not isinstance(env, dict):
            return None

        # AUTO-REPAIR: If envelope is flat (not nested under 'targets'), wrap it.
        if "targets" not in env:
            try:
                has_audit = "audit_payload" in env
                has_notify = "notify_payload" in env or "notify" in env

                if has_audit or has_notify:
                    if self.logger:
                        self.logger.info(f"🔧 Auto-repairing flat envelope for sid={env.get('sid', 'unknown')}")
                    targets = {}

                    if "audit_payload" in env:
                        targets["audit_payload"] = env.pop("audit_payload")

                    if "notify_payload" in env:
                        targets["notify"] = env.pop("notify_payload")
                    elif "notify" in env:
                        targets["notify"] = env.pop("notify")

                    if "signal_stream_payload" in env:
                        targets["signal_stream_payload"] = env.pop("signal_stream_payload")

                    env["targets"] = targets

                    if "meta" not in env:
                        env["meta"] = {}

                    for key in ["audit_stream", "signal_stream", "manual_stream"]:
                        if key in env and key not in env["meta"]:
                            env["meta"][key] = env.pop(key)
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"⚠️ Failed to auto-repair flat envelope: {e}")

        # VALIDATION: Ensure envelope structure is correct
        if "audit_payload" in env or "meta" not in env or "targets" not in env:
            try:
                if self.logger:
                    self.logger.warning("⚠️ Malformed envelope structure detected: audit_payload on top level or missing required fields")
                    self.logger.warning(f"   env keys: {list(env.keys())}")
                    self.logger.warning(f"   sid: {env.get('sid', 'unknown')}")
                payload = {
                    "ts": get_ny_time_millis(),
                    "reason": "malformed_envelope_structure",
                    "sid": (env.get("sid") or ""),
                    "env_keys": list(env.keys()),
                    "has_audit_payload_top": "audit_payload" in env,
                    "has_meta": "meta" in env,
                    "has_targets": "targets" in env,
                    "raw": raw[:1000] if isinstance(raw, str) else str(raw)[:1000],
                }
                self.redis.xadd(self.dlq_stream, {"data": json.dumps(payload, ensure_ascii=False)}, maxlen=200000, approximate=True)
            except Exception:
                pass
            return None

        # FAIL-CLOSED (DLQ) on fingerprint mismatch BEFORE ANY MUTATION.
        try:
            meta = env.get("meta") or {}
            expected = meta.get("payload_sha1") if isinstance(meta, dict) else None
            if isinstance(expected, str) and expected:
                env_safe = to_json_safe(env)
                got, _nbytes = fingerprint_tradeable_payload(env_safe)
                if str(got) != str(expected):
                    try:
                        payload = {
                            "ts": get_ny_time_millis(),
                            "reason": "payload_fingerprint_mismatch",
                            "sid": (env.get("sid") or ""),
                            "expected": str(expected),
                            "got": str(got),
                            "env": env_safe,
                        }
                        self.redis.xadd(self.dlq_stream, {"data": json.dumps(payload, ensure_ascii=False)}, maxlen=200000, approximate=True)
                    except Exception:
                        pass
                    return None
        except Exception:
            pass

        # VALIDATE DELTA/Z PRESERVATION
        try:
            targets = env.get("targets") or {}
            for target_name, target_payload in targets.items():
                if isinstance(target_payload, dict):
                    signal_type = target_payload.get("type")
                    has_delta = "delta" in target_payload
                    has_delta_z = "delta_z" in target_payload or "z" in target_payload

                    if signal_type == "delta_spike" or has_delta:
                        delta_val = target_payload.get("delta")
                        delta_z_val = target_payload.get("delta_z") or target_payload.get("z")

                        if (delta_val is None or delta_z_val is None) and (has_delta or has_delta_z):
                            if self.logger:
                                self.logger.warning(
                                    f"⚠️ [{env.get('sid', 'unknown')}] Potential delta/z zeroing detected in {target_name}: "
                                    f"delta={delta_val}, z={delta_z_val}, signal_type={signal_type}"
                                )
        except Exception as exc:
            if self.logger:
                self.logger.warning(f"⚠️ [{env.get('sid', 'unknown')}] Delta/z validation error: {exc}")

        return env
