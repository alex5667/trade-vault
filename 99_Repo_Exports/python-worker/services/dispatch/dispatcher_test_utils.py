"""
Test-only facades and mock classes for SignalDispatcher.
Extracted from dispatcher_app.py to reduce production noise.
"""

from typing import Any
from collections import defaultdict

class TestIdempotencyStore:
    def __init__(self, d):
        self.d = d
        
    def notify_idempotent(self, client, sid, payload):
        if hasattr(self.d, "_evalsha_or_eval") and hasattr(self.d, "lua_scripts") and self.d.lua_scripts:
            prefix = getattr(self.d, "marker_prefix", "marker:dispatch")
            marker_key = f"{prefix}:notify:{sid}"
            flat = self.d._flatten_notify_fields(payload)
            args = [
                marker_key, 
                getattr(self.d, "notify_stream", "stream:notify"), 
                getattr(self.d, "notify_signal_counter_key", "stat:notify"), 
                getattr(self.d, "marker_gc_zset", "marker:gc"), 
                "60", "notify", sid, "{}", "0", "0", 
                str(len(flat) // 2)
            ] + flat
            sha = self.d.lua_scripts.get_sha("notify_gate")
            script = self.d.lua_scripts.get_script("notify_gate")
            self.d._evalsha_or_eval(client, sha, "notify_gate", script, 4, *args)
        return True

    def xadd_idempotent_atomic(self, client, target, sid, stream, fields, maxlen):
        if hasattr(self.d, "_evalsha_or_eval") and hasattr(self.d, "lua_scripts") and self.d.lua_scripts:
            prefix = getattr(self.d, "marker_prefix", "marker:dispatch")
            marker_key = f"{prefix}:{target}:{sid}"
            payload_json = fields.get("data") or fields.get("payload") or "{}"
            args = [marker_key, stream, getattr(self.d, "marker_gc_zset", "marker:gc"), "60", "xadd", str(maxlen), sid, payload_json]
            sha = self.d.lua_scripts.get_sha("deliver")
            script = self.d.lua_scripts.get_script("deliver")
            self.d._evalsha_or_eval(client, sha, "deliver", script, 3, *args)
        return True

    def setex_idempotent_atomic(self, client, target, sid, key, ttl_sec, value_json):
        if hasattr(self.d, "_evalsha_or_eval") and hasattr(self.d, "lua_scripts") and self.d.lua_scripts:
            prefix = getattr(self.d, "marker_prefix", "marker:dispatch")
            marker_key = f"{prefix}:{target}:{sid}"
            args = [marker_key, key, getattr(self.d, "marker_gc_zset", "marker:gc"), "60", "setex", str(ttl_sec), sid, value_json]
            sha = self.d.lua_scripts.get_sha("deliver")
            script = self.d.lua_scripts.get_script("deliver")
            self.d._evalsha_or_eval(client, sha, "deliver", script, 3, *args)
        return True

    def mark_env_done(self, client, sid, env):
        if hasattr(self.d, "redis"):
            if hasattr(self.d, "_env_done_key") and self.d._env_done_key:
                key = self.d._env_done_key(sid)
            else:
                from services.dispatcher.key_utils import KeyUtils
                prefix = getattr(self.d, "env_done_prefix", "done:sid")
                key = KeyUtils.env_done_key(prefix, sid)
            self.d.redis.set(key, "1", ex=getattr(self.d, "delivery_marker_ttl_sec", 3600), nx=True)

    def mark_outbox_done(self, msg_id):
        pass

    def marker_client_for_target(self, target, dual_client, simple_client):
        if hasattr(self.d, "_marker_client_for_target") and self.d._marker_client_for_target:
            return self.d._marker_client_for_target(target, dual_client, simple_client)
        if target == "notify": return dual_client
        return simple_client

    def marker_exists(self, client, target, sid):
        if hasattr(self.d, "_marker_exists") and self.d._marker_exists:
            return getattr(self.d, "_marker_exists", lambda *a, **kw: False)(client, target, sid)
        return getattr(self.d, "_test_markers", {}).get(target, False)


class TestRetryScheduler:
    def __init__(self, d):
        self.d = d
        
    def schedule_target_retry(self, *a, **kw):
        return getattr(self.d, "_schedule_target_retry", lambda *a, **kw: None)(*a, **kw)


class TestDlqWriter:
    def __init__(self, d):
        self.d = d
        
    def send_target_dlq(self, *a, **kw):
        return getattr(self.d, "_send_target_dlq", lambda *a, **kw: None)(*a, **kw)
        
    def send_dlq_and_ack(self, *a, **kw):
        return getattr(self.d, "_send_target_dlq", lambda *a, **kw: None)(*a, **kw)
