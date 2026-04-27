import re
with open("/home/alex/front/trade/scanner_infra/python-worker/handlers/crypto_orderflow/utils/smt_coherence_gate.py", "r") as f:
    text = f.read()

# Make it safe for awaitables
text = text.replace("import json", "import json\nimport inspect\nimport asyncio")

new_func = """
def _sync_get(val: Any) -> Any:
    if inspect.isawaitable(val):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                val.close()
                return None
            return loop.run_until_complete(val)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(val)
    return val
"""
text = text.replace("def _redis_read_bundle_state(", new_func + "\n\ndef _redis_read_bundle_state(")
text = text.replace("v = redis_client.get(key)", "v = _sync_get(redis_client.get(key))")
text = text.replace("d = redis_client.hgetall(key) or {}", "d = _sync_get(redis_client.hgetall(key)) or {}")
text = text.replace("self.redis.xadd(", "_sync_get(self.redis.xadd(")
text = text.replace("self.redis.hset(", "_sync_get(self.redis.hset(")

with open("/home/alex/front/trade/scanner_infra/python-worker/handlers/crypto_orderflow/utils/smt_coherence_gate.py", "w") as f:
    f.write(text)
