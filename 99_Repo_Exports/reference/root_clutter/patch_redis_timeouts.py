import re

fname = "/home/alex/front/trade/scanner_infra/python-worker/services/crypto_orderflow_service.py"
with open(fname, "r") as f:
    content = f.read()

# Instead of modifying REDIS_SOCKET_TIMEOUT defaults (which might affect other things),
# we explicitly increase it on the ticks connection and the main connection slightly if needed,
# but the most direct fix is to change the default block_ms locally when doing XREADGROUP so it's safely lower than socket timeout,
# or catch it cleanly.
# Actually, the error is an asyncio.TimeoutError OR redis.exceptions.TimeoutError.

# Let's adjust block_ms for consume_books and consume_ticks to be smaller or just handle it. 
# Wait, they are ALREADY handled in the EXCEPT blocks:
# is_timeout = isinstance(exc, TimeoutError) or "Timeout" in error_str
# But they trigger a `continue` which goes into a tight loop if backoff is 0.

# Let's fix the socket_timeout initialization to be larger (e.g., 30s) so it doesn't fire prematurely.
content = re.sub(
    r'sock_to = float\(os\.getenv\("REDIS_SOCKET_TIMEOUT", "15"\)\)',
    r'sock_to = float(os.getenv("REDIS_SOCKET_TIMEOUT", "30"))',
    content
)

with open(fname, "w") as f:
    f.write(content)
print("Updated REDIS_SOCKET_TIMEOUT default to 30.")
