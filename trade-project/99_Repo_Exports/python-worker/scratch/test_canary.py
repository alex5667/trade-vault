import hashlib


def _stable_u01(key: str) -> float:
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return (int(h[:8], 16) % 10_000) / 10_000.0

print(_stable_u01("BTCUSDT|LONG"))
