import os
import zlib
from dataclasses import dataclass

@dataclass
class NotifyGateSettings:
    mode: str = "hash"
    every_n: int = 10

class NotifyGate:
    def __init__(self, settings):
        self.settings = settings

    def should_send(self, sid: str) -> bool:
        n = int(self.settings.every_n)
        if n <= 1:
            return True
        mode = (self.settings.mode or "hash").strip().lower()
        if mode == "hash":
            return (zlib.crc32(sid.encode("utf-8")) % n) == 0
        return True

def test():
    sid = "crypto-of:XRPUSDT:1771134230135"
    gate = NotifyGate(NotifyGateSettings(every_n=10))
    allowed = gate.should_send(sid)
    print(f"SID: {sid}, Allowed: {allowed}")
    
    crc = zlib.crc32(sid.encode("utf-8"))
    print(f"CRC32: {crc}, Mod 10: {crc % 10}")

    # Test distribution
    count = 0
    total = 10000
    for i in range(total):
        s = f"crypto-of:XRPUSDT:{1771134230135 + i}"
        if gate.should_send(s):
            count += 1
    
    print(f"Allowed {count} out of {total} ({count/total*100:.2f}%)")

if __name__ == "__main__":
    test()
