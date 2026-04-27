import re

with open("python-worker/core/outbox_envelope.py", "r") as f:
    text = f.read()

# Add imports
text = text.replace("import json", "import json\nimport uuid\nimport time")
text = text.replace("from typing import Any, Dict, Optional", "from typing import Any, Dict, Optional, List")

# Add fields
p1 = """    signal_id: str
    ts_ms: int
    kind: str
    symbol: str
    side: Optional[str] = None"""

r1 = """    signal_id: str
    ts_ms: int
    kind: str
    symbol: str
    event_id: str
    source: str
    ingest_time_ms: int
    trace_id: str
    quality_flags: Optional[List[str]] = None
    side: Optional[str] = None"""
if p1 in text:
    text = text.replace(p1, r1)

# Add factory
p2 = """    schema_version: int = 1

    def to_stream_fields(self) -> Dict[str, str]:"""

r2 = """    schema_version: int = 1

    @classmethod
    def make_envelope(cls, **kwargs) -> "OutboxEnvelope":
        if "event_id" not in kwargs:
            kwargs["event_id"] = str(uuid.uuid4())
        if "ingest_time_ms" not in kwargs:
            kwargs["ingest_time_ms"] = int(time.time() * 1000)
        return cls(**kwargs)

    def to_stream_fields(self) -> Dict[str, str]:"""
if p2 in text:
    text = text.replace(p2, r2)
    
# Update to_stream_fields
p3 = """        d: Dict[str, Any] = {
            "schema": self.schema_version,
            "signal_id": self.signal_id,
            "ts_ms": int(self.ts_ms),
            "kind": self.kind,
            "symbol": self.symbol,
        }"""
r3 = """        d: Dict[str, Any] = {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "source": self.source,
            "signal_id": self.signal_id,
            "event_time_ms": int(self.ts_ms),
            "ts_ms": int(self.ts_ms),  # backward compat alias
            "ingest_time_ms": int(self.ingest_time_ms),
            "kind": self.kind,
            "symbol": self.symbol,
            "trace_id": self.trace_id,
            "quality_flags": json.dumps(self.quality_flags or [], separators=(",", ":")),
        }"""
if p3 in text:
    text = text.replace(p3, r3)

with open("python-worker/core/outbox_envelope.py", "w") as f:
    f.write(text)
print("done")
