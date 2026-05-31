from __future__ import annotations

import json


def _maybe_json_load(v: Any) -> Any:
    """Safely decode JSON strings or bytes."""
    try:
        if isinstance(v, (bytes, bytearray)):
            return json.loads(v.decode("utf-8", errors="ignore"))
        if isinstance(v, str):
            return json.loads(v)
        return v
    except Exception:
        return v


def normalize_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Merge JSON content from data/payload fields into main entry."""
    # Try data field first (legacy)
    if "data" in entry:
        v = _maybe_json_load(entry["data"])
        if isinstance(v, dict):
            # Merge without overwriting existing keys
            for k, val in v.items():
                entry.setdefault(k, val)
    # Try payload field (new)
    if "payload" in entry:
        v = _maybe_json_load(entry["payload"])
        if isinstance(v, dict):
            for k, val in v.items():
                entry.setdefault(k, val)
    # Decode nested JSON fields
    for field in ["signal_payload", "signal_settings", "risk"]:
        if field in entry:
            entry[field] = _maybe_json_load(entry[field])
    return entry


def test_normalize_entry_merges_data_json():
    entry = {"stream": "notify", "data": json.dumps({"signal_payload": {"a": 1}})}
    normalized = normalize_entry(entry)
    assert normalized["signal_payload"] == {"a": 1}
    assert "data" in normalized  # original preserved
