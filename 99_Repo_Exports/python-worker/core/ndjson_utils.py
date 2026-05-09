import json
from collections.abc import Generator
from typing import Any


def read_concatenated_json(content: str) -> Generator[Any, None, None]:
    """
    Parses a string containing multiple JSON objects (either NDJSON or concatenated {}{}).
    Tries robustly to separate objects.
    """
    if not content:
        return

    # 1. Try standard line-based splitting (NDJSON)
    lines = content.strip().split('\n')
    parsed_objects = []
    all_lines_valid = True

    non_empty_lines = [l for l in lines if l.strip()]
    if not non_empty_lines:
        return

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            parsed_objects.append(json.loads(line))
        except json.JSONDecodeError:
            all_lines_valid = False
            break

    if all_lines_valid:
        yield from parsed_objects
        return

    # 2. Fallback: Character-by-character balance matching for concatenated objects like {...}{...}
    decoder = json.JSONDecoder()
    idx = 0
    length = len(content)

    while idx < length:
        # Skip whitespace
        while idx < length and content[idx].isspace():
            idx += 1

        if idx >= length:
            break

        try:
            obj, end_idx = decoder.raw_decode(content, idx)
            yield obj
            idx = end_idx
        except json.JSONDecodeError:
            # Skip invalid char and try next (fail-open mostly)
            idx += 1

def load_ndjson_file(path: str) -> list[Any]:
    """Reads a file that might be NDJSON or concatenated JSON."""
    try:
        with open(path, encoding='utf-8') as f:
            content = f.read()
        return list(read_concatenated_json(content))
    except Exception:
        return []
