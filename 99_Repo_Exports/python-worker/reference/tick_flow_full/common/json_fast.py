from __future__ import annotations

import json
from typing import Any

# One-line JSON with stable compact separators, UTF-8.
# Keep this centralized to avoid repeating kwargs in hot paths.

_SEPARATORS = (",", ":")


def dumps1(obj: Any) -> str:
    """
    Compact one-line JSON:
      - ensure_ascii=False (keep unicode)
      - separators=(',', ':') (no spaces)
    """
    return json.dumps(obj, ensure_ascii=False, separators=_SEPARATORS)
