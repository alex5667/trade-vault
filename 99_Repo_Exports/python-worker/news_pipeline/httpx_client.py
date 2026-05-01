# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
import requests


@dataclass(frozen=True)
class HttpRetryPolicy:
    timeout_sec: float = 12.0
    max_attempts: int = 4
    base_backoff_sec: float = 0.6
    max_backoff_sec: float = 8.0


def _sleep_backoff(attempt: int, base: float, cap: float) -> None:
    # decorrelated jitter
    t = min(cap, base * (2 ** max(0, attempt - 1)))
    t = t * (0.6 + random.random() * 0.8)  # 0.6..1.4x
    time.sleep(max(0.0, t))


def http_get_json(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    policy: HttpRetryPolicy = HttpRetryPolicy(),
) -> Tuple[Optional[Any], Optional[str]]:
    """
    Возвращает (json, error_str).
    Fail-open: если не смогли — json=None и error_str заполнен.
    """
    last_err = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=policy.timeout_sec)
            # 429/5xx: ретраим
            if r.status_code == 429 or (500 <= r.status_code <= 599):
                last_err = f"http {r.status_code}: {r.text[:240]}"
                _sleep_backoff(attempt, policy.base_backoff_sec, policy.max_backoff_sec)
                continue
            if r.status_code != 200:
                return None, f"http {r.status_code}: {r.text[:240]}"
            try:
                return r.json(), None
            except Exception:
                # иногда приходят строки/битый JSON
                try:
                    return json.loads(r.text), None
                except Exception:
                    return None, "bad json"
        except Exception as e:
            last_err = str(e)[:240]
            _sleep_backoff(attempt, policy.base_backoff_sec, policy.max_backoff_sec)
    return None, (last_err or "unknown http error")
