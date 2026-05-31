from __future__ import annotations

from typing import Optional, Dict, Any
import time
from common.transient import is_transient_error


def setup_logger(name):
    import logging
    return logging.getLogger(name)


class ErrorHandler:

    def is_transient_error(self, e: Exception) -> bool:
        """Единый источник истины (E): типы + токены."""
        try:
            import redis
            from redis.exceptions import ConnectionError as RConn, TimeoutError as RTimeout, ResponseError as RResp
            if isinstance(e, (RConn, RTimeout)):
                return True
            # Redis LOADING / BUSY / TRYAGAIN обычно transient
            if isinstance(e, RResp):
                s = str(e).lower()
                if ("loading" in s) or ("busy" in s) or ("tryagain" in s) or ("timeout" in s):
                    return True
        except Exception:
            pass
        if isinstance(e, (OSError, TimeoutError)):
            return True
        _TRANSIENT_TOKENS = (
            "timeout", "timed out", "connection", "broken pipe", "try again",
            "temporarily", "reset by peer", "busy loading", "loading the dataset"
        )
        msg = str(e).lower()
        return any(t in msg for t in _TRANSIENT_TOKENS)

    # backward compat
    def _is_transient_error(self, e: Exception) -> bool:
        return self.is_transient_error(e)

