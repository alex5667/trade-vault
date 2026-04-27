from __future__ import annotations

"""
Centralized Redis connection monkey-patch:
 - disables CLIENT SETINFO recursion
 - keeps parser initialization (on_connect) intact to avoid None reader
"""

from typing import Any


def apply_redis_connection_patches() -> None:
    try:
        import redis
        import redis.connection

        if not hasattr(redis.connection.Connection, "_original_on_connect"):
            redis.connection.Connection._original_on_connect = redis.connection.Connection.on_connect  # type: ignore[attr-defined]

        def safe_on_connect(self: Any) -> None:
            try:
                if hasattr(self, "_parser") and self._parser is not None:
                    self._parser.on_connect(self)
            except Exception:
                self.disconnect()
                raise

            if getattr(self, "client_name", None):
                try:
                    self.send_command("CLIENT", "SETNAME", self.client_name)
                    if self.read_response() != "OK":
                        raise redis.ConnectionError("CLIENT SETNAME failed")
                except Exception:
                    pass

            # prevent recursive health-checks
            self.health_check_interval = 0

        if redis.connection.Connection.on_connect != safe_on_connect:  # type: ignore[comparison-overlap]
            redis.connection.Connection.on_connect = safe_on_connect  # type: ignore[assignment]
    except Exception:
        pass
