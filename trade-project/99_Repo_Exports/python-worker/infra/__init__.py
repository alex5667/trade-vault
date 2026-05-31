"""
Infrastructure layer for Redis persistence and repository patterns.

This module provides:
- Redis-based trade repository
- Position state persistence
- Event streaming
- Recovery mechanisms
"""

from infra.redis_repo import RedisTradeRepository

__all__ = [
    "RedisTradeRepository",
]

