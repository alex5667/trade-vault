"""
Lifecycle management package for handlers.

Provides state management, thread lifecycle, and shutdown coordination.
"""

from .state_manager import HandlerStateManager

__all__ = ["HandlerStateManager"]
