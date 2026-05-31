"""
Dispatcher package for signal delivery.

Extracted from monolithic signal_dispatcher.py for better maintainability.
"""

from .lua_scripts import LuaScriptManager

__all__ = ["LuaScriptManager"]
