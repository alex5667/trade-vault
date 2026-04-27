"""
Handlers package.

Keep package init import-light to avoid heavy side-effect imports during tools/tests
collection (e.g. redis clients, network initializers, optional deps).

Import concrete handlers explicitly where needed, e.g.:


    from handlers.signal_processor import SignalProcessor
"""

__all__ = []