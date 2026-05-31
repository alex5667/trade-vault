import inspect
from typing import Any, Callable, Dict

def _filter_kwargs_for_callable(func: Callable, **kwargs) -> Dict[str, Any]:
    """
    Returns a subset of kwargs that are accepted by func (positional_or_keyword or keyword_only).
    Handles functions, bound methods, and callables gracefully.
    """
    try:
        sig = inspect.signature(func)
    except (ValueError, TypeError):
        # Fallback: if we can't inspect (e.g. some builtins or mocks), return empty or all?
        # Safe strategy: return empty to avoid crashing, assuming func handles args or **kwargs
        # But if func has **kwargs, we should ideally pass all.
        # Let's assume for our specific use case (eval_reversal) we want safety.
        return {}

    filtered = {}
    has_varkw = False
    
    for param in sig.parameters.values():
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            has_varkw = True
            break
        if param.name in kwargs:
            filtered[param.name] = kwargs[param.name]
            
    if has_varkw:
        return kwargs
        
    return filtered

