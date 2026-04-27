import os
import sys
import traceback
from pathlib import Path
import pytest
import redis

# --- AGGRESSIVE PATH LOCKDOWN ---
def _lockdown_path() -> None:
    root = Path(__file__).resolve().parents[1]
    s_root = str(root)
    s_repo = str(root.parent)
    
    # 1. Ensure ROOT is at the start
    if s_root in sys.path:
        while s_root in sys.path:
            sys.path.remove(s_root)
    sys.path.insert(0, s_root)
    
    # 2. Strict prune sys.path
    allowed_substrings = [s_root, "/usr/lib", "/usr/local/lib", ".local/lib", "site-packages", "dist-packages"]
    new_path = []
    for p in sys.path:
        # Keep if it matches allowed substrings AND does not contain scanner_infra (unless it's in our root)
        is_allowed = any(a in p for a in allowed_substrings)
        is_shadow = ("scanner_infra" in p) and (s_root not in p)
        if is_allowed and not is_shadow:
            new_path.append(p)
        elif not p: # Current dir
            new_path.append(p)
            
    sys.path[:] = new_path
            
    # 3. Filter out redundant sub-directories that were added by hacks
    subdirs = ["services", "tools", "ml_analysis", "tick_flow_full", "infra", "core", "orderflow_services"]
    # 4. Clean up sys.modules to force re-import if shadowed
    to_cleanup = ["ml_analysis", "infra", "tools", "services", "core", "orderflow_services", "tick_flow_full", "utils"]
    shadowed_keys = []
    for k, m in list(sys.modules.items()):
        # Remove if it's from scanner_infra but not from our root
        if hasattr(m, "__file__") and m.__file__ and "scanner_infra" in m.__file__ and s_root not in m.__file__:
            shadowed_keys.append(k)
        else:
            # Also check by base package name
            base_pkg = k.split(".")[0]
            if base_pkg in to_cleanup:
                if hasattr(m, "__file__") and m.__file__ and s_root not in m.__file__:
                    shadowed_keys.append(k)

    if shadowed_keys:
        for k in shadowed_keys:
            del sys.modules[k]
    
    # 5. Diagnostic: where is infra coming from?
    try:
        import infra
    except ImportError:
        pass

    try:
        import ml_analysis
    except:
        pass

    try:
        import orderflow_services
    except Exception:
        pass

_lockdown_path()
# --- END LOCKDOWN ---

def pytest_configure() -> None:
    _lockdown_path()
    # Add any global test configuration here
    # Example: silencing specific logger
    import logging
    logging.getLogger("faker").setLevel(logging.ERROR)
    print(f"CONFTEST: sys.path[0] = {sys.path[0]}", file=sys.stderr)
    print(f"CONFTEST: infra exists? {os.path.exists(os.path.join(sys.path[0], 'infra'))}", file=sys.stderr)
    os.environ.setdefault("PYTHONHASHSEED", "0")
    os.environ.setdefault("SIGNAL_ONE_JSON_LOG", "1")
    os.environ.setdefault("SIGNAL_REDIS_URL", "redis://localhost:6379/15")
    os.environ.setdefault("REDIS_HOST", "localhost")
    os.environ.setdefault("REDIS_PORT", "6379")

@pytest.fixture(scope="session")
def redis_url():
    return os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")

@pytest.fixture()
def r(redis_url):
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    try:
        client.ping()
        client.flushdb()
    except redis.exceptions.ConnectionError:
        pytest.skip("Local Redis is not available")
    yield client
    try:
        client.flushdb()
    except Exception:
        pass

@pytest.fixture()
def redis_client(r):
    return r
