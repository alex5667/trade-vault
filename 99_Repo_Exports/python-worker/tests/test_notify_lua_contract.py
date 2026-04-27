from __future__ import annotations

from services import signal_dispatcher


def test_notify_lua_script_exists_and_has_cjson():
    s = getattr(signal_dispatcher, "_LUA_NOTIFY_GATE_XADD_THEN_MARK", "")
    assert isinstance(s, str)
    assert "XADD" in s
    assert "INCR" in s
    assert "SET" in s
