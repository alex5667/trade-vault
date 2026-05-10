import services.dispatch.dispatcher_app as sd


def test_notify_lua_script_exists_and_has_marker_write():
    s = getattr(sd, "_LUA_NOTIFY_GATE_XADD_THEN_MARK", "")
    assert isinstance(s, str) and len(s) > 50
    assert "INCR" in s
    assert "XADD" in s
    assert "SET" in s
