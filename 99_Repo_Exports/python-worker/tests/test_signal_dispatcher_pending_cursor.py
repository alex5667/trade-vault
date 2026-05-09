

def test_try_restore_pending_cursor_none():
    from services.signal_dispatcher import _try_restore_pending_cursor
    assert _try_restore_pending_cursor(None, "k") is None


def test_try_restore_pending_cursor_bytes():
    from services.signal_dispatcher import _try_restore_pending_cursor

    class R:
        def get(self, key):
            return b"abc:123"

    assert _try_restore_pending_cursor(R(), "k") == "abc:123"


def test_try_restore_pending_cursor_str():
    from services.signal_dispatcher import _try_restore_pending_cursor

    class R:
        def get(self, key):
            return "zzz"

    assert _try_restore_pending_cursor(R(), "k") == "zzz"
