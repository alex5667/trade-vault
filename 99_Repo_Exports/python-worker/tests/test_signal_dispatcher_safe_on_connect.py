import pytest


def test_safe_on_connect_success_sets_healthcheck_zero():
    redis = pytest.importorskip("redis")

    # Import the safe_on_connect function that was monkey-patched
    if hasattr(redis.connection.Connection, '_original_on_connect'):
        # Get the patched function
        patched_func = redis.connection.Connection.on_connect

        class Parser:
            def __init__(self):
                self.called = False
            def on_connect(self, conn):
                self.called = True

        class Conn:
            def __init__(self):
                self._parser = Parser()
                self.client_name = "worker-1"
                self.health_check_interval = 30
                self.disconnected = False
                self._sent = []
            def send_command(self, *args):
                self._sent.append(args)
            def read_response(self):
                return "OK"
            def disconnect(self):
                self.disconnected = True

        c = Conn()
        # Call the patched function directly
        patched_func(c)
        assert c._parser.called is True
        assert c.health_check_interval == 0
        assert c.disconnected is False
    else:
        pytest.skip("Redis connection patching not applied")


def test_safe_on_connect_parser_failure_disconnects_and_raises():
    redis = pytest.importorskip("redis")

    if hasattr(redis.connection.Connection, '_original_on_connect'):
        patched_func = redis.connection.Connection.on_connect

        class Parser:
            def on_connect(self, conn):
                raise RuntimeError("boom")

        class Conn:
            def __init__(self):
                self._parser = Parser()
                self.client_name = None
                self.health_check_interval = 30
                self.disconnected = False
            def disconnect(self):
                self.disconnected = True

        c = Conn()
        with pytest.raises(RuntimeError):
            patched_func(c)
        assert c.disconnected is True
    else:
        pytest.skip("Redis connection patching not applied")


def test_safe_on_connect_setname_failure_is_fail_open():
    redis = pytest.importorskip("redis")

    if hasattr(redis.connection.Connection, '_original_on_connect'):
        patched_func = redis.connection.Connection.on_connect

        class Parser:
            def on_connect(self, conn):
                return None

        class Conn:
            def __init__(self):
                self._parser = Parser()
                self.client_name = "worker-1"
                self.health_check_interval = 30
                self.disconnected = False
            def send_command(self, *args):
                return None
            def read_response(self):
                return "ERR"
            def disconnect(self):
                self.disconnected = True

        c = Conn()
        # must not raise
        patched_func(c)
        assert c.health_check_interval == 0
        assert c.disconnected is False
    else:
        pytest.skip("Redis connection patching not applied")
