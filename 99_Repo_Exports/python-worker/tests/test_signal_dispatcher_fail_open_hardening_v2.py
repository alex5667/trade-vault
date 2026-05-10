from __future__ import annotations


class _BoomRedis:
    def __init__(self, *, boom: set[str]):
        self._boom = set(boom)

    def setex(self, *a, **k):
        if "setex" in self._boom:
            raise RuntimeError("boom:setex")
        return True

    def sadd(self, *a, **k):
        if "sadd" in self._boom:
            raise RuntimeError("boom:sadd")
        return 1

    def expire(self, *a, **k):
        if "expire" in self._boom:
            raise RuntimeError("boom:expire")
        return True


def _mk_sd_without_init():
    # robust to __init__ changes
    from services.dispatch.dispatcher_app import SignalDispatcher

    sd = SignalDispatcher.__new__(SignalDispatcher)
    sd.logger = None
    sd.metrics_prefix = "sd"
    sd._metrics = []

    def _incr(key: str):
        sd._metrics.append(str(key))

    sd._incr = _incr

    # minimal config used by methods under test
    sd.done_ttl_sec = 10
    sd.env_state_ttl_sec = 10

    # minimal key builders
    sd._done_key = lambda msg_id: f"done:{msg_id}"
    sd._env_req_key = lambda sid: f"env:req:{sid}"
    sd._env_done_target_key = lambda sid, target: f"env:done:{sid}:{target}"

    return sd


def test_mark_outbox_done_fail_open_increments_metric():
    sd = _mk_sd_without_init()
    sd.redis = _BoomRedis(boom={"setex"})

    sd._mark_outbox_done("m1")  # must not raise
    assert "sd:mark_outbox_done_errors_total" in sd._metrics


def test_update_env_req_fail_open_increments_metric_on_sadd():
    sd = _mk_sd_without_init()
    sd.redis = _BoomRedis(boom={"sadd"})

    sd._update_env_req("sid1", {"a", "b"})  # must not raise
    assert "sd:env_req_sadd_expire_errors_total" in sd._metrics


def test_update_env_req_fail_open_increments_metric_on_expire():
    sd = _mk_sd_without_init()
    sd.redis = _BoomRedis(boom={"expire"})

    sd._update_env_req("sid1", {"a", "b"})  # must not raise
    assert "sd:env_req_sadd_expire_errors_total" in sd._metrics


def test_mark_env_done_fail_open_increments_metric():
    sd = _mk_sd_without_init()
    sd.redis = _BoomRedis(boom={"setex"})

    sd._mark_env_done("sid1", "tg")  # must not raise
    assert "sd:mark_env_done_errors_total" in sd._metrics
