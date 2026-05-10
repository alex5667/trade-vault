from concurrent.futures import ThreadPoolExecutor

import pytest

from services.dispatch.dispatcher_app import SignalDispatcher
from utils.time_utils import get_ny_time_millis


@pytest.fixture()
def dispatcher(r, monkeypatch):
    d = SignalDispatcher()
    d.redis = r
    d.simple_redis = r
    d.dual_redis = r

    d.marker_prefix = "signal:delivery:marker"
    d.delivery_marker_ttl_sec = 120
    d.marker_gc_zset = "signal:delivery:gc"

    # ключи
    if not hasattr(d, "_marker_key"):
        d._marker_key = lambda target, sid: f"{d.marker_prefix}:{target}:{sid}"
    if not hasattr(d, "_delivery_key"):
        d._delivery_key = lambda target, sid: d._marker_key(target, sid)
    if not hasattr(d, "_env_done_key"):
        d._env_done_key = lambda sid: f"signal:env_done:{sid}"

    # имитируем атомарное поведение Lua: "deliver only if marker NX"
    d._side_effects = 0
    def fake_eval(client, sha, tag, script, nkeys, *argv):
        marker_key = argv[0]
        # атомарный dedup (то, что должно быть внутри Lua)
        ok = client.set(marker_key, str(get_ny_time_millis()), ex=int(d.delivery_marker_ttl_sec), nx=True)
        if ok:
            d._side_effects += 1
        return "OK"

    monkeypatch.setattr(d, "_evalsha_or_eval", fake_eval, raising=True)

    # prerequisites
    d.notify_stream = "stream:signals:notify"
    d.notify_signal_counter_key = "signal:notify:ctr"

    if not hasattr(d, "_flatten_notify_fields"):
        monkeypatch.setattr(d, "_flatten_notify_fields", lambda payload: ["sid", (payload.get("sid",""))], raising=False)

    return d


def test_concurrent_runs_do_not_duplicate_delivery(dispatcher, r):
    sid = "sid_conc_1"
    env = {"sid": sid, "targets": {"notify": {"sid": sid}}, "meta": {}}

    # 10 параллельных попыток доставить один и тот же SID/target
    def run():
        dispatcher._deliver_targets_with_retry(env, sid, targets=["notify"])

    with ThreadPoolExecutor(max_workers=10) as ex:
        list(ex.map(lambda _: run(), range(10)))

    # marker стоит
    assert r.exists(dispatcher._marker_key("notify", sid)) == 1
    # side-effects максимум 1 раз
    assert dispatcher._side_effects == 1
