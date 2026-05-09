import types

import pytest

# Подстройте импорт под ваш реальный путь:
from services.signal_dispatcher import SignalDispatcher
from utils.time_utils import get_ny_time_millis


@pytest.fixture()
def dispatcher(r, monkeypatch):
    d = SignalDispatcher()

    # жёстко фиксируем клиентов на fixture redis
    d.redis = r
    d.simple_redis = r
    d.dual_redis = r

    # стабильные префиксы/стримы
    d.marker_prefix = "signal:delivery:marker"
    d.done_prefix = "signal:done"
    d.marker_gc_zset = "signal:delivery:gc"
    d.notify_stream = "stream:signals:notify"
    d.notify_signal_counter_key = "signal:notify:ctr"
    d.delivery_marker_ttl_sec = 120

    # required helpers exist? если нет — патчим минимально
    if not hasattr(d, "_env_done_key"):
        d._env_done_key = types.MethodType(lambda self, sid: f"signal:env_done:{sid}", d)
    if not hasattr(d, "_marker_key"):
        d._marker_key = types.MethodType(lambda self, target, sid: f"{self.marker_prefix}:{target}:{sid}", d)
    if not hasattr(d, "_delivery_key"):
        d._delivery_key = types.MethodType(lambda self, target, sid: self._marker_key(target, sid), d)

    # marker exists checks
    if not hasattr(d, "_marker_exists"):
        d._marker_exists = types.MethodType(lambda self, client, t, sid: bool(client.exists(self._marker_key(t, sid))), d)

    # симуляция Lua (атомарность в тесте заменяем "marker after side-effect")
    calls = {"deliver": 0}
    def fake_eval(client, sha, tag, script, nkeys, *argv):
        calls["deliver"] += 1
        # argv layout из вашего кода нам не важен: просто ставим marker_key
        marker_key = argv[0]
        client.set(marker_key, str(get_ny_time_millis()), ex=int(d.delivery_marker_ttl_sec))
        # для signal_stream/audit/manual ваш код кладёт payload_json — мы не проверяем его здесь
        return "OK"

    monkeypatch.setattr(d, "_evalsha_or_eval", fake_eval, raising=True)

    # notify flatten
    if not hasattr(d, "_flatten_notify_fields"):
        monkeypatch.setattr(d, "_flatten_notify_fields", lambda payload: ["sid", (payload.get("sid",""))], raising=False)

    # retries/dlq — только логируем вызовы
    d._scheduled = []
    d._dlq = []

    monkeypatch.setattr(d, "_schedule_target_retry",
                        lambda target, sid, env, attempt, last_error: d._scheduled.append((target, sid, attempt, last_error)),
                        raising=False)
    monkeypatch.setattr(d, "_send_target_dlq",
                        lambda t, sid, env, reason, err: d._dlq.append((t, sid, reason, err)),
                        raising=False)

    # transient classifier: всё неизвестное — permanent (для предсказуемости)
    monkeypatch.setattr("services.signal_dispatcher.is_transient_error", lambda e: False, raising=False)

    return d


def test_all_targets_success_sets_markers_and_done(dispatcher, r):
    sid = "sid_ok_1"
    env = {
        "sid": sid,
        "targets": {
            "notify": {"sid": sid, "x": 1},
            "signal_stream_payload": {"sid": sid, "y": 2},
        },
        "meta": {
            "signal_stream": "stream:signals:main",
        },
    }

    dispatcher._deliver_targets_with_retry(env, sid, targets=["notify", "signal_stream"])

    # markers exist
    assert r.exists(dispatcher._marker_key("notify", sid)) == 1
    assert r.exists(dispatcher._marker_key("signal_stream", sid)) == 1

    # done exists
    assert r.exists(dispatcher._env_done_key(sid)) == 1

    # no retries, no dlq
    assert dispatcher._scheduled == []
    assert dispatcher._dlq == []


def test_missing_notify_payload_is_permanent_failure_no_done(dispatcher, r):
    sid = "sid_fail_1"
    env = {
        "sid": sid,
        "targets": {
            # notify missing on purpose
        },
        "meta": {},
    }

    dispatcher._deliver_targets_with_retry(env, sid, targets=["notify"])

    # no marker, no done
    assert r.exists(dispatcher._marker_key("notify", sid)) == 0
    assert r.exists(dispatcher._env_done_key(sid)) == 0

    # scheduled retry + dlq (по вашей логике: permanent -> schedule + immediate dlq)
    assert len(dispatcher._scheduled) >= 1
    assert len(dispatcher._dlq) >= 1
    assert dispatcher._dlq[0][0] == "notify"


def test_idempotency_second_run_skips_due_to_marker(dispatcher, r, monkeypatch):
    sid = "sid_once_1"
    env = {
        "sid": sid,
        "targets": {"notify": {"sid": sid}},
        "meta": {},
    }

    # первый прогон
    dispatcher._deliver_targets_with_retry(env, sid, targets=["notify"])
    assert r.exists(dispatcher._marker_key("notify", sid)) == 1

    # второй прогон: _evalsha_or_eval не должен дернуться
    cnt = {"n": 0}
    def count_eval(*a, **k):
        cnt["n"] += 1
        return "OK"
    monkeypatch.setattr(dispatcher, "_evalsha_or_eval", count_eval, raising=True)

    dispatcher._deliver_targets_with_retry(env, sid, targets=["notify"])
    assert cnt["n"] == 0
