import pytest
from hypothesis import given, settings, strategies as st

targets_st = st.lists(st.sampled_from(["notify", "signal_stream", "audit", "manual"]), unique=True, min_size=1, max_size=4)
missing_st = st.sets(st.sampled_from(["notify", "signal_stream", "audit", "manual"]), min_size=0, max_size=4)

@settings(max_examples=50, deadline=None)
@given(targets=targets_st, missing=missing_st)
def test_done_marker_iff_all_targets_delivered(dispatcher, r, targets, missing):
    sid = "sid_prop_" + "_".join(targets)

    t_obj = {}
    meta = {}

    # готовим env минимально под ваш _deliver_one_target контракт
    if "notify" in targets and "notify" not in missing:
        t_obj["notify"] = {"sid": sid}
    if "signal_stream" in targets and "signal_stream" not in missing:
        t_obj["signal_stream_payload"] = {"sid": sid}
        meta["signal_stream"] = "stream:signals:main"
    if "audit" in targets and "audit" not in missing:
        t_obj["audit_payload"] = {"sid": sid}
        meta["audit_stream"] = "stream:signals:audit"
    if "manual" in targets and "manual" not in missing:
        t_obj["manual_payload"] = {"sid": sid}
        meta["manual_stream"] = "stream:signals:manual"

    env = {"sid": sid, "targets": t_obj, "meta": meta}

    dispatcher._deliver_targets_with_retry(env, sid, targets=targets)

    # проверяем: done <=> для каждого target есть marker
    all_markers = True
    for t in targets:
        if r.exists(dispatcher._marker_key(t, sid)) != 1:
            all_markers = False
            break

    done = (r.exists(dispatcher._env_done_key(sid)) == 1)
    assert done == all_markers
