from tools.auto_train_conf_calibration import gate_decision


def _rep(delta_brier, delta_ece, n_val=500):
    return {
        "groups": {"global": {"n_val": float(n_val), "brier": 0.10, "ece": 0.05}},
        "baseline_groups": {"global": {"n_val": float(n_val), "brier": 0.10, "ece": 0.05}},
        "delta_groups": {"global": {"brier": float(delta_brier), "ece": float(delta_ece), "n_val": float(n_val)}},
    }

def _rep_with_groups(global_db=0.0, global_de=0.0, groups=None):
    groups = groups or {}
    cand = {"global": {"n_val": 500.0, "brier": 0.10, "ece": 0.05}}
    base = {"global": {"n_val": 500.0, "brier": 0.10, "ece": 0.05}}
    delta = {"global": {"brier": float(global_db), "ece": float(global_de), "n_val": 500.0}}
    for k, v in groups.items():
        n_val = float(v.get("n_val", 300.0))
        db = float(v.get("d_brier", 0.0))
        de = float(v.get("d_ece", 0.0))
        cand[k] = {"n_val": n_val, "brier": 0.10, "ece": 0.05}
        base[k] = {"n_val": n_val, "brier": 0.10, "ece": 0.05}
        delta[k] = {"n_val": n_val, "brier": db, "ece": de}
    return {"groups": cand, "baseline_groups": base, "delta_groups": delta}


def test_gate_soft_pass_not_worse(monkeypatch):
    monkeypatch.setenv("CONF_CAL_GATE_MODE", "soft")
    monkeypatch.setenv("CONF_CAL_GATE_MAX_BRIER_UP", "0.002")
    monkeypatch.setenv("CONF_CAL_GATE_MAX_ECE_UP", "0.01")
    monkeypatch.setenv("CONF_CAL_GATE_TOPK", "0")  # отключим группы для этого теста
    ok, reason, _ = gate_decision(_rep(0.001, 0.005))
    assert ok is True
    assert reason.startswith("pass")


def test_gate_soft_reject_brier(monkeypatch):
    monkeypatch.setenv("CONF_CAL_GATE_MODE", "soft")
    monkeypatch.setenv("CONF_CAL_GATE_MAX_BRIER_UP", "0.002")
    monkeypatch.setenv("CONF_CAL_GATE_TOPK", "0")
    ok, reason, d = gate_decision(_rep(0.01, 0.0))
    assert ok is False
    assert "brier" in reason


def test_gate_strict_requires_improve(monkeypatch):
    monkeypatch.setenv("CONF_CAL_GATE_MODE", "strict")
    monkeypatch.setenv("CONF_CAL_GATE_MIN_BRIER_IMPROVE", "0.0005")
    monkeypatch.setenv("CONF_CAL_GATE_TOPK", "0")
    ok, reason, _ = gate_decision(_rep(0.0, 0.0))
    assert ok is False
    assert "no_brier_improve" in reason


def test_gate_topk_reject_if_too_many_group_regressions(monkeypatch):
    monkeypatch.setenv("CONF_CAL_GATE_MODE", "soft")
    monkeypatch.setenv("CONF_CAL_GATE_TOPK", "3")
    monkeypatch.setenv("CONF_CAL_GATE_MIN_VAL_GROUP", "100")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_MAX_BRIER_UP", "0.004")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_MAX_ECE_UP", "0.02")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_ALLOW_FAILS", "1")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_ALLOW_FAIL_FRAC", "0.0")  # только 1 fail

    rep = _rep_with_groups(
        global_db=0.0,
        global_de=0.0,
        groups={
            "kind:a|symbol:BTCUSDT": {"n_val": 1000, "d_brier": 0.01, "d_ece": 0.0},  # fail
            "kind:a|symbol:ETHUSDT": {"n_val": 900, "d_brier": 0.01, "d_ece": 0.0},   # fail
            "kind:b|symbol:BTCUSDT": {"n_val": 800, "d_brier": 0.0, "d_ece": 0.0},    # pass
        }
    )
    ok, reason, details = gate_decision(rep)
    assert ok is False
    assert "reject_groups" in reason


def test_gate_topk_pass_with_one_allowed_regression(monkeypatch):
    monkeypatch.setenv("CONF_CAL_GATE_MODE", "soft")
    monkeypatch.setenv("CONF_CAL_GATE_TOPK", "3")
    monkeypatch.setenv("CONF_CAL_GATE_MIN_VAL_GROUP", "100")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_MAX_BRIER_UP", "0.004")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_MAX_ECE_UP", "0.02")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_ALLOW_FAILS", "1")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_ALLOW_FAIL_FRAC", "0.0")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_AGG_ENABLE", "0")  # disable aggregated checks for this test
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_TOPM_ENABLE", "0")  # disable topm checks for this test

    rep = _rep_with_groups(
        global_db=0.0,
        global_de=0.0,
        groups={
            "kind:a|symbol:BTCUSDT": {"n_val": 1000, "d_brier": 0.005, "d_ece": 0.0},  # one fail allowed (0.005 > 0.004)
            "kind:a|symbol:ETHUSDT": {"n_val": 900, "d_brier": 0.0, "d_ece": 0.0},
            "kind:b|symbol:BTCUSDT": {"n_val": 800, "d_brier": 0.0, "d_ece": 0.0},
        }
    )
    ok, reason, details = gate_decision(rep)
    assert ok is True
    assert "pass_global_plus_topk" in reason


def test_gate_topk_reject_weighted_mean(monkeypatch):
    monkeypatch.setenv("CONF_CAL_GATE_MODE", "soft")
    monkeypatch.setenv("CONF_CAL_GATE_TOPK", "2")
    monkeypatch.setenv("CONF_CAL_GATE_MIN_VAL_GROUP", "100")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_AGG_ENABLE", "1")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_AGG_MIN_TOTAL_VAL", "0")  # не блокируем по total
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_WMEAN_MAX_BRIER_UP", "0.002")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_WMEAN_MAX_ECE_UP", "1.0")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_TOPM_ENABLE", "0")  # disable topm checks for this test

    rep = _rep_with_groups(
        global_db=0.0,
        global_de=0.0,
        groups={
            "kind:a|symbol:BTCUSDT": {"n_val": 1000, "d_brier": 0.01, "d_ece": 0.0},  # heavy regress
            "kind:a|symbol:ETHUSDT": {"n_val": 1000, "d_brier": 0.0,  "d_ece": 0.0},
        }
    )
    ok, reason, details = gate_decision(rep)
    assert ok is False
    assert "reject_groups_weighted_mean_brier" in reason


def test_gate_topk_reject_weighted_quantile(monkeypatch):
    monkeypatch.setenv("CONF_CAL_GATE_MODE", "soft")
    monkeypatch.setenv("CONF_CAL_GATE_TOPK", "5")
    monkeypatch.setenv("CONF_CAL_GATE_MIN_VAL_GROUP", "100")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_AGG_ENABLE", "1")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_AGG_MIN_TOTAL_VAL", "0")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_AGG_Q", "0.90")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_WMEAN_MAX_BRIER_UP", "0.010")  # allow higher mean
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_WMEAN_MAX_ECE_UP", "1.0")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_WQ_MAX_BRIER_UP", "0.003")    # strict quantile threshold
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_WQ_MAX_ECE_UP", "1.0")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_TOPM_ENABLE", "0")  # disable topm checks for this test

    rep = _rep_with_groups(
        global_db=0.0,
        global_de=0.0,
        groups={
            # 4/5 групп с регрессом => wq90=0.004 > 0.003 (reject), but wmean=0.0032 < 0.01 (pass)
            "kind:k1|symbol:BTCUSDT": {"n_val": 500, "d_brier": 0.004, "d_ece": 0.0},
            "kind:k2|symbol:BTCUSDT": {"n_val": 500, "d_brier": 0.004, "d_ece": 0.0},
            "kind:k3|symbol:BTCUSDT": {"n_val": 500, "d_brier": 0.004, "d_ece": 0.0},
            "kind:k4|symbol:BTCUSDT": {"n_val": 500, "d_brier": 0.004, "d_ece": 0.0},
            "kind:k5|symbol:BTCUSDT": {"n_val": 500, "d_brier": 0.0,   "d_ece": 0.0},
        }
    )
    ok, reason, details = gate_decision(rep)
    assert ok is False
    assert "reject_groups_weighted_quantile_brier" in reason


def test_gate_topm_reject_worst_brier(monkeypatch):
    monkeypatch.setenv("CONF_CAL_GATE_MODE", "soft")
    monkeypatch.setenv("CONF_CAL_GATE_TOPK", "5")
    monkeypatch.setenv("CONF_CAL_GATE_MIN_VAL_GROUP", "100")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_TOPM_ENABLE", "1")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_TOPM", "3")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_TOPM_MIN_VAL_GROUP", "100")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_TOPM_MAX_BRIER_UP", "0.003")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_TOPM_MAX_ECE_UP", "1.0")

    rep = _rep_with_groups(
        global_db=0.0,
        global_de=0.0,
        groups={
            # top3 по n_val включают BTC/ETH/XRP; BTC имеет худший d_brier -> reject
            "kind:a|symbol:BTCUSDT": {"n_val": 5000, "d_brier": 0.010, "d_ece": 0.0},
            "kind:a|symbol:ETHUSDT": {"n_val": 4000, "d_brier": 0.000, "d_ece": 0.0},
            "kind:a|symbol:XRPUSDT": {"n_val": 3000, "d_brier": 0.000, "d_ece": 0.0},
            "kind:a|symbol:ADAUSDT": {"n_val": 2000, "d_brier": 0.000, "d_ece": 0.0},
            "kind:a|symbol:SOLUSDT": {"n_val": 1000, "d_brier": 0.000, "d_ece": 0.0},
        }
    )
    ok, reason, details = gate_decision(rep)
    assert ok is False
    assert "reject_groups_topm_worst_brier" in reason
    assert details.get("topm", {}).get("topm") == 3


def test_gate_topm_pass_when_top3_clean(monkeypatch):
    monkeypatch.setenv("CONF_CAL_GATE_MODE", "soft")
    monkeypatch.setenv("CONF_CAL_GATE_TOPK", "5")
    monkeypatch.setenv("CONF_CAL_GATE_MIN_VAL_GROUP", "100")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_TOPM_ENABLE", "1")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_TOPM", "3")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_TOPM_MAX_BRIER_UP", "0.003")
    monkeypatch.setenv("CONF_CAL_GATE_GROUP_TOPM_MAX_ECE_UP", "0.03")

    rep = _rep_with_groups(
        global_db=0.0,
        global_de=0.0,
        groups={
            # top3 по n_val имеют маленький regression -> pass
            "kind:a|symbol:BTCUSDT": {"n_val": 5000, "d_brier": 0.001, "d_ece": 0.005},
            "kind:a|symbol:ETHUSDT": {"n_val": 4000, "d_brier": 0.001, "d_ece": 0.006},
            "kind:a|symbol:XRPUSDT": {"n_val": 3000, "d_brier": 0.002, "d_ece": 0.007},
            # вне top3 может быть хуже, это покроет allow-fails/agg gates (но не top-M gate)
            "kind:a|symbol:ADAUSDT": {"n_val": 500, "d_brier": 0.010, "d_ece": 0.0},  # too small n_val for topm
            "kind:a|symbol:SOLUSDT": {"n_val": 500, "d_brier": 0.000, "d_ece": 0.0},
        }
    )
    ok, reason, details = gate_decision(rep)
    assert ok is True
    assert "pass_global_plus_topk" in reason
