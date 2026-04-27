import pytest
pytest.importorskip("redis")

from services.ml_confirm_gate import MLConfirmGate


class DummyRedis:
    def get(self, key):
        return None

    def xadd(self, *args, **kwargs):
        return b"0-0"


class DummyModel:
    horizons = [1000, 3000]

    def __init__(self, *, util_1000: float, util_3000: float, unc_1000: float = 0.0, unc_3000: float = 0.0, unc_k: float = 0.0):
        self._util = {1000: float(util_1000), 3000: float(util_3000)}
        self._unc = {1000: float(unc_1000), 3000: float(unc_3000)}
        self.unc_k = float(unc_k)

    def predict_util(self, x_row):
        return {h: [self._util[h]] for h in self.horizons}

    def predict_unc(self, x_row):
        return {h: [self._unc[h]] for h in self.horizons}


def _make_gate(*, cfg: dict, model: DummyModel) -> MLConfirmGate:
    gate = MLConfirmGate(
        r=DummyRedis(),
        mode="ENFORCE",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )
    # isolate from redis / metrics / replay capture
    gate._refresh_cache_if_needed = lambda *a, **k: None  # type: ignore
    gate._emit_metrics = lambda *a, **k: None  # type: ignore
    gate._capture_replay_input = lambda *a, **k: None  # type: ignore
    gate._metrics_enable = False
    gate._replay_capture = False

    gate._cfg = cfg
    gate._refresh_selective_knobs_from_cfg()
    gate._model = model
    gate._model_load_error = ""
    gate._abstain_band = cfg.get("abstain_band", 0.0)
    gate._conf_min = cfg.get("conf_min", 0.0)
    # keep feature builder minimal: _decide_util_mh will pass it to DummyModel but we ignore it
    gate._build_feature_row = lambda *a, **k: ([0.0], [])  # type: ignore
    return gate


def test_abstain_band_triggers_allow_and_marks_abstain():
    cfg = {
        "kind": "util_mh_v1",
        "util_floors": {"global": {"floor": 0.30}, "by_bucket": {}},
        "unc_k": 0.0,
        "abstain_band": 0.05,
        "conf_min": 0.0,
    }
    gate = _make_gate(cfg=cfg, model=DummyModel(util_1000=0.31, util_3000=0.10))
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="trend_continuation",
        indicators={"bucket_id": 1, "scenario_v4": "trend_continuation"},
        rule_score=0.9,
        rule_have=5,
        rule_need=7,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    assert dec.allow is True
    assert dec.abstain is True
    assert dec.reason.startswith("ml_abstain_band(")
    assert dec.status == "ABSTAIN_BAND"
    assert abs(dec.p_margin) <= 0.05 + 1e-9


def test_block_when_below_floor_and_no_abstain():
    cfg = {
        "kind": "util_mh_v1",
        "util_floors": {"global": {"floor": 0.30}, "by_bucket": {}},
        "unc_k": 0.0,
        "abstain_band": 0.0,
        "conf_min": 0.0,
    }
    gate = _make_gate(cfg=cfg, model=DummyModel(util_1000=0.10, util_3000=0.05))
    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="trend_continuation",
        indicators={"bucket_id": 1, "scenario_v4": "trend_continuation"},
        rule_score=0.9,
        rule_have=5,
        rule_need=7,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    assert dec.allow is False
    assert dec.abstain is False
    assert "util_mh(" in dec.reason
    assert dec.status == "BLOCK"

