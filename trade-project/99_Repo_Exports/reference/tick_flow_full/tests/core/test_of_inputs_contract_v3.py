import json

from core.of_inputs_contract import OFInputsV1, OFInputsV2, OFInputsV3


def test_of_inputs_to_json_is_deterministic() -> None:
    x = OFInputsV1(
        v=1,
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        regime="na",
        direction="LONG",
        scenario="na",
        delta_z=1.23,
        weak_progress=0,
        sweep_recent=0,
        reclaim_recent=0,
        obi_stable=0,
        iceberg_strict=0,
        abs_lvl_ok=0,
        trend_dir="NONE",
        hidden_ctx_recent=0,
        cont_ctx_recent=0,
        cfg={"b": 2, "a": 1},
        fp_eff_quote=0.0,
        fp_quote_delta=0.0,
    )

    s1 = x.to_json()
    s2 = x.to_json()
    assert s1 == s2

    d = json.loads(s1)
    assert d["symbol"] == "BTCUSDT"
    # cfg must be normalized and stable
    assert d["cfg"] == {"a": 1, "b": 2}


def test_of_inputs_v3_to_dict_has_v3_fields() -> None:
    x = OFInputsV3(
        v=3,
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        regime="na",
        direction="LONG",
        scenario="na",
        delta_z=1.23,
        weak_progress=0,
        sweep_recent=0,
        reclaim_recent=0,
        obi_stable=0,
        iceberg_strict=0,
        abs_lvl_ok=0,
        trend_dir="NONE",
        hidden_ctx_recent=0,
        cont_ctx_recent=0,
        cfg={"a": 1},
        fp_eff_quote=0.0,
        fp_quote_delta=0.0,
        ofi=0.0,
        ofi_z=0.0,
        ofi_stable=0,
        ofi_dir_ok=0,
        ofi_stable_secs=0.0,
        qimb_wmean=0.5,
        mp_mid_bps=1.0,
        obi_dw=0.1,
        ofi_ml_norm=0.2,
        book_age_ms=123,
    )

    d = x.to_dict()
    assert d["v"] == 3
    assert "qimb_wmean" in d
    assert "mp_mid_bps" in d
    assert "obi_dw" in d
    assert "ofi_ml_norm" in d
    assert d["book_age_ms"] == 123

    # to_json should remain deterministic
    assert x.to_json() == x.to_json()
