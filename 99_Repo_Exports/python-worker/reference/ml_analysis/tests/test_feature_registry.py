from __future__ import annotations

"""Tests for tick_flow_full.core.feature_registry.

Запуск:
    # Из корня репозитория:
    PYTHONPATH=./tick_flow_full ./python-worker/.venv/bin/pytest \
        python-worker/ml_analysis/tests/test_feature_registry.py -v

    # Или из python-worker:
    PYTHONPATH=../:../tick_flow_full python -m pytest \
        ml_analysis/tests/test_feature_registry.py -v
"""


import sys
from pathlib import Path

# --- PYTHONPATH setup: добавляем tick_flow_full из нескольких возможных мест ---
_HERE = Path(__file__).resolve()
# python-worker/ml_analysis/tests/test_feature_registry.py
# → python-worker (parents[2]) → repo root (parents[3])
_PW_ROOT = _HERE.parents[2]      # python-worker/
_REPO_ROOT = _HERE.parents[3]    # scanner_infra/
_TFF = _REPO_ROOT / "tick_flow_full"

for _p in [str(_PW_ROOT), str(_REPO_ROOT), str(_TFF)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _import_registry():
    """Импортирует модуль feature_registry, пропускает тест если недоступен."""
    import importlib
    try:
        return importlib.import_module("core.feature_registry")
    except ImportError as e:
        import pytest
        pytest.skip(f"core.feature_registry недоступен (PYTHONPATH?): {e}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_all_schema_versions_valid():
    """get_schema_info возвращает корректный FeatureSchemaInfo для v2/v3/v4_of."""
    reg = _import_registry()
    for ver in ("v2", "v3", "v4_of"):
        info = reg.get_schema_info(ver)
        # базовые инварианты
        assert info.ver == ver, f"ver мismatch: {info.ver!r} != {ver!r}"
        assert len(info.feature_names) >= 30, f"слишком мало фич для {ver}: {len(info.feature_names)}"
        assert len(info.feature_names) == len(info.column_names), "feature_names и column_names разной длины"
        assert len(info.schema_hash) == 64, "schema_hash должен быть 64-значным hex SHA-256"
        # все записи feature_names непустые строки
        assert all(isinstance(n, str) and n for n in info.feature_names)


def test_schema_hash_stable():
    """schema_hash одинаков при повторных вызовах get_schema_info()."""
    reg = _import_registry()
    for ver in ("v2", "v3", "v4_of"):
        h1 = reg.get_schema_info(ver).schema_hash
        h2 = reg.get_schema_info(ver).schema_hash  # cached
        assert h1 == h2, f"hash нестабилен для {ver}"


def test_column_names_no_colon():
    """column_names не содержат ':', чтобы быть безопасными для Parquet/DataFrame."""
    reg = _import_registry()
    for ver in ("v2", "v3", "v4_of"):
        info = reg.get_schema_info(ver)
        bad = [c for c in info.column_names if ":" in c]
        assert not bad, f"column_names содержат ':' для {ver}: {bad[:5]}"


def test_feature_names_contain_standard_blocks():
    """feature_names должны содержать стандартный набор блоков (dir/bucket/hour/dow)."""
    reg = _import_registry()
    for ver in ("v2", "v3", "v4_of"):
        names = reg.get_schema_info(ver).feature_names
        name_set = set(names)
        # block: direction
        assert "dir:LONG" in name_set, f"dir:LONG отсутствует в {ver}"
        assert "dir:SHORT" in name_set
        # block: bucket
        assert "bucket:trend" in name_set
        assert "bucket:range" in name_set
        assert "bucket:other" in name_set
        # block: time one-hots
        assert "hour:0" in name_set
        assert "hour:23" in name_set
        assert "dow:0" in name_set
        assert "dow:6" in name_set


def test_v4_of_feature_count_matches_schema():
    """FeatureSchemaInfo v4_of.n_features совпадает с MLFeatureSchemaV4OF.n_features (если доступен)."""
    reg = _import_registry()
    info = reg.get_schema_info("v4_of")
    # Ожидаем ≥ 100 фич (v4_of: 48+21+2+3+24+7 = 105)
    assert len(info.feature_names) >= 100, f"v4_of слишком мало фич: {len(info.feature_names)}"

    # Попробуем сверить с MLFeatureSchemaV4OF непосредственно
    try:
        from core.ml_feature_schema_v4_of import MLFeatureSchemaV4OF
        schema = MLFeatureSchemaV4OF()
        expected = schema.n_features
        assert len(info.feature_names) == expected, (
            f"Расхождение: feature_registry вернул {len(info.feature_names)}, "
            f"MLFeatureSchemaV4OF.n_features={expected}"
        )
    except ImportError:
        pass  # MLFeatureSchemaV4OF недоступен — тест всё равно прошёл проверку ≥ 100


def test_v5_v6_v7_schema_smoke():
    """Smoke: feature_registry должен уметь отдавать OF-схемы v5/v6/v7 (+stable)."""
    reg = _import_registry()
    for ver in ("v5_of", "v5_of_stable", "v6_of", "v6_of_stable", "v7_of", "v7_of_stable"):
        info = reg.get_schema_info(ver)
        assert info.ver in (ver, "v5_of", "v5_of_stable", "v6_of", "v6_of_stable", "v7_of", "v7_of_stable")
        assert len(info.feature_names) > 0
        assert len(info.feature_names) == len(info.column_names)
        assert len(info.schema_hash) == 64
        # colon-safe columns
        bad = [c for c in info.column_names if ":" in c]
        assert not bad, f"column_names содержат ':' для {ver}: {bad[:5]}"


def test_edge_stack_spec_v4_of():
    """EdgeStackFeatureSpec: feature_cols в формате infer_feature_cols(), хэш корректен."""
    reg = _import_registry()
    spec = reg.get_edge_stack_feature_spec("v4_of")
    assert spec.ver == "v4_of"
    assert len(spec.feature_cols) >= 50, "слишком мало feature_cols для v4_of"
    assert len(spec.feature_cols_hash) == 64, "feature_cols_hash должен быть 64 символа"
    # колонки в формате f_{key}
    f_cols = [c for c in spec.feature_cols if c.startswith("f_")]
    assert len(f_cols) >= 30, "мало f_* колонок в edge spec для v4_of"
    # direction/bucket/time
    assert "direction_BUY" in spec.feature_cols
    assert "direction_SELL" in spec.feature_cols
    assert "bucket:trend" in spec.feature_cols
    assert "hour:0" in spec.feature_cols
    assert "dow:0" in spec.feature_cols


def test_edge_stack_spec_hash_stable():
    """feature_cols_hash одинаков при повторных вызовах get_edge_stack_feature_spec()."""
    reg = _import_registry()
    for ver in ("v2", "v3", "v4_of"):
        h1 = reg.get_edge_stack_feature_spec(ver).feature_cols_hash
        h2 = reg.get_edge_stack_feature_spec(ver).feature_cols_hash
        assert h1 == h2, f"feature_cols_hash нестабилен для {ver}"


def test_get_schema_alias():
    """get_schema() — backward-compat алиас для get_schema_info()."""
    reg = _import_registry()
    info_a = reg.get_schema_info("v3")
    info_b = reg.get_schema("v3")
    assert info_a.schema_hash == info_b.schema_hash
    assert info_a.feature_names == info_b.feature_names


def test_unknown_version_raises():
    """get_schema_info(ver) вызывает ValueError для неизвестной версии."""
    reg = _import_registry()
    try:
        reg.get_schema_info("v999_unknown")
        assert False, "Ожидался ValueError"
    except ValueError as exc:
        assert "v999_unknown" in str(exc)
    # аналогично для edge spec
    try:
        reg.get_edge_stack_feature_spec("vBad")
        assert False, "Ожидался ValueError"
    except ValueError as exc:
        assert "vBad" in str(exc)


def test_schema_v2_subset_of_v3():
    """v3 содержит все num-фичи v2 (расширение назад-совместимо)."""
    reg = _import_registry()
    names_v2 = set(reg.get_schema_info("v2").feature_names)
    names_v3 = set(reg.get_schema_info("v3").feature_names)
    missing = names_v2 - names_v3
    assert not missing, f"v3 не содержит фичи из v2: {missing}"


def test_schema_v3_subset_of_v4_of():
    """v4_of содержит все фичи v3 (расширение назад-совместимо)."""
    reg = _import_registry()
    names_v3 = set(reg.get_schema_info("v3").feature_names)
    names_v4 = set(reg.get_schema_info("v4_of").feature_names)
    missing = names_v3 - names_v4
    assert not missing, f"v4_of не содержит фичи из v3: {missing}"


def test_to_dict():
    """FeatureSchemaInfo.to_dict() и EdgeStackFeatureSpec.to_dict() возвращают корректный dict."""
    reg = _import_registry()
    info = reg.get_schema_info("v3")
    d = info.to_dict()
    assert d["ver"] == "v3"
    assert d["schema_hash"] == info.schema_hash
    assert len(d["feature_names"]) == len(info.feature_names)
    assert len(d["column_names"]) == len(info.column_names)

    spec = reg.get_edge_stack_feature_spec("v3")
    sd = spec.to_dict()
    assert sd["ver"] == "v3"
    assert sd["feature_cols_hash"] == spec.feature_cols_hash
    assert sd["n_cols"] == len(spec.feature_cols)


def test_feature_registry_schema_versions_include_v5_v6():
    """Smoke: v5_of/v6_of/v7_of schemas resolve and have deterministic non-empty feature list."""
    reg = _import_registry()

    for ver in ("v5_of", "v5_of_stable", "v6_of", "v6_of_stable", "v7_of", "v7_of_stable"):
        info = reg.get_schema_info(ver)
        assert info.ver in (ver, "v5_of", "v5_of_stable", "v6_of", "v6_of_stable", "v7_of", "v7_of_stable")
        assert isinstance(info.feature_names, list)
        assert len(info.feature_names) > 10
        # must be unique and stable order
        assert len(set(info.feature_names)) == len(info.feature_names)
        # colon-safe columns (no ':' in column_names)
        bad = [c for c in info.column_names if ":" in c]
        assert not bad, f"column_names contain ':' for {ver}: {bad[:5]}"
        # must contain direction + bucket fields
        assert any(x.startswith("dir") for x in info.feature_names)
        assert any(x.startswith("bucket") for x in info.feature_names)


def test_feature_registry_schema_versions_include_v7():
    """Smoke: v7_of/v7_of_stable schemas resolve, have more features than v6_of, contain Hawkes keys."""
    from core.feature_registry import get_schema_info

    info_v6 = get_schema_info("v6_of")
    for ver in ("v7_of", "v7_of_stable"):
        info = get_schema_info(ver)
        assert info.ver == ver
        assert isinstance(info.feature_names, list)
        assert len(info.feature_names) > 10
        # must be unique
        assert len(set(info.feature_names)) == len(info.feature_names)
        # v7 must contain direction + bucket fields
        assert any(x.startswith("dir") for x in info.feature_names)
        assert any(x.startswith("bucket") for x in info.feature_names)
        # v7_of must be superset of v6_of (modulo stable denylist)
        if ver == "v7_of":
            assert len(info.feature_names) >= len(info_v6.feature_names), (
                f"v7_of only has {len(info.feature_names)} vs v6_of {len(info_v6.feature_names)}"
            )
        # must contain Hawkes-like features
        assert any("hawkes" in x.lower() for x in info.feature_names), (
            f"v7_of schema missing hawkes keys: {info.feature_names[:10]}"
        )
        # must contain VPIN
        assert any("vpin" in x.lower() for x in info.feature_names), (
            "v7_of schema missing vpin keys"
        )


def test_v7_stable_applies_denylist(tmp_path, monkeypatch):
    """v7_of_stable must drop denylisted keys (by ML_FEATURE_DENYLIST_PATH)."""
    import importlib

    # Deny one of A5 bool keys
    deny_path = tmp_path / "deny.json"
    deny_path.write_text('{"deny_bool":["flag_high_vol"]}', encoding="utf-8")

    monkeypatch.setenv("ML_FEATURE_DENYLIST_PATH", str(deny_path))

    # Clear caches (both denylist loader and registry module-level caches)
    import core.feature_denylist_v1 as dl
    dl.clear_denylist_cache()

    import core.feature_registry as fr
    importlib.reload(fr)

    v7 = fr.get_schema_info("v7_of")
    v7s = fr.get_schema_info("v7_of_stable")

    assert "b:flag_high_vol" in set(v7.feature_names)
    assert "b:flag_high_vol" not in set(v7s.feature_names)

    # hash deterministic
    assert v7s.schema_hash == fr.get_schema_info("v7_of_stable").schema_hash

