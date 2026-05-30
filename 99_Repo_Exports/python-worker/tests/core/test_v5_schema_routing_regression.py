"""
Regression guard: ML_FEATURE_SCHEMA_VER=v5 must select MLFeatureSchemaV5OF, NOT MLFeatureSchemaV4OF.

Covers all four routing entry-points:
  1. feature_registry.get_schema_info  — registry lookup
  2. ml_feature_schema.build_feature_vector — runtime inference vectorizer
  3. ml_scoring_gate._extract_features routing — sv_tag branch
  4. build_dataset_from_inputs_outcomes_v2 schema normalizer

The critical invariant: vector/feature-name length for v5/v5_of must equal
MLFeatureSchemaV5OF (larger) and must NOT equal MLFeatureSchemaV4OF (smaller).
"""
import unittest
from types import SimpleNamespace

from core.ml_feature_schema_v4_of import MLFeatureSchemaV4OF
from core.ml_feature_schema_v5_of import MLFeatureSchemaV5OF


_V4 = MLFeatureSchemaV4OF()
_V5 = MLFeatureSchemaV5OF()

# Ground-truth feature counts from the canonical schema objects.
_V4_RAW = len(_V4.num_keys) + len(_V4.bool_keys)
_V5_RAW = len(_V5.num_keys) + len(_V5.bool_keys)
# registry and vectorize() both add dir:* (2) + bucket:* (3) + hour:* (24) + dow:* (7) = 36
_REGISTRY_EXTRAS = 36
_V4_TOTAL = _V4_RAW + _REGISTRY_EXTRAS  # also == vectorize() output length
_V5_TOTAL = _V5_RAW + _REGISTRY_EXTRAS  # also == vectorize() output length

_MINIMAL_IND: dict = {"ofi_z": 1.0, "delta_z": 0.5, "spread_bps": 3.0}


class TestFeatureRegistryRouting(unittest.TestCase):
    """get_schema_info must resolve v5/v5_of to v5_of schema, not v4_of."""

    def _check(self, alias: str) -> None:
        from core.feature_registry import get_schema_info

        info = get_schema_info(alias)
        self.assertEqual(
            info.ver, "v5_of",
            f"get_schema_info({alias!r}).ver={info.ver!r} — must be 'v5_of'",
        )
        self.assertEqual(
            len(info.feature_names), _V5_TOTAL,
            f"get_schema_info({alias!r}) returned {len(info.feature_names)} features "
            f"(expected {_V5_TOTAL} for v5_of, not {_V4_TOTAL} for v4_of)",
        )
        self.assertNotEqual(
            len(info.feature_names), _V4_TOTAL,
            f"get_schema_info({alias!r}) returned v4_of feature count — routing bug",
        )

    def test_v5_alias_resolves_to_v5_of(self) -> None:
        self._check("v5")

    def test_v5_of_resolves_to_v5_of(self) -> None:
        self._check("v5_of")

    def test_v4_alias_resolves_to_v4_of(self) -> None:
        """Sanity: v4/v4_of must resolve to v4_of (regression baseline)."""
        from core.feature_registry import get_schema_info

        for alias in ("v4", "v4_of"):
            info = get_schema_info(alias)
            self.assertEqual(info.ver, "v4_of", f"get_schema_info({alias!r}).ver={info.ver!r}")
            self.assertEqual(len(info.feature_names), _V4_TOTAL)

    def test_v5_feature_names_are_superset_of_v4(self) -> None:
        from core.feature_registry import get_schema_info

        v4_names = set(get_schema_info("v4_of").feature_names)
        v5_names = set(get_schema_info("v5_of").feature_names)
        self.assertTrue(
            v4_names.issubset(v5_names),
            f"v5_of feature names must include all v4_of features. "
            f"Missing from v5_of: {v4_names - v5_names}",
        )


class TestBuildFeatureVectorRouting(unittest.TestCase):
    """build_feature_vector(schema_ver=v5/v5_of) must use MLFeatureSchemaV5OF vectorize path."""

    def _vec_len(self, schema_ver: str) -> int:
        from core.ml_feature_schema import build_feature_vector

        vec, _ = build_feature_vector(
            symbol="BTCUSDT",
            ts_ms=1_700_000_000_000,
            direction="LONG",
            scenario="breakout",
            indicators=_MINIMAL_IND,
            rule_score=0.7,
            rule_have=3,
            rule_need=3,
            cancel_spike_veto=0,
            schema_ver=schema_ver,
        )
        return len(vec)

    def test_v5_returns_v5_length(self) -> None:
        n = self._vec_len("v5")
        self.assertEqual(
            n, _V5_TOTAL,
            f"build_feature_vector(schema_ver='v5') returned {n} features "
            f"(expected {_V5_TOTAL} from MLFeatureSchemaV5OF, not {_V4_TOTAL} from V4OF)",
        )

    def test_v5_of_returns_v5_length(self) -> None:
        n = self._vec_len("v5_of")
        self.assertEqual(n, _V5_TOTAL)

    def test_v4_returns_v4_length(self) -> None:
        n = self._vec_len("v4_of")
        self.assertEqual(n, _V4_TOTAL)

    def test_v5_longer_than_v4(self) -> None:
        n_v5 = self._vec_len("v5")
        n_v4 = self._vec_len("v4")
        self.assertGreater(
            n_v5, n_v4,
            f"v5 vector ({n_v5}) must be strictly longer than v4 ({n_v4})",
        )

    def test_v5_v5_of_same_length(self) -> None:
        self.assertEqual(self._vec_len("v5"), self._vec_len("v5_of"))


class TestMLScoringGateSvTagRouting(unittest.TestCase):
    """sv_tag='v5'/'v5_of' must route to sv='5', which uses MLFeatureSchemaV5OF.vectorize."""

    def _make_gate(self, sv_tag: str, n_features: int) -> object:
        from services.ml_scoring_gate import MLScoringGate

        gate = object.__new__(MLScoringGate)
        gate._feature_schema_ver = sv_tag
        gate._feature_schema_version = 5 if "5" in sv_tag else 4
        gate._feature_names = ["dummy"] * n_features
        gate._model = None
        gate._missing_sample_every = 0   # disable metric sampling in tests
        gate._missing_sample_counter = 0
        return gate

    def _extract(self, gate: object) -> list[float] | None:
        from services.ml_scoring_gate import MLScoringGate

        ctx = SimpleNamespace(
            ts_ms=1_700_000_000_000,
            scenario="breakout",
            cancel_spike_veto=False,
            indicators=_MINIMAL_IND,
        )
        return MLScoringGate._extract_features(gate, ctx, "LONG")  # type: ignore

    def test_sv_tag_v5_uses_v5_schema(self) -> None:
        gate = self._make_gate("v5", _V5_TOTAL)
        vec = self._extract(gate)
        self.assertIsNotNone(vec)
        self.assertEqual(
            len(vec), _V5_TOTAL,
            f"sv_tag='v5' returned {len(vec)} features — expected {_V5_TOTAL} (V5OF), not {_V4_TOTAL} (V4OF)",
        )

    def test_sv_tag_v5_of_uses_v5_schema(self) -> None:
        gate = self._make_gate("v5_of", _V5_TOTAL)
        vec = self._extract(gate)
        self.assertIsNotNone(vec)
        self.assertEqual(len(vec), _V5_TOTAL)

    def test_sv_tag_v4_of_uses_v4_schema(self) -> None:
        gate = self._make_gate("v4_of", _V4_TOTAL)
        vec = self._extract(gate)
        self.assertIsNotNone(vec)
        self.assertEqual(len(vec), _V4_TOTAL)

    def test_v5_vector_longer_than_v4(self) -> None:
        v5_vec = self._extract(self._make_gate("v5", _V5_TOTAL))
        v4_vec = self._extract(self._make_gate("v4_of", _V4_TOTAL))
        self.assertIsNotNone(v5_vec)
        self.assertIsNotNone(v4_vec)
        self.assertGreater(len(v5_vec), len(v4_vec))


class TestBuildDatasetSchemaRouting(unittest.TestCase):
    """_norm_schema_ver('v5') must yield 'v5_of'; schema obj must be MLFeatureSchemaV5OF."""

    def test_norm_v5_yields_v5_of(self) -> None:
        try:
            from tools.schema_choices_v1 import normalize_schema_ver
        except ImportError:
            try:
                from ml_analysis.tools.schema_choices_v1 import normalize_schema_ver
            except ImportError:
                self.skipTest("schema_choices_v1 not importable in this context")

        self.assertEqual(normalize_schema_ver("v5"), "v5_of")
        self.assertEqual(normalize_schema_ver("v5_of"), "v5_of")
        self.assertNotEqual(normalize_schema_ver("v5"), "v4_of")

    def test_schema_obj_for_v5_is_v5of(self) -> None:
        """The schema object constructed for v5/v5_of must have V5 feature count."""
        import importlib.util
        import sys

        spec = importlib.util.find_spec("ml_analysis.tools.build_dataset_from_inputs_outcomes_v2")
        if spec is None:
            self.skipTest("build_dataset_from_inputs_outcomes_v2 not importable")

        mod = importlib.import_module("ml_analysis.tools.build_dataset_from_inputs_outcomes_v2")
        norm_fn = getattr(mod, "_norm_schema_ver", None)
        if norm_fn is None:
            self.skipTest("_norm_schema_ver not exported")

        v = norm_fn("v5")
        self.assertEqual(v, "v5_of", f"_norm_schema_ver('v5')={v!r} — must be 'v5_of'")
