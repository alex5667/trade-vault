from __future__ import annotations

import json
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.meta_model_lr import MetaModelLR
from core.meta_model_guard import validate_meta_model


def test_signature_roundtrip(tmp_path: Path) -> None:
    p = tmp_path / "mm.json"

    mm = MetaModelLR(
        features=["a", "b"],
        intercept=0.1,
        coef=[1.0, -2.0],
        threshold=0.55,
        schema_name="meta_feat_v1",
        schema_version=1,
        schema_hash="deadbeefdeadbeef",
        transforms={"a": {"type": "log1p"}},
    )
    mm.dump(str(p))

    mm2 = MetaModelLR.load(str(p))
    assert mm2.signature_ok() is True
    assert mm2.model_signature


def test_signature_tamper_detect(tmp_path: Path) -> None:
    p = tmp_path / "mm.json"

    mm = MetaModelLR(
        features=["a", "b"],
        intercept=0.1,
        coef=[1.0, -2.0],
        threshold=0.55,
        schema_name="meta_feat_v1",
        schema_version=1,
        schema_hash="deadbeefdeadbeef",
    )
    mm.dump(str(p))

    d = json.loads(p.read_text(encoding="utf-8"))
    d["coef"][0] = 9.0  # tamper
    p.write_text(json.dumps(d, indent=2), encoding="utf-8")

    mm2 = MetaModelLR.load(str(p))
    assert mm2.signature_ok() is False


def test_schema_pinning_guard(tmp_path: Path) -> None:
    p = tmp_path / "mm.json"

    mm = MetaModelLR(
        features=["a", "b"],
        intercept=0.1,
        coef=[1.0, -2.0],
        threshold=0.55,
        schema_name="meta_feat_v1",
        schema_version=1,
        schema_hash="aaaa1111aaaa1111",
    )
    mm.dump(str(p))
    mm2 = MetaModelLR.load(str(p))

    ok, reason, _ = validate_meta_model(
        mm2,
        require_signature=True,
        expected_schema_hash="bbbb2222bbbb2222",
    )
    assert ok is False
    assert reason in ("schema_hash_mismatch", "missing_schema_hash")
