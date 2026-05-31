from __future__ import annotations

"""Edge Stack Feature Contract v1.

This defines the *interface* between:
  - dataset builders (ml_analysis.tools.build_edge_stack_dataset_from_redis)
  - trainers (ml_analysis.tools.train_edge_stack_v1_oof)
  - inference (tick_flow_full.services.ml_confirm)

The contract is intentionally simple:
- a *set/order* of `feature_cols` used to build the ML feature vector
- schema_version for backward compatibility

The training bundle writes `feature_contract.json` next to the model artifact.
Inference uses the feature_cols embedded in the model pack; if you also provide a
contract file, it can be compared out-of-band.
"""


import hashlib
import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EdgeStackFeatureContractV1:
    schema_version: int
    kind: str
    feature_cols: list[str]
    scenario_prefix: str = "scenario_v4_"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": int(self.schema_version),
            "kind": str(self.kind),
            "scenario_prefix": str(self.scenario_prefix),
            "feature_cols": [str(x) for x in (self.feature_cols or [])],
        }

    @staticmethod
    def from_feature_cols(feature_cols: list[str]) -> EdgeStackFeatureContractV1:
        cols = [str(x) for x in (feature_cols or [])]
        return EdgeStackFeatureContractV1(schema_version=1, kind="edge_stack_v1", feature_cols=cols)

    def fingerprint(self) -> str:
        """Stable fingerprint used for quick compatibility checks."""
        payload = json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def validate(self) -> None:
        if int(self.schema_version) != 1:
            raise ValueError(f"unsupported schema_version: {self.schema_version}")
        if str(self.kind) != "edge_stack_v1":
            raise ValueError(f"unsupported kind: {self.kind}")
        if not self.feature_cols or len(self.feature_cols) < 8:
            raise ValueError("feature_cols is empty/too small")


def write_contract(path: str, feature_cols: list[str]) -> str:
    c = EdgeStackFeatureContractV1.from_feature_cols(feature_cols)
    c.validate()
    payload = c.to_dict()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return c.fingerprint()


def load_contract(path: str) -> EdgeStackFeatureContractV1:
    data = json.loads(open(path, encoding="utf-8").read())
    c = EdgeStackFeatureContractV1(
        schema_version=int(data.get("schema_version", 0)),
        kind=(data.get("kind", "")),
        feature_cols=[str(x) for x in (data.get("feature_cols") or [])],
        scenario_prefix=(data.get("scenario_prefix", "scenario_v4_")),
    )
    c.validate()
    return c

