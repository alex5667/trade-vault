from __future__ import annotations

from dataclasses import dataclass

from common.contextual_bundle_store_v1 import ContextualBundleStoreV1
from core.ofc_contextual_gate_v1 import ContextualGateV1
from core.ofc_exec_cost_model_v1 import ExecCostModelV1
from core.ofc_rule_success_model_v1 import RuleSuccessModelV1


@dataclass(frozen=True)
class OFCBundleV1:
    version: str
    exec_cost_model: ExecCostModelV1
    rule_success_model: RuleSuccessModelV1
    gate: ContextualGateV1
    manifest: dict


class OFCBundleLoaderV1:
    def __init__(self, bundle_path: str, reload_sec: int = 30) -> None:
        self.store = ContextualBundleStoreV1(bundle_path, reload_sec=reload_sec)
        self.bundle: OFCBundleV1 | None = None

    def maybe_reload(self) -> None:
        self.store.maybe_reload()
        manifest = self.store.get_manifest()
        if not manifest:
            return
        version = (manifest.get("bundle_version", "") or "")
        if self.bundle is not None and self.bundle.version == version:
            return
        self.bundle = OFCBundleV1(
            version=version,
            exec_cost_model=ExecCostModelV1(self.store.get_exec_cost_payload()),
            rule_success_model=RuleSuccessModelV1(self.store.get_rule_success_payload()),
            gate=ContextualGateV1(self.store.get_gate_cfg()),
            manifest=dict(manifest),
        )

    def get(self) -> OFCBundleV1 | None:
        return self.bundle
