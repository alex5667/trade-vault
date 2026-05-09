from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis


@dataclass(frozen=True)
class ContextualBundleInfo:
    version: str
    created_ts_ms: int
    path: str
    sha256: str = ""


class ContextualBundleStoreV1:
    """
    Lightweight bundle loader for OFC contextual artifacts.
    Bundle layout:
      manifest.json
      exec_cost_model.json
      rule_success_model.json
      gate_cfg.json
    """
    def __init__(self, path: str, reload_sec: int = 30) -> None:
        self.path = (path or "")
        self.reload_sec = int(reload_sec or 30)
        self._last_check_ms = 0
        self._mtime_ns = -1
        self._manifest: dict[str, Any] = {}
        self._exec_cost_payload: dict[str, Any] = {}
        self._rule_success_payload: dict[str, Any] = {}
        self._gate_cfg: dict[str, Any] = {}

    def _bundle_mtime_ns(self) -> int:
        try:
            return int(os.stat(self.path).st_mtime_ns)
        except Exception:
            return -1

    def _read_json(self, name: str) -> dict[str, Any]:
        p = os.path.join(self.path, name)
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}

    def load(self) -> None:
        if not self.path:
            raise ValueError("empty bundle path")
        self._manifest = self._read_json("manifest.json")
        self._exec_cost_payload = self._read_json("exec_cost_model.json")
        self._rule_success_payload = self._read_json("rule_success_model.json")
        self._gate_cfg = self._read_json("gate_cfg.json")
        self._mtime_ns = self._bundle_mtime_ns()
        self._last_check_ms = get_ny_time_millis()

    def maybe_reload(self) -> None:
        now_ms = get_ny_time_millis()
        if self._last_check_ms > 0 and (now_ms - self._last_check_ms) < (self.reload_sec * 1000):
            return
        cur = self._bundle_mtime_ns()
        if cur < 0:
            self._last_check_ms = now_ms
            return
        if cur != self._mtime_ns or not self._manifest:
            self.load()
        else:
            self._last_check_ms = now_ms

    def get_manifest(self) -> dict[str, Any]:
        return dict(self._manifest or {})

    def get_gate_cfg(self) -> dict[str, Any]:
        return dict(self._gate_cfg or {})

    def get_exec_cost_payload(self) -> dict[str, Any]:
        return dict(self._exec_cost_payload or {})

    def get_rule_success_payload(self) -> dict[str, Any]:
        return dict(self._rule_success_payload or {})

    def get_info(self) -> ContextualBundleInfo:
        m = self._manifest or {}
        return ContextualBundleInfo(
            version=(m.get("bundle_version", "") or ""),
            created_ts_ms=int(m.get("created_ts_ms", 0) or 0),
            path=str(self.path),
            sha256=(m.get("sha256", "") or ""),
        )
