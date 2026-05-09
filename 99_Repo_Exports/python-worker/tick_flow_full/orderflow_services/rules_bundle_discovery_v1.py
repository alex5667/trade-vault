from __future__ import annotations

"""Rules bundle discovery helper.

This module centralizes *one* include-list (manifest) for all tools that need
consistent Prometheus rules discovery:
  - validate_prometheus_rules_bundle_v1.py
  - promtool_check_rules_wrapper_v1.py
  - prom_rules_loaded_probe_v1.py

Manifest resolution order
  1) PROM_RULES_BUNDLE_MANIFEST (env, relative to repo root or absolute path)
  2) orderflow_services/prometheus_rules_bundle_manifest_v2.yml
  3) orderflow_services/prometheus_rules_bundle_manifest_v1.yml
  4) fallback: legacy discovery (a small default pattern set)

The manifest itself uses glob patterns (relative to repo root).
"""

import glob
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import yaml

DEFAULT_MANIFEST_V2 = "orderflow_services/prometheus_rules_bundle_manifest_v2.yml"
DEFAULT_MANIFEST_V1 = "orderflow_services/prometheus_rules_bundle_manifest_v1.yml"

# Safe fallback if manifests are missing.
FALLBACK_PATTERNS: list[str] = [
    "orderflow_services/prometheus_alerts_*.yml",
    "orderflow_services/prometheus_rules_*.yml",
    "tick_flow_full/orderflow_services/prometheus_alerts_*.yml",
    "tick_flow_full/orderflow_services/prometheus_rules_*.yml",
    "ok_rate_logic/prometheus_alerts_*.yml",
    "services/orderflow/prometheus_alerts_*.yml",
    "tick_flow_full/services/orderflow/prometheus_alerts_*.yml",
]


# Files that are *not* Prometheus rule files but may match the bundle glob patterns.
# Example: bundle manifests themselves (they are YAML but not `groups:` rule docs).
EXCLUDE_BASENAME_PREFIXES: tuple[str, ...] = (
    "prometheus_rules_bundle_manifest_",
)


def _looks_like_include_stub(*, path: Path) -> bool:
    """Detect small YAML "include" stubs used by legacy repos.

    Example content:
      include: ../orderflow_services/prometheus_alerts_enforce_health_v82.yml

    These stubs are not valid Prometheus rule files (no `groups:`), so they must
    be excluded from discovery to keep validation and probes deterministic.
    """

    try:
        with open(path, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
    except Exception:
        return False

    return isinstance(doc, dict) and ("include" in doc) and ("groups" not in doc)


@dataclass(frozen=True)
class BundleDiscoveryResult:
    manifest_path: Path | None
    patterns: list[str]
    files: list[Path]


def _repo_root_from_file() -> Path:
    # file: <repo>/orderflow_services/rules_bundle_discovery_v1.py
    return Path(__file__).resolve().parents[1]


def _resolve_manifest_path(repo_root: Path, manifest_ref: str | None) -> Path | None:
    if manifest_ref:
        ref = str(manifest_ref).strip()
        if ref:
            p = Path(ref)
            if p.is_absolute():
                return p
            return (repo_root / ref).resolve()

    env_ref = (os.getenv("PROM_RULES_BUNDLE_MANIFEST") or "").strip()
    if env_ref:
        p = Path(env_ref)
        if p.is_absolute():
            return p
        return (repo_root / env_ref).resolve()

    cand_v2 = (repo_root / DEFAULT_MANIFEST_V2).resolve()
    if cand_v2.exists():
        return cand_v2

    cand_v1 = (repo_root / DEFAULT_MANIFEST_V1).resolve()
    if cand_v1.exists():
        return cand_v1

    return None


def _load_patterns_from_manifest(path: Path) -> list[str]:
    try:
        with open(path, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
    except Exception:
        return []

    if not isinstance(doc, dict):
        return []

    # v2 key
    if "rule_files" in doc and isinstance(doc.get("rule_files"), list):
        out: list[str] = []
        for x in doc.get("rule_files"):
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
        return out

    # v1 key
    if "rule_file_globs" in doc and isinstance(doc.get("rule_file_globs"), list):
        out = []
        for x in doc.get("rule_file_globs"):
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
        return out

    return []


def _iter_glob_paths(repo_root: Path, patterns: Iterable[str]) -> list[Path]:
    files: list[Path] = []
    for pat in patterns:
        pat = (pat or "").strip()
        if not pat:
            continue
        abs_pat = str((repo_root / pat).resolve())
        # Support ** patterns
        matches = glob.glob(abs_pat, recursive=True)
        for m in matches:
            p = Path(m)
            if p.is_file() and p.suffix.lower() in (".yml", ".yaml"):
                base = p.name
                if any(base.startswith(pref) for pref in EXCLUDE_BASENAME_PREFIXES):
                    continue
                # Exclude legacy include stubs (not valid Prometheus rules).
                if _looks_like_include_stub(path=p):
                    continue
                files.append(p)

    # De-dupe + sort
    uniq: dict[str, Path] = {}
    for p in files:
        uniq[str(p)] = p
    return sorted(uniq.values(), key=lambda x: str(x))


def discover_rules_bundle(
    *,
    repo_root: Path | None = None,
    manifest_ref: str | None = None,
) -> BundleDiscoveryResult:
    root = repo_root or _repo_root_from_file()
    manifest_path = _resolve_manifest_path(root, manifest_ref)

    patterns: list[str] = []
    if manifest_path and manifest_path.exists():
        patterns = _load_patterns_from_manifest(manifest_path)

    if not patterns:
        patterns = list(FALLBACK_PATTERNS)

    files = _iter_glob_paths(root, patterns)
    return BundleDiscoveryResult(manifest_path=manifest_path, patterns=patterns, files=files)


def discover_rule_files(*, repo_root: Path | None = None, manifest_ref: str | None = None) -> list[Path]:
    return discover_rules_bundle(repo_root=repo_root, manifest_ref=manifest_ref).files
