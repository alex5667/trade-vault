"""Tests for P113: OF-gate alert rules normalization + bundle discovery fixes.

Covers:
  - EXCLUDE_BASENAME_PREFIXES correctly filters manifest files
  - _looks_like_include_stub correctly identifies include-stub YAMLs
  - discover_rules_bundle excludes manifest files from discovered files
  - discover_rules_bundle excludes include-stub files from discovered files
  - prometheus_alerts_of_gate_dlq_exporter_p82.yml exists in both trees and is valid
  - prometheus_alerts_of_gate_archiver_p78.yml has correct P113 YAML structure
  - prometheus_alerts_of_gate_dlq_p82.yml has correct P113 YAML structure
  - prometheus_alerts_of_gate_ok_rate_v1.yml has correct P113 YAML structure
  - All of-gate alert files have required labels: severity, component
  - Runbooks for ok_rate_v1 and exporters_smoke_p111 exist

Component: Python (orderflow_services tests)
"""

from __future__ import annotations

import os
import tempfile
import textwrap
from pathlib import Path

import pytest
import yaml

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve().parent
_OF_SERVICES = _HERE.parent  # python-worker/orderflow_services/
_TFF_OF_SERVICES = _HERE.parent.parent / "tick_flow_full" / "orderflow_services"


def _of_path(filename: str) -> Path:
    return _OF_SERVICES / filename


def _tff_path(filename: str) -> Path:
    return _TFF_OF_SERVICES / filename


# ---------------------------------------------------------------------------
# Import the discovery module
# ---------------------------------------------------------------------------

def _import_discovery():
    import orderflow_services.rules_bundle_discovery_v1 as mod
    return mod


# ---------------------------------------------------------------------------
# P113-A: EXCLUDE_BASENAME_PREFIXES and _looks_like_include_stub unit tests
# ---------------------------------------------------------------------------

class TestExcludeBasenamePrefix:
    """EXCLUDE_BASENAME_PREFIXES must filter discovery of manifest files."""

    def test_exclude_prefixes_constant_exists(self):
        mod = _import_discovery()
        assert hasattr(mod, "EXCLUDE_BASENAME_PREFIXES"), (
            "EXCLUDE_BASENAME_PREFIXES constant missing from rules_bundle_discovery_v1"
        )

    def test_exclude_prefixes_contains_manifest(self):
        mod = _import_discovery()
        prefixes = mod.EXCLUDE_BASENAME_PREFIXES
        assert any("prometheus_rules_bundle_manifest_" in p for p in prefixes), (
            f"Expected 'prometheus_rules_bundle_manifest_' in EXCLUDE_BASENAME_PREFIXES; got: {prefixes}"
        )

    def test_manifest_filename_matches_prefix(self):
        mod = _import_discovery()
        prefixes = mod.EXCLUDE_BASENAME_PREFIXES
        for name in ("prometheus_rules_bundle_manifest_v1.yml",
                     "prometheus_rules_bundle_manifest_v2.yml"):
            assert any(name.startswith(p) for p in prefixes), (
                f"{name} should be excluded by EXCLUDE_BASENAME_PREFIXES"
            )

    def test_regular_alert_file_not_excluded(self):
        mod = _import_discovery()
        prefixes = mod.EXCLUDE_BASENAME_PREFIXES
        # Regular alert files must NOT match any prefix
        for name in (
            "prometheus_alerts_of_gate_archiver_p78.yml",
            "prometheus_alerts_of_gate_dlq_p82.yml",
            "prometheus_alerts_of_gate_ok_rate_v1.yml",
        ):
            assert not any(name.startswith(p) for p in prefixes), (
                f"{name} should NOT be excluded by EXCLUDE_BASENAME_PREFIXES"
            )


class TestLooksLikeIncludeStub:
    """_looks_like_include_stub must detect legacy include-style YAMLs."""

    def test_function_exists(self):
        mod = _import_discovery()
        assert callable(getattr(mod, "_looks_like_include_stub", None)), (
            "_looks_like_include_stub must be a callable in rules_bundle_discovery_v1"
        )

    def test_include_stub_detected(self, tmp_path: Path):
        """A file with only 'include:' key (no 'groups') is a stub."""
        mod = _import_discovery()
        stub = tmp_path / "stub.yml"
        stub.write_text(
            "include: ../orderflow_services/prometheus_alerts_enforce_health_v82.yml\n",
            encoding="utf-8",
        )
        assert mod._looks_like_include_stub(path=stub) is True, (
            "include-stub must be detected by _looks_like_include_stub"
        )

    def test_real_prometheus_rules_not_stub(self, tmp_path: Path):
        """A file with 'groups:' must NOT be detected as include stub."""
        mod = _import_discovery()
        rule_file = tmp_path / "real_rule.yml"
        rule_file.write_text(
            textwrap.dedent("""\
                groups:
                  - name: test_group
                    rules:
                      - alert: TestAlert
                        expr: up == 0
                        for: 1m
                        labels:
                          severity: warning
                        annotations:
                          summary: "Test"
            """),
            encoding="utf-8",
        )
        assert mod._looks_like_include_stub(path=rule_file) is False, (
            "Real rules file must NOT be detected as include stub"
        )

    def test_empty_file_not_stub(self, tmp_path: Path):
        """An empty / None-loading YAML is not a stub (bool False expected)."""
        mod = _import_discovery()
        empty = tmp_path / "empty.yml"
        empty.write_text("", encoding="utf-8")
        assert mod._looks_like_include_stub(path=empty) is False

    def test_malformed_yaml_not_stub(self, tmp_path: Path):
        """A file with broken YAML must return False (exception caught)."""
        mod = _import_discovery()
        bad = tmp_path / "bad.yml"
        bad.write_text(": invalid: yaml: [content\n", encoding="utf-8")
        assert mod._looks_like_include_stub(path=bad) is False

    def test_groups_and_include_not_stub(self, tmp_path: Path):
        """A file with both 'include' and 'groups' keys is NOT a stub."""
        mod = _import_discovery()
        f = tmp_path / "both.yml"
        f.write_text("include: foo.yml\ngroups: []\n", encoding="utf-8")
        # has 'groups' key → NOT a stub
        assert mod._looks_like_include_stub(path=f) is False


# ---------------------------------------------------------------------------
# P113-B: discover_rules_bundle excludes manifests and stubs
# ---------------------------------------------------------------------------

class TestDiscoveryExclusion:
    """discover_rules_bundle must exclude manifest and include-stub files."""

    def _make_manifest_yml(self, tmp_path: Path, patterns: list[str]) -> Path:
        """Create a v2-style manifest (rule_files key) pointing at tmp_path patterns."""
        manifest = tmp_path / "orderflow_services" / "prometheus_rules_bundle_manifest_v2.yml"
        manifest.parent.mkdir(parents=True, exist_ok=True)
        doc = {"rule_files": patterns}
        manifest.write_text(yaml.dump(doc), encoding="utf-8")
        return manifest

    def test_manifest_yml_excluded_from_discovery(self, tmp_path: Path):
        """prometheus_rules_bundle_manifest_*.yml must never appear in discovered files."""
        mod = _import_discovery()
        of_dir = tmp_path / "orderflow_services"
        of_dir.mkdir(parents=True, exist_ok=True)

        # Create a real alert file and a manifest file in the same pattern
        real = of_dir / "prometheus_alerts_of_gate_dlq_p82.yml"
        real.write_text(
            "groups:\n  - name: test\n    rules:\n      - alert: Test\n        expr: up==0\n",
            encoding="utf-8",
        )
        manifest = of_dir / "prometheus_rules_bundle_manifest_v1.yml"
        manifest.write_text(
            "rule_file_globs:\n  - orderflow_services/prometheus_alerts_*.yml\n",
            encoding="utf-8",
        )

        result = mod.discover_rules_bundle(
            repo_root=tmp_path,
            manifest_ref=None,
        )
        discovered_names = [p.name for p in result.files]
        # Manifest file must NOT appear in discovered files
        assert "prometheus_rules_bundle_manifest_v1.yml" not in discovered_names, (
            f"Manifest file leaked into discovered files: {discovered_names}"
        )
        # Real alert file should be discovered
        assert "prometheus_alerts_of_gate_dlq_p82.yml" in discovered_names, (
            f"Real alert file missing from discovered files: {discovered_names}"
        )

    def test_include_stub_excluded_from_discovery(self, tmp_path: Path):
        """Legacy include-stub YAMLs must be excluded from discovered files."""
        mod = _import_discovery()
        of_dir = tmp_path / "orderflow_services"
        of_dir.mkdir(parents=True, exist_ok=True)

        real = of_dir / "prometheus_alerts_of_gate_ok_rate_v1.yml"
        real.write_text(
            "groups:\n  - name: test\n    rules:\n      - alert: T\n        expr: up==0\n",
            encoding="utf-8",
        )
        stub = of_dir / "prometheus_alerts_enforce_health_v82.yml"
        stub.write_text(
            "include: ../orderflow_services/prometheus_alerts_enforce_health_v82.yml\n",
            encoding="utf-8",
        )

        result = mod.discover_rules_bundle(
            repo_root=tmp_path,
            manifest_ref=None,
        )
        discovered_names = [p.name for p in result.files]
        assert "prometheus_alerts_enforce_health_v82.yml" not in discovered_names, (
            f"Include-stub file leaked into discovered files: {discovered_names}"
        )
        assert "prometheus_alerts_of_gate_ok_rate_v1.yml" in discovered_names, (
            f"Real alert file missing from discovered files: {discovered_names}"
        )


# ---------------------------------------------------------------------------
# P113-C: dlq_exporter_p82.yml exists in both trees and is valid YAML
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tree", ["main", "tff"])
def test_dlq_exporter_p82_yml_exists(tree: str) -> None:
    """prometheus_alerts_of_gate_dlq_exporter_p82.yml must exist in both trees (P113)."""
    path = _of_path("prometheus_alerts_of_gate_dlq_exporter_p82.yml") \
        if tree == "main" else _tff_path("prometheus_alerts_of_gate_dlq_exporter_p82.yml")
    assert path.is_file(), (
        f"[P113] prometheus_alerts_of_gate_dlq_exporter_p82.yml not found at: {path}"
    )


@pytest.mark.parametrize("tree", ["main", "tff"])
def test_dlq_exporter_p82_yml_valid_prometheus_rules(tree: str) -> None:
    """The DLQ exporter alert file must be valid Prometheus rules YAML."""
    path = _of_path("prometheus_alerts_of_gate_dlq_exporter_p82.yml") \
        if tree == "main" else _tff_path("prometheus_alerts_of_gate_dlq_exporter_p82.yml")
    if not path.is_file():
        pytest.skip(f"File not found: {path}")
    with open(path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    assert isinstance(doc, dict), f"Top level must be dict: {path}"
    assert "groups" in doc, f"'groups' key missing: {path}"
    all_rules = [r for g in doc["groups"] for r in g.get("rules", [])]
    alerts = [r for r in all_rules if "alert" in r]
    assert len(alerts) >= 1, f"No alert rules in {path}"


@pytest.mark.parametrize("tree", ["main", "tff"])
def test_dlq_exporter_p82_yml_has_required_labels(tree: str) -> None:
    """All alerts in dlq_exporter_p82.yml must have severity and component labels."""
    path = _of_path("prometheus_alerts_of_gate_dlq_exporter_p82.yml") \
        if tree == "main" else _tff_path("prometheus_alerts_of_gate_dlq_exporter_p82.yml")
    if not path.is_file():
        pytest.skip(f"File not found: {path}")
    with open(path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    for group in doc.get("groups", []):
        for rule in group.get("rules", []):
            if "alert" not in rule:
                continue
            labels = rule.get("labels", {})
            name = rule["alert"]
            assert "severity" in labels, f"[P113] Alert '{name}' missing 'severity' label"
            assert "component" in labels, f"[P113] Alert '{name}' missing 'component' label"
            ann = rule.get("annotations", {})
            assert "runbook_path" in ann, f"[P113] Alert '{name}' missing 'runbook_path' annotation"
            assert "dashboard_path" in ann, f"[P113] Alert '{name}' missing 'dashboard_path' annotation"


# ---------------------------------------------------------------------------
# P113-D: OF-gate alert files normalized style checks
# ---------------------------------------------------------------------------

_OF_GATE_ALERT_FILES = [
    "prometheus_alerts_of_gate_archiver_p78.yml",
    "prometheus_alerts_of_gate_dlq_p82.yml",
    "prometheus_alerts_of_gate_dlq_exporter_p82.yml",
    "prometheus_alerts_of_gate_ok_rate_v1.yml",
]


@pytest.mark.parametrize("filename", _OF_GATE_ALERT_FILES)
@pytest.mark.parametrize("tree", ["main", "tff"])
def test_of_gate_alert_file_exists(filename: str, tree: str) -> None:
    """[P113] All normalized OF-gate alert files must exist in both trees."""
    path = _of_path(filename) if tree == "main" else _tff_path(filename)
    assert path.is_file(), f"[P113] Missing: {path}"


@pytest.mark.parametrize("filename", _OF_GATE_ALERT_FILES)
@pytest.mark.parametrize("tree", ["main", "tff"])
def test_of_gate_alert_file_has_groups(filename: str, tree: str) -> None:
    """[P113] All OF-gate alert files must parse as valid Prometheus rules YAML."""
    path = _of_path(filename) if tree == "main" else _tff_path(filename)
    if not path.is_file():
        pytest.skip(f"File not found: {path}")
    with open(path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    assert isinstance(doc, dict), f"Top level must be dict: {path}"
    assert "groups" in doc, f"'groups' key missing: {path}"
    groups = doc["groups"]
    assert isinstance(groups, list) and len(groups) >= 1, (
        f"'groups' must be non-empty list: {path}"
    )


@pytest.mark.parametrize("filename", _OF_GATE_ALERT_FILES)
@pytest.mark.parametrize("tree", ["main", "tff"])
def test_of_gate_alert_file_no_tbd_runbooks(filename: str, tree: str) -> None:
    """[P113] No OF-gate alert file must contain /runbooks/tbd.md placeholders."""
    path = _of_path(filename) if tree == "main" else _tff_path(filename)
    if not path.is_file():
        pytest.skip(f"File not found: {path}")
    content = path.read_text(encoding="utf-8")
    assert "/runbooks/tbd.md" not in content, (
        f"[P113] Stale /runbooks/tbd.md placeholder found in {path}"
    )
    assert "/d/tbd" not in content, (
        f"[P113] Stale /d/tbd placeholder found in {path}"
    )


@pytest.mark.parametrize("filename", _OF_GATE_ALERT_FILES)
@pytest.mark.parametrize("tree", ["main", "tff"])
def test_of_gate_alert_file_no_legacy_severity(filename: str, tree: str) -> None:
    """[P113] No alert must use legacy severity values 'warn' or 'page'."""
    path = _of_path(filename) if tree == "main" else _tff_path(filename)
    if not path.is_file():
        pytest.skip(f"File not found: {path}")
    with open(path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    for group in doc.get("groups", []):
        for rule in group.get("rules", []):
            if "alert" not in rule:
                continue
            severity = rule.get("labels", {}).get("severity", "")
            name = rule["alert"]
            assert severity not in ("warn", "page"), (
                f"[P113] Alert '{name}' uses legacy severity '{severity}'. "
                f"Use 'warning' / 'critical' / 'info'."
            )


@pytest.mark.parametrize("filename", _OF_GATE_ALERT_FILES)
@pytest.mark.parametrize("tree", ["main", "tff"])
def test_of_gate_alert_file_groups_are_list_style(filename: str, tree: str) -> None:
    """[P113] groups must use YAML list style (- name:) not dict style."""
    path = _of_path(filename) if tree == "main" else _tff_path(filename)
    if not path.is_file():
        pytest.skip(f"File not found: {path}")
    content = path.read_text(encoding="utf-8")
    # Old style: 'groups:\n- name:' (no leading dash with indent)
    # New style: 'groups:\n  - name:'
    # Check that every group uses list-item syntax by verifying YAML parse gives list
    with open(path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    assert isinstance(doc.get("groups"), list), (
        f"[P113] 'groups' must be a YAML list: {path}"
    )
    for g in doc["groups"]:
        assert isinstance(g, dict), f"[P113] Each group must be a dict: {path}"
        assert "rules" in g, f"[P113] Group missing 'rules' key: {path}"
        assert isinstance(g["rules"], list), (
            f"[P113] 'rules' must be a list in group '{g.get('name', '?')}': {path}"
        )


# ---------------------------------------------------------------------------
# P113-E: Expected alert names present
# ---------------------------------------------------------------------------

def test_archiver_p78_expected_alerts() -> None:
    """[P113] archiver_p78.yml must contain all expected alert names."""
    path = _of_path("prometheus_alerts_of_gate_archiver_p78.yml")
    if not path.is_file():
        pytest.skip(f"File not found: {path}")
    with open(path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    names = {r.get("alert") for g in doc["groups"] for r in g["rules"] if "alert" in r}
    expected = {
        "OF_Gate_Archiver_Metrics_Stale",
        "OF_Gate_Archiver_Quarantine_Stale",
        "OF_Gate_RollupsRefresh_Stale",
        "OF_Gate_Archiver_Errors",
        "OF_Gate_QuarantineArchiver_Errors",
        "OF_Gate_RollupsRefresh_Errors",
        "OF_Gate_RollupsFreshnessProbe_Stale",
        "OF_Gate_Rollups_5m_Stale",
        "OF_Gate_Rollups_1h_Stale",
        "OF_Gate_TimescalePolicyProbe_Stale",
        "OF_Gate_TimescaleMissing",
        "OF_Gate_TimescalePoliciesMissing",
        "OF_Gate_TimescalePoliciesDisabled",
    }
    missing = expected - names
    assert not missing, f"[P113] Missing alerts in archiver_p78.yml: {sorted(missing)}"


def test_dlq_p82_expected_alerts() -> None:
    """[P113] dlq_p82.yml must contain all expected alert names."""
    path = _of_path("prometheus_alerts_of_gate_dlq_p82.yml")
    if not path.is_file():
        pytest.skip(f"File not found: {path}")
    with open(path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    names = {r.get("alert") for g in doc["groups"] for r in g["rules"] if "alert" in r}
    expected = {
        "OF_Gate_DLQ_ExporterDown",
        "OF_Gate_DLQ_NonZero15m",
        "OF_Gate_DLQ_OldestAgeHigh",
    }
    missing = expected - names
    assert not missing, f"[P113] Missing alerts in dlq_p82.yml: {sorted(missing)}"


def test_dlq_exporter_p82_expected_alerts() -> None:
    """[P113] dlq_exporter_p82.yml must contain size-based alert names."""
    path = _of_path("prometheus_alerts_of_gate_dlq_exporter_p82.yml")
    if not path.is_file():
        pytest.skip(f"File not found: {path}")
    with open(path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    names = {r.get("alert") for g in doc["groups"] for r in g["rules"] if "alert" in r}
    expected = {"OF_Gate_DLQ_NonZero", "OF_Gate_DLQ_Large"}
    missing = expected - names
    assert not missing, f"[P113] Missing alerts in dlq_exporter_p82.yml: {sorted(missing)}"


def test_ok_rate_v1_expected_alerts() -> None:
    """[P113] ok_rate_v1.yml must contain all expected alert names."""
    path = _of_path("prometheus_alerts_of_gate_ok_rate_v1.yml")
    if not path.is_file():
        pytest.skip(f"File not found: {path}")
    with open(path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    names = {r.get("alert") for g in doc["groups"] for r in g["rules"] if "alert" in r}
    expected = {
        "OF_Gate_EligibleAbsent15m",
        "OF_Gate_NoEligible15m",
        "OF_Gate_OkRateStrictLow",
        "OF_Gate_SoftShareHigh",
        "OF_Gate_QuarantineShareHigh",
        "OF_Gate_QuarantineRateHigh",
        "OF_Gate_ContractSmokeStale2h",
        "OF_Gate_ContractBadShareHigh",
        "OF_Gate_ContractMissingSchemaShareHigh",
        "OF_Gate_ContractSchemaVersionMissing",
    }
    missing = expected - names
    assert not missing, f"[P113] Missing alerts in ok_rate_v1.yml: {sorted(missing)}"


# ---------------------------------------------------------------------------
# P113-F: Runbooks exist
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("filename, tree", [
    ("runbook_of_gate_ok_rate_v1.md", "main"),
    ("runbook_of_gate_ok_rate_v1.md", "tff"),
    ("runbook_of_gate_exporters_smoke_p111.md", "main"),
    ("runbook_of_gate_exporters_smoke_p111.md", "tff"),
])
def test_p113_runbook_exists(filename: str, tree: str) -> None:
    """[P113] Required runbooks must exist in both trees."""
    path = _of_path(filename) if tree == "main" else _tff_path(filename)
    assert path.is_file(), f"[P113] Runbook not found: {path}"


def test_runbook_ok_rate_mentions_dlq_drilldown() -> None:
    """[P113] ok_rate runbook must mention DLQ drilldown commands."""
    path = _of_path("runbook_of_gate_ok_rate_v1.md")
    if not path.is_file():
        pytest.skip(f"File not found: {path}")
    content = path.read_text(encoding="utf-8")
    assert "of_gate_dlq_drilldown_p83" in content, (
        "[P113] ok_rate runbook must mention of_gate_dlq_drilldown_p83"
    )


def test_runbook_exporters_smoke_mentions_targets() -> None:
    """[P113] exporters smoke runbook must mention archiver and DLQ exporter ports."""
    path = _of_path("runbook_of_gate_exporters_smoke_p111.md")
    if not path.is_file():
        pytest.skip(f"File not found: {path}")
    content = path.read_text(encoding="utf-8")
    assert "9152" in content, "[P113] exporters smoke runbook must mention port 9152 (archiver)"
    assert "9154" in content, "[P113] exporters smoke runbook must mention port 9154 (dlq)"
