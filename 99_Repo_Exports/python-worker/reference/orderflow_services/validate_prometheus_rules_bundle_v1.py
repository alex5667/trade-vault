from __future__ import annotations

"""Prometheus rules bundle validator (CI/local).

Goal
----
Catch invalid YAML / malformed alert rules *before* deploy.
Designed for low-friction usage in CI (pytest) and local dev.

Discovery
---------
Uses a single include-list (manifest) so all tooling validates the same bundle:
  - PROM_RULES_BUNDLE_MANIFEST (env, optional)
  - orderflow_services/prometheus_rules_bundle_manifest_v2.yml (preferred)
  - orderflow_services/prometheus_rules_bundle_manifest_v1.yml
  - fallback patterns (legacy)

What it validates
-----------------
- YAML parses
- Top-level has `groups: [...]`
- Each group has non-empty `rules: [...]`
- Each rule has exactly one of: `alert` | `record`
- Each rule has non-empty `expr`
- (Optional) runs `promtool check rules` if available

Exit codes
----------
- 0: OK
- 2: validation failed
"""

import argparse
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import yaml

from orderflow_services.rules_bundle_discovery_v1 import discover_rules_bundle


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    files_checked: int
    errors: list[str]


def _repo_root_from_file() -> Path:
    # file: <repo>/orderflow_services/validate_prometheus_rules_bundle_v1.py
    return Path(__file__).resolve().parents[1]


def _validate_rules_yaml_doc(doc: object, *, path: Path) -> list[str]:
    errs: list[str] = []
    if not isinstance(doc, dict):
        return [f"{path}: top-level must be a dict"]

    groups = doc.get("groups")
    if not isinstance(groups, list) or len(groups) == 0:
        return [f"{path}: missing/invalid groups (expected non-empty list)"]

    seen_alerts: set[str] = set()
    for gi, g in enumerate(groups):
        if not isinstance(g, dict):
            errs.append(f"{path}: groups[{gi}] must be a dict")
            continue
        rules = g.get("rules")
        if not isinstance(rules, list) or len(rules) == 0:
            errs.append(f"{path}: groups[{gi}] missing/empty rules list")
            continue
        for ri, r in enumerate(rules):
            if not isinstance(r, dict):
                errs.append(f"{path}: groups[{gi}].rules[{ri}] must be a dict")
                continue

            has_alert = "alert" in r
            has_record = "record" in r
            if has_alert == has_record:
                errs.append(
                    f"{path}: groups[{gi}].rules[{ri}] must contain exactly one of 'alert' or 'record'"
                )
                continue

            expr = r.get("expr")
            if not isinstance(expr, str) or not expr.strip():
                errs.append(f"{path}: groups[{gi}].rules[{ri}] missing/empty expr")

            if has_alert:
                alert = r.get("alert")
                if not isinstance(alert, str) or not alert.strip():
                    errs.append(f"{path}: groups[{gi}].rules[{ri}] missing/empty alert name")
                else:
                    if alert in seen_alerts:
                        errs.append(f"{path}: duplicate alert name: {alert}")
                    seen_alerts.add(alert)

            for k in ("labels", "annotations"):
                if k in r and not isinstance(r.get(k), dict):
                    errs.append(f"{path}: groups[{gi}].rules[{ri}] '{k}' must be a dict")
    return errs


def validate_repo_rules(*, repo_root: Path, use_promtool: bool, manifest_ref: str | None = None) -> ValidationResult:
    errors: list[str] = []

    disc = discover_rules_bundle(repo_root=repo_root, manifest_ref=manifest_ref)
    files = disc.files

    if not files:
        mp = str(disc.manifest_path) if disc.manifest_path else "<none>"
        errors.append(f"no rule files discovered (manifest={mp})")
        return ValidationResult(ok=False, files_checked=0, errors=errors)

    for path in files:
        try:
            with open(path, "r", encoding="utf-8") as fh:
                doc = yaml.safe_load(fh)
        except Exception as e:
            errors.append(f"{path}: YAML parse error: {type(e).__name__}: {e}")
            continue

        if "manifest" in str(path.name).lower():
            continue

        if isinstance(doc, dict) and set(doc.keys()) == {"include"}:
            continue

        errors.extend(_validate_rules_yaml_doc(doc, path=path))

    if use_promtool:
        promtool = shutil.which("promtool")
        if not promtool:
            errors.append("promtool requested but not found in PATH")
        else:
            for path in files:
                proc = subprocess.run(
                    [promtool, "check", "rules", str(path)]
                    capture_output=True
                    text=True
                )
                if proc.returncode != 0:
                    out = ((proc.stdout or "") + (proc.stderr or "")).strip()
                    if len(out) > 800:
                        out = out[:800] + "…"
                    errors.append(f"promtool check failed for {path}: {out}")

    return ValidationResult(ok=(len(errors) == 0), files_checked=len(files), errors=errors)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--root"
        default=None
        help="Repo root (defaults to auto-detected root)"
    )
    p.add_argument(
        "--manifest"
        default=None
        help="Manifest path (relative to repo root or absolute). Default: env PROM_RULES_BUNDLE_MANIFEST or v2 manifest."
    )
    p.add_argument(
        "--promtool"
        choices=("auto", "on", "off")
        default="auto"
        help="Run promtool check rules: auto|on|off"
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(args.root).resolve() if args.root else _repo_root_from_file()

    if args.promtool == "on":
        use_promtool = True
    elif args.promtool == "off":
        use_promtool = False
    else:
        use_promtool = bool(shutil.which("promtool"))

    res = validate_repo_rules(repo_root=repo_root, use_promtool=use_promtool, manifest_ref=args.manifest)
    if res.ok:
        print(f"OK: validated {res.files_checked} Prometheus rules files")
        return 0

    print(f"FAIL: {len(res.errors)} error(s) while validating {res.files_checked} rules files")
    for e in res.errors:
        print(f"- {e}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
