import os
import shutil

repo_root = "/home/alex/front/trade/scanner_infra/python-worker"

file1 = """# Prometheus rules bundle manifest (include list)
#
# Purpose:
# - single source of truth for which rule files belong to the "bundle" shipped with the repo
# - used by validators / smoke-check / (optionally) deploy scripts
#
# NOTE: patterns are evaluated relative to repo root.

version: 2
bundle: orderflow_services
rule_files:
  - orderflow_services/prometheus_alerts_*.yml
  - orderflow_services/prometheus_rules_*.yml
  - tick_flow_full/orderflow_services/prometheus_alerts_*.yml
  - tick_flow_full/orderflow_services/prometheus_rules_*.yml
"""

file2 = """#!/usr/bin/env python3
\"\"\"promtool_check_rules_wrapper_v1.py

Single entrypoint wrapper for `promtool check rules` over the repo rules bundle.

Why:
- `promtool` output is sometimes hard to associate with a specific file in CI/logs.
- we want a stable module you can call from orchestrators/timers.

Discovery:
- Prefer `orderflow_services/prometheus_rules_bundle_manifest_v2.yml` (relative globs).
- Fallback to legacy discovery under orderflow_services/ and tick_flow_full/orderflow_services/.

ENV:
- PROMTOOL_BIN (default: promtool)
- REPO_ROOT (default: /app if exists, else auto)

Exit:
- 0: all files passed
- 2: at least one file failed / promtool missing
\"\"\"

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import os
import shutil
import subprocess
from pathlib import Path

import yaml


def _get_repo_root(arg_root: str | None) -> Path:
    if arg_root:
        return Path(arg_root).resolve()
    env_root = (os.getenv("REPO_ROOT") or "").strip()
    if env_root:
        return Path(env_root).resolve()
    if Path("/app").exists():
        return Path("/app").resolve()
    return Path(__file__).resolve().parents[1]


def _load_manifest_patterns(repo_root: Path) -> list[str]:
    mf = repo_root / "orderflow_services" / "prometheus_rules_bundle_manifest_v2.yml"
    if not mf.exists():
        return []
    try:
        with open(mf, "r", encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
    except Exception:
        return []
    if not isinstance(doc, dict):
        return []
    pats = doc.get("rule_files")
    if not isinstance(pats, list):
        return []
    out: list[str] = []
    for p in pats:
        if isinstance(p, str) and p.strip():
            out.append(p.strip())
    return out


def _iter_files(repo_root: Path) -> list[Path]:
    patterns = _load_manifest_patterns(repo_root)
    files: list[Path] = []
    if patterns:
        for pat in patterns:
            pat = pat.lstrip("/")
            files.extend(sorted(repo_root.glob(pat)))
    else:
        for d in (repo_root / "orderflow_services", repo_root / "tick_flow_full" / "orderflow_services"):
            if d.exists() and d.is_dir():
                files.extend(sorted(d.glob("prometheus_alerts_*.yml")))
                files.extend(sorted(d.glob("prometheus_rules_*.yml")))

    uniq: dict[str, Path] = {}
    for p in files:
        if p.is_file():
            uniq[str(p)] = p
    return list(uniq.values())


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=None, help="Repo root")
    ap.add_argument("--promtool", default=None, help="promtool binary path (overrides PROMTOOL_BIN)")
    args = ap.parse_args(argv)

    repo_root = _get_repo_root(args.root)

    promtool = (args.promtool or os.getenv("PROMTOOL_BIN") or "").strip() or "promtool"
    if os.path.sep not in promtool:
        promtool = shutil.which(promtool) or ""

    if not promtool:
        print("promtool not found (set PROMTOOL_BIN or install promtool)")
        return 2

    files = _iter_files(repo_root)
    if not files:
        print("no rules files discovered")
        return 2

    errors: list[str] = []
    for p in files:
        proc = subprocess.run(
            [promtool, "check", "rules", str(p)],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            out = ((proc.stdout or "") + "\\n" + (proc.stderr or "")).strip()
            if len(out) > 900:
                out = out[:900] + "…"
            errors.append(f"{p}: {out}")

    if not errors:
        print(f"OK: promtool check rules passed for {len(files)} file(s)")
        return 0

    print(f"FAIL: promtool check rules failed for {len(errors)} of {len(files)} file(s)")
    for e in errors[:12]:
        print(f"- {e}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
"""

file3 = """from __future__ import annotations

\"\"\"prom_rules_loaded_probe_v1.py

Nightly/orchestrated probe to detect "rules file not loaded" separately from
"rules file invalid".

It verifies that the rule groups defined in the repo bundle are actually present
in Prometheus runtime by querying Prometheus internal metrics:
  prometheus_rule_group_last_evaluation_timestamp_seconds

Mechanism
- Parse local rule files (bundle manifest preferred) and extract `groups[].name`.
- Query Prometheus `/api/v1/query` for the list of evaluated rule groups.
- Compare expected groups vs loaded groups.
- Persist low-cardinality state into Redis (so exporter + alerts can track it).

State keys (Redis, prefix `state:prom_rules_loaded` by default)
- last_run_ts_ms
- last_ok_ts_ms
- last_ok (1/0)
- files_expected / files_loaded / files_missing
- groups_expected / groups_loaded
- error_head
- missing_files_json (short list)

ENV
- PROMETHEUS_URL or PROMETHEUS_BASE_URL  [required by default]
- PROM_RULES_LOADED_PROBE_REQUIRE_URL (default 1)
- PROM_RULES_LOADED_STATE_PREFIX (default state:prom_rules_loaded)
- PROM_RULES_LOADED_STATE_TTL_S (default 14d)
- REDIS_URL (default redis://redis-worker-1:6379/0)
- REPO_ROOT (default /app if exists, else auto)

Exit
- 0 OK (all expected files/groups present)
- 2 FAIL (missing groups/files, query errors, prom url missing when required)
\"\"\"

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import urlopen, Request

import yaml

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None


def _now_ms() -> int:
    return get_ny_time_millis()


def _get_repo_root(arg_root: str | None) -> Path:
    if arg_root:
        return Path(arg_root).resolve()
    env_root = (os.getenv("REPO_ROOT") or "").strip()
    if env_root:
        return Path(env_root).resolve()
    if Path("/app").exists():
        return Path("/app").resolve()
    return Path(__file__).resolve().parents[1]


def _load_manifest_patterns(repo_root: Path) -> list[str]:
    mf = repo_root / "orderflow_services" / "prometheus_rules_bundle_manifest_v2.yml"
    if not mf.exists():
        return []
    try:
        with open(mf, "r", encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
    except Exception:
        return []
    if not isinstance(doc, dict):
        return []
    pats = doc.get("rule_files")
    if not isinstance(pats, list):
        return []
    out: list[str] = []
    for p in pats:
        if isinstance(p, str) and p.strip():
            out.append(p.strip())
    return out


def _iter_rule_files(repo_root: Path) -> list[Path]:
    pats = _load_manifest_patterns(repo_root)
    files: list[Path] = []
    if pats:
        for pat in pats:
            pat = pat.lstrip("/")
            files.extend(sorted(repo_root.glob(pat)))
    else:
        for d in (repo_root / "orderflow_services", repo_root / "tick_flow_full" / "orderflow_services"):
            if d.exists() and d.is_dir():
                files.extend(sorted(d.glob("prometheus_alerts_*.yml")))
                files.extend(sorted(d.glob("prometheus_rules_*.yml")))

    uniq: dict[str, Path] = {}
    for p in files:
        if p.is_file():
            uniq[str(p)] = p
    return list(uniq.values())


def _extract_group_names(path: Path) -> list[str]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
    except Exception:
        return []

    if not isinstance(doc, dict):
        return []
    groups = doc.get("groups")
    if not isinstance(groups, list):
        return []

    out: list[str] = []
    for g in groups:
        if not isinstance(g, dict):
            continue
        name = g.get("name")
        if isinstance(name, str) and name.strip():
            out.append(name.strip())

    seen = set()
    uniq: list[str] = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def _get_prom_url() -> str:
    url = (os.getenv("PROMETHEUS_URL") or os.getenv("PROMETHEUS_BASE_URL") or "").strip()
    return url.rstrip("/")


def _http_get_json(url: str, timeout_s: int = 10) -> Any:
    req = Request(url, headers={"User-Agent": "prom_rules_loaded_probe_v1"})
    with urlopen(req, timeout=timeout_s) as r:
        raw = r.read()
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def _prom_query_groups(prom_url: str) -> set[str]:
    q = "count by (rule_group) (prometheus_rule_group_last_evaluation_timestamp_seconds)"
    url = f"{prom_url}/api/v1/query?query={quote(q)}"
    obj = _http_get_json(url, timeout_s=int(os.getenv("PROM_RULES_LOADED_PROBE_TIMEOUT_S", "10")))
    if not isinstance(obj, dict):
        return set()
    if obj.get("status") != "success":
        return set()
    data = obj.get("data")
    if not isinstance(data, dict):
        return set()
    res = data.get("result")
    if not isinstance(res, list):
        return set()
    out: set[str] = set()
    for it in res:
        if not isinstance(it, dict):
            continue
        m = it.get("metric")
        if not isinstance(m, dict):
            continue
        rg = m.get("rule_group")
        if isinstance(rg, str) and rg.strip():
            out.add(rg.strip())
    return out


def _connect_redis():
    if redis is None:
        return None
    url = os.getenv("REDIS_URL") or os.getenv("CRYPTO_NOTIFY_REDIS_URL") or "redis://redis-worker-1:6379/0"
    try:
        return redis.Redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _write_state(prefix: str, payload: dict[str, Any]) -> None:
    r = _connect_redis()
    if r is None:
        return

    ttl_s = int(os.getenv("PROM_RULES_LOADED_STATE_TTL_S", str(14 * 24 * 3600)))

    pipe = r.pipeline(transaction=False)
    for k, v in payload.items():
        if v is None:
            continue
        pipe.set(f"{prefix}:{k}", str(v))
        pipe.expire(f"{prefix}:{k}", ttl_s)
    try:
        pipe.execute()
    except Exception:
        return


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=None, help="Repo root (default: auto)")
    args = ap.parse_args(argv)

    repo_root = _get_repo_root(args.root)

    prefix = (os.getenv("PROM_RULES_LOADED_STATE_PREFIX") or "state:prom_rules_loaded").strip() or "state:prom_rules_loaded"
    require_url = str(os.getenv("PROM_RULES_LOADED_PROBE_REQUIRE_URL", "1")).lower() in ("1", "true", "yes", "on")

    payload: dict[str, Any] = {
        "last_run_ts_ms": _now_ms(),
        "last_ok": 0,
    }

    prom_url = _get_prom_url()
    if not prom_url:
        payload["error_head"] = "missing_prometheus_url"
        _write_state(prefix, payload)
        return 2 if require_url else 0

    files = _iter_rule_files(repo_root)
    if not files:
        payload["error_head"] = "no_rules_files_discovered"
        _write_state(prefix, payload)
        return 2

    expected: dict[str, list[str]] = {}
    groups_expected = 0
    for p in files:
        rel = str(p.relative_to(repo_root)) if p.is_absolute() else str(p)
        g = _extract_group_names(p) or []
        expected[rel] = g
        groups_expected += len(g)

    loaded_groups = _prom_query_groups(prom_url)
    if not loaded_groups:
        payload["error_head"] = "prom_query_empty_or_failed"
        payload["files_expected"] = len(expected)
        payload["groups_expected"] = groups_expected
        _write_state(prefix, payload)
        return 2

    missing_files: list[str] = []
    loaded_files = 0
    groups_loaded = 0

    for f, groups in expected.items():
        if not groups:
            missing_files.append(f)
            continue

        miss = [g for g in groups if g not in loaded_groups]
        if miss:
            missing_files.append(f)
        else:
            loaded_files += 1
            groups_loaded += len(groups)

    payload.update({
        "files_expected": len(expected),
        "files_loaded": loaded_files,
        "files_missing": max(0, len(expected) - loaded_files),
        "groups_expected": groups_expected,
        "groups_loaded": groups_loaded,
    })

    if missing_files:
        try:
            mf = json.dumps(missing_files[:8], ensure_ascii=False)
            if len(mf) > 1500:
                mf = mf[:1500] + "…"
            payload["missing_files_json"] = mf
        except Exception:
            payload["missing_files_json"] = "[]"
        payload["error_head"] = "missing_rule_groups"
        _write_state(prefix, payload)
        return 2

    payload["last_ok"] = 1
    payload["last_ok_ts_ms"] = _now_ms()
    payload["error_head"] = ""
    _write_state(prefix, payload)
    print({k: payload[k] for k in sorted(payload.keys())})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""

file4 = """# Prometheus rules "loaded" probe alerts (runtime include-list correctness)
# Requires exporter metrics:
# - of_prom_rules_loaded_probe_last_ok
# - of_prom_rules_loaded_probe_last_ok_age_sec
# - of_prom_rules_files_missing

groups:
  - name: of.prom.rules.loaded.probe.health
    interval: 30s
    rules:
      - alert: OF_PromRulesLoadedProbeStale_Crit
        expr: of_prom_rules_loaded_probe_last_ok_age_sec > 7200
        for: 10m
        labels:
          severity: critical
          component: prom_rules_loaded_probe
        annotations:
          summary: "prom rules loaded probe stale"
          description: "No successful prom_rules_loaded_probe run in >2h. Nightly/orchestrator may be stuck. Run: python -m orderflow_services.prom_rules_loaded_probe_v1"

      - alert: OF_PromRulesFilesMissing_Crit
        expr: of_prom_rules_files_missing > 0
        for: 5m
        labels:
          severity: critical
          component: prom_rules_loaded_probe
        annotations:
          summary: "Prometheus is missing one or more expected rules files"
          description: "At least one rules file from the repo bundle is not loaded by Prometheus (missing groups). Check Prometheus rule_files include list / reload. See runbook section: Prom rules loaded probe."
"""

file5 = """from pathlib import Path


def test_prom_rules_loaded_probe_extracts_group_names():
    repo_root = Path(__file__).resolve().parents[2]

    from orderflow_services.prom_rules_loaded_probe_v1 import _extract_group_names

    p = repo_root / "orderflow_services" / "prometheus_alerts_prom_rules_bundle_health_v1.yml"
    groups = _extract_group_names(p)
    assert groups, "expected at least one group name"
    assert any("prom.rules.bundle" in g for g in groups)
"""

paths = [
    ("orderflow_services/prometheus_rules_bundle_manifest_v2.yml", file1),
    ("orderflow_services/promtool_check_rules_wrapper_v1.py", file2),
    ("orderflow_services/prom_rules_loaded_probe_v1.py", file3),
    ("orderflow_services/prometheus_alerts_prom_rules_loaded_probe_health_v1.yml", file4),
    ("orderflow_services/tests/test_prom_rules_loaded_probe_v1.py", file5),
]

for p, content in paths:
    # Write to main
    main_p = os.path.join(repo_root, p)
    os.makedirs(os.path.dirname(main_p), exist_ok=True)
    with open(main_p, "w") as f:
        f.write(content)
        
    if p.endswith(".py"):
        os.chmod(main_p, 0o755)
        
    # Write to mirror
    if p.startswith("orderflow_services/"):
        mirror_p = os.path.join(repo_root, "tick_flow_full", p)
        os.makedirs(os.path.dirname(mirror_p), exist_ok=True)
        with open(mirror_p, "w") as f:
            f.write(content)
        if p.endswith(".py"):
            os.chmod(mirror_p, 0o755)

print("done")
