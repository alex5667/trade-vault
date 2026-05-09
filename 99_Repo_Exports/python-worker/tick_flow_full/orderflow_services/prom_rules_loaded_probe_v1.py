from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Runtime probe: verify repo rule files are actually loaded by Prometheus.

This is intentionally different from `promtool check rules`:
- promtool validates syntax/semantics of rule files.
- this probe validates *deployment wiring*: Prometheus `rule_files:` include-list
  must actually pick up every expected file.

Output
- Writes a small state blob into Redis under PROM_RULES_LOADED_STATE_PREFIX
- Exit 0 if OK, 2 if missing files or errors

State keys (default prefix: state:prom_rules_loaded)
- :last_ok            1/0
- :last_run_ts_ms     probe run timestamp
- :last_ok_ts_ms      last successful probe timestamp
- :files_expected     number of expected rule files from manifest
- :files_loaded       number of expected files observed in /api/v1/rules
- :missing_n          expected - loaded
- :missing_head       first missing file (relative)
- :missing_json       JSON array of missing files (relative, capped)
- :error_head         short error string (on failures)

ENV
- PROMETHEUS_URL (default: http://prometheus:9090)
- REDIS_URL (default: redis://redis-worker-1:6379/0)
- PROM_RULES_LOADED_STATE_PREFIX (default: state:prom_rules_loaded)
"""

import argparse
import json
import os
from pathlib import Path
from typing import Any

from orderflow_services.rules_bundle_discovery_v1 import discover_rule_files
import contextlib


def _now_ms() -> int:
    return get_ny_time_millis()


def _get_repo_root(arg_root: str | None) -> Path:
    if arg_root:
        return Path(arg_root).resolve()
    if Path("/app").exists():
        return Path("/app").resolve()
    # file: <repo>/orderflow_services/prom_rules_loaded_probe_v1.py
    return Path(__file__).resolve().parents[1]


def _http_get_json(url: str, timeout_s: int = 5) -> dict[str, Any]:
    import urllib.request

    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read()
    obj = json.loads(raw.decode("utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError("unexpected JSON payload")
    return obj


def _extract_loaded_rule_files(resp: dict[str, Any]) -> set[str]:
    """Return a set of file paths reported by Prometheus /api/v1/rules.

    Prometheus returns groups with a `file` field. We treat these as authoritative
    for "what is loaded".
    """
    if (resp.get("status")) != "success":
        raise RuntimeError(f"prometheus status != success: {resp.get('status')}")

    data = resp.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("missing/invalid data field")

    groups = data.get("groups")
    if not isinstance(groups, list):
        # Some versions may return {groups: []}
        return set()

    out: set[str] = set()
    for g in groups:
        if not isinstance(g, dict):
            continue
        fp = g.get("file")
        if isinstance(fp, str) and fp.strip():
            out.add(fp.strip())
    return out


def _compute_loaded_expected(
    *,
    expected_rel: list[str],
    loaded_files: set[str],
) -> tuple[int, list[str]]:
    loaded_n = 0
    missing: list[str] = []
    for rel in expected_rel:
        rel = rel.replace("\\\\", "/")
        ok = any(lf.replace("\\\\", "/").endswith(rel) for lf in loaded_files)
        if ok:
            loaded_n += 1
        else:
            missing.append(rel)
    return loaded_n, missing


def _write_state(redis_url: str, prefix: str, payload: dict[str, Any]) -> None:
    try:
        import redis  # type: ignore
    except Exception as e:
        raise RuntimeError("redis-py is required for this probe") from e

    r = redis.Redis.from_url(redis_url, decode_responses=True)
    pipe = r.pipeline(transaction=False)

    # Required basics
    pipe.set(f"{prefix}:last_ok", str(int(payload.get("last_ok") or 0)))
    pipe.set(f"{prefix}:last_run_ts_ms", str(int(payload.get("last_run_ts_ms") or 0)))

    if int(payload.get("last_ok") or 0) == 1:
        pipe.set(f"{prefix}:last_ok_ts_ms", str(int(payload.get("last_ok_ts_ms") or 0)))

    for k in ("files_expected", "files_loaded", "missing_n"):
        pipe.set(f"{prefix}:{k}", str(int(payload.get(k) or 0)))

    for k in ("missing_head", "error_head"):
        v = payload.get(k)
        if v is None:
            pipe.delete(f"{prefix}:{k}")
        else:
            pipe.set(f"{prefix}:{k}", str(v))

    for k in ("missing_json",):
        v = payload.get(k)
        if v is None:
            pipe.delete(f"{prefix}:{k}")
        else:
            pipe.set(f"{prefix}:{k}", str(v))

    pipe.execute()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--root", default=None, help="Repo root (default: /app or auto)")
    p.add_argument("--manifest", default=None, help="Rules bundle manifest path (optional)")
    p.add_argument("--prom-url", default=None, help="Prometheus base URL (default: env PROMETHEUS_URL or http://prometheus:9090)")
    p.add_argument("--timeout", type=int, default=6, help="HTTP timeout seconds")
    p.add_argument("--redis", default=None, help="Redis URL (default: env REDIS_URL)")
    p.add_argument("--prefix", default=None, help="State key prefix (default: env PROM_RULES_LOADED_STATE_PREFIX)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    repo_root = _get_repo_root(args.root)
    prom_base = (args.prom_url or os.getenv("PROMETHEUS_URL") or "http://prometheus:9090").strip().rstrip("/")
    redis_url = (args.redis or os.getenv("REDIS_URL") or "redis://redis-worker-1:6379/0").strip()
    prefix = (args.prefix or os.getenv("PROM_RULES_LOADED_STATE_PREFIX") or "state:prom_rules_loaded").strip()

    now = _now_ms()

    try:
        files = discover_rule_files(repo_root=repo_root, manifest_ref=args.manifest)
        expected_rel = [str(p.relative_to(repo_root)).replace("\\\\", "/") for p in files]
        expected_rel = sorted({x for x in expected_rel if x.strip()})
        if not expected_rel:
            raise RuntimeError("no rule files discovered")

        resp = _http_get_json(f"{prom_base}/api/v1/rules", timeout_s=int(args.timeout))
        loaded_files = _extract_loaded_rule_files(resp)

        loaded_n, missing = _compute_loaded_expected(expected_rel=expected_rel, loaded_files=loaded_files)
        missing_n = len(missing)
        ok = (missing_n == 0)

        payload: dict[str, Any] = {
            "last_ok": 1 if ok else 0,
            "last_run_ts_ms": now,
            "last_ok_ts_ms": now if ok else 0,
            "files_expected": len(expected_rel),
            "files_loaded": int(loaded_n),
            "missing_n": int(missing_n),
            "missing_head": (missing[0] if missing else None),
            "missing_json": json.dumps(missing[:200], ensure_ascii=False, separators=(",", ":")) if missing else None,
            "error_head": None,
        }

        _write_state(redis_url, prefix, payload)

        if ok:
            print(json.dumps({"ok": True, "expected": len(expected_rel), "loaded": loaded_n}, separators=(",", ":")))
            return 0

        print(json.dumps({"ok": False, "expected": len(expected_rel), "loaded": loaded_n, "missing_n": missing_n, "missing_head": payload["missing_head"]}, separators=(",", ":")))
        return 2

    except Exception as e:
        payload = {
            "last_ok": 0,
            "last_run_ts_ms": now,
            "files_expected": 0,
            "files_loaded": 0,
            "missing_n": 0,
            "missing_head": None,
            "missing_json": None,
            "error_head": f"{type(e).__name__}: {e}",
        }
        with contextlib.suppress(Exception):
            _write_state(redis_url, prefix, payload)
        print(json.dumps({"ok": False, "error": payload["error_head"]}, separators=(",", ":")))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
