#!/usr/bin/env python3
"""aiops_metric_coverage.py

Diff the AIOps metric inventory (QUERIES in aiops_agent.py) against what is
ACTUALLY exposed by Prometheus targets, and categorise every missing metric.

Why this exists
---------------
Operators saw "43/99 metrics не экспортируются" alerts without a clear next
action. Without ground truth (active targets, scraped /metrics text, code
definitions) every missing metric collapsed into a single bucket. This utility
splits them into three actionable categories:

  (a) service_down              — owning scrape target unhealthy/missing
  (b) metric_undefined_in_code  — name not found in any *.py source file
  (c) defined_not_observed      — defined in code but never appears on /metrics
                                  (no observe()/inc() reached, or feature-flag off)

Usage
-----
  python -m tools.aiops_metric_coverage [--prom URL] [--json OUT.json] [--repo PATH]

Defaults: --prom from $PROMETHEUS_URL or http://prometheus:9090,
          --repo  parent of this file's parent (python-worker/).

The script is read-only. It performs HTTP GETs against Prometheus + each
target's /metrics. No writes to Redis, Postgres or files (unless --json).

Run from inside the scanner network (e.g. exec into any python-worker
container or a sidecar) so the docker-DNS hostnames in scrapeUrl resolve.
Running from the host produces empty /metrics scrapes because targets like
"python-worker:8000" are not reachable outside the compose network.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

# Reserved words in PromQL that should NEVER be classified as metric names.
_PROMQL_FUNCS = frozenset({
    "sum", "max", "min", "avg", "count", "stddev", "stdvar", "topk", "bottomk",
    "quantile", "histogram_quantile", "rate", "irate", "increase", "delta",
    "idelta", "deriv", "predict_linear", "resets", "changes", "absent",
    "absent_over_time", "avg_over_time", "max_over_time", "min_over_time",
    "sum_over_time", "count_over_time", "last_over_time", "quantile_over_time",
    "stddev_over_time", "stdvar_over_time", "clamp_min", "clamp_max", "clamp",
    "ceil", "floor", "round", "abs", "exp", "ln", "log2", "log10", "sqrt",
    "vector", "scalar", "time", "year", "month", "day_of_week", "day_of_month",
    "days_in_month", "hour", "minute", "label_replace", "label_join",
    "group_left", "group_right", "on", "ignoring", "without", "by", "bool",
    "or", "and", "unless", "offset",
})

_METRIC_NAME_RE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b")
_PROM_LINE_RE = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)\b")


# ---------------------------------------------------------------------------
# Step 1: parse QUERIES from aiops_agent.py
# ---------------------------------------------------------------------------

def load_aiops_queries(repo: Path) -> dict[str, str]:
    """Import QUERIES dict without running side-effects (env loading etc.)."""
    agent_path = repo / "tools" / "aiops_agent.py"
    src = agent_path.read_text(encoding="utf-8")
    # Slice the literal dict assignment; safer than importing the module
    # because aiops_agent does file IO at import time.
    m = re.search(r"^QUERIES\s*:\s*dict\[str,\s*str\]\s*=\s*(\{.*?^\})", src, re.MULTILINE | re.DOTALL)
    if not m:
        raise RuntimeError(f"QUERIES dict not found in {agent_path}")
    # The dict body is a python literal; eval is acceptable here because the
    # source is repo-controlled. Strip trailing-comma issues by relying on ast.
    import ast
    return ast.literal_eval(m.group(1))


_PROMQL_STRING_RE = re.compile(r'"[^"\\]*(?:\\.[^"\\]*)*"|\'[^\'\\]*(?:\\.[^\'\\]*)*\'')
_PROMQL_LABEL_BLOCK_RE = re.compile(r'\{[^}]*\}')


def extract_metric_names(promql: str) -> set[str]:
    """Best-effort extraction of base metric names from a PromQL expression.

    Pipeline:
      1. Remove quoted string literals (label values, regex patterns).
      2. Remove the contents of `{...}` label blocks (label keys aren't metrics).
      3. Tokenize what's left and reject reserved keywords/functions.
    """
    cleaned = _PROMQL_STRING_RE.sub('""', promql)
    cleaned = _PROMQL_LABEL_BLOCK_RE.sub("{}", cleaned)
    names: set[str] = set()
    for tok in _METRIC_NAME_RE.findall(cleaned):
        if tok in _PROMQL_FUNCS:
            continue
        # Bare label tokens that appear in `by(...)`, `on(...)`, `without(...)`
        # groupings. Add new ones here when you spot them in noise.
        if tok in {"le", "symbol", "regime", "mode", "venue", "outcome",
                   "metric", "stage", "reason", "field", "status", "job",
                   "type"}:
            continue
        names.add(tok)
    return names


# ---------------------------------------------------------------------------
# Step 2: discover Prometheus targets + /metrics outputs
# ---------------------------------------------------------------------------

def _http_get(url: str, timeout: float = 5.0) -> tuple[int, str]:
    req = urllib.request.Request(url, headers={"Accept": "text/plain"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, ""
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return 0, str(e)


@dataclass
class Target:
    job: str
    instance: str  # host:port
    scrape_url: str
    health: str  # up | down | unknown
    last_error: str = ""
    metrics_seen: set[str] = field(default_factory=set)


def fetch_targets(prom_url: str) -> list[Target]:
    url = f"{prom_url.rstrip('/')}/api/v1/targets?state=any"
    status, body = _http_get(url, timeout=10.0)
    if status != 200:
        raise RuntimeError(f"Prometheus targets API failed ({status}): {body[:200]}")
    data = json.loads(body)
    out: list[Target] = []
    for t in data.get("data", {}).get("activeTargets", []):
        out.append(Target(
            job=t.get("labels", {}).get("job", ""),
            instance=t.get("labels", {}).get("instance", ""),
            scrape_url=t.get("scrapeUrl", ""),
            health=t.get("health", "unknown"),
            last_error=t.get("lastError", ""),
        ))
    return out


def fetch_exposed_via_prom(prom_url: str) -> set[str]:
    """Return all metric names Prometheus currently knows about.

    Uses /api/v1/label/__name__/values — equivalent to scraping every target
    but works from any host that can reach Prometheus (no docker-DNS needed
    for the target endpoints themselves). Cheaper too: one HTTP call.
    """
    url = f"{prom_url.rstrip('/')}/api/v1/label/__name__/values"
    status, body = _http_get(url, timeout=15.0)
    if status != 200:
        raise RuntimeError(f"Prometheus label values API failed ({status}): {body[:200]}")
    data = json.loads(body)
    names = set(data.get("data", []) or [])
    # Also include base forms (without _bucket/_count/_sum) so the caller's
    # match logic works uniformly with histogram/counter suffixes.
    extras: set[str] = set()
    for n in names:
        for sfx in ("_bucket", "_count", "_sum"):
            if n.endswith(sfx) and len(n) > len(sfx):
                extras.add(n[: -len(sfx)])
                break
    return names | extras


def scrape_metrics(target: Target, timeout: float = 8.0) -> set[str]:
    """Return the set of base metric names exposed by the target."""
    if not target.scrape_url:
        return set()
    status, body = _http_get(target.scrape_url, timeout=timeout)
    if status != 200:
        return set()
    seen: set[str] = set()
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _PROM_LINE_RE.match(line)
        if not m:
            continue
        name = m.group(1)
        # Strip histogram/summary suffixes to match base series name.
        for sfx in ("_bucket", "_count", "_sum", "_total"):
            if name.endswith(sfx) and len(name) > len(sfx):
                # Don't strip _total (it's a real counter suffix kept by name);
                # but most code references the *with*-suffix form too.
                # We keep BOTH the suffixed and stripped form for matching.
                seen.add(name[: -len(sfx)])
                break
        seen.add(name)
    return seen


# ---------------------------------------------------------------------------
# Step 3: scan the codebase for metric definitions
# ---------------------------------------------------------------------------

# Patterns matching how metrics are declared in this repo:
#   _metric(Counter, "name", ...)
#   Counter("name", ...) / Histogram("name", ...) / Gauge("name", ...) / Summary("name", ...)
#   promauto.NewCounter(... Name: "name" ...)  (Go side, optional)
_PY_DEF_RE = re.compile(
    r'(?:'
    # _metric(Counter, "name", ...)
    r'_metric\(\s*(?:Counter|Histogram|Gauge|Summary)\s*,\s*|'
    # Counter("name", ...) / Histogram(...) / Gauge(...) / Summary(...)
    r'(?:Counter|Histogram|Gauge|Summary)\s*\(\s*|'
    # _get_or_create_prom_{histogram,counter,gauge,summary}("name", ...)
    # plus any other helper ending in _histogram/_counter/_gauge/_summary
    r'[a-zA-Z_][a-zA-Z0-9_]*_(?:histogram|counter|gauge|summary)\s*\(\s*'
    r')'
    r'["\']([a-zA-Z_][a-zA-Z0-9_]*)["\']'
)
_GO_DEF_RE = re.compile(r'Name:\s*"([a-zA-Z_][a-zA-Z0-9_]*)"')


def scan_code_definitions(repo: Path) -> set[str]:
    defined: set[str] = set()
    # Python side: python-worker tree
    for path in repo.rglob("*.py"):
        # Skip noise
        s = str(path)
        if "/tests/" in s or "/__pycache__/" in s or "/.venv/" in s:
            continue
        try:
            txt = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        for m in _PY_DEF_RE.finditer(txt):
            defined.add(m.group(1))
    # Go side: sibling go-worker tree (optional)
    go_root = repo.parent / "go-worker"
    if go_root.is_dir():
        for path in go_root.rglob("*.go"):
            try:
                txt = path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
            for m in _GO_DEF_RE.finditer(txt):
                defined.add(m.group(1))
    return defined


# ---------------------------------------------------------------------------
# Step 4: classify
# ---------------------------------------------------------------------------

@dataclass
class QueryResult:
    alias: str
    promql: str
    metrics: list[str]
    present: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    category: str = "unknown"
    likely_jobs_down: list[str] = field(default_factory=list)
    notes: str = ""


_HIST_SUFFIXES = ("_bucket", "_count", "_sum")


def _base_name(name: str) -> str:
    for sfx in _HIST_SUFFIXES:
        if name.endswith(sfx) and len(name) > len(sfx):
            return name[: -len(sfx)]
    return name


def _is_exposed(name: str, exposed: set[str]) -> bool:
    return name in exposed or _base_name(name) in exposed


def _is_defined(name: str, defined: set[str]) -> bool:
    return name in defined or _base_name(name) in defined


def classify(
    queries: dict[str, str],
    exposed: set[str],
    defined: set[str],
    targets: list[Target],
) -> list[QueryResult]:
    # Build a hint-map: metric name → likely owning job (substring match)
    # to flag "service_down" when relevant targets are unhealthy.
    down_targets = [t for t in targets if t.health != "up"]
    down_jobs = {t.job for t in down_targets}

    results: list[QueryResult] = []
    for alias, promql in queries.items():
        names = sorted(extract_metric_names(promql))
        present, missing = [], []
        for n in names:
            if _is_exposed(n, exposed):
                present.append(n)
            else:
                missing.append(n)

        qr = QueryResult(alias=alias, promql=promql, metrics=names,
                         present=present, missing=missing)

        if not missing:
            qr.category = "ok"
        else:
            # Classify by FIRST missing metric (queries usually centre on one)
            primary = missing[0]
            owning_down = _guess_owning_jobs(primary, down_jobs)
            qr.likely_jobs_down = owning_down

            if owning_down:
                # The owning scrape target is down → that's the root cause.
                # Whether metric is defined or not, the operator action is
                # "bring up the service or remove the scrape job".
                qr.category = "service_down"
                qr.notes = (
                    "metric defined; owning scrape target unhealthy"
                    if _is_defined(primary, defined)
                    else "name not found in code AND owning scrape target unhealthy"
                )
            elif _is_defined(primary, defined):
                # Defined, owning target up → either feature-flag off or no
                # code path reached observe()/inc().
                qr.category = "defined_not_observed"
            else:
                # No definition in repo at all — typo, renamed, retired.
                qr.category = "metric_undefined_in_code"
        results.append(qr)
    return results


def _guess_owning_jobs(metric: str, down_jobs: set[str]) -> list[str]:
    """Heuristic: if metric prefix matches a down job's name, flag it.

    Requires ≥2 token prefix to avoid single-word false positives
    (e.g. "of" matching every of_layer_* job for any of_* metric).
    """
    if not down_jobs:
        return []
    hits: list[str] = []
    parts = metric.split("_")
    for n in (3, 2):
        prefix = "_".join(parts[:n])
        for job in down_jobs:
            jn = job.replace("-", "_")
            if prefix and (prefix in jn or jn.startswith(prefix)):
                hits.append(job)
        if hits:
            break
    return sorted(set(hits))


# ---------------------------------------------------------------------------
# Step 5: render
# ---------------------------------------------------------------------------

def render_summary(results: list[QueryResult], targets: list[Target]) -> str:
    buckets: dict[str, list[QueryResult]] = defaultdict(list)
    for r in results:
        buckets[r.category].append(r)

    lines: list[str] = []
    up = sum(1 for t in targets if t.health == "up")
    dn = sum(1 for t in targets if t.health != "up")
    lines.append(f"Prometheus targets: {up} up, {dn} not up (of {len(targets)})")
    lines.append("")
    order = ["ok", "service_down", "defined_not_observed", "metric_undefined_in_code"]
    for cat in order:
        items = buckets.get(cat, [])
        lines.append(f"=== {cat}: {len(items)} ===")
        for r in items:
            tail = f"  missing={r.missing}" if r.missing else ""
            note = f" [{r.notes}]" if r.notes else ""
            jobs = f" jobs_down={r.likely_jobs_down}" if r.likely_jobs_down else ""
            lines.append(f"  - {r.alias}{tail}{note}{jobs}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--prom", default=os.environ.get("PROMETHEUS_URL", "http://prometheus:9090"))
    p.add_argument("--repo", default=str(Path(__file__).resolve().parent.parent))
    p.add_argument("--json", dest="json_out", default=None, help="optional path for JSON report")
    p.add_argument("--scrape-timeout", type=float, default=8.0)
    p.add_argument(
        "--source",
        choices=("api", "scrape"),
        default="api",
        help="how to enumerate exposed metric names: 'api' (one call to "
             "/api/v1/label/__name__/values, works from any host) or "
             "'scrape' (hit each target's /metrics, requires docker-DNS).",
    )
    args = p.parse_args(argv)

    repo = Path(args.repo)
    print(f"[1/4] loading QUERIES from {repo / 'tools' / 'aiops_agent.py'} ...", file=sys.stderr)
    queries = load_aiops_queries(repo)
    print(f"      {len(queries)} queries", file=sys.stderr)

    print(f"[2/4] fetching active targets from {args.prom} ...", file=sys.stderr)
    targets = fetch_targets(args.prom)
    print(f"      {len(targets)} active targets ({sum(1 for t in targets if t.health == 'up')} up)", file=sys.stderr)

    if args.source == "api":
        print("[3/4] enumerating exposed metrics via Prometheus API ...", file=sys.stderr)
        exposed = fetch_exposed_via_prom(args.prom)
    else:
        print("[3/4] scraping /metrics from healthy targets ...", file=sys.stderr)
        exposed = set()
        for t in targets:
            if t.health != "up":
                continue
            m = scrape_metrics(t, timeout=args.scrape_timeout)
            t.metrics_seen = m
            exposed |= m
    print(f"      {len(exposed)} unique metric names exposed", file=sys.stderr)

    print(f"[4/4] scanning {repo} for metric definitions ...", file=sys.stderr)
    defined = scan_code_definitions(repo)
    print(f"      {len(defined)} metric definitions in code", file=sys.stderr)

    results = classify(queries, exposed, defined, targets)

    if args.json_out:
        out = {
            "prometheus": args.prom,
            "summary": {
                "queries_total": len(queries),
                "targets_total": len(targets),
                "targets_up": sum(1 for t in targets if t.health == "up"),
                "metrics_exposed": len(exposed),
                "metrics_defined_in_code": len(defined),
            },
            "results": [
                {
                    "alias": r.alias,
                    "promql": r.promql,
                    "metrics": r.metrics,
                    "present": r.present,
                    "missing": r.missing,
                    "category": r.category,
                    "likely_jobs_down": r.likely_jobs_down,
                    "notes": r.notes,
                } for r in results
            ],
            "down_targets": [
                {"job": t.job, "instance": t.instance, "health": t.health, "last_error": t.last_error}
                for t in targets if t.health != "up"
            ],
        }
        Path(args.json_out).write_text(json.dumps(out, indent=2, ensure_ascii=False))
        print(f"wrote {args.json_out}", file=sys.stderr)

    print(render_summary(results, targets))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
