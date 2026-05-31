from __future__ import annotations

"""Prometheus textfile exporter: feature-denylist proposal status.

Exports low-cardinality gauges based on proposals manifests on disk.
Intended for node_exporter textfile collector or similar.

Metrics
-------
feature_denylist_proposals_total{status="pending_ab|ab_done|ab_failed|approved|applied"}
feature_denylist_oldest_pending_age_seconds
feature_denylist_oldest_approved_not_applied_age_seconds

Optionally also exports last nightly AB runner stats from Redis hash
FEATURE_DENYLIST_AB_METRICS_KEY (default metrics:feature_denylist_ab:last):
  feature_denylist_ab_runner_pending
  feature_denylist_ab_runner_processed
  feature_denylist_ab_runner_fail
  feature_denylist_ab_runner_oldest_pending_age_seconds
  feature_denylist_ab_runner_age_seconds

Env
---
FEATURE_DENYLIST_PROPOSALS_DIR (default ./proposals)
FEATURE_DENYLIST_EXPORT_PATH (default /tmp/feature_denylist.prom)
REDIS_URL (optional)

"""


import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

UTC = UTC


def _get_redis():
    if redis is None:
        return None
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    try:
        return redis.Redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _scrape_manifest_statuses(proposals_dir: Path) -> dict[str, int]:
    counts: dict[str, int] = {"pending_ab": 0, "ab_done": 0, "ab_failed": 0, "approved": 0, "applied": 0}
    if not proposals_dir.exists():
        return counts
    for p in proposals_dir.glob("denylist_proposal_*.manifest.json"):
        try:
            m = _read_json(p)
            st = (m.get("status") or "")
            if st in counts:
                counts[st] += 1
        except Exception:
            continue
    return counts


def _oldest_pending_age_s(proposals_dir: Path) -> int:
    if not proposals_dir.exists():
        return 0
    pending: list[Path] = []
    for p in proposals_dir.glob("denylist_proposal_*.manifest.json"):
        try:
            m = _read_json(p)
            if (m.get("status") or "") == "pending_ab":
                pending.append(p)
        except Exception:
            continue
    if not pending:
        return 0
    pending.sort(key=lambda x: x.stat().st_mtime)
    return int(time.time() - pending[0].stat().st_mtime)


def _oldest_approved_not_applied_age_s(proposals_dir: Path) -> int:
    """Max age among proposals with status=approved (approved but not applied yet)."""
    if not proposals_dir.exists():
        return 0
    approved: list[Path] = []
    for p in proposals_dir.glob("denylist_proposal_*.manifest.json"):
        try:
            m = _read_json(p)
            if (m.get("status") or "") == "approved":
                approved.append(p)
        except Exception:
            continue
    if not approved:
        return 0
    approved.sort(key=lambda x: x.stat().st_mtime)
    return int(time.time() - approved[0].stat().st_mtime)


def _format_metric(name: str, labels: dict[str, str], value: float) -> str:
    if labels:
        lab = ",".join([f'{k}="{v}"' for k, v in labels.items()])
        return f"{name}{{{lab}}} {value}\n"
    return f"{name} {value}\n"


def main() -> int:
    proposals_dir = Path(os.getenv("FEATURE_DENYLIST_PROPOSALS_DIR", "proposals")).expanduser().resolve()
    out_path = Path(os.getenv("FEATURE_DENYLIST_EXPORT_PATH", "/tmp/feature_denylist.prom")).expanduser().resolve()

    counts = _scrape_manifest_statuses(proposals_dir)
    oldest_age = _oldest_pending_age_s(proposals_dir)
    oldest_approved_age = _oldest_approved_not_applied_age_s(proposals_dir)

    lines: list[str] = []
    lines.append("# HELP feature_denylist_proposals_total Number of denylist proposals by status\n")
    lines.append("# TYPE feature_denylist_proposals_total gauge\n")
    for st, n in sorted(counts.items()):
        lines.append(_format_metric("feature_denylist_proposals_total", {"status": st}, float(n)))

    lines.append("# HELP feature_denylist_oldest_pending_age_seconds Age in seconds of oldest pending_ab proposal\n")
    lines.append("# TYPE feature_denylist_oldest_pending_age_seconds gauge\n")
    lines.append(_format_metric("feature_denylist_oldest_pending_age_seconds", {}, float(oldest_age)))

    lines.append("# HELP feature_denylist_oldest_approved_not_applied_age_seconds Age in seconds of oldest proposal with status=approved\n")
    lines.append("# TYPE feature_denylist_oldest_approved_not_applied_age_seconds gauge\n")
    lines.append(_format_metric("feature_denylist_oldest_approved_not_applied_age_seconds", {}, float(oldest_approved_age)))

    # Optional: AB runner last stats from Redis
    r = _get_redis()
    key = os.getenv("FEATURE_DENYLIST_AB_METRICS_KEY", "metrics:feature_denylist_ab:last")
    if r is not None:
        try:
            h = r.hgetall(key) or {}
            ts_utc = (h.get("ts_utc") or "").strip()
            age_s = 0
            if ts_utc:
                try:
                    dt = datetime.fromisoformat(ts_utc)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=UTC)
                    age_s = int((datetime.now(tz=UTC) - dt).total_seconds())
                except Exception:
                    age_s = 0

            def _i(name: str) -> int:
                try:
                    return int(float((h.get(name) or "0")))
                except Exception:
                    return 0

            lines.append("# HELP feature_denylist_ab_runner_age_seconds Age of last AB runner tick\n")
            lines.append("# TYPE feature_denylist_ab_runner_age_seconds gauge\n")
            lines.append(_format_metric("feature_denylist_ab_runner_age_seconds", {}, float(age_s)))

            lines.append("# HELP feature_denylist_ab_runner_pending Pending proposals seen at last tick\n")
            lines.append("# TYPE feature_denylist_ab_runner_pending gauge\n")
            lines.append(_format_metric("feature_denylist_ab_runner_pending", {}, float(_i("pending_n"))))

            lines.append("# HELP feature_denylist_ab_runner_processed Processed proposals at last tick\n")
            lines.append("# TYPE feature_denylist_ab_runner_processed gauge\n")
            lines.append(_format_metric("feature_denylist_ab_runner_processed", {}, float(_i("processed_n"))))

            lines.append("# HELP feature_denylist_ab_runner_fail Failed processed count at last tick\n")
            lines.append("# TYPE feature_denylist_ab_runner_fail gauge\n")
            lines.append(_format_metric("feature_denylist_ab_runner_fail", {}, float(_i("fail_n"))))

            lines.append("# HELP feature_denylist_ab_runner_oldest_pending_age_seconds Oldest pending age at last tick\n")
            lines.append("# TYPE feature_denylist_ab_runner_oldest_pending_age_seconds gauge\n")
            lines.append(_format_metric("feature_denylist_ab_runner_oldest_pending_age_seconds", {}, float(_i("oldest_pending_age_s"))))
        except Exception:
            pass

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text("".join(lines), encoding="utf-8")
    tmp.replace(out_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
