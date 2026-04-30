"""Nightly runner for feature-denylist proposals (AB gate).

Scans proposals dir for manifests with status=pending_ab and runs
`feature_denylist_replay_ab_v1` for a small batch (default 1). Updates
low-cardinality Redis gauges so Prometheus can alert on staleness.

Design
------
- Deterministic: does not mutate global state except manifest transitions
  performed by the replay/AB tool itself.
- Fail-closed: if Redis is unavailable, it still runs AB; metrics update
  is best-effort.
- Dedup: processes oldest pending first.

Env
---
REDIS_URL
FEATURE_DENYLIST_PROPOSALS_DIR (default: <fs_run_dir>/proposals is common; here default is ./proposals)
FEATURE_DENYLIST_AB_MAX_PENDING (default 1)
FEATURE_DENYLIST_AB_METRICS_KEY (default metrics:feature_denylist_ab:last)

"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

UTC = timezone.utc


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _utc_iso() -> str:
    return _utc_now().isoformat()


def _get_redis():
    if redis is None:
        return None
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    try:
        return redis.Redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_metrics(payload: Dict[str, Any]) -> None:
    key = os.getenv("FEATURE_DENYLIST_AB_METRICS_KEY", "metrics:feature_denylist_ab:last")
    r = _get_redis()
    if r is None:
        return
    flat: Dict[str, str] = {}
    for k, v in payload.items():
        if v is None:
            continue
        if isinstance(v, (dict, list)):
            flat[k] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
        else:
            flat[k] = str(v)
    try:
        r.hset(key, mapping=flat)
        r.expire(key, 172800)  # 48h
    except Exception:
        return


def _list_pending(proposals_dir: Path) -> List[Path]:
    out: List[Path] = []
    if not proposals_dir.exists():
        return out
    for p in proposals_dir.glob("denylist_proposal_*.manifest.json"):
        try:
            m = _read_json(p)
            if str(m.get("status") or "") == "pending_ab":
                out.append(p)
        except Exception:
            continue
    # oldest first (by mtime)
    out.sort(key=lambda x: x.stat().st_mtime)
    return out


def _run_replay_ab(manifest: Path, out_dir: Optional[Path] = None) -> Tuple[int, str, str]:
    cmd = [sys.executable, "-m", "ml_analysis.tools.feature_denylist_replay_ab_v1", "--manifest", str(manifest)]
    if out_dir is not None:
        cmd += ["--out_dir", str(out_dir)]
    p = subprocess.run(cmd, text=True, capture_output=True)
    return int(p.returncode), p.stdout, p.stderr


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--proposals-dir"
        default=os.getenv("FEATURE_DENYLIST_PROPOSALS_DIR", "proposals")
        help="Directory with denylist_proposal_*.manifest.json"
    )
    ap.add_argument(
        "--max-pending"
        type=int
        default=int(os.getenv("FEATURE_DENYLIST_AB_MAX_PENDING", "1"))
        help="How many pending manifests to process per run"
    )
    ap.add_argument(
        "--out-dir"
        default=""
        help="Where AB runs are written (default: <proposals-dir>/ab_runs)"
    )
    ap.add_argument("--dry-run", type=int, default=0)
    args = ap.parse_args()

    proposals_dir = Path(args.proposals_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else (proposals_dir / "ab_runs")

    pending = _list_pending(proposals_dir)
    now = _utc_now()

    oldest_age_s = 0
    if pending:
        oldest_age_s = int(time.time() - pending[0].stat().st_mtime)

    if int(args.dry_run) == 1:
        _write_metrics(
            {
                "ts_utc": _utc_iso()
                "pending_n": len(pending)
                "oldest_pending_age_s": oldest_age_s
                "dry_run": 1
            }
        )
        print(json.dumps({"pending_n": len(pending), "oldest_pending_age_s": oldest_age_s}, ensure_ascii=False))
        return 0

    processed: List[Dict[str, Any]] = []
    ok_n = 0
    fail_n = 0

    for mp in pending[: max(0, int(args.max_pending))]:
        rc, out, err = _run_replay_ab(mp, out_dir=out_dir)
        item = {
            "manifest": str(mp)
            "rc": rc
            "stdout_tail": "\n".join((out or "").splitlines()[-10:])
            "stderr_tail": "\n".join((err or "").splitlines()[-10:])
        }
        processed.append(item)
        if rc == 0:
            ok_n += 1
        else:
            fail_n += 1

    payload = {
        "ts_utc": _utc_iso()
        "pending_n": len(pending)
        "processed_n": len(processed)
        "ok_n": ok_n
        "fail_n": fail_n
        "oldest_pending_age_s": oldest_age_s
        "processed": processed[:3],  # small
    }
    _write_metrics(payload)

    # Print one-line JSON for logs
    print(json.dumps({k: payload[k] for k in ("pending_n", "processed_n", "ok_n", "fail_n", "oldest_pending_age_s")}, ensure_ascii=False))

    # Fail if any processed failed (so orchestration can alert)
    return 0 if fail_n == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
