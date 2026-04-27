#!/usr/bin/env python3
"""P58: Multi-archive retention + inventory.

Deletes old day-partitioned NDJSON files and writes a manifest per archive directory.

Scope:
- Works with archives written by stream_archiver_ndjson_v1 / replay_inputs_archiver, using file names:
    YYYY-MM-DD.ndjson or YYYY-MM-DD.ndjson.gz

Env:
  ARCHIVE_DIRS (comma-separated dirs)
    default:
      /var/lib/trade/archives/ml_replay_inputs_v1,
      /var/lib/trade/archives/signals_of_inputs,
      /var/lib/trade/archives/trades_closed
  KEEP_DAYS (default: 14)
  MANIFEST_NAME (default: manifest.json)
"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

DAY_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.ndjson(?:\.gz)?$")


def _utc_day_now() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(time.time()))


def _day_to_epoch(day: str) -> int:
    # UTC midnight
    return int(time.mktime(time.strptime(day, "%Y-%m-%d")))


def _list_files(d: Path) -> List[Tuple[str, Path]]:
    out: List[Tuple[str, Path]] = []
    if not d.exists() or not d.is_dir():
        return out
    for p in d.iterdir():
        m = DAY_RE.match(p.name)
        if not m:
            continue
        out.append((m.group(1), p))
    out.sort(key=lambda x: x[0])
    return out


def _inventory_one(d: Path, keep_days: int, manifest_name: str) -> Dict[str, object]:
    files = _list_files(d)
    today = _utc_day_now()
    cutoff_epoch = _day_to_epoch(today) - int(keep_days) * 86400
    deleted: List[str] = []

    kept_files: List[Dict[str, object]] = []
    for day, p in files:
        try:
            day_epoch = _day_to_epoch(day)
        except Exception:
            day_epoch = cutoff_epoch + 1

        if day_epoch < cutoff_epoch:
            try:
                p.unlink()
                deleted.append(p.name)
            except Exception:
                pass
            continue

        try:
            st = p.stat()
            kept_files.append(
                {
                    "day": day,
                    "name": p.name,
                    "bytes": int(st.st_size),
                    "mtime": int(st.st_mtime),
                }
            )
        except Exception:
            kept_files.append({"day": day, "name": p.name})

    kept_files.sort(key=lambda x: str(x.get("day", "")))
    manifest = {
        "dir": str(d),
        "generated_utc_day": today,
        "keep_days": int(keep_days),
        "files": kept_files[-120:],  # limit manifest size
        "deleted": deleted[-500:],
    }

    try:
        (d / manifest_name).write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return manifest


def main(argv: Optional[List[str]] = None) -> int:
    dirs = os.getenv(
        "ARCHIVE_DIRS",
        "/var/lib/trade/archives/ml_replay_inputs_v1,/var/lib/trade/archives/signals_of_inputs,/var/lib/trade/archives/trades_closed",
    )
    keep_days = int(os.getenv("KEEP_DAYS", "14"))
    manifest_name = os.getenv("MANIFEST_NAME", "manifest.json")

    out: Dict[str, object] = {"generated_utc_day": _utc_day_now(), "keep_days": keep_days, "archives": []}
    for part in [p.strip() for p in dirs.split(",") if p.strip()]:
        d = Path(part).expanduser()
        d.mkdir(parents=True, exist_ok=True)
        out["archives"].append(_inventory_one(d, keep_days=keep_days, manifest_name=manifest_name))

    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
