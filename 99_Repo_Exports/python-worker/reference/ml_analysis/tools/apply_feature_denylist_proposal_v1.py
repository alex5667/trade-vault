#!/usr/bin/env python3
from __future__ import annotations

"""Apply an *approved* feature denylist proposal to the active denylist json.

Safety properties:
 - Only applies when manifest.status == 'approved'
 - Applies by *union* with manifest['denylist_after'] (never removes keys)
 - Writes a backup copy and an audit record
 - Updates manifest.status -> 'applied'
"""


import argparse
import json
import os
import shutil
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _sha256_short(s: str, n: int = 8) -> str:
    return sha256(s.encode("utf-8")).hexdigest()[:n]


def _stable_sorted_unique(xs: list[str]) -> list[str]:
    # stable deterministic ordering for diffs
    return sorted(set(xs))


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_write_json(path: Path, obj: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _resolve_default_denylist_path() -> Path:
    return Path("tick_flow_full/core/feature_denylist_v1.json").resolve()


def _extract_after(m: dict[str, Any]) -> tuple[list[str], list[str]]:
    after = m.get("denylist_after") or {}
    dn = after.get("deny_num") or []
    db = after.get("deny_bool") or []
    if not isinstance(dn, list) or not isinstance(db, list):
        raise ValueError("denylist_after must contain lists deny_num/deny_bool")
    return [str(x) for x in dn], [str(x) for x in db]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, help="Path to denylist_proposal_*.manifest.json")
    ap.add_argument(
        "--denylist-path",
        default="",
        help="Optional. Defaults to tick_flow_full/core/feature_denylist_v1.json (repo-relative).",
    )
    ap.add_argument("--applier", default=os.environ.get("USER", ""))
    ap.add_argument("--apply", type=int, default=0, help="1 = apply (otherwise dry-run)")
    args = ap.parse_args()

    mp = Path(args.manifest).expanduser().resolve()
    if not mp.exists():
        print(f"manifest not found: {mp}")
        return 2

    m = _load_json(mp)
    if m.get("kind") != "feature_denylist_proposal_v1":
        print("ERROR: manifest kind mismatch")
        return 2

    st = (m.get("status") or "").strip()
    if st != "approved":
        print(f"ERROR: cannot apply: status must be 'approved' (got '{st}')")
        return 2

    denylist_path = Path(args.denylist_path).expanduser().resolve() if args.denylist_path else _resolve_default_denylist_path()
    if not denylist_path.exists():
        print(f"denylist json not found: {denylist_path}")
        return 2

    active = _load_json(denylist_path)
    before_dn = [str(x) for x in (active.get("deny_num") or [])]
    before_db = [str(x) for x in (active.get("deny_bool") or [])]

    after_dn, after_db = _extract_after(m)

    # Apply by union.
    new_dn = _stable_sorted_unique(before_dn + after_dn)
    new_db = _stable_sorted_unique(before_db + after_db)

    add_dn = [x for x in new_dn if x not in before_dn]
    add_db = [x for x in new_db if x not in before_db]

    print(f"denylist_path: {denylist_path}")
    print(f"adds: deny_num={len(add_dn)} deny_bool={len(add_db)}")
    if add_dn:
        print("+ deny_num:")
        for k in add_dn:
            print(f"  - {k}")
    if add_db:
        print("+ deny_bool:")
        for k in add_db:
            print(f"  - {k}")

    if int(args.apply) != 1:
        print("dry-run: pass --apply 1 to apply")
        return 0

    # Backup
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    bak = denylist_path.with_suffix(denylist_path.suffix + f".bak.{ts}")
    shutil.copy2(denylist_path, bak)

    active["deny_num"] = new_dn
    active["deny_bool"] = new_db
    active["updated_utc"] = _utc_now()
    _atomic_write_json(denylist_path, active)

    # Audit record
    ph = (m.get("proposal_hash") or "")
    ph8 = _sha256_short(ph or mp.as_posix(), 8)
    audit = {
        "kind": "feature_denylist_apply_record_v1",
        "applied_utc": _utc_now(),
        "applier": args.applier,
        "manifest": str(mp),
        "proposal_hash": ph,
        "denylist_path": str(denylist_path),
        "backup_path": str(bak),
        "adds": {"deny_num": add_dn, "deny_bool": add_db},
        "counts_before": {"deny_num": len(before_dn), "deny_bool": len(before_db)},
        "counts_after": {"deny_num": len(new_dn), "deny_bool": len(new_db)},
    }
    audit_path = denylist_path.parent / f"feature_denylist_apply_{ts}_{ph8}.json"
    _atomic_write_json(audit_path, audit)

    # Update manifest
    m["status"] = "applied"
    m["applied_utc"] = _utc_now()
    m["applied_by"] = args.applier
    m["applied_denylist_path"] = str(denylist_path)
    m["applied_audit_record"] = str(audit_path)
    mp.write_text(json.dumps(m, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"OK: applied. audit_record={audit_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
