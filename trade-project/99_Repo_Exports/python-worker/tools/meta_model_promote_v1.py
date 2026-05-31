#!/usr/bin/env python3
"""meta_model_promote_v1

Atomic promotion of a trained MetaModelLR JSON to a stable artifact path.

Writes a manifest JSON with:
  - schema
  - sha256
  - promoted_model_json
  - latest_link (optional)

This tool is intentionally simple: copy + fsync + os.replace.
"""

import argparse
import hashlib
import json
import os
import shutil
import time
from pathlib import Path
import contextlib


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_copy(src: str, dst: str) -> None:
    tmp = dst + ".tmp"
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(src, "rb") as fsrc, open(tmp, "wb") as fdst:
        shutil.copyfileobj(fsrc, fdst, length=1024 * 1024)
        fdst.flush()
        os.fsync(fdst.fileno())
    os.replace(tmp, dst)


def try_symlink_atomic(target: str, link_path: str) -> str:
    # Best-effort. On filesystems without symlink support, just skip.
    try:
        link_tmp = link_path + ".tmp"
        with contextlib.suppress(FileNotFoundError):
            os.remove(link_tmp)
        os.symlink(os.path.basename(target), link_tmp)
        os.replace(link_tmp, link_path)
        return link_path
    except Exception:
        return ""


def main() -> int:
    ap = argparse.ArgumentParser(description="Promote MetaModelLR model.json atomically")
    ap.add_argument("--in-json", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--out-manifest-json", required=True)
    ap.add_argument("--link-latest", action="store_true")
    args = ap.parse_args()

    in_path = Path(args.in_json)
    if not in_path.exists():
        raise SystemExit(f"input_not_found: {in_path}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    sha = sha256_file(str(in_path))
    ts = time.strftime("%Y%m%d_%H%M%S")
    name = f"meta_model_{args.schema}_{ts}_{sha[:12]}.json"
    promoted = out_dir / name

    atomic_copy(str(in_path), str(promoted))

    latest_link = ""
    if args.link_latest:
        link_path = out_dir / f"latest_{args.schema}.json"
        latest_link = try_symlink_atomic(str(promoted), str(link_path))

    manifest = {
        "schema": args.schema,
        "sha256": sha,
        "input_model_json": str(in_path),
        "promoted_model_json": str(promoted),
        "latest_link": latest_link,
    }
    with open(args.out_manifest_json, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(json.dumps(manifest, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
