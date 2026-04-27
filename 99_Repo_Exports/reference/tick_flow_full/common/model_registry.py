from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class WriteResult:
    path: str
    sha256: str
    size: int


def _sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_bytes_atomic(dst_path: str, data: bytes, mode: int = 0o644) -> WriteResult:
    """
    Atomic write: write to a temp file in the same directory, fsync, then replace.
    """
    dst = Path(dst_path)
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(f".{dst.name}.tmp.{os.getpid()}.{int(time.time() * 1000)}")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(tmp, mode)
    os.replace(tmp, dst)
    return WriteResult(path=str(dst), sha256=_sha256_bytes(data), size=len(data))


def write_json_atomic(dst_path: str, obj: Dict[str, Any], mode: int = 0o644) -> WriteResult:
    data = (json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
    return write_bytes_atomic(dst_path, data, mode=mode)


def atomic_copy(src_path: str, dst_path: str, mode: int = 0o644) -> WriteResult:
    """
    Copy src -> dst atomically (read all bytes; safe for model sizes ~KB/MB).
    """
    with open(src_path, "rb") as f:
        data = f.read()
    return write_bytes_atomic(dst_path, data, mode=mode)


def ensure_dir(path: str) -> str:
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def version_stamp(ts_ms: Optional[int] = None) -> str:
    ts = ts_ms if ts_ms is not None else int(time.time() * 1000)
    # YYYYMMDD_HHMMSS_mmm (UTC)
    t = time.gmtime(ts / 1000.0)
    return time.strftime("%Y%m%d_%H%M%S", t) + f"_{ts % 1000:03d}"


def write_versioned_model(
    model_path: str,
    registry_dir: str,
    *,
    kind: str = "meta_lr",
    ts_ms: Optional[int] = None,
    extra_meta: Optional[Dict[str, Any]] = None,
) -> Tuple[WriteResult, str]:
    """
    Store a copy into registry dir under a versioned name and write a small metadata json.
    Returns (write_result, version_id).
    """
    ensure_dir(registry_dir)
    v = version_stamp(ts_ms)
    dst_model = Path(registry_dir) / f"{kind}.{v}.json"
    wr = atomic_copy(model_path, str(dst_model))
    meta = {
        "kind": kind,
        "version": v,
        "model_file": dst_model.name,
        "sha256": wr.sha256,
        "size": wr.size,
        "ts_ms": ts_ms if ts_ms is not None else int(time.time() * 1000),
    }
    if extra_meta:
        meta.update(extra_meta)
    write_json_atomic(str(Path(registry_dir) / f"{kind}.{v}.meta.json"), meta)
    return wr, v


def promote_version(registry_dir: str, kind: str, version: str, dst_path: str) -> Dict[str, Any]:
    """
    Promote a version from registry to dst_path atomically.
    """
    model_file = Path(registry_dir) / f"{kind}.{version}.json"
    if not model_file.exists():
        raise FileNotFoundError(str(model_file))
    wr = atomic_copy(str(model_file), dst_path)
    # keep pointer
    pointer = {
        "kind": kind,
        "version": version,
        "dst_path": dst_path,
        "sha256": wr.sha256,
        "size": wr.size,
        "applied_ts_ms": int(time.time() * 1000),
    }
    write_json_atomic(str(Path(registry_dir) / f"{kind}.champion.json"), pointer)
    return pointer



def _copytree_atomic(src_dir: str, dst_dir: str) -> Dict[str, Any]:
    """
    Atomically replace dst_dir with a copy of src_dir.
    Uses a temp sibling directory + os.replace for the final pointer swap.
    """
    src = Path(src_dir)
    dst = Path(dst_dir)
    if not src.exists() or not src.is_dir():
        raise FileNotFoundError(str(src))
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.parent / f".{dst.name}.tmp.{os.getpid()}.{int(time.time() * 1000)}"
    if tmp.exists():
        shutil.rmtree(tmp, ignore_errors=True)
    shutil.copytree(src, tmp)
    os.replace(tmp, dst)
    return {"path": str(dst), "entries": sum(1 for _ in dst.rglob('*'))}


def promote_bundle_dir(registry_dir: str, kind: str, version: str, dst_dir: str) -> Dict[str, Any]:
    """
    Promote a versioned bundle directory from registry into dst_dir atomically.

    Expected registry layout:
      <registry_dir>/<kind>.<version>/
        manifest.json
        ... other bundle files ...
    """
    src_dir = Path(registry_dir) / f"{kind}.{version}"
    if not src_dir.exists() or not src_dir.is_dir():
        raise FileNotFoundError(str(src_dir))
    info = _copytree_atomic(str(src_dir), dst_dir)
    pointer = {
        "kind": kind,
        "version": version,
        "dst_dir": dst_dir,
        "src_dir": str(src_dir),
        "entries": int(info.get("entries", 0)),
        "applied_ts_ms": int(time.time() * 1000),
    }
    write_json_atomic(str(Path(registry_dir) / f"{kind}.champion.json"), pointer)
    return pointer
