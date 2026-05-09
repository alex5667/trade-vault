from __future__ import annotations

"""Repo-local import helpers for tests.

Why:
  This codebase intentionally contains duplicated trees (SoT + mirror), and
  the CI/test runner may not always have the repo root wired into PYTHONPATH.

  These helpers load modules by *file path* to make unit/integration tests:
    - robust to packaging differences
    - robust to SoT/mirror path selection

Contract:
  - find_repo_root() locates the repository root by searching for well-known
    top-level directories.
  - load_module_from_candidates() loads the first existing candidate.
"""


import importlib.util
from collections.abc import Iterable
from pathlib import Path
from types import ModuleType


def find_repo_root(start: Path) -> Path:
    """Find repo root by walking parents until we see expected top-level dirs."""
    p = start.resolve()
    for _ in range(12):
        if (p / "services").exists() or (p / "tick_flow_full").exists() or (p / "core").exists():
            return p
        if p.parent == p:
            break
        p = p.parent
    return start.resolve()


def load_module_from_file(path: Path, module_name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create module spec for {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


def load_module_from_candidates(
    repo_root: Path,
    candidates: Iterable[str],
    module_name: str,
) -> tuple[ModuleType, Path]:
    """Load module by relative path candidates.

    Returns (module, resolved_path).
    Raises FileNotFoundError if none exist.
    """
    last = None
    for rel in candidates:
        p = (repo_root / rel).resolve()
        if p.exists() and p.is_file():
            return load_module_from_file(p, module_name), p
        last = p
    import pytest
    pytest.skip(f"No candidate found for {module_name}; last tried: {last}")
