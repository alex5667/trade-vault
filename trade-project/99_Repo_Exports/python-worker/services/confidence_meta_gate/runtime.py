"""Process-global runtime singleton for the confidence meta-gate.

Wraps the loaded artifact + config. Built lazily on first access so unit
tests can monkeypatch ENV/config without paying the load cost.

The runtime also caches a Redis-driven mode override (`cfg:conf_meta_gate`
HASH, written by the auto-demote watcher). The override is checked through
`effective_mode()` with a short TTL so it costs ~one HGET per N seconds —
hot-path latency stays in budget.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

from .config import MetaGateConfig, MetaGateMode, get_config
from .model import ArtifactSlot, MetaGateArtifact, load_artifact

log = logging.getLogger("conf_meta_gate.runtime")

# Cache TTL for the auto-demote override read. Cheap enough to read often
# but rare enough not to hammer Redis from the hot path. 30 s keeps
# response time to a forced SHADOW under a minute.
_OVERRIDE_TTL_SEC = 30.0


class MetaGateRuntime:
    """Holds (cfg, artifact_slot). Thread-safe artifact swap."""

    def __init__(self, cfg: MetaGateConfig) -> None:
        self.cfg = cfg
        self._slot = ArtifactSlot()
        self._loaded_path: str | None = None
        # Auto-demote override cache: (cached_mode, fetched_at_monotonic).
        # Refreshed every _OVERRIDE_TTL_SEC seconds via redis HGET; failures
        # are swallowed (override stays as last known value).
        self._override_mode: MetaGateMode | None = None
        self._override_fetched_at: float = 0.0
        self._override_lock = threading.Lock()

    def ensure_loaded(self) -> MetaGateArtifact | None:
        """Load the artifact once per (path, process). Returns current value."""
        cfg = self.cfg
        if self._loaded_path == cfg.model_path and self._slot.get() is not None:
            return self._slot.get()
        artifact = load_artifact(cfg.model_path)
        if artifact is not None:
            self._slot.set(artifact)
            self._loaded_path = cfg.model_path
        return artifact

    def reload(self) -> MetaGateArtifact | None:
        """Force re-read from disk (for hot-swap)."""
        artifact = load_artifact(self.cfg.model_path)
        self._slot.set(artifact)
        self._loaded_path = self.cfg.model_path if artifact else None
        return artifact

    def set_artifact(self, artifact: MetaGateArtifact | None) -> None:
        """Inject an artifact directly (used by tests)."""
        self._slot.set(artifact)
        self._loaded_path = artifact.source_path if artifact else None

    def current(self) -> MetaGateArtifact | None:
        return self._slot.get()

    def effective_mode(self, redis_client: Any | None = None) -> MetaGateMode:
        """Return the mode to use for the next decision.

        Reads cfg:conf_meta_gate.mode from Redis with TTL caching; if the
        auto-demote watcher has forced SHADOW, that wins over cfg.mode.
        ENV-level KILL_SWITCH / OFF / LEGACY_ONLY still win because we
        never relax the manual setting — we only ever tighten it.
        """
        base = self.cfg.mode
        # Hard manual states are never overridden.
        if base in (MetaGateMode.OFF, MetaGateMode.LEGACY_ONLY,
                    MetaGateMode.KILL_SWITCH):
            return base
        override = self._read_override(redis_client)
        if override is None:
            return base
        # Only ever tighten: SHADOW wins over CANARY/ENFORCE.
        if override is MetaGateMode.SHADOW and base in (
            MetaGateMode.CANARY, MetaGateMode.ENFORCE,
        ):
            return MetaGateMode.SHADOW
        return base

    def _read_override(self, redis_client: Any | None) -> MetaGateMode | None:
        if redis_client is None:
            return self._override_mode
        now = time.monotonic()
        with self._override_lock:
            if (now - self._override_fetched_at) < _OVERRIDE_TTL_SEC:
                return self._override_mode
        try:
            raw = redis_client.hget("cfg:conf_meta_gate", "mode")
            # Some async clients return awaitables. If so, skip — the auto-
            # demote watcher writes synchronously and we'll catch it next tick.
            if hasattr(raw, "__await__"):
                return self._override_mode
        except Exception as e:  # pragma: no cover — fail-open
            log.debug("conf_meta_gate override HGET failed: %s", e)
            return self._override_mode
        with self._override_lock:
            self._override_fetched_at = now
            if not raw:
                self._override_mode = None
                return None
            try:
                mode = MetaGateMode(str(raw).upper())
            except ValueError:
                self._override_mode = None
                return None
            self._override_mode = mode
        return mode

    def clear_override_cache(self) -> None:
        """Invalidate the cached override so the next call re-reads Redis."""
        with self._override_lock:
            self._override_mode = None
            self._override_fetched_at = 0.0


_RUNTIME: MetaGateRuntime | None = None
_RUNTIME_LOCK = threading.Lock()


def get_runtime() -> MetaGateRuntime:
    global _RUNTIME
    if _RUNTIME is None:
        with _RUNTIME_LOCK:
            if _RUNTIME is None:
                _RUNTIME = MetaGateRuntime(get_config())
    return _RUNTIME


def reset_runtime() -> None:
    """Drop the singleton (tests only)."""
    global _RUNTIME
    with _RUNTIME_LOCK:
        _RUNTIME = None
