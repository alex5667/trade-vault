from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


def _to_bool(x: Any, default: bool = False) -> bool:
    """
    Robust bool parsing for flags coming from:
      - env ("1/0", "true/false", "yes/no")
      - JSON (true/false)
      - accidental numeric strings
    Fail-closed to default (do NOT raise in prod path).
    """
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(int(x))
    if isinstance(x, str):
        s = x.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
    return default


@dataclass(frozen=True)
class FeatureFlagsSnapshot:
    """
    Immutable snapshot that is cheap to pass around and safe to put into payload/labels.
    IMPORTANT: keep label cardinality low -> expose only ff_mask (0..15) + revision int.
    """
    use_unified_scoring: bool = False
    use_l3_veto_for_breakout: bool = False
    absorption_require_2ofn_confirmations: bool = True
    regime_detector_v2: bool = False

    revision: int = 0         # monotonic-ish config revision for debugging/rollbacks
    loaded_ms: int = 0        # when snapshot was loaded
    source: str = "env"       # "env" | "redis" | "file" | "env+redis" | ...

    def mask(self) -> int:
        """
        Compact stable bitmask (0..15):
          bit0: USE_UNIFIED_SCORING
          bit1: USE_L3_VETO_FOR_BREAKOUT
          bit2: ABSORPTION_REQUIRE_2OFN_CONFIRMATIONS
          bit3: REGIME_DETECTOR_V2
        """
        m = 0
        if self.use_unified_scoring:
            m |= 1 << 0
        if self.use_l3_veto_for_breakout:
            m |= 1 << 1
        if self.absorption_require_2ofn_confirmations:
            m |= 1 << 2
        if self.regime_detector_v2:
            m |= 1 << 3
        return int(m)


class FeatureFlagsManager:
    """
    Hot-reloadable feature flags without redeploy.

    Sources (in priority order):
      1) Redis JSON (optional)  -> FEATURE_FLAGS_REDIS_KEY
      2) File JSON  (optional)  -> FEATURE_FLAGS_FILE
      3) Env:
         - FEATURE_FLAGS_JSON='{"USE_UNIFIED_SCORING":true,...,"rev":12}'
         - or per-flag env vars:
             USE_UNIFIED_SCORING=1
             USE_L3_VETO_FOR_BREAKOUT=0
             ABSORPTION_REQUIRE_2OFN_CONFIRMATIONS=1
             REGIME_DETECTOR_V2=0

    Refresh policy:
      - cached snapshot + periodic refresh by FEATURE_FLAGS_REFRESH_MS (default 1000ms).
      - safe fail-open: on parse/read errors keeps the previous snapshot.
    """

    def __init__(self, *, redis: Any | None = None, logger: Any | None = None) -> None:
        self._redis = redis
        self._logger = logger

        self._refresh_ms = int(os.getenv("FEATURE_FLAGS_REFRESH_MS", "1000"))
        self._redis_key = os.getenv("FEATURE_FLAGS_REDIS_KEY", "feature_flags:json")
        self._file_path = os.getenv("FEATURE_FLAGS_FILE", "").strip()

        # cached snapshot
        now = _now_ms()
        self._snap: FeatureFlagsSnapshot = self._load_from_env(now_ms=now)
        self._last_refresh_ms = now

        # file mtime cache (for cheap change detection)
        self._file_mtime: float = 0.0

    def get(self, *, force_refresh: bool = False) -> FeatureFlagsSnapshot:
        now = _now_ms()
        if force_refresh or (now - self._last_refresh_ms) >= self._refresh_ms:
            self.refresh(force=True)
        return self._snap

    def refresh(self, *, force: bool = False) -> FeatureFlagsSnapshot:
        now = _now_ms()
        if (not force) and (now - self._last_refresh_ms) < self._refresh_ms:
            return self._snap

        prev = self._snap
        self._last_refresh_ms = now

        # Priority: Redis -> File -> Env
        snap = None

        if self._redis is not None:
            snap = self._try_load_from_redis(now_ms=now)
        if snap is None and self._file_path:
            snap = self._try_load_from_file(now_ms=now)
        if snap is None:
            snap = self._load_from_env(now_ms=now)

        # fail-open: never drop to None
        if snap is None:
            return prev
        self._snap = snap
        return self._snap

    def _try_load_from_redis(self, *, now_ms: int) -> FeatureFlagsSnapshot | None:
        try:
            raw = self._redis.get(self._redis_key)
            if not raw:
                return None
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="replace")
            if not isinstance(raw, str):
                raw = str(raw)
            d = json.loads(raw)
            if not isinstance(d, dict):
                return None
            return self._from_dict(d, now_ms=now_ms, source="redis")
        except Exception as e:
            if self._logger:
                self._logger.warning(f"FeatureFlagsManager: redis load failed: {e}")
            return None

    def _try_load_from_file(self, *, now_ms: int) -> FeatureFlagsSnapshot | None:
        try:
            st = os.stat(self._file_path)
            # cheap skip if no changes
            if st.st_mtime <= self._file_mtime:
                return None
            self._file_mtime = float(st.st_mtime)
            with open(self._file_path, encoding="utf-8") as f:
                raw = f.read()
            d = json.loads(raw)
            if not isinstance(d, dict):
                return None
            return self._from_dict(d, now_ms=now_ms, source="file")
        except FileNotFoundError:
            return None
        except Exception as e:
            if self._logger:
                self._logger.warning(f"FeatureFlagsManager: file load failed: {e}")
            return None

    def _load_from_env(self, *, now_ms: int) -> FeatureFlagsSnapshot:
        # FEATURE_FLAGS_JSON overrides per-flag envs
        raw_json = os.getenv("FEATURE_FLAGS_JSON", "").strip()
        if raw_json:
            try:
                d = json.loads(raw_json)
                if isinstance(d, dict):
                    return self._from_dict(d, now_ms=now_ms, source="env_json")
            except Exception as e:
                if self._logger:
                    self._logger.warning(f"FeatureFlagsManager: FEATURE_FLAGS_JSON parse failed: {e}")
                # continue to per-flag env

        # per-flag env
        d2 = {
            "USE_UNIFIED_SCORING": os.getenv("USE_UNIFIED_SCORING", "0"),
            "USE_L3_VETO_FOR_BREAKOUT": os.getenv("USE_L3_VETO_FOR_BREAKOUT", "0"),
            "ABSORPTION_REQUIRE_2OFN_CONFIRMATIONS": os.getenv("ABSORPTION_REQUIRE_2OFN_CONFIRMATIONS", "1"),
            "REGIME_DETECTOR_V2": os.getenv("REGIME_DETECTOR_V2", "0"),
            # optional "rev"
            "rev": os.getenv("FEATURE_FLAGS_REV", "0"),
        }
        return self._from_dict(d2, now_ms=now_ms, source="env")

    def _from_dict(self, d: dict[str, Any], *, now_ms: int, source: str) -> FeatureFlagsSnapshot:
        # allow flexible key spellings (env-style / human)
        def g(*keys: str, default: Any = None) -> Any:
            for k in keys:
                if k in d:
                    return d.get(k)
            return default

        rev_raw = g("rev", "revision", "FEATURE_FLAGS_REV", default=0)
        try:
            rev = int(float(rev_raw)) if rev_raw is not None else 0
        except Exception:
            rev = 0

        return FeatureFlagsSnapshot(
            use_unified_scoring=_to_bool(g("USE_UNIFIED_SCORING", "use_unified_scoring", default=False)),
            use_l3_veto_for_breakout=_to_bool(g("USE_L3_VETO_FOR_BREAKOUT", "use_l3_veto_for_breakout", default=False)),
            absorption_require_2ofn_confirmations=_to_bool(
                g("ABSORPTION_REQUIRE_2OFN_CONFIRMATIONS", "absorption_require_2ofn_confirmations", default=True),
                default=True,
            ),
            regime_detector_v2=_to_bool(g("REGIME_DETECTOR_V2", "regime_detector_v2", default=False)),
            revision=rev,
            loaded_ms=int(now_ms),
            source=str(source),
        )
