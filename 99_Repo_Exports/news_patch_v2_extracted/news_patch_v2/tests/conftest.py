"""
conftest.py — stubs for external dependencies not present in the patch directory.

Allows tests for analyzer_worker, calendar_store_worker, and calendar_store_service
to run in isolation without the full news_pipeline package installed.
"""
from __future__ import annotations

import sys
import time as _time
import types
from dataclasses import dataclass, field
from unittest.mock import MagicMock


def _make_module(name: str, **attrs) -> types.ModuleType:
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


# ---------------------------------------------------------------------------
# Stub: news_pipeline family
# ---------------------------------------------------------------------------

class _StreamWorker:
    def __init__(self, *, redis, stream, group, consumer, dlq_stream,
                 block_ms=2000, count=100, claim_idle_ms=60_000):
        self.r = redis

    def run_forever(self):
        raise NotImplementedError

    def handle_message(self, msg_id: str, fields: dict) -> None:
        raise NotImplementedError


_sw_mod = _make_module("news_pipeline.stream_worker", StreamWorker=_StreamWorker)
_np_mod = _make_module("news_pipeline", stream_worker=_sw_mod)
sys.modules["news_pipeline"] = _np_mod
sys.modules["news_pipeline.stream_worker"] = _sw_mod

_llm_mod = _make_module("news_pipeline.llm_client", GeminiHTTPClient=MagicMock)
sys.modules["news_pipeline.llm_client"] = _llm_mod


class _NewsPostgresWriter:
    @classmethod
    def from_env(cls):
        return cls()
    def ensure_schema(self): pass
    def insert_calendar_event(self, **kw): pass
    def insert_calendar_feature_scope(self, **kw): pass


_pg_mod = _make_module("news_pipeline.postgres_writer", NewsPostgresWriter=_NewsPostgresWriter)
sys.modules["news_pipeline.postgres_writer"] = _pg_mod

# ---------------------------------------------------------------------------
# Stub: contexts (used by enricher_sync)
# ---------------------------------------------------------------------------

@dataclass
class _NewsFeatures:
    ref: str = ""
    news_risk: float = 0.0
    surprise_score: float = 0.0
    news_grade_id: int = 0
    tags_mask: int = 0
    primary_tag_id: int = 0
    confidence: float = 0.0
    horizon_sec: int = 0
    asof_ts_ms: int = 0
    event_tminus_sec: int = -1
    event_grade_id: int = 0

@dataclass
class _OrderflowSignalContext:
    symbol: str = ""
    news: object = None
    data_quality_flags: list = field(default_factory=list)

_ctx_mod = _make_module(
    "contexts",
    NewsFeatures=_NewsFeatures,
    OrderflowSignalContext=_OrderflowSignalContext,
)
sys.modules["contexts"] = _ctx_mod

# ---------------------------------------------------------------------------
# Stubs for calendar_store_service relative imports
# (.models, .redis_streams, .utils, .config)
# calendar_store_service is part of a package; we register the parent
# package so relative imports resolve.
# ---------------------------------------------------------------------------

# Parent package stub (news_patch_v2)
_pkg_name = "news_patch_v2"
_pkg = types.ModuleType(_pkg_name)
_pkg.__path__ = []  # make it a package
_pkg.__package__ = _pkg_name
sys.modules[_pkg_name] = _pkg


@dataclass
class _CalendarEvent:
    event_id: str = ""
    title: str = ""
    ts_ms: int = 0
    grade_id: int = 0
    currency: str = ""
    region: str = ""
    symbols: list = field(default_factory=list)
    payload: str = ""

    @classmethod
    def from_stream_fields(cls, fields: dict) -> "_CalendarEvent":
        return cls(
            event_id=str(fields.get("event_id", "")),
            title=str(fields.get("title", "")),
            ts_ms=int(float(fields.get("ts_ms", 0) or 0)),
            grade_id=int(float(fields.get("grade_id", 0) or 0)),
            currency=str(fields.get("currency", "")),
            region=str(fields.get("region", "")),
            symbols=list(fields.get("symbols", [])),
            payload=str(fields.get("payload", "")),
        )


_models_mod = _make_module(f"{_pkg_name}.models", CalendarEvent=_CalendarEvent)
sys.modules[f"{_pkg_name}.models"] = _models_mod
setattr(_pkg, "models", _models_mod)

# redis_streams stubs
def _ensure_group(r, stream, group, mkstream=False): pass
def _xreadgroup_block(r, stream, group, consumer, count, block_ms): return []
def _xack(r, stream, group, msg_id): pass

_rs_mod = _make_module(
    f"{_pkg_name}.redis_streams",
    ensure_group=_ensure_group,
    xreadgroup_block=_xreadgroup_block,
    xack=_xack,
)
sys.modules[f"{_pkg_name}.redis_streams"] = _rs_mod
setattr(_pkg, "redis_streams", _rs_mod)

# utils stubs
_utils_mod = _make_module(
    f"{_pkg_name}.utils",
    now_ms=lambda: int(_time.time() * 1000),
    safe_int=lambda v, d=0: int(float(v)) if v else d,
)
sys.modules[f"{_pkg_name}.utils"] = _utils_mod
setattr(_pkg, "utils", _utils_mod)

# config stubs
_config_mod = _make_module(
    f"{_pkg_name}.config",
    CALENDAR_EVENTS_STREAM="calendar:events",
    CALENDAR_FEATURE_GROUP="calendar-feature-store",
    CALENDAR_EVENT_TTL_SEC=604800,
    CALENDAR_AGG_TTL_SEC=3600,
)
sys.modules[f"{_pkg_name}.config"] = _config_mod
setattr(_pkg, "config", _config_mod)

# Now register calendar_store_service as a submodule of the package
# so `from .models import ...` resolves correctly
import importlib
import importlib.util
import pathlib

_css_path = pathlib.Path(__file__).parent.parent / "calendar_store_service.py"
_spec = importlib.util.spec_from_file_location(
    f"{_pkg_name}.calendar_store_service",
    str(_css_path),
    submodule_search_locations=[],
)
if _spec and _spec.loader:
    _css_mod = importlib.util.module_from_spec(_spec)
    _css_mod.__package__ = _pkg_name
    sys.modules[f"{_pkg_name}.calendar_store_service"] = _css_mod
    _spec.loader.exec_module(_css_mod)
    # Also register under bare name for `from calendar_store_service import ...`
    sys.modules["calendar_store_service"] = _css_mod
    setattr(_pkg, "calendar_store_service", _css_mod)
