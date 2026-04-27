from __future__ import annotations
import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

from utils.time_utils import get_ny_time_millis

"""
DecisionTrace v2 (сквозной, fail-open)
=====================================
Цели:
  1) Единая трасса по всему пути:
        detector -> gates -> outbox -> dispatcher targets
  2) Нулевой риск "исполнения из diagnostics":
        - tradeable payload (outbox env/targets) НЕ содержит полного trace/events
        - env содержит только trace_id + trace_summary (короткое)
        - полный trace хранится в sidecar meta-key (OUTBOX_META_PREFIX + sid)
  3) Тайминги (duration_ms) по gate'ам и targets.
  4) Никаких исключений наружу: instrumentation ВСЕГДА fail-open.

Контракт безопасности:
  - build_outbox_envelope(...) должен возвращать env без trace/events.
  - dispatcher может держать bounded trace в env["trace"] ТОЛЬКО для ретраев/DLQ,
    но bounded (max_events/max_bytes) и не для трейдинга.
"""

# ---------------------------------------------------------------------
# Runtime config caching (hot-path hardening)
# ---------------------------------------------------------------------
_CFG: Dict[str, Any] = {"loaded_mono_ms": 0.0}

def _mono_ms() -> float:
    return time.perf_counter() * 1000.0

def _now_ms() -> int:
    return get_ny_time_millis()

def _get_env_bool(name: str, default: bool) -> bool:
    try:
        v = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
        return v in {"1", "true", "yes", "y", "on"}
    except Exception:
        return default

def _get_env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _get_env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def _cfg_refresh_if_needed() -> Dict[str, Any]:
    now = _mono_ms()
    ttl_ms = float(max(1000.0, _get_env_int("DECISION_TRACE_CFG_REFRESH_MS", 5000)))
    if (now - float(_CFG.get("loaded_mono_ms") or 0.0)) < ttl_ms:
        return _CFG

    _CFG["loaded_mono_ms"] = now
    _CFG["enabled"] = _get_env_bool("DECISION_TRACE_ENABLE", True)
    _CFG["max_events"] = max(50, _get_env_int("DECISION_TRACE_MAX_EVENTS", 400))
    _CFG["summary_max_len"] = max(80, _get_env_int("DECISION_TRACE_SUMMARY_MAX_LEN", 240))
    _CFG["log_sample_rate"] = float(_get_env_float("DECISION_TRACE_LOG_SAMPLE_RATE", 0.02))
    _CFG["sidecar_success_sample_rate"] = float(_get_env_float("DECISION_TRACE_SIDECAR_SUCCESS_SAMPLE_RATE", 0.05))
    _CFG["sidecar_max_bytes"] = max(20_000, _get_env_int("DECISION_TRACE_SIDECAR_MAX_BYTES", 120_000))
    return _CFG

def trace_enabled() -> bool:
    try:
        return bool(_cfg_refresh_if_needed().get("enabled", True))
    except Exception:
        return True

def should_sample(trace_id: str, rate01: float) -> bool:
    try:
        r = float(rate01)
        if r <= 0:
            return False
        if r >= 1:
            return True
        s = (trace_id or "").encode("utf-8", "ignore")
        h = hashlib.md5(s).hexdigest()  # noqa: S324
        v = int(h[:8], 16) / float(0xFFFFFFFF)
        return v < r
    except Exception:
        return False

# ---------------------------------------------------------------------
# Timing Span
# ---------------------------------------------------------------------
class _MsProxy:
    __slots__ = ("_span",)
    def __init__(self, span: Span) -> None:
        self._span = span
    def __call__(self) -> float:
        return self._span._elapsed_ms()
    def __float__(self) -> float:
        return float(self._span._elapsed_ms())
    def __repr__(self) -> str:
        try:
            return f"{float(self):.3f}ms"
        except Exception:
            return "0.000ms"

class Span:
    __slots__ = ("_t0", "_t1", "ms")
    def __init__(self) -> None:
        self._t0 = time.perf_counter()
        self._t1 = None
        self.ms = _MsProxy(self)
    def __enter__(self) -> Span:
        return self
    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self._t1 = time.perf_counter()
        except Exception:
            self._t1 = None
    def _elapsed_ms(self) -> float:
        try:
            t1 = self._t1 if self._t1 is not None else time.perf_counter()
            return max(0.0, (float(t1) - float(self._t0)) * 1000.0)
        except Exception:
            return 0.0

# ---------------------------------------------------------------------
# DecisionTrace Core
# ---------------------------------------------------------------------
EventDict = Dict[str, Any]

def _trim_str(v: Any, n: int = 512) -> Any:
    try:
        if isinstance(v, str) and len(v) > n:
            return v[:n] + "..."
        return v
    except Exception:
        return v

def _cap_events(events: List[EventDict], max_events: int) -> List[EventDict]:
    if max_events <= 0:
        return []
    if len(events) <= max_events:
        return events
    return events[-max_events:]

@dataclass
class DecisionTrace:
    trace_id: str = ""
    created_ts_ms: int = 0
    sid: str = ""
    symbol: str = ""
    kind: str = ""
    tags: Dict[str, Any] = field(default_factory=dict)
    events: List[EventDict] = field(default_factory=list)

    @staticmethod
    def new(*, sid: str = "", trace_id: str = "") -> DecisionTrace:
        tid = str(trace_id or "").strip() or uuid.uuid4().hex
        return DecisionTrace(
            trace_id=tid,
            created_ts_ms=_now_ms(),
            sid=str(sid or ""),
        )

    @staticmethod
    def from_env(env: Dict[str, Any]) -> DecisionTrace:
        try:
            tid = str(env.get("trace_id") or env.get("corr_id") or env.get("correlation_id") or "").strip()
            sid = str(env.get("sid") or "").strip()
            tr = env.get("trace")
            if isinstance(tr, dict):
                out = DecisionTrace.new(sid=sid, trace_id=tid or str(tr.get("trace_id") or ""))
                out.sid = str(tr.get("sid") or sid or "")
                out.symbol = str(tr.get("symbol") or "")
                out.kind = str(tr.get("kind") or "")
                evs = tr.get("events")
                if isinstance(evs, list):
                    out.events = [e for e in evs if isinstance(e, dict)]
                return out
            return DecisionTrace.new(sid=sid, trace_id=tid)
        except Exception:
            return DecisionTrace.new(sid=str(env.get("sid") or ""), trace_id=str(env.get("trace_id") or ""))

    def add(
        self,
        *,
        where: str,
        name: str,
        ok: bool,
        veto: bool = False,
        reason_code: str = "",
        metrics: Optional[Dict[str, Any]] = None,
        duration_ms: Optional[float] = None,
        etype: str = "gate",
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            ev: Dict[str, Any] = {
                "type": str(etype or ""),
                "stage": str(where or ""),
                "name": str(name or ""),
                "t_ms": _now_ms(),
            }
            if etype == "gate":
                ev["passed"] = bool(ok) and (not bool(veto))
                ev["veto"] = bool(veto)
                ev["reason_code"] = str(reason_code or ("OK" if ok else "VETO"))
            elif etype == "target":
                ev["ok"] = bool(ok)
                ev["reason_code"] = str(reason_code or ("OK" if ok else "ERR"))
            else:
                ev["ok"] = bool(ok)
                ev["veto"] = bool(veto)
                ev["reason_code"] = str(reason_code or "")

            if duration_ms is not None:
                ev["duration_ms"] = float(duration_ms)

            if isinstance(metrics, dict) and metrics:
                safe_m: Dict[str, Any] = {}
                for i, (k, v) in enumerate(metrics.items()):
                    if i >= 32: break
                    safe_m[str(k)] = _trim_str(v, 256)
                ev["metrics"] = safe_m

            if isinstance(extra, dict) and extra:
                for i, (k, v) in enumerate(extra.items()):
                    if i >= 32: break
                    ev[str(k)] = _trim_str(v, 256)

            self.events.append(ev)
            mx = int(_cfg_refresh_if_needed().get("max_events", 400))
            if len(self.events) > mx:
                self.events = self.events[-mx:]
        except Exception:
            pass

    def to_dict(self, *, max_events: Optional[int] = None) -> Dict[str, Any]:
        try:
            mx = int(max_events) if isinstance(max_events, int) and max_events > 0 else int(_cfg_refresh_if_needed().get("max_events", 400))
            evs = _cap_events([e for e in self.events if isinstance(e, dict)], mx)
            return {
                "trace_id": str(self.trace_id or ""),
                "created_ts_ms": int(self.created_ts_ms or 0),
                "sid": str(self.sid or ""),
                "symbol": str(self.symbol or ""),
                "kind": str(self.kind or ""),
                "tags": dict(self.tags or {}),
                "events": evs,
            }
        except Exception:
            return {"trace_id": str(self.trace_id or ""), "sid": str(self.sid or ""), "events": []}

# ---------------------------------------------------------------------
# Public API Helpers
# ---------------------------------------------------------------------
def ensure_trace(ctx: Any, *, sid: str = "", trace_id: str = "") -> DecisionTrace:
    try:
        tr = getattr(ctx, "_decision_trace_obj", None)
        if isinstance(tr, DecisionTrace):
            if sid and not tr.sid: tr.sid = str(sid)
            if trace_id and not tr.trace_id: tr.trace_id = str(trace_id)
            return tr
    except Exception:
        pass

    tid = str(trace_id or "").strip()
    if not tid:
        tid = getattr(ctx, "trace_id", "") or getattr(ctx, "corr_id", "") or ""
    
    tr2 = DecisionTrace.new(sid=sid, trace_id=tid)
    try:
        setattr(ctx, "_decision_trace_obj", tr2)
        setattr(ctx, "trace_id", tr2.trace_id)
        setattr(ctx, "corr_id", tr2.trace_id)
    except Exception:
        pass
    return tr2

def get_trace_obj(ctx: Any) -> Optional[DecisionTrace]:
    try:
        tr = getattr(ctx, "_decision_trace_obj", None)
        return tr if isinstance(tr, DecisionTrace) else None
    except Exception:
        return None

def serialize_trace_from_ctx(ctx: Any) -> Dict[str, Any]:
    try:
        tr = get_trace_obj(ctx)
        if isinstance(tr, DecisionTrace):
            return tr.to_dict()
        # fallback legacy shapes
        d = getattr(ctx, "_decision_trace", None) or getattr(ctx, "decision_trace", None)
        if isinstance(d, dict):
            return dict(d)
    except Exception:
        pass
    return {}

def make_trace_summary(trace: Union[DecisionTrace, Dict[str, Any], None]) -> str:
    if trace is None: return ""
    try:
        cfg = _cfg_refresh_if_needed()
        mx = int(cfg.get("summary_max_len", 240))
        if isinstance(trace, DecisionTrace):
            d = trace.to_dict(max_events=64)
        else:
            d = trace if isinstance(trace, dict) else {}
        
        tid = str(d.get("trace_id") or "")
        sid = str(d.get("sid") or "")
        
        g_ok = g_veto = t_ok = t_fail = 0
        last_veto = ""
        evs = d.get("events") or []
        for ev in reversed(evs):
            if isinstance(ev, dict) and ev.get("type") == "gate" and bool(ev.get("veto", False)):
                last_veto = f"{ev.get('name','')}:{ev.get('reason_code','')}"
                break
        for ev in evs:
            if not isinstance(ev, dict): continue
            et = ev.get("type")
            if et == "gate":
                if bool(ev.get("veto", False)): g_veto += 1
                elif bool(ev.get("passed", False)): g_ok += 1
            elif et == "target":
                if bool(ev.get("ok", False)): t_ok += 1
                else: t_fail += 1

        s = f"tid={tid} sid={sid} g_ok={g_ok} g_veto={g_veto} t_ok={t_ok} t_fail={t_fail} last_veto={last_veto}"
        s = s.replace("\n", " ").strip()
        if len(s) > mx: s = s[: mx - 3] + "..."
        return s
    except Exception:
        return ""

def set_summary_fields(env: Dict[str, Any], tr: Optional[Union[DecisionTrace, Dict[str, Any]]]) -> None:
    if not isinstance(env, dict) or tr is None:
        return
    try:
        if isinstance(tr, DecisionTrace):
            tid = str(tr.trace_id or "")
        else:
            tid = str(tr.get("trace_id") or "") if isinstance(tr, dict) else ""
        if tid:
            env["trace_id"] = tid
            env["corr_id"] = tid
        env["trace_summary"] = make_trace_summary(tr)
    except Exception:
        pass

def to_dict_bounded(
    trace: Union[DecisionTrace, Dict[str, Any], None],
    *,
    max_events: int = 64,
    max_bytes: int = 16_000,
) -> Dict[str, Any]:
    if trace is None:
        return {}
    if isinstance(trace, DecisionTrace):
        d = trace.to_dict(max_events=max_events)
    elif isinstance(trace, dict):
        d = dict(trace)
    else:
        return {}

    evs = d.get("events")
    if isinstance(evs, list):
        if len(evs) > max_events:
            d["events"] = evs[-max_events:]
            d["events_truncated"] = int(len(evs) - max_events)
        for ev in d.get("events", []):
            if isinstance(ev, dict):
                for k, v in list(ev.items()):
                    if isinstance(v, str) and len(v) > 512:
                        ev[k] = v[:512] + "..."

    try:
        raw = json.dumps(d, ensure_ascii=False, separators=(",", ":"))
        if len(raw.encode("utf-8", "ignore")) > max_bytes:
            return {
                "trace_id": d.get("trace_id", ""),
                "sid": d.get("sid", ""),
                "trace_too_large": True,
                "events_truncated": len(d.get("events") or []),
            }
    except Exception:
        return {"trace_id": d.get("trace_id", ""), "trace_error": "serialization_failed"}

    return d

def build_sidecar_meta(trace: Union[DecisionTrace, Dict[str, Any]]) -> Dict[str, Any]:
    try:
        cfg = _cfg_refresh_if_needed()
        max_bytes = int(cfg.get("sidecar_max_bytes", 120_000) or 120_000)

        if isinstance(trace, DecisionTrace):
            d = trace.to_dict(max_events=int(cfg.get("max_events", 400)))
        else:
            d = trace if isinstance(trace, dict) else {}

        meta: Dict[str, Any] = {
            "schema": "decision_trace_sidecar:v1",
            "trace_id": str(d.get("trace_id") or ""),
            "trace_summary": make_trace_summary(d),
            "decision_trace": d,
            "updated_ms": _now_ms(),
        }

        try:
            raw = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
            if len(raw.encode("utf-8", "ignore")) > max_bytes:
                dt = meta.get("decision_trace")
                if isinstance(dt, dict) and isinstance(dt.get("events"), list):
                    evs = dt["events"]
                    dt["events"] = evs[-max(50, int(len(evs) * 0.5)) :]
                    meta["decision_trace"] = dt
                    meta["trace_summary"] = make_trace_summary(dt)
        except Exception:
            pass

        return meta
    except Exception:
        return {}

def trace_gate(
    ctx: Any,
    *,
    stage: str,
    name: str,
    passed: bool,
    veto: bool = False,
    reason_code: str = "",
    metrics: Optional[Dict[str, Any]] = None,
    duration_ms: Optional[float] = None,
) -> None:
    if not trace_enabled(): return
    try:
        ensure_trace(ctx).add(
            where=stage,
            name=name,
            ok=passed,
            veto=veto,
            reason_code=reason_code,
            metrics=metrics,
            duration_ms=duration_ms,
            etype="gate",
        )
    except Exception:
        pass

def trace_target(
    ctx: Any,
    *,
    stage: str,
    target: str,
    ok: bool,
    reason_code: str = "",
    metrics: Optional[Dict[str, Any]] = None,
    duration_ms: Optional[float] = None,
) -> None:
    if not trace_enabled(): return
    try:
        ensure_trace(ctx).add(
            where=stage,
            name=str(target),
            ok=ok,
            veto=False,
            reason_code=reason_code,
            metrics=metrics,
            duration_ms=duration_ms,
            etype="target",
            extra={"target": str(target)},
        )
    except Exception:
        pass

def patch_trace_sidecar_best_effort(redis: Any, *, key: str, patch_events: List[Dict[str, Any]]) -> None:
    if not patch_events: return
    try:
        raw = redis.get(key)
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        obj: Dict[str, Any] = {}
        if isinstance(raw, str) and raw:
            try:
                j = json.loads(raw)
                if isinstance(j, dict): obj = j
            except Exception: pass
        elif isinstance(raw, dict):
            obj = raw
        
        merged = patch_trace_sidecar_obj(obj, patch_events)
        ttl = redis.ttl(key)
        val = json.dumps(merged, ensure_ascii=False, separators=(",", ":"))
        if ttl and ttl > 0:
            redis.setex(key, ttl, val)
        else:
            redis.set(key, val)
    except Exception:
        pass

def patch_trace_sidecar_obj(sidecar: Dict[str, Any], patch_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    try:
        out = dict(sidecar or {})
        tr = out.get("trace") or out.get("decision_trace")
        if not isinstance(tr, dict):
            tr = {"v": 1, "events": []}
        
        evs = tr.get("events") or []
        if not isinstance(evs, list): evs = []

        dedup_on = _get_env_bool("DECISION_TRACE_TARGET_EVENT_DEDUP", True)
        seen: set[str] = set()
        if dedup_on:
            for e in evs:
                if isinstance(e, dict):
                    eid = _target_event_eid(e)
                    if eid: seen.add(eid)

        for e in patch_events or []:
            if not isinstance(e, dict): continue
            if str(e.get("type") or "") == "target" and str(e.get("stage") or "") == "dispatcher":
                se = _sanitize_target_patch_event(e)
                eid = _target_event_eid(se) if dedup_on else ""
                if eid and eid in seen: continue
                if eid: seen.add(eid)
                evs.append(se)
            else:
                evs.append(e)

        mx = int(_cfg_refresh_if_needed().get("max_events", 400))
        if len(evs) > mx:
            evs = evs[-mx:]

        tr["events"] = evs
        out["trace"] = tr
        out["decision_trace"] = tr
        out["trace_summary"] = make_trace_summary(tr)
        out["updated_ms"] = _now_ms()
        if "schema" not in out:
            out["schema"] = "decision_trace_sidecar:v1"
        return out
    except Exception:
        return dict(sidecar or {})

def _target_event_eid(ev: Dict[str, Any]) -> str:
    try:
        if ev.get("stage") != "dispatcher" or ev.get("type") != "target":
            return ""
        tgt = str(ev.get("target") or ev.get("name") or "")
        if not tgt: return ""
        ok = "1" if bool(ev.get("ok", False)) else "0"
        att = int(ev.get("attempt") or 0)
        if att <= 0: return ""
        return f"t:dispatcher:{tgt}:{att}:{ok}"
    except Exception:
        return ""

def _sanitize_target_patch_event(ev: Dict[str, Any]) -> Dict[str, Any]:
    try:
        out: Dict[str, Any] = {"type": "target", "stage": "dispatcher"}
        out["target"] = str(ev.get("target") or ev.get("name") or "")
        out["ok"] = bool(ev.get("ok", False))
        if "reason_code" in ev: out["reason_code"] = _trim_str(ev.get("reason_code"), 64)
        if "err" in ev: out["err"] = _trim_str(ev.get("err"), 512)
        out["attempt"] = int(ev.get("attempt") or 0)
        out["duration_ms"] = float(ev.get("duration_ms") or 0.0)
        if "t_ms" in ev: out["t_ms"] = int(ev.get("t_ms") or 0)
        return out
    except Exception:
        return {}

# --- Compatibility Aliases ---
def build_trace_summary(tr: Union[DecisionTrace, Dict[str, Any]]) -> str:
    return make_trace_summary(tr)

def emit_trace_event(ctx: Any, **kwargs) -> None:
    tr = ensure_trace(ctx)
    where = kwargs.pop("where", kwargs.pop("stage", ""))
    kwargs["ok"] = kwargs.pop("ok", True)
    tr.add(where=where, **kwargs)

trace_event = emit_trace_event
merge_trace_events = patch_trace_sidecar_obj
