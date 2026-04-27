from __future__ import annotations
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

import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

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
        return bool(default)


def _get_env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


def _get_env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


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
    """
    Global switch for DecisionTrace. Must be cheap.
    Fail-open: enabled unless explicitly disabled.
    """
    try:
        return bool(_cfg_refresh_if_needed().get("enabled", True))
    except Exception:
        return True


def should_sample(trace_id: str, rate01: float) -> bool:
    """
    Детерминированное семплирование:
      одинаковый trace_id -> одинаковое решение.
    """
    try:
        r = float(rate01)
        if r <= 0:
            return False
        if r >= 1:
            return True
        s = (trace_id or "").encode("utf-8", "ignore")
        h = hashlib.md5(s).hexdigest()  # noqa: S324 (не крипто, только sampling)
        v = int(h[:8], 16) / float(0xFFFFFFFF)
        return v < r
    except Exception:
        return False


class Span:
    """
    Ultra-cheap timing helper.
      sp = Span()
      ...
      dur_ms = sp.ms()
    """

    __slots__ = ("_t0",)

    def __init__(self) -> None:
        self._t0 = time.perf_counter()

    def ms(self) -> float:
        try:
            return max(0.0, (time.perf_counter() - self._t0) * 1000.0)
        except Exception:
            return 0.0


EventDict = Dict[str, Any]


def _trim_str(v: Any, n: int = 512) -> Any:
    try:
        if isinstance(v, str) and len(v) > n:
            return v[:n] + "..."
    except Exception:
        return v
    return v


def _cap_events(events: List[EventDict], max_events: int) -> List[EventDict]:
    if max_events <= 0:
        return []
    if len(events) <= max_events:
        return events
    return events[-max_events:]


@dataclass
class DecisionTrace:
    """
    "Железный" объект трассы, но сериализуется в dict (json-safe).
    """

    trace_id: str = ""
    created_ts_ms: int = 0
    sid: str = ""
    symbol: str = ""
    kind: str = ""
    tags: Dict[str, Any] = field(default_factory=dict)
    events: List[EventDict] = field(default_factory=list)

    @staticmethod
    def new(*, sid: str = "", trace_id: str = "") -> "DecisionTrace":
        tid = str(trace_id or "").strip() or uuid.uuid4().hex
        return DecisionTrace(
            trace_id=tid,
            created_ts_ms=_now_ms(),
            sid=str(sid or ""),
        )

    @staticmethod
    def from_env(env: Dict[str, Any]) -> "DecisionTrace":
        """
        Восстановление из envelope (retry/DLQ).
        Поддерживает env["trace"] (dict), env["trace_id"], env["trace_summary"].
        """
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
        """
        Универсальный event.
        etype: "gate" | "target" | ...
        """
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
                try:
                    ev["duration_ms"] = float(duration_ms)
                except Exception:
                    pass

            if isinstance(metrics, dict) and metrics:
                # минимальная защита от раздувания
                safe_m: Dict[str, Any] = {}
                i = 0
                for k, v in metrics.items():
                    if i >= 32:
                        break
                    safe_m[str(k)] = _trim_str(v, 256)
                    i += 1
                ev["metrics"] = safe_m

            if isinstance(extra, dict) and extra:
                for k, v in list(extra.items())[:32]:
                    ev[str(k)] = _trim_str(v, 256)

            self.events.append(ev)
            mx = int(_cfg_refresh_if_needed().get("max_events", 400) or 400)
            if len(self.events) > mx:
                self.events = self.events[-mx:]
        except Exception:
            return

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


def _count_for_summary(tr: Dict[str, Any]) -> Tuple[int, int, int, int, str]:
    g_ok = g_veto = t_ok = t_fail = 0
    last_veto = ""
    evs = tr.get("events")
    if not isinstance(evs, list):
        return 0, 0, 0, 0, ""
    for ev in reversed(evs):
        if isinstance(ev, dict) and ev.get("type") == "gate" and bool(ev.get("veto", False)):
            last_veto = f"{ev.get('name','')}:{ev.get('reason_code','')}"
            break
    for ev in evs:
        if not isinstance(ev, dict):
            continue
        if ev.get("type") == "gate":
            if bool(ev.get("veto", False)):
                g_veto += 1
            elif bool(ev.get("passed", False)):
                g_ok += 1
        elif ev.get("type") == "target":
            if bool(ev.get("ok", False)):
                t_ok += 1
            else:
                t_fail += 1
    return g_ok, g_veto, t_ok, t_fail, last_veto


def make_trace_summary(trace: Union[DecisionTrace, Dict[str, Any]]) -> str:
    """
    Однострочная summary для env/logs.
    Строго bounded по длине (DECISION_TRACE_SUMMARY_MAX_LEN).
    """
    try:
        cfg = _cfg_refresh_if_needed()
        mx = int(cfg.get("summary_max_len", 240) or 240)
        if isinstance(trace, DecisionTrace):
            d = trace.to_dict(max_events=64)
        else:
            d = trace if isinstance(trace, dict) else {}
        tid = str(d.get("trace_id") or "")
        sid = str(d.get("sid") or "")
        g_ok, g_veto, t_ok, t_fail, last_veto = _count_for_summary(d)
        s = f"tid={tid} sid={sid} g_ok={g_ok} g_veto={g_veto} t_ok={t_ok} t_fail={t_fail} last_veto={last_veto}"
        s = s.replace("\n", " ").replace("\r", " ").strip()
        if len(s) > mx:
            s = s[: mx - 3] + "..."
        return s
    except Exception:
        return ""


def ensure_trace(ctx: Any, *, sid: str = "", trace_id: str = "") -> DecisionTrace:
    """
    Гарантирует наличие DecisionTrace на ctx:
      ctx._decision_trace_obj: DecisionTrace
      ctx.trace_id / ctx.corr_id: str
    """
    try:
        tr = getattr(ctx, "_decision_trace_obj", None)
        if isinstance(tr, DecisionTrace):
            # обновим sid/tid если пришли новые
            if sid and not tr.sid:
                tr.sid = str(sid)
            if trace_id and not tr.trace_id:
                tr.trace_id = str(trace_id)
            return tr
    except Exception:
        pass

    # create new
    tid = str(trace_id or "").strip()
    if not tid:
        try:
            tid = str(getattr(ctx, "trace_id", "") or getattr(ctx, "corr_id", "") or "").strip()
        except Exception:
            tid = ""
    tr2 = DecisionTrace.new(sid=sid, trace_id=tid)
    try:
        setattr(ctx, "_decision_trace_obj", tr2)
    except Exception:
        pass
    try:
        setattr(ctx, "trace_id", tr2.trace_id)
        setattr(ctx, "corr_id", tr2.trace_id)
    except Exception:
        pass
    return tr2


def set_summary_fields(env: Dict[str, Any], tr: Optional[Union[DecisionTrace, Dict[str, Any]]]) -> None:
    """
    В env кладём только:
      - trace_id / corr_id
      - trace_summary
    Полный trace/events — только в sidecar.
    """
    if not isinstance(env, dict) or tr is None:
        return
    try:
        if isinstance(tr, DecisionTrace):
            tid = str(tr.trace_id or "")
        else:
            tid = str(tr.get("trace_id") or "") if isinstance(tr, dict) else ""
        if tid:
            env["trace_id"] = tid
            env["corr_id"] = tid  # back-compat alias
        env["trace_summary"] = make_trace_summary(tr)
    except Exception:
        return


def to_dict_bounded(
    trace: Union[DecisionTrace, Dict[str, Any], None],
    *,
    max_events: int = 64,
    max_bytes: int = 16_000,
) -> Dict[str, Any]:
    """
    Bounded trace dict for retry/DLQ context.
    Гарантия: JSON <= max_bytes (best-effort), events <= max_events.
    """
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
        # trim long strings
        for ev in d.get("events", []):
            if isinstance(ev, dict):
                for k, v in list(ev.items()):
                    if isinstance(v, str) and len(v) > 512:
                        ev[k] = v[:512] + "..."

    # size cap
    try:
        s = json.dumps(d, ensure_ascii=False, separators=(",", ":"))
        if len(s.encode("utf-8", "ignore")) > max_bytes:
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
    """
    Canonical sidecar meta (diagnostics-only):
      {
        "schema": "...",
        "trace_id": "...",
        "trace_summary": "...",
        "decision_trace": { ... full trace/events ... },
        "updated_ms": ...
      }
    """
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

        # hard size guard
        try:
            raw = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
            if len(raw.encode("utf-8", "ignore")) > max_bytes:
                # trim events aggressively
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


def env_trace_append(
    env: Dict[str, Any],
    *,
    trace_id: str,
    stage: str,
    name: str,
    passed: bool,
    veto: bool,
    reason_code: str = "",
    metrics: Optional[Dict[str, Any]] = None,
    duration_ms: Optional[float] = None,
) -> None:
    """
    Dispatcher-friendly helper: дописывает event в env["trace"] (bounded later).
    FAIL-OPEN.
    """
    if not isinstance(env, dict):
        return
    try:
        tr = env.get("trace")
        if not isinstance(tr, dict):
            tr = {"trace_id": str(trace_id or ""), "sid": str(env.get("sid") or ""), "events": []}
            env["trace"] = tr
        evs = tr.get("events")
        if not isinstance(evs, list):
            evs = []
            tr["events"] = evs
        ev: Dict[str, Any] = {
            "type": "gate",
            "stage": str(stage or ""),
            "name": str(name or ""),
            "passed": bool(passed) and (not bool(veto)),
            "veto": bool(veto),
            "reason_code": str(reason_code or ("OK" if passed else "VETO")),
            "t_ms": _now_ms(),
        }
        if duration_ms is not None:
            ev["duration_ms"] = float(duration_ms)
        if isinstance(metrics, dict) and metrics:
            ev["metrics"] = metrics
        evs.append(ev)
    except Exception:
        return


def append_env_trace_event(
    env: Dict[str, Any],
    *,
    stage: str,
    name: str,
    passed: bool,
    veto: bool,
    reason_code: str = "",
    metrics: Optional[Dict[str, Any]] = None,
) -> None:
    # alias for existing call sites
    try:
        tid = str(env.get("trace_id") or env.get("corr_id") or env.get("correlation_id") or env.get("sid") or "")
        env_trace_append(
            env,
            trace_id=tid,
            stage=stage,
            name=name,
            passed=passed,
            veto=veto,
            reason_code=reason_code,
            metrics=metrics,
        )
    except Exception:
        return


def trace_gate(
    ctx: Any,
    *,
    stage: str,
    name: str,
    passed: bool,
    veto: bool,
    reason_code: str = "",
    metrics: Optional[Dict[str, Any]] = None,
    duration_ms: Optional[float] = None,
) -> None:
    if not trace_enabled():
        return
    try:
        tr = ensure_trace(ctx)
        tr.add(
            where=stage,
            name=name,
            ok=bool(passed) and (not bool(veto)),
            veto=bool(veto),
            reason_code=reason_code,
            metrics=metrics,
            duration_ms=duration_ms,
            etype="gate",
        )
    except Exception:
        return


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
    if not trace_enabled():
        return
    try:
        tr = ensure_trace(ctx)
        tr.add(
            where=stage,
            name=str(target),
            ok=bool(ok),
            veto=False,
            reason_code=reason_code,
            metrics=metrics,
            duration_ms=duration_ms,
            etype="target",
            extra={"target": str(target)},
        )
    except Exception:
        return
