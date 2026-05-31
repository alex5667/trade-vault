import os
import time
import uuid
import math
import json
import logging
import hashlib
from dataclasses import dataclass
from dataclasses import field
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

T = TypeVar("T")

from common.decision_trace import Span, trace_gate, trace_enabled, ensure_trace
from common.payload_fingerprint import fingerprint_tradeable_payload
from common.metrics_stage import stage_counter, stage_ms_hist
from common.json_fast import dumps1
from common.json_safe import to_json_safe
from common.runtime_snapshot import RuntimeSnapshot, RuntimeRefresher
from common.ctx_cache import cached_on_ctx
from common.outbox_contract import contract_check_best_effort
from common.payload_policy import enforce_and_validate_payload
from common.json_contract import enforce_payload_budgets, maybe_assert_json_safe
from common.contracts.tradeable_contracts import assert_tradeable_dict, assert_outbox_sidecar_meta
from services.outbox.envelope_builder import (
    build_trace_sidecar_meta,
    build_entry_policy_diag_event,
    emit_entry_policy_diag_best_effort,
)
from handlers.base_orderflow_handler import ensure_levels
from signals.level_enricher import attach_trade_levels_to_ctx
from news_pipeline.enricher_sync import NewsEnricherSync

from common.json_safe import to_json_safe

# ------------------------------------------------------------
# JSON-safe helpers (3.2): payload must be json-safe by construction.
# Anything heavy/odd -> payload_meta (sidecar).
# ------------------------------------------------------------
_JSON_SCALARS = (str, int, float, bool, type(None))

logger = logging.getLogger(__name__)

import json
import hashlib
from common.json_safe import to_json_safe

def _cfg_hash(cfg: Dict[str, Any]) -> str:
    """
    Быстрый детерминированный хэш конфига.
    Важно: cfg уже должен быть json-safe (dict из скаляров/листов/…).
    """
    try:
        s = json.dumps(cfg or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.blake2s(s.encode("utf-8", "ignore"), digest_size=16).hexdigest()
    except Exception:
        return "cfg:err"

def ensure_trade_levels_once(
    *, ctx: Any, symbol: str, side: str, kind: str, cfg: Optional[Dict[str, Any]],
    regime: Any = None, empirical: Any = None, logger: Any = None,
) -> None:
    """
    Железно: attach_trade_levels_to_ctx(...) вызывается один раз на key.
    Key хранится на ctx (общий для нескольких кандидатов).
    """
    if ctx is None:
        return

    # 1) дешевые инварианты всегда первыми
    try:
        ensure_levels(ctx, side=side)
    except Exception:
        logger.debug("ensure_levels_once failed (fail-open)", exc_info=True)

    try:
        cfgd = dict(cfg or {})
    except Exception:
        cfgd = {}

    key = ("trade_levels", str(symbol), str(side), str(kind), _cfg_hash(cfgd))
    try:
        prev = getattr(ctx, "_trade_levels_key", None)
        if prev == key:
            return
    except Exception:
        pass  # expected if attribute missing

    try:
        attach_trade_levels_to_ctx(
            ctx,
            side=str(side),
            symbol=str(symbol),
            cfg=cfgd,
            kind=str(kind or ""),
            regime=regime,
            empirical=empirical,
            overwrite=False,
            logger=logger,
        )
    except Exception:
        # fail-open: не ломаем пайплайн
        return

    try:
        setattr(ctx, "_trade_levels_key", key)
    except Exception:
        logger.debug("failed to set _trade_levels_key", exc_info=True)


# -------------------------
# Small local helpers
# -------------------------
def _replay_stable_signal_id_enabled() -> bool:
    """
    Replay mode: force deterministic signal_id generation when upstream doesn't provide one.

    Motivation: Golden/Replay tests must be able to assert *bit-identical* envelopes
    for the same input. UUID-based IDs make that impossible.

    Enabled via ENV: REPLAY_STABLE_SIGNAL_ID=1
    """
    v = str(os.getenv('REPLAY_STABLE_SIGNAL_ID', '0') or '0').strip().lower()
    return v in {'1','true','yes','on'}


def _safe_str(v: Any) -> str:
    try:
        return "" if v is None else str(v)
    except Exception:
        return ""


def _safe_lower(v: Any) -> str:
    try:
        return _safe_str(v).strip().lower()
    except Exception:
        return ""


def _safe_reason_u16(v: Any) -> int:
    """
    Fail-open mapping for reason_code/decision_code -> uint16.
    Deterministic: CRC32(lowercase) & 0xFFFF.
    """
    try:
        s = _safe_lower(v)
        if not s:
            return 0
        import zlib
        return int(zlib.crc32(s.encode("utf-8")) & 0xFFFF)
    except Exception:
        return 0


def _sanitize_u16_list(v: Any, *, max_len: int = 64) -> list[int]:
    out: list[int] = []
    try:
        if v is None:
            return out
        if not isinstance(v, (list, tuple, set)):
            v = [v]
        lim = int(max_len or 0) or 64
        for x in v:
            if len(out) >= lim:
                break
            try:
                out.append(int(x) & 0xFFFF)
            except Exception:
                out.append(_safe_reason_u16(x))
    except Exception:
        return out
    return out


def _replay_stable_signal_id_enabled() -> bool:
    """
    Replay mode: force deterministic signal_id generation when upstream doesn't provide one.

    Motivation: Golden/Replay tests must be able to assert *bit-identical* envelopes
    for the same input. UUID-based IDs make that impossible.

    Enabled via ENV: REPLAY_STABLE_SIGNAL_ID=1
    """
    v = str(os.getenv('REPLAY_STABLE_SIGNAL_ID', '0') or '0').strip().lower()
    return v in {'1','true','yes','on'}


def _parse_side_int(side_val: Any) -> int:
    """
    Normalize side to +1/-1.
    Accepts:
      - ints/floats: sign
      - strings: buy/long/1 -> +1, sell/short/-1 -> -1
      - enum-like: falls back to string parsing
    """
    try:
        if isinstance(side_val, bool):
            return 0
        if isinstance(side_val, (int, float)):
            f = float(side_val)
            return 1 if f > 0 else (-1 if f < 0 else 0)
    except Exception:
        pass
    s = _safe_lower(side_val)
    if s in {"buy", "long", "1", "+1"}:
        return 1
    if s in {"sell", "short", "-1"}:
        return -1
    return 0


def _stable_signal_id(*, symbol: str, kind_key: str, side_int: int, entry_ts_ms: int, entry_price: float) -> str:
    """
    Deterministic signal_id for golden/replay tests.
    Enabled only when REPLAY_STABLE_SIGNAL_ID=1.
    """
    try:
        raw = f"{symbol}|{kind_key}|{int(side_int)}|{int(entry_ts_ms)}|{float(entry_price):.8f}"
        import hashlib
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]
    except Exception:
        return "0" * 24


def _clamp01(x: float) -> float:
    try:
        if x < 0.0:
            return 0.0
        if x > 1.0:
            return 1.0
        return float(x)
    except Exception:
        return 0.0


def _finite_or(x: Any, default: float) -> float:
    try:
        f = float(x)
        if f == f and f not in (float("inf"), float("-inf")):
            return f
        return float(default)
    except Exception:
        return float(default)


def _cfg_hash(cfg: Dict[str, Any]) -> str:
    """
    Stable cfg hash for cache keys.
    IMPORTANT: do NOT use dumps1() here because it doesn't sort keys.
    """
    try:
        safe = to_json_safe(cfg or {})  # гарантирует json-совместимые типы
        s = json.dumps(
            safe,
            ensure_ascii=False,
            sort_keys=True,              # критично для детерминизма
            separators=(",", ":"),
        )
        return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()
    except Exception:
        return "cfg:err"


@dataclass(frozen=True)
class CandidateFrame:
    """
    Immutable per-candidate frame with pre-extracted fields.
    Keeps handler+ctx+candidate together and avoids repeated getattr calls.
    """
    handler: Any
    ctx: Any
    cand: Any
    kind_str: str
    kind_key: str
    side_int: int
    ctx_symbol: str
    ctx_ts: Any
    ctx_price: Any
    # ------------------------------------------------------------
    # 3.3: per-candidate memoization ("ещё выше"):
    # - frozen dataclass still allows mutating the internal dict object
    # - used to guarantee "compute once per candidate" semantics
    # Keys examples:
    #   "risk_cfg", "levels_attached", ("consistency", kind, side)
    # ------------------------------------------------------------
    memo: Dict[Any, Any] = field(default_factory=dict, compare=False, repr=False)

    def memo_get(self, key: Any, compute: Callable[[], T]) -> T:
        """
        Железный compute-once для кандидата.
        frozen dataclass допускает мутацию внутреннего dict (memo).
        Fail-open: если memo сломан — просто compute().
        """
        try:
            if key in self.memo:
                return self.memo[key]
        except Exception:
            return compute()
        val = compute()
        try:
            self.memo[key] = val
        except Exception:
            pass  # memo update failed check
        return val


def _side_payload_from_frame(f: CandidateFrame) -> str:
    """Return LONG/SHORT string (best-effort) for gates that require side as str."""
    try:
        s = str(getattr(f.ctx, "side", "") or "").strip().upper()
        if s in ("LONG", "SHORT"):
            return s
    except Exception:
        pass
    try:
        si = int(getattr(f.ctx, "side_int", 0) or f.side_int or 0)
        if si == 1:
            return "LONG"
        if si == -1:
            return "SHORT"
    except Exception:
        pass
    return ""


class CandidateExtractor:
    """Stage 1: obtain candidates and create CandidateFrame objects."""

    def extract(self, handler: Any, ctx: Any) -> List[CandidateFrame]:
        det = getattr(handler, "_detect_candidates", None)
        if not callable(det):
            return []
        cands = det(ctx) or []
        out: List[CandidateFrame] = []
        ctx_symbol = getattr(ctx, "symbol", None)
        ctx_ts = getattr(ctx, "ts", None)
        ctx_price = getattr(ctx, "price", None)
        for cand in list(cands):
            kind_str = getattr(handler, "_safe_str", _safe_str)(getattr(cand, "kind", None))
            kind_key = getattr(handler, "_safe_lower", _safe_lower)(getattr(cand, "kind", None))
            side_val = getattr(cand, "side", 0)
            side_int = _parse_side_int(side_val)
            out.append(
                CandidateFrame(
                    handler=handler,
                    ctx=ctx,
                    cand=cand,
                    kind_str=kind_str,
                    kind_key=kind_key,
                    side_int=side_int,
                    ctx_symbol=_safe_str(ctx_symbol or getattr(handler, "symbol", "") or ""),
                    ctx_ts=ctx_ts,
                    ctx_price=ctx_price,
                )
            )
        return out


class ContextEnricher:
    """
    Stage 2: enforce invariants needed by downstream gates:
      - ensure_levels(ctx, side=side_int) (FAIL-OPEN, DQ flags inside)
    """

    def ensure_invariants(self, f: CandidateFrame) -> None:
        # FAIL-OPEN: ensure_levels must never raise, but we still guard just in case.
        try:
            ensure_levels(f.ctx, side=f.side_int)
        except Exception:
            # If ensure_levels ever raises (shouldn't), we keep fail-open behavior.
            try:
                append_flag = getattr(f.handler, "_append_flag", None)
                if callable(append_flag):
                    append_flag(f.ctx, "ensure_levels_failed")
            except Exception:
                pass  # nested error in fallback

    # ------------------------------------------------------------
    # 3.3: attach levels ровно 1 раз на кандидата.
    # Реальная функция: handlers/base_orderflow_handler.ensure_levels(ctx, side=?)
    # ensure_levels FAIL-OPEN и НЕ возвращает veto/rc.
    # ------------------------------------------------------------
    def ensure_levels_once(self, f: CandidateFrame) -> Tuple[bool, str]:
        if f.memo.get("levels_done") is True:
            return True, ""
        ok = True
        rc = ""
        with Span() as sp:
            try:
                # side можно дать явно: f.side_int или ctx.side_int/ctx.side
                side_any = None
                try:
                    side_any = getattr(f.ctx, "side_int", None)
                except Exception:
                    side_any = None
                if side_any is None:
                    side_any = f.side_int
                ensure_levels(f.ctx, side=side_any)
            except Exception:
                # fail-open
                ok = False
                rc = "VETO_LEVELS_ERROR"
        f.memo["levels_done"] = True
        try:
            if trace_enabled():
                trace_gate(
                    f.ctx,
                    name="ensure_levels",
                    passed=bool(ok),
                    veto=not bool(ok),
                    reason_code=str(rc or ("OK" if ok else "VETO")),
                    duration_ms=float(sp.ms()),
                )
        except Exception:
            logger.debug("trace_gate failed", exc_info=True)
        return bool(ok), str(rc or "")


class GateRunner:
    """
    Stage 3: gates (each gate is fail-open by design).

    Currently wired:
      - Regime gate: handler._apply_regime_gate(...)
      - Cost/Edge gate: handler._legacy_gate_cost_edge(frame=..., pre=...)
      - Confirmations validate: handler._confirmations.validate(...)
    """

    def _memo_get(self, f: CandidateFrame, key: Any, default: Any = None) -> Any:
        try:
            return f.memo.get(key, default)
        except Exception:
            return default

    def _memo_set(self, f: CandidateFrame, key: Any, val: Any) -> None:
        try:
            f.memo[key] = val
        except Exception:
            pass  # memo set failed

    def _ensure_levels_once(self, f: CandidateFrame) -> None:
        if self._memo_get(f, "ensure_levels_done"):
            return
        try:
            ensure_levels(f.ctx, side=f.side_int)
        except Exception:
            logger.debug("_ensure_levels_once failed", exc_info=True)
        self._memo_set(f, "ensure_levels_done", True)

    def _resolve_risk_cfg_once(self, f: CandidateFrame) -> Dict[str, Any]:
        cached = self._memo_get(f, "risk_cfg")
        if isinstance(cached, dict):
            return cached

        cfg: Dict[str, Any] = {}
        # Варианты источника cfg (в порядке предпочтения):
        # 1) handler.resolve_risk_cfg(symbol, ctx, cand)
        # 2) handler._resolve_risk_cfg_for_levels()
        # 3) handler._resolve_risk_cfg_cached(symbol)
        try:
            fn = getattr(f.handler, "resolve_risk_cfg", None)
            if callable(fn):
                out = fn(symbol=str(f.ctx_symbol), ctx=f.ctx, cand=f.cand)
                cfg = dict(out or {}) if isinstance(out, dict) else {}
            else:
                fn2 = getattr(f.handler, "_resolve_risk_cfg_for_levels", None)
                if callable(fn2):
                    out = fn2()
                    cfg = dict(out or {}) if isinstance(out, dict) else {}
                else:
                    fn3 = getattr(f.handler, "_resolve_risk_cfg_cached", None)
                    if callable(fn3):
                        out = fn3(str(f.ctx_symbol))
                        cfg = dict(out or {}) if isinstance(out, dict) else {}
        except Exception:
            cfg = {}

        # Нормализуем и фиксируем hash один раз на кандидата
        cfg = to_json_safe(cfg)
        self._memo_set(f, "risk_cfg", cfg)
        self._memo_set(f, "risk_cfg_hash", _cfg_hash(cfg))

        return cfg

    def _ensure_trade_levels_once(self, f: CandidateFrame, *, cfg: Dict[str, Any]) -> None:
        """
        Тяжёлая часть: attach_trade_levels_to_ctx(...)
        Делаем 1 раз на кандидата и фиксируем key на ctx, чтобы другие ветки не пересчитали.
        """
        if self._memo_get(f, "trade_levels_done"):
            return

        self._ensure_levels_once(f)

        symbol = str(f.ctx_symbol or "")
        side = str(f.side_int)
        kind = str(f.kind_key or f.kind_str or "")

        cfg_h = self._memo_get(f, "risk_cfg_hash") or _cfg_hash(cfg)
        key = (symbol, side, kind, str(cfg_h))

        try:
            prev = getattr(f.ctx, "_trade_levels_key", None)
            if prev == key:
                self._memo_set(f, "trade_levels_done", True)
                return
        except Exception:
            pass  # failed to check prev key

        # Если у handler уже есть canonical wrapper — используем его
        fn = getattr(f.handler, "_ensure_trade_levels_once", None)
        if callable(fn):
            try:
                fn(ctx=f.ctx, side=f.side_int, symbol=symbol, kind=kind, cfg=dict(cfg), overwrite=False)
            except Exception:
                logger.debug("handler._ensure_trade_levels_once failed", exc_info=True)
        else:
            try:
                attach_trade_levels_to_ctx(
                    f.ctx,
                    side=side,
                    symbol=symbol,
                    cfg=dict(cfg),
                    kind=kind,
                    overwrite=False,
                )
            except Exception:
                logger.debug("attach_trade_levels_to_ctx failed", exc_info=True)

        try:
            setattr(f.ctx, "_trade_levels_key", key)
        except Exception:
            pass  # failed to set key attribute

        self._memo_set(f, "trade_levels_done", True)

    def regime(self, f: CandidateFrame) -> Tuple[bool, str]:
        fn = getattr(f.handler, "_apply_regime_gate", None)
        if not callable(fn):
            return True, ""
        try:
            allowed, reason = fn(signal_kind=f.kind_key or f.kind_str, ctx=f.ctx)
            if allowed:
                return True, ""
            rc = _safe_str(reason or "VETO_REGIME")
            emit_veto = getattr(f.handler, "_emit_veto_metric", None)
            if callable(emit_veto):
                emit_veto(kind=f.kind_key or f.kind_str, ctx=f.ctx, reason_code=rc)
            return False, rc
        except Exception:
            return True, ""

    def edge_cost(self, f: CandidateFrame) -> Tuple[bool, str]:
        """
        Реальный gate: handler._legacy_gate_cost_edge(frame=f, pre={...}) -> (ok, rc)
        Требует инвариантов ctx: entry/tp1/sl/price/side.
        """
        self._ensure_levels_once(f)
        cfg = self._resolve_risk_cfg_once(f)
        self._ensure_trade_levels_once(f, cfg=cfg)

        fn = getattr(f.handler, "_legacy_gate_cost_edge", None)
        if not callable(fn):
            return True, ""

        pre = {"ctx_symbol": str(f.ctx_symbol or "")}
        try:
            ok, rc = fn(frame=f, pre=pre)
            return bool(ok), str(rc or "")
        except Exception:
            return True, ""  # fail-open

    def confirmations(self, f: CandidateFrame) -> Any:
        """
        Returns validation result object.
        If veto: caller decides what to do (emit veto metric etc).
        """
        conf = getattr(f.handler, "_confirmations", None)
        if conf is None or not callable(getattr(conf, "validate", None)):
            return None
        try:
            lp = getattr(f.cand, "level_price", None)
            try:
                level_price = float(lp) if lp is not None else None
            except Exception:
                level_price = None
            return conf.validate(
                kind=f.kind_key or f.kind_str,
                ctx=f.ctx,
                l2=getattr(f.handler, "_last_l2_snapshot", None),
                l3=getattr(f.ctx, "l3", None),
                level_price=level_price,
            )
        except Exception:
            return None

    # ------------------------------------------------------------
    # 3.3: consistency gate ровно 1 раз на кандидата.
    # Реальный класс: SignalConsistencyGate.evaluate(ctx, symbol, kind, side) -> QualityGateDecision
    # Decision fields: apply(bool), veto(bool), reason_code(str)
    # ------------------------------------------------------------
    def consistency_once(self, f: CandidateFrame) -> Tuple[bool, str]:
        def _compute() -> Tuple[bool, str]:
            ok = True
            rc = ""
            apply = False
            veto = False

            # Каноническое поле (CryptoOrderflowInitMixin):
            #   self._consistency_gate: SignalConsistencyGate = SignalConsistencyGate.from_env()
            gate = None
            try:
                gate = getattr(f.handler, "_consistency_gate", None)
            except Exception:
                gate = None

            if gate is None or not callable(getattr(gate, "evaluate", None)):
                return True, ""

            kind = str(f.kind_key or f.kind_str or "")
            side = _side_payload_from_frame(f)
            symbol = str(f.ctx_symbol or "")

            with Span() as sp:
                try:
                    decision = gate.evaluate(ctx=f.ctx, symbol=symbol, kind=kind, side=side)
                except Exception:
                    # fail-open, но наблюдаемо через trace
                    decision = None

            try:
                apply = bool(getattr(decision, "apply", False)) if decision is not None else False
            except Exception:
                apply = False
            try:
                veto = bool(getattr(decision, "veto", False)) if decision is not None else False
            except Exception:
                veto = False
            try:
                rc = str(getattr(decision, "reason_code", "") or "")
            except Exception:
                rc = ""

            # apply=False => gate not applicable => PASS
            if apply and veto:
                ok = False
                if not rc:
                    rc = "VETO_CONSISTENCY"
            else:
                ok = True
                rc = rc or ""

            try:
                if trace_enabled():
                    trace_gate(
                        f.ctx,
                        name="consistency_gate",
                        passed=bool(ok),
                        veto=bool(apply and veto),
                        reason_code=str(rc or ("OK" if ok else "VETO")),
                        duration_ms=float(sp.ms()),
                    )
            except Exception:
                logger.debug("trace_gate consistency failed", exc_info=True)

            return bool(ok), str(rc or "")

        return f.memo_get(("consistency", str(f.kind_key or f.kind_str), _side_payload_from_frame(f)), _compute)

    # ------------------------------------------------------------
    # 3.3: edge/cost gate ровно 1 раз на кандидата.
    # Реальная функция: handler._legacy_gate_cost_edge(frame=f, pre={...}) -> (ok, rc)
    # ------------------------------------------------------------
    def edge_cost_once(self, f: CandidateFrame) -> Tuple[bool, str]:
        def _compute() -> Tuple[bool, str]:
            sp = Span()

            # ------------------------------------------------------------
            # HARD CONTRACT:
            #   EV/Cost gate must run only AFTER levels are attached.
            #   This makes ordering explicit and prevents silent drift.
            # ------------------------------------------------------------
            try:
                levels_ok = bool(f.memo.get("levels_ensured") or f.memo.get("ensure_levels_done"))
                trade_ok = bool(f.memo.get("trade_levels_attached") or f.memo.get("trade_levels_done"))
                if not (levels_ok and trade_ok):
                    ok, rc = False, "VETO_EDGE_COST_PRECONDITION"
                    trace_gate(
                        f.ctx,
                        name="edge_cost",
                        passed=False,
                        veto=True,
                        reason_code=rc,
                        duration_ms=float(sp.ms),
                    )
                    return ok, rc
            except Exception:
                logger.debug("pre-check memo access failed", exc_info=True)

            # ------------------------------------------------------------
            # FAIL-OPEN CONTRACT (predictable):
            #   missing tp1/sl => explicit veto reason
            # ------------------------------------------------------------
            try:
                tp1 = getattr(f.ctx, "tp1_price", None) or getattr(f.ctx, "tp1", None)
                if tp1 is None:
                    tpl = getattr(f.ctx, "tp_levels", None)
                    if isinstance(tpl, (list, tuple)) and tpl:
                        tp1 = tpl[0]
                sl = getattr(f.ctx, "sl_price", None) or getattr(f.ctx, "sl", None)
                if tp1 is None or sl is None:
                    ok, rc = False, "VETO_EDGE_COST_MISSING_LEVELS"
                    trace_gate(
                        f.ctx,
                        name="edge_cost",
                        passed=False,
                        veto=True,
                        reason_code=rc,
                        duration_ms=float(sp.ms),
                    )
                    return ok, rc
            except Exception:
                pass

            ok, rc = True, ""
            try:
                ok, rc = self.edge_cost(f)  # edge_cost уже должен опираться на инварианты
            except Exception:
                ok, rc = False, "VETO_EDGE_COST_ERROR"
                try:
                    ok, rc = self.edge_cost(f)  # edge_cost уже должен опираться на инварианты
                except Exception:
                    ok, rc = False, "VETO_EDGE_COST_ERROR"
                try:
                    if trace_enabled():
                        trace_gate(
                            f.ctx,
                            name="edge_cost_gate",
                            passed=bool(ok),
                            veto=not bool(ok),
                            reason_code=str(rc or ("OK" if ok else "VETO")),
                            duration_ms=float(sp.ms()),
                        )
                except Exception:
                    logger.debug("trace_gate edge_cost failed", exc_info=True)
            return bool(ok), str(rc or "")

        return f.memo_get(("edge_cost_once", str(f.kind_key or f.kind_str)), _compute)


class ScoringRunner:
    """Stage 4: raw_score + conf_factor -> final_score, then confidence_pct."""

    def compute(self, f: CandidateFrame, res: Any) -> Tuple[float, float, float, float, Dict[str, Any]]:
        sp = Span()
        raw_score = _finite_or(getattr(f.cand, "raw_score", None), 0.0)
        conf_factor01 = _clamp01(_finite_or(getattr(res, "conf_factor01", None), 1.0))
        final_score = raw_score * conf_factor01
        # handler._confidence_pct(kind, ctx, final_score) -> 0..100
        conf_fn = getattr(f.handler, "_confidence_pct", None)
        if callable(conf_fn):
            try:
                confidence_pct = float(conf_fn(kind=getattr(f.cand, "kind", ""), ctx=f.ctx, final_score=final_score))
            except Exception:
                confidence_pct = 0.0
        else:
            confidence_pct = 0.0
        parts = dict(getattr(res, "parts", None) or {})
        parts.setdefault("conf_factor", conf_factor01)
        parts.setdefault("raw_score", raw_score)
        parts.setdefault("final_score", final_score)

        # 4.1 METRICS
        try:
            k = str(getattr(f.cand, "kind", "") or "")
            stage_counter(f.handler, "candidates_total", kind=k)
            stage_ms_hist(f.handler, stage="scoring", ms=sp.ms())
            dist(f.handler, name="conf_factor", value=float(conf_factor01), kind=k)
        except Exception:
            pass
        return raw_score, conf_factor01, final_score, confidence_pct, parts


class PayloadBuilder:
    """
    Stage 5: build strictly JSON-safe payload (minimal stable contract).
    We intentionally keep this payload small; the legacy body can still add extra fields.
    """

    def build(
        self,
        f: CandidateFrame,
        *,
        raw_score: float,
        conf_factor01: float,
        final_score: float,
        confidence_pct: float,
        parts: Dict[str, Any],
        res: Any,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        JSON-SAFE BY CONSTRUCTION:
          - payload: только str/int/float/bool/None/list/dict
          - payload_meta: heavy/debug структуры (parts_full и т.п.) -> sidecar через OutboxWriter
        """
        sp = Span()

        # Side contract:
        # ensure_levels(...) должен проставить ctx.side="LONG"/"SHORT" при валидном side_int.
        side_payload = str(getattr(f.ctx, "side", "") or "").strip().upper()
        if not side_payload:
            side_payload = str(getattr(f.cand, "side", "") or "").strip().upper()

        reasons = list(getattr(f.cand, "reasons", None) or [])
        reasons = [str(x) for x in reasons][:16]

        # ------------------------------------------------------------------
        # Signal id (SID)
        #
        # Replay determinism: for record/replay or golden tests you may want
        # the same input to produce the same SID.
        #
        # - If candidate already has signal_id -> use it (canonical).
        # - Else if REPLAY_STABLE_SIGNAL_ID=1 -> derive a stable sid from the
        #   tradeable payload fields (excluding sid itself).
        # - Else -> random uuid.
        # ------------------------------------------------------------------
        sid = str(getattr(f.cand, "signal_id", "") or "").strip()

        payload_base: Dict[str, Any] = {
            "kind": str(getattr(f.cand, "kind", "") or ""),
            "side": side_payload,
            "symbol": str(f.ctx_symbol or ""),
            "ts": int(float(getattr(f, "ctx_ts", 0) or 0)),
            "price": float(getattr(f, "ctx_price", 0.0) or 0.0),
            "raw_score": float(raw_score),
            "final_score": float(final_score),
            "confidence": float(confidence_pct),
            "conf_factor": float(conf_factor01),
            "reasons": reasons,
        }

        if not sid:
            if _replay_stable_signal_id_enabled():
                try:
                    sha1, _ = fingerprint_tradeable_payload(payload_base)
                    sid = f"s_{sha1[:24]}"
                except Exception:
                    sid = f"s_{uuid.uuid4().hex}"
            else:
                sid = f"s_{uuid.uuid4().hex}"

        payload: Dict[str, Any] = {
            "sid": sid,              # back-compat
            "signal_id": sid,        # canonical
            **payload_base,
        }

        # decision axis (optional)
        try:
            dc = str(getattr(res, "decision_code", "") or "").strip()
            du16 = int(getattr(res, "decision_u16", 0) or 0)
            if dc:
                payload["decision_code"] = dc
            if du16 > 0:
                payload["decision_u16"] = du16
                payload["rc"] = du16
        except Exception:
            pass

        # tf back-compat
        try:
            tfv = getattr(f.ctx, "tf", None) or getattr(f.ctx, "timeframe", None)
            if tfv:
                payload["tf"] = str(tfv)
        except Exception:
            pass

        # ------------------------------------------------------------
        # Split parts -> payload.parts (small) + payload_meta.parts_full (heavy)
        # ------------------------------------------------------------
        def _is_small_json_value(v: Any) -> bool:
            if isinstance(v, _JSON_SCALARS):
                return True
            if isinstance(v, list):
                return len(v) <= 32 and all(isinstance(x, _JSON_SCALARS) for x in v)
            if isinstance(v, dict):
                if len(v) > 32:
                    return False
                return all(isinstance(x, _JSON_SCALARS) for x in v.values())
            return False

        parts_in = parts if isinstance(parts, dict) else {}
        parts_safe = to_json_safe(parts_in)  # гарантирует json-safe структуру

        parts_small: Dict[str, Any] = {}
        parts_full: Dict[str, Any] = {}
        if isinstance(parts_safe, dict):
            for k, v in parts_safe.items():
                if _is_small_json_value(v):
                    parts_small[str(k)] = v
                else:
                    parts_full[str(k)] = v

        payload["parts"] = parts_small if parts_small else {}

        payload_meta: Dict[str, Any] = {}
        if parts_full:
            payload_meta["parts_full"] = parts_full

        # финальная страховка: никакой NaN/Inf/объектов
        payload = to_json_safe(payload)
        payload_meta = to_json_safe(payload_meta)

        # ------------------------------------------------------------------
        # NEXT LAYER (железно):
        #  - enforce size budget (PAYLOAD_MAX_BYTES, PAYLOAD_MAX_STRLEN, ...)
        #  - validate minimal schema (required keys + strict side LONG/SHORT)
        #  - fail-open by default (warn), fail-close only in CI (raise)
        # ------------------------------------------------------------------
        try:
            payload, payload_meta = enforce_and_validate_payload(
                payload=payload,
                payload_meta=payload_meta,
                logger=getattr(f.handler, "logger", None),
                where="PayloadBuilder.build",
            )
        except Exception:
            # fail-open: do not block publishing
            logger.debug("enforce_and_validate_payload failed", exc_info=True)

        # ------------------------------------------------------------------
        # ------------------------------------------------------------------
        # "Железный" слой: size budgets + optional hard assertions.
        #  - payload должен быть компактным и trade-safe
        #  - meta sidecar тоже не должен раздуваться бесконечно
        # ------------------------------------------------------------------
        try:
            payload_max = int(os.getenv("OUTBOX_PAYLOAD_MAX_BYTES", "8192") or "8192")
        except Exception:
            payload_max = 8192
        try:
            meta_max = int(os.getenv("OUTBOX_META_MAX_BYTES", "32768") or "32768")
        except Exception:
            meta_max = 32768

        payload, payload_meta = enforce_payload_budgets(
            payload,
            payload_meta,
            payload_max_bytes=payload_max,
            meta_max_bytes=meta_max,
        )
        # optional: assert strict json contract in runtime when debugging incidents
        maybe_assert_json_safe(payload, payload_meta)

        # ------------------------------------------------------------------
        # ЖЁСТКИЙ КОНТРАКТ: tradeable payload/env = JSON-safe + без forbidden keys + с бюджетами
        # ------------------------------------------------------------------
        strict = os.getenv("STRICT_TRADEABLE_CONTRACTS", "0").lower() in {"1", "true", "yes"}

        try:
            assert_tradeable_dict(payload, where="PayloadBuilder.payload")
        except Exception as e:
            # fail-open: нельзя ломать публикацию
            try:
                # переносим потенциально опасные ключи в meta (если вдруг появились)
                moved = {}
                for k in ("trace", "events", "parts_full"):
                    if k in payload:
                        moved[k] = payload.pop(k)
                if moved:
                    payload_meta.setdefault("contract_moved", {})
                    payload_meta["contract_moved"].update(moved)
                payload.setdefault("contract_violation", str(e)[:256])
            except Exception:
                pass
            if strict:
                raise

        try:
            assert_outbox_sidecar_meta(payload_meta, where="PayloadBuilder.payload_meta")
        except Exception as e:
            # meta обязано быть json-safe тоже; здесь проще — если strict, падаем, иначе подрезаем.
            if strict:
                raise
            try:
                payload_meta = {"contract_violation": str(e)[:256]}
            except Exception:
                payload_meta = {}

        # 4.1 METRICS distributions (payload stage):
        try:
            k = str(getattr(f.cand, "kind", "") or "")
            dist(f.handler, name="confidence_pct", value=float(confidence_pct), kind=k)
            dist(f.handler, name="conf_factor", value=float(conf_factor01), kind=k)
            stage_ms_hist(f.handler, stage="emit", ms=sp.ms())
        except Exception:
            pass
        return payload, payload_meta


class OutboxWriter:
    """Stage 6: emit into outbox via handler._emitter.emit (dedup on)."""

    def emit(self, f: CandidateFrame, payload: Dict[str, Any], *, meta_extra: Optional[Dict[str, Any]] = None) -> bool:
        em = getattr(f.handler, "_emitter", None)
        fn = getattr(em, "emit", None) if em is not None else None
        if not callable(fn):
            return False
        try:
            # FULL DecisionTrace -> outbox meta sidecar (OUTBOX_META_PREFIX + sid).
            # Payload remains tradeable; trace is diagnostics-only and must not affect execution.
            sid = str(payload.get("signal_id") or payload.get("sid") or "").strip()
            meta = None
            if sid:
                try:
                    meta = build_trace_sidecar_meta(ctx=f.ctx, sid=sid)
                except Exception:
                    meta = None
            # Merge payload meta into the SAME sidecar dict (critical: sidecar written with NX).
            if isinstance(meta_extra, dict) and meta_extra:
                try:
                    meta_extra = to_json_safe(meta_extra)
                    if meta is None:
                        meta = {}
                    pm = meta.get("payload_meta")
                    if not isinstance(pm, dict):
                        pm = {}
                        meta["payload_meta"] = pm
                    pm.update(meta_extra)
                except Exception:
                    pass

            # ------------------------------------------------------------
            # HARD CONTRACT CHECKS (warn/raise/off):
            #  - payload must be trade-safe (no trace/events/payload_meta/parts_full)
            #  - meta must be json-safe (sidecar can contain trace/payload_meta)
            # ------------------------------------------------------------
            try:
                sid0 = str(payload.get("signal_id") or payload.get("sid") or sid or "")
            except Exception:
                sid0 = str(sid or "")
            contract_check_best_effort(kind="payload", obj=to_json_safe(payload), where="OutboxWriter.emit", sid=sid0, logger=getattr(f.handler, "logger", None))
            if isinstance(meta, dict):
                contract_check_best_effort(kind="meta", obj=to_json_safe(meta), where="OutboxWriter.emit", sid=sid0, logger=getattr(f.handler, "logger", None))

            return bool(fn(payload, labels=None, dedup=True, meta=meta))
        except Exception:
            return False


class Observability:
    """Stage 7: minimal observability hooks (fail-open)."""

    def veto_metric(self, f: CandidateFrame, reason_code: str) -> None:
        fn = getattr(f.handler, "_emit_veto_metric", None)
        if callable(fn):
            try:
                fn(kind=f.kind_key or f.kind_str, ctx=f.ctx, reason_code=_safe_str(reason_code))
            except Exception:
                try:
                    logger.debug("veto_metric emit failed", exc_info=True)
                except:
                    pass


class CandidateEmitPipelineV2:
    """
    Orchestrator: CandidateExtractor -> ContextEnricher -> Gates -> Scoring -> Payload -> Outbox.
    """

    def __init__(self, handler: Any):
        self.h = handler
        self.extractor = CandidateExtractor()
        self.enricher = ContextEnricher()
        self.gates = GateRunner()
        self.scoring = ScoringRunner()
        # ------------------------------------------------------------------
        # RuntimeSnapshot: убираем ENV parsing из hot-path.
        # refresh_every_s можно крутить через ENV (если нужно).
        # ------------------------------------------------------------------
        self._rt = RuntimeRefresher(refresh_every_s=float(getattr(handler, "runtime_refresh_every_s", 10.0) or 10.0))
        self.trace_log_sample_rate = float(getattr(handler, "trace_log_sample_rate", 0.02) or 0.02)
        self.conf_gates = ConfidenceGateRunner()
        self.builder = PayloadBuilder()
        self.writer = OutboxWriter()
        self.obs = Observability()

        # News enricher - zero-IO tick-loop using shadow cache
        try:
            from core.redis_client import get_redis_fast_news
            from news_pipeline.enricher_shadow import NewsEnricherShadow

            # Get fast Redis client for background refresher (tight timeouts)
            redis_fast = get_redis_fast_news()
            self.news_enricher = NewsEnricherShadow(redis=redis_fast)
            # Start background refresher thread
            self.news_enricher.start()
        except Exception:
            logger.warning("news_enricher init failed (shadow mode)", exc_info=True)
            self.news_enricher = None

    # -----------------------------
    # Memo helpers (compute-once)
    # -----------------------------
    @staticmethod
    def _memo_get(f: CandidateFrame, key: Any, fn):
        try:
            if key in f.memo:
                return f.memo[key]
            v = fn()
            f.memo[key] = v
            return v
        except Exception:
            # fail-open: no memoization, just compute
            return fn()

    def resolve_risk_cfg_once(self, f: CandidateFrame) -> Any:
        return f.memo_get("risk_cfg", lambda: self._resolve_risk_cfg(f))

    def _resolve_risk_cfg(self, f: CandidateFrame) -> Any:
        cfg = None
        try:
            fn = getattr(f.handler, "resolve_risk_cfg", None)
            if callable(fn):
                cfg = fn(symbol=str(f.ctx_symbol), ctx=f.ctx, cand=f.cand)
        except Exception:
            cfg = None
        return cfg

    def _ensure_levels_once(self, f: CandidateFrame):
        def _compute():
            # Use global ensure_levels() (your real function in base_orderflow_handler.py)
            try:
                from handlers.base_orderflow_handler import ensure_levels  # adjust import if your module path differs
                ensure_levels(f.ctx, side=f.side_int)
            except Exception:
                # fail-open
                pass
            return True
        return self._memo_get(f, "levels_attached", _compute)

    def ensure_trade_levels_once(self, f: CandidateFrame, *, risk_cfg: dict | None):
        """
        Heavy: attach_trade_levels_to_ctx(...).
        Must be done once per candidate (ctx), not in multiple gates/branches.
        """
        if f.memo.get("trade_levels_attached"):
            return

        self._ensure_levels_once(f)

        try:
            # use handler helper if exists (you already have _ensure_trade_levels_once in handler)
            fn = getattr(f.handler, "_ensure_trade_levels_once", None)
            if callable(fn):
                fn(
                    ctx=f.ctx,
                    side=str(f.side_int),
                    symbol=str(f.ctx_symbol),
                    kind=str(f.kind_key or f.kind_str),
                    cfg=dict(risk_cfg or {}),
                    overwrite=False,
                )
            else:
                # fallback direct call (still once)
                attach_trade_levels_to_ctx(
                    f.ctx,
                    side=str(f.side_int),
                    symbol=str(f.ctx_symbol),
                    cfg=dict(risk_cfg or {}),
                    kind=str(f.kind_key or f.kind_str),
                    overwrite=False,
                )
        except Exception:
            pass

        f.memo["trade_levels_attached"] = True

    def _consistency_once(self, f: CandidateFrame):
        # Cache is per (kind, side) because the same ctx may produce multiple candidates.
        kind = str(f.kind_key or f.kind_str or "")
        side = ""
        try:
            side = str(getattr(f.ctx, "side", "") or "")
        except Exception:
            side = ""
        memo_key = ("consistency", kind, side)

        def _compute():
            gate = getattr(f.handler, "_consistency_gate", None)
            if gate is None or not callable(getattr(gate, "evaluate", None)):
                return None
            try:
                return gate.evaluate(ctx=f.ctx, symbol=str(f.ctx_symbol), kind=kind, side=side)
            except Exception:
                return None

        return self._memo_get(f, memo_key, _compute)

    def ensure_levels_once(self, f: CandidateFrame) -> None:
        if f.memo.get("levels_ensured"):
            return
        try:
            ensure_levels(f.ctx, side=f.side_int)
        except Exception:
            pass
        f.memo["levels_ensured"] = True

    def consistency_once(self, f: CandidateFrame) -> Any:
        if "consistency" in f.memo:
            return f.memo["consistency"]
        g = getattr(f.handler, "_consistency_gate", None)
        if g is None or not callable(getattr(g, "evaluate", None)):
            f.memo["consistency"] = None
            return None
        try:
            d = g.evaluate(
                ctx=f.ctx,
                symbol=str(f.ctx_symbol),
                kind=str(f.kind_key or f.kind_str),
                side=str(getattr(f.ctx, "side", None) or ("LONG" if f.side_int == 1 else "SHORT")),
            )
        except Exception:
            d = None
        f.memo["consistency"] = d
        return d

    # ------------------------------------------------------------------
    # 3.3: "once per candidate" helpers (Frame-level cache)
    #
    # Contract:
    #   - must be fail-open (never block)
    #   - must not change semantics: only reduce repeats
    # ------------------------------------------------------------------
    def _ensure_levels_once(self, f: CandidateFrame) -> None:
        if f.memo.get("levels_ensured") is True:
            return
        try:
            ensure_levels(f.ctx, side=f.side_int)
        except Exception:
            # ensure_levels should be fail-open; keep pipeline stable anyway
            logger.debug("ensure_levels_once failed (memo check)", exc_info=True)
        f.memo["levels_ensured"] = True

    def _attach_trade_levels_once(self, f: CandidateFrame, risk_cfg: Any) -> None:
        # attach_trade_levels_to_ctx already has overwrite=False + early-return if entry/tp1 exist,
        # but we still cache to avoid repeated resolve/compute attempts when inputs are missing.
        if f.memo.get("trade_levels_attached") is True:
            return
        try:
            cfg = dict(risk_cfg) if isinstance(risk_cfg, dict) else {}
            rg = getattr(f.ctx, "regime", None) or getattr(getattr(f.ctx, "of", None), "regime", None)
            kind = str(f.kind_key or f.kind_str or "")
            side_payload = str(getattr(f.ctx, "side", None) or ("LONG" if int(f.side_int) == 1 else "SHORT"))
            attach_trade_levels_to_ctx(
                f.ctx,
                side=side_payload,
                symbol=str(f.ctx_symbol),
                cfg=cfg,
                kind=kind,
                regime=rg,
                empirical=getattr(f.handler, "_empirical_levels", None),
                overwrite=False,
                logger=getattr(f.handler, "logger", None),
            )
        except Exception:
            pass
        f.memo["trade_levels_attached"] = True


    def _emit_policy_diag_best_effort(
        self,
        f: CandidateFrame,
        *,
        stage: str,
        name: str,
        reason_code: str,
        metrics: Optional[Dict[str, Any]] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Diagnostics-only outbox.

        Writes a compact record to ENTRY_POLICY_DIAG_STREAM (Redis stream) whenever a
        candidate is vetoed/diagnosed.

        Hard requirements (4.2):
          - tradeable outbox MUST contain only OK/SOFT_* signals
          - any veto/diag information MUST go ONLY to the diagnostics stream

        FAIL-OPEN: never blocks the main emission path.
        """
        try:
            stream = str(os.getenv("ENTRY_POLICY_DIAG_STREAM", "") or "").strip()
            if not stream:
                return

            # Best-effort Redis client discovery.
            r = (
                getattr(self.h, "redis", None)
                or getattr(self.h, "simple_redis", None)
                or getattr(self.h, "dual_redis", None)
                or getattr(self.h, "_redis", None)
            )
            if r is None:
                return

            # Prefer candidate's SID if available (stable across retries/replays).
            sid = str(getattr(getattr(f, "cand", None), "signal_id", "") or "").strip()
            if not sid:
                sid = str(getattr(f.ctx, "sid", "") or "").strip()

            trace_id = str(
                getattr(f.ctx, "trace_id", "")
                or getattr(f.ctx, "correlation_id", "")
                or sid
            ).strip() or sid

            kind = str(getattr(f, "kind_key", "") or getattr(f, "kind_str", "") or "").strip()
            symbol = str(getattr(f, "ctx_symbol", "") or "").strip()

            ev = build_entry_policy_diag_event(
                sid=sid or trace_id,
                trace_id=trace_id or sid,
                kind=kind,
                symbol=symbol,
                stage=str(stage or ""),
                name=str(name or ""),
                reason_code=str(reason_code or ""),
                metrics=metrics or {},
                extra=extra or {},
            )
            emit_entry_policy_diag_best_effort(r, ev, stream=stream)
        except Exception:
            try:
                logger.debug("emit_policy_diag failed", exc_info=True)
            except:
                pass
            return

    def emit(self, *, ctx: Any) -> bool:
        any_sent = False

        sp_det = Span()
        frames = self.extractor.extract(self.h, ctx)  # detector begin/end
        try:
            stage_ms_hist(self.h, stage="detector", ms=float(sp_det.ms()), kind="", symbol=str(getattr(ctx, "symbol", "") or ""))
        except Exception:
            logger.debug("stage_ms_hist detector failed", exc_info=True)
        if not frames:
            return False

        # News enrichment - zero-IO from shadow cache, before processing candidates
        try:
            if self.news_enricher:
                self.news_enricher.attach(ctx, asset_class=getattr(ctx, "asset_class", "crypto"))
        except Exception:
            # fail-open: не мешаем сигналам
            try:
                setattr(ctx, "news", None)
            except Exception:
                logger.debug("fallback cleanup failed", exc_info=True)

        for f in frames:
            # 4.1 METRICS: candidates_total{kind}
            try:
                stage_counter(self.h, "candidates_total", kind=str(getattr(f, "kind_str", "") or getattr(f, "kind", "") or ""))
            except Exception:
                logger.debug("stage_counter candidates_total failed", exc_info=True)

            sym = str(getattr(f, "ctx_symbol", "") or getattr(getattr(f, "ctx", None), "symbol", "") or "")
            kind_s = str(getattr(f, "kind_str", "") or getattr(f, "kind", "") or "")

            # -------- Gate timings + DecisionTrace (duration_ms обязателен) --------
            # FAIL-OPEN: любые ошибки trace не ломают emission.

            # Stage 2: invariants MUST be set before any gate that uses levels/side.
            with Span() as sp:
                self.enricher.ensure_invariants(f)
            try:
                trace_gate(
                    f.ctx,
                    name="ensure_invariants",
                    passed=True,
                    veto=False,
                    reason_code="OK",
                    duration_ms=float(sp.ms()),
                )
            except Exception:
                logger.debug("trace_gate ensure_invariants failed", exc_info=True)

            # ------------------------------------------------------------------
            # Stage 2.5 ("ещё выше"): compute-once per candidate
            # - risk cfg resolved once
            # - trade levels attached once (idempotent, see ensure_levels() patch)
            # - consistency decision computed once (cached per (kind, side))
            # ------------------------------------------------------------------
            self.resolve_risk_cfg_once(f)
            self.ensure_levels_once(f)
            _ = self.consistency_once(f)  # keep decision in memo for downstream

            # Stage 3.1: regime gate
            sp_g1 = Span()
            ok, rc = self.gates.regime(f)
            try:
                trace_gate(
                    f.ctx,
                    name="regime_gate",
                    passed=bool(ok),
                    veto=not bool(ok),
                    reason_code=str(rc or "OK"),
                    duration_ms=float(sp_g1.ms()),
                    metrics={"kind": kind_s, "symbol": sym},
                )
            except Exception:
                logger.debug("trace_gate regime_gate failed", exc_info=True)
            try:
                stage_ms_hist(self.h, stage="gates", ms=float(sp_g1.ms()), kind=kind_s, symbol=sym)
            except Exception:
                logger.debug("stage_ms_hist gates failed", exc_info=True)
            if not ok:
                try:
                    stage_counter(self.h, "veto_total", kind=kind_s)
                except Exception:
                    logger.debug("stage_counter veto_total failed", exc_info=True)

                try:
                    self._emit_policy_diag_best_effort(
                        f,
                        stage="gates",
                        name="regime_gate",
                        reason_code=str(rc or "VETO_REGIME"),
                        metrics={"kind": kind_s, "symbol": sym},
                    )
                except Exception:
                    logger.debug("diag regime failed", exc_info=True)
                continue

            # Stage 3.2: confirmations validate
            sp_g2 = Span()
            res = self.gates.confirmations(f)
            if res is None:
                try:
                    trace_gate(
                        f.ctx,
                        stage="gates",
                        name="confirmations",
                        passed=False,
                        veto=True,
                        reason_code="VETO_CONFIRMATIONS_NONE",
                        duration_ms=float(sp_g2.ms()),
                    )
                except Exception:
                    logger.debug("trace_gate confirmations none failed", exc_info=True)
                try:
                    stage_ms_hist(self.h, stage="gates", ms=float(sp_g2.ms()), kind=kind_s, symbol=sym)
                    stage_counter(self.h, "veto_total", kind=kind_s)
                except Exception:
                    logger.debug("stage metrics (confirmations none) failed", exc_info=True)

                try:
                    self._emit_policy_diag_best_effort(
                        f,
                        stage="gates",
                        name="confirmations",
                        reason_code="VETO_CONFIRMATIONS_NONE",
                        metrics={"kind": kind_s, "symbol": sym},
                    )
                except Exception:
                    logger.debug("diag failed (confirmations none)", exc_info=True)
                continue
            if bool(getattr(res, "veto", False)):
                veto_rc = _safe_str(getattr(res, "reason_code", "") or "VETO_UNKNOWN")
                self.obs.veto_metric(f, veto_rc)
                try:
                    trace_gate(
                        f.ctx,
                        name="confirmations",
                        passed=False,
                        veto=True,
                        reason_code=veto_rc,
                        duration_ms=float(sp_g2.ms()),
                    )
                except Exception:
                    logger.debug("trace_gate confirmations veto failed", exc_info=True)
                try:
                    stage_ms_hist(self.h, stage="gates", ms=float(sp_g2.ms()), kind=kind_s, symbol=sym)
                    stage_counter(self.h, "veto_total", kind=kind_s)
                except Exception:
                    logger.debug("metrics failed (veto)", exc_info=True)

                try:
                    self._emit_policy_diag_best_effort(
                        f,
                        stage="gates",
                        name="confirmations",
                        reason_code=str(veto_rc or "VETO_UNKNOWN"),
                        metrics={"kind": kind_s, "symbol": sym},
                    )
                except Exception:
                    logger.debug("diag failed (veto)", exc_info=True)
                continue
            else:
                try:
                        trace_gate(
                            f.ctx,
                            name="confirmations",
                            passed=True,
                            veto=False,
                            reason_code="OK",
                            duration_ms=float(sp_g2.ms()),
                        )
                except Exception:
                    logger.debug("trace_gate confirmations OK failed", exc_info=True)
                try:
                    stage_ms_hist(self.h, stage="gates", ms=float(sp_g2.ms()), kind=kind_s, symbol=sym)
                except Exception:
                    logger.debug("metrics failed (OK)", exc_info=True)

            # Stage 4: scoring / confidence
            sp_score = Span()
            raw_score, conf_factor01, final_score, confidence_pct, parts = self.scoring.compute(f, res)
            try:
                trace_gate(
                    f.ctx,
                    stage="score",
                    name="score_model",
                    passed=True,
                    veto=False,
                    reason_code="OK",
                    duration_ms=float(sp_score.ms()),
                    metrics={"confidence_pct": float(confidence_pct), "conf_factor01": float(conf_factor01)},
                )
            except Exception:
                logger.debug("trace_gate score failed", exc_info=True)
            try:
                stage_ms_hist(self.h, stage="scoring", ms=float(sp_score.ms()), kind=kind_s, symbol=sym)
                dist(self.h, name="confidence_pct", value=float(confidence_pct), kind=kind_s, symbol=sym)
                dist(self.h, name="conf_factor", value=float(conf_factor01), kind=kind_s, symbol=sym)
            except Exception:
                logger.debug("dist/stage_ms_hist scoring failed", exc_info=True)

            # Stage 4.5: cheap confidence gates (isolated)
            sp_conf = Span()
            ok, rc = self.conf_gates.check(
                f,
                confidence_pct=float(confidence_pct),
                conf_factor01=float(conf_factor01),
                rt=self._rt.runtime,
            )
            # ВАЖНО: trace только 1 раз (раньше было 2 раза и с неправильным sp.ms)
            try:
                trace_gate(
                    f.ctx,
                    stage="gates",
                    name="confidence_gate",
                    passed=bool(ok),
                    veto=not bool(ok),
                    reason_code=str(rc or ("OK" if ok else "VETO_CONF")),
                    duration_ms=float(sp_conf.ms()),
                    metrics={"confidence_pct": float(confidence_pct), "conf_factor01": float(conf_factor01)},
                )
            except Exception:
                logger.debug("trace_gate confidence_gate failed", exc_info=True)
            try:
                stage_ms_hist(self.h, stage="gates", ms=float(sp_conf.ms()), kind=kind_s, symbol=sym)
            except Exception:
                logger.debug("stage_ms_hist failed", exc_info=True)
            if not ok:
                try:
                    stage_counter(self.h, "veto_total", kind=kind_s)
                except Exception:
                    logger.debug("stage_counter veto_total gate failed", exc_info=True)
                # diagnostics-only outbox (never tradeable)
                try:
                    self._emit_policy_diag_best_effort(
                        f,
                        stage="gates",
                        name="confidence_gate",
                        reason_code=str(rc or "VETO_CONF"),
                        metrics={"confidence_pct": float(confidence_pct), "conf_factor01": float(conf_factor01)},
                    )
                except Exception:
                    logger.debug("diag conf failed", exc_info=True)
                continue

            # ------------------------------------------------------------
            # 3.3: compute heavy prerequisites ONCE per candidate:
            #   - levels attach
            #   - consistency evaluate
            #   - cost/edge gate
            # Any internal repeated calls should switch to *_once methods.
            # ------------------------------------------------------------
            sp_g3 = Span()
            ok, rc = self.enricher.ensure_levels_once(f)
            if not ok:
                self.obs.veto_metric(f, rc or "VETO_LEVELS")
                try:
                    veto_total(self.h, kind=kind_s, reason_code=str(rc or "VETO_LEVELS"))
                except Exception:
                    logger.debug("veto_total levels failed", exc_info=True)
                try:
                    trace_gate(
                        f.ctx,
                        stage="gates",
                        name="ensure_levels",
                        passed=False,
                        veto=True,
                        reason_code=str(rc or "VETO_LEVELS"),
                        duration_ms=float(sp_g3.ms),
                    )
                except Exception:
                    logger.debug("trace_gate ensure_levels failed", exc_info=True)
                try:
                    self._emit_policy_diag_best_effort(f, stage="ensure_levels", reason_code=str(rc or "VETO_LEVELS"))
                except Exception:
                    logger.debug("metrics consistency failed", exc_info=True)
                continue

            sp_g4 = Span()
            ok, rc = self.gates.consistency_once(f)
            if not ok:
                self.obs.veto_metric(f, rc or "VETO_CONSISTENCY")
                try:
                    veto_total(self.h, kind=kind_s, reason_code=str(rc or "VETO_CONSISTENCY"))
                except Exception:
                    logger.debug("veto_total consistency failed", exc_info=True)
                try:
                    trace_gate(
                        f.ctx,
                        stage="gates",
                        name="consistency",
                        passed=False,
                        veto=True,
                        reason_code=str(rc or "VETO_CONSISTENCY"),
                        duration_ms=float(sp_g4.ms),
                    )
                except Exception:
                    logger.debug("diag consistency failed", exc_info=True)
                try:
                    self._emit_policy_diag_best_effort(f, stage="consistency", reason_code=str(rc or "VETO_CONSISTENCY"))
                except Exception:
                    logger.debug("diag consistency failed", exc_info=True)
                continue

            # ------------------------------------------------------------
            # Stage 3.2.9 (NEW): ensure levels + attach trade levels ONCE.
            # Rationale:
            #   - edge/cost gate accuracy depends on entry/tp1/sl/side invariants
            #   - reduces repeats from downstream legacy paths
            # ------------------------------------------------------------
            try:
                self._ensure_levels_once(f)
                cfg = None
                try:
                    cfg = self.resolve_risk_cfg_once(f)
                except Exception:
                    cfg = None
                self._attach_trade_levels_once(f, cfg)
            except Exception:
                # fail-open
                logger.debug("levels/risk attach failed", exc_info=True)

            sp_g5 = Span()
            ok, rc = self.gates.edge_cost_once(f)
            if not ok:
                try:
                    veto_total(self.h, kind=kind_s, reason_code=str(rc or "VETO_EDGE_COST"))
                except Exception:
                    logger.debug("metrics edge_cost failed", exc_info=True)
                try:
                    trace_gate(
                        f.ctx,
                        stage="gates",
                        name="edge_cost",
                        passed=False,
                        veto=True,
                        reason_code=str(rc or "VETO_EDGE_COST"),
                        duration_ms=float(sp_g5.ms),
                        metrics={"raw_score": float(raw_score), "final_score": float(final_score)},
                    )
                except Exception:
                    logger.debug("trace_gate edge_cost failed", exc_info=True)
                try:
                    self._emit_policy_diag_best_effort(
                        f,
                        stage="edge_cost",
                        reason_code=str(rc or "VETO_EDGE_COST"),
                        extra={"raw_score": float(raw_score), "final_score": float(final_score)},
                    )
                except Exception:
                    logger.debug("diag edge_cost failed", exc_info=True)
                continue

            # Stage 5: payload
            payload, payload_meta = self.builder.build(
                f,
                raw_score=raw_score,
                conf_factor01=conf_factor01,
                final_score=final_score,
                confidence_pct=confidence_pct,
                parts=parts,
                res=res,
            )

            # Stage 6: outbox
            sp_emit = Span()
            sent = self.writer.emit(f, payload, meta_extra=payload_meta)
            try:
                stage_ms_hist(self.h, stage="emit", ms=float(sp_emit.ms()), kind=kind_s, symbol=sym)
            except Exception:
                logger.debug("metrics emit failed", exc_info=True)
            if sent:
                try:
                    stage_counter(self.h, "emit_ok_total", kind=kind_s)
                except Exception:
                    logger.debug("metrics emit ok failed", exc_info=True)
            any_sent = any_sent or sent

            if trace_enabled():
                try:
                    ctx = getattr(f, "ctx", None) or getattr(f, "context", None)
                    tr = ensure_trace(ctx) if ctx is not None else None
                    if isinstance(tr, dict):
                        tid = str(tr.get("trace_id") or "")
                        rate = float(self.trace_log_sample_rate or 0.02)
                        if should_sample(tid, rate):
                            # build_trace_summary == make_trace_summary (alias в decision_trace.py)
                            from common.decision_trace import build_trace_summary
                            summ = build_trace_summary(tr)
                            logger.info(dumps1({
                                "event": "trace_summary",
                                "where": "candidate_emit",
                                "sent": bool(sent),
                                "trace_id": tid,
                                "sid": str(payload.get("signal_id") or ""),
                                "trace_summary": summ,
                            }))
                except Exception:
                    logger.debug("trace summary failed", exc_info=True)

        return any_sent


class ConfidenceGateRunner:
    """
    Stage 4.5: cheap confidence gates (explicit, isolated).
    Mirrors logic from the legacy mega-method, but in a testable unit.
    """
    def __init__(self, runtime: Optional[Any] = None) -> None:
        self._runtime_holder = runtime

    def check(
        self,
        f: CandidateFrame,
        *,
        confidence_pct: float,
        conf_factor01: float,
        rt: Optional[RuntimeSnapshot] = None,
    ) -> Tuple[bool, str]:
        sym_u = _safe_str(f.ctx_symbol).strip().upper()
        
        # Resolve runtime snapshot
        snapshot = rt
        if snapshot is None:
            # Try self._runtime_holder
            rh = self._runtime_holder
            if rh is not None:
                # helper to unwrap RuntimeRefresher if needed
                if hasattr(rh, "runtime"):
                     snapshot = rh.runtime
                elif isinstance(rh, RuntimeSnapshot):
                     snapshot = rh
        
        if snapshot is None:
             # fallback
             try:
                 from common.runtime_snapshot import RuntimeSnapshot as _RS
                 snapshot = _RS.load()
             except Exception:
                 logger.debug("Runtime fallback load failed", exc_info=True)
                 snapshot = None
             
        rt = snapshot # ensure we use the resolved one

        min_conf = rt.min_conf(sym_u)
        min_cf = rt.min_conf_factor(sym_u)

        # Audit logging
        audit = getattr(f.ctx, "audit_event", None)
        if audit is not None:
            try:
                audit.gate_confidence_pct = float(confidence_pct)
                audit.gate_conf_factor01 = float(conf_factor01)
                audit.gate_min_conf = float(min_conf)
                audit.gate_min_conf_factor = float(min_cf)
            except Exception:
                pass

        if float(confidence_pct) < float(min_conf):
            rc = "VETO_CONF_MIN"
            try:
                h = getattr(f, "handler", None) or getattr(f, "_handler", None)
                if h is not None and hasattr(h, "_emit_veto_metric"):
                    h._emit_veto_metric(kind=f.kind_key or f.kind_str, ctx=f.ctx, reason_code=rc)
            except Exception:
                logger.debug("veto metric fail", exc_info=True)
            return False, rc

        if float(conf_factor01) < float(min_cf):
            rc = "VETO_CONF_FACTOR_MIN"
            try:
                h = getattr(f, "handler", None) or getattr(f, "_handler", None)
                if h is not None and hasattr(h, "_emit_veto_metric"):
                    h._emit_veto_metric(kind=f.kind_key or f.kind_str, ctx=f.ctx, reason_code=rc)
            except Exception:
                logger.debug("veto metric fail", exc_info=True)
            return False, rc

        return True, ""
