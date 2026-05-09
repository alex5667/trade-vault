from __future__ import annotations

"""BookTradeConsistencyGate.

Separates trade-to-book microstructure checks from generic BookSanity/DataQuality:
- stale-book vs current event/trade timestamp
- trade printing outside current BBO (adverse-cross symptom)

Design:
- deterministic, fail-open, hot-path safe
- emits low-cardinality Prometheus histograms/counters via
  services.orderflow.metrics_stream_integrity_p5
- returns a small decision object which can be consumed by both:
    * DataQualityGate (hard veto path)
    * EntryPolicyGate (soft tighten / optional hard veto)
"""

import math
import os
from dataclasses import dataclass
from typing import Any

from services.orderflow.book_sanity import trade_outside_bbo

try:
    from services.orderflow.metrics_stream_integrity_p5 import (
        emit_book_staleness_metrics,
        emit_trade_to_book_metrics,
    )
except Exception:  # pragma: no cover
    emit_book_staleness_metrics = None  # type: ignore
    emit_trade_to_book_metrics = None  # type: ignore


@dataclass(frozen=True)
class BookTradeConsistencyDecision:
    apply: bool
    veto: bool
    reason_code: str
    flags: list[str]
    book_staleness_ms: float = 0.0
    adverse_cross_bps: float = 0.0
    stream: str = "tick"
    notes: str = ""


_ALLOWED_FLAGS = {"stale_book", "adverse_cross", "missing_bbo", "missing_book_ts"}


def _env_bool(name: str, default: bool) -> bool:
    try:
        v = os.getenv(name, "")
        if v == "":
            return bool(default)
        return v.strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return bool(default)


def _env_float(name: str, default: float) -> float:
    try:
        v = float(os.getenv(name, str(default)) or default)
    except Exception:
        v = float(default)
    return float(v) if math.isfinite(v) else default


def _profile() -> str:
    return os.getenv("GATE_PROFILE", os.getenv("BOOK_TRADE_CONSISTENCY_PROFILE", "default") or "default").strip().lower()


def _effective_mode(raw: str) -> str:
    mode = (raw or "auto").strip().lower()
    if mode in {"monitor", "tighten", "veto"}:
        return mode
    p = _profile()
    if p == "hard":
        return "veto"
    if p == "strict":
        return "tighten"
    return "monitor"


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(x: Any) -> int | None:
    try:
        return int(x)
    except Exception:
        return None


def _get_attr_any(obj: Any, names: tuple[str, ...]) -> Any:
    for n in names:
        try:
            if hasattr(obj, n):
                return getattr(obj, n)
        except Exception:
            continue
    return None


def _metric(ctx: Any, names: tuple[str, ...]) -> Any:
    v = _get_attr_any(ctx, names)
    if v is not None:
        return v
    of = getattr(ctx, 'of', None)
    if of is not None:
        v = _get_attr_any(of, names)
        if v is not None:
            return v
    l2 = getattr(ctx, 'l2', None) or getattr(ctx, 'l2_snapshot', None) or getattr(ctx, 'book', None)
    if l2 is not None:
        v = _get_attr_any(l2, names)
        if v is not None:
            return v
    return None


def _stream_name(ctx: Any) -> str:
    return str(getattr(ctx, 'stream_type', None) or getattr(ctx, 'source_stage', None) or 'tick')


@dataclass
class BookTradeConsistencyGate:
    """
    Gate that checks trade-to-book microstructure consistency.

    Evaluates two conditions per tick:
      1. book_staleness_ms = event_ts_ms - book_ts_ms   (stale BBO vs trade)
      2. adverse_cross_bps = distance if trade_px outside current BBO

    Modes (resolved via BOOK_TRADE_CONSISTENCY_MODE or GATE_PROFILE):
      monitor  — only emit metrics + flags, never veto (default)
      tighten  — annotate ctx.entry_policy_tighten_k for downstream gates
      veto     — hard VETO_BOOK_STALE / VETO_TRADE_ADVERSE_CROSS
    """
    enabled: bool
    mode: str
    max_book_staleness_ms: float
    outside_bbo_eps_bps: float
    adverse_cross_bps: float
    veto_on_stale_book: bool
    veto_on_adverse_cross: bool

    @classmethod
    def from_env(cls) -> BookTradeConsistencyGate:
        return cls(
            enabled=_env_bool('BOOK_TRADE_CONSISTENCY_ENABLED', True),
            mode=(os.getenv('BOOK_TRADE_CONSISTENCY_MODE', 'auto') or 'auto'),
            max_book_staleness_ms=_env_float('BOOK_TRADE_CONSISTENCY_MAX_BOOK_STALENESS_MS', 1200.0),
            outside_bbo_eps_bps=_env_float('BOOK_TRADE_CONSISTENCY_OUTSIDE_BBO_EPS_BPS', 1.0),
            adverse_cross_bps=_env_float('BOOK_TRADE_CONSISTENCY_ADVERSE_CROSS_BPS', 1.5),
            veto_on_stale_book=_env_bool('BOOK_TRADE_CONSISTENCY_VETO_ON_STALE_BOOK', True),
            veto_on_adverse_cross=_env_bool('BOOK_TRADE_CONSISTENCY_VETO_ON_ADVERSE_CROSS', True),
        )

    def evaluate(self, *, ctx: Any, symbol: str, kind: str = '') -> BookTradeConsistencyDecision:
        if not self.enabled:
            return BookTradeConsistencyDecision(False, False, 'OK', [], stream=_stream_name(ctx))

        flags: list[str] = []
        stream = _stream_name(ctx)
        trade_px = _safe_float(_metric(ctx, ('trade_px', 'last_trade_px', 'last_price', 'price')), 0.0)
        best_bid = _safe_float(_metric(ctx, ('best_bid_px', 'best_bid', 'bid', 'b')), 0.0)
        best_ask = _safe_float(_metric(ctx, ('best_ask_px', 'best_ask', 'ask', 'a')), 0.0)
        event_ts_ms = _safe_int(_metric(ctx, ('ts_event_ms', 'ts_ms', 'ts')))
        book_ts_ms = _safe_int(_metric(ctx, (
            'book_ts_ms', 'bbo_ts_ms', 'l2_ts_ms',
            'book_last_update_ms', 'last_book_ts_ms',
            'best_bid_ts_ms', 'best_ask_ts_ms',
        )))

        # Compute book staleness: how old is our best BBO vs the incoming event timestamp.
        book_staleness_ms = 0.0
        if book_ts_ms is not None and event_ts_ms is not None and book_ts_ms > 0 and event_ts_ms > 0:
            book_staleness_ms = float(max(0, int(event_ts_ms) - int(book_ts_ms)))
        elif book_ts_ms is not None and book_ts_ms > 0:
            # Best-effort fallback: if only processing ts exists on ctx, compare to it.
            proc_ts_ms = _safe_int(_metric(ctx, ('ts_processing_ms', 'processing_ts_ms', 'now_ts_ms')))
            if proc_ts_ms is not None and proc_ts_ms > 0:
                book_staleness_ms = float(max(0, int(proc_ts_ms) - int(book_ts_ms)))
        elif best_bid > 0 and best_ask > 0:
            # BBO exists but no timestamp to compare against.
            flags.append('missing_book_ts')

        if book_staleness_ms > 0 and emit_book_staleness_metrics is not None:
            try:
                emit_book_staleness_metrics(symbol=symbol, staleness_ms=book_staleness_ms)
            except Exception:
                pass

        stale_book = bool(book_staleness_ms >= float(self.max_book_staleness_ms) > 0)
        if stale_book:
            flags.append('stale_book')

        # Adverse cross: trade price printing outside current BBO beyond tolerance.
        outside = False
        adverse_cross_bps = 0.0
        if best_bid > 0 and best_ask > 0:
            outside, adverse_cross_bps = trade_outside_bbo(
                trade_px=float(trade_px),
                best_bid=float(best_bid),
                best_ask=float(best_ask),
                eps_bps=float(self.outside_bbo_eps_bps),
            )
            if outside and float(adverse_cross_bps) >= float(self.adverse_cross_bps):
                flags.append('adverse_cross')
        else:
            flags.append('missing_bbo')

        # Restrict to known flag names to avoid cardinality drift.
        flags = [f for f in flags if f in _ALLOWED_FLAGS]
        adverse = 'adverse_cross' in flags
        mode = _effective_mode(self.mode)
        veto = False
        reason = 'OK'

        if mode == 'veto':
            # Combined condition takes priority over individual checks.
            if stale_book and self.veto_on_stale_book and adverse and self.veto_on_adverse_cross:
                veto = True
                reason = 'VETO_BOOK_STALE_ADVERSE_CROSS'
            elif stale_book and self.veto_on_stale_book:
                veto = True
                reason = 'VETO_BOOK_STALE'
            elif adverse and self.veto_on_adverse_cross:
                veto = True
                reason = 'VETO_TRADE_ADVERSE_CROSS'

        if emit_trade_to_book_metrics is not None:
            try:
                emit_trade_to_book_metrics(
                    symbol=symbol,
                    stream=stream,
                    book_staleness_ms=float(book_staleness_ms),
                    adverse_cross_bps=float(max(0.0, adverse_cross_bps)),
                    stale_book=stale_book,
                    adverse_cross=adverse,
                    veto_reason=reason if veto else '',
                )
            except Exception:
                pass

        notes = ''
        if flags:
            notes = (
                f"mode={mode} book_staleness_ms={book_staleness_ms:.0f} "
                f"cross_bps={float(max(0.0, adverse_cross_bps)):.4f} trade_px={trade_px:.8f} "
                f"bb={best_bid:.8f} ba={best_ask:.8f} kind={(kind or '')}"
            )[:256]

        return BookTradeConsistencyDecision(
            apply=bool(flags),
            veto=veto,
            reason_code=reason,
            flags=list(flags),
            book_staleness_ms=float(book_staleness_ms),
            adverse_cross_bps=float(max(0.0, adverse_cross_bps)),
            stream=stream,
            notes=notes,
        )
