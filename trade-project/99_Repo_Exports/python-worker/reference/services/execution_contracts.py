from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Canonical execution contracts for Binance executor materialized state and events.

This module does not own the online source of truth — Redis stream ``orders:exec``
remains the fact journal.  The helpers here ensure the mutable Redis materialized
state follows a stable schema, and that event payloads carry deterministic time
fields and nested plain/algo references.
"""

from dataclasses import dataclass, field
from typing import Any


def _ms_now() -> int:
    return get_ny_time_millis()


def _i(v: Any) -> int | None:
    try:
        if v in (None, '', 'None'):
            return None
        return int(v)
    except Exception:
        return None


def _f(v: Any) -> float | None:
    try:
        if v in (None, '', 'None'):
            return None
        return float(v)
    except Exception:
        return None


def _s(v: Any) -> str | None:
    s = (v or '').strip()
    return s or None


@dataclass(frozen=True)
class BinancePlainOrderRef:
    order_id: int | None = None
    client_order_id: str | None = None
    status: str | None = None
    qty: float | None = None
    avg_price: float | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.order_id is not None:
            out['order_id'] = int(self.order_id)
        if self.client_order_id:
            out['client_order_id'] = str(self.client_order_id)
        if self.status:
            out['status'] = str(self.status)
        if self.qty is not None:
            out['qty'] = float(self.qty)
        if self.avg_price is not None:
            out['avg_price'] = float(self.avg_price)
        return out


@dataclass(frozen=True)
class BinanceAlgoOrderRef:
    algo_id: int | None = None
    client_algo_id: str | None = None
    trigger_price: float | None = None
    working_type: str | None = None
    status: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.algo_id is not None:
            out['algo_id'] = int(self.algo_id)
        if self.client_algo_id:
            out['client_algo_id'] = str(self.client_algo_id)
        if self.trigger_price is not None:
            out['trigger_price'] = float(self.trigger_price)
        if self.working_type:
            out['working_type'] = str(self.working_type)
        if self.status:
            out['status'] = str(self.status)
        return out


@dataclass(frozen=True)
class ExecutionEvent:
    sid: str
    symbol: str
    action: str
    event_type: str
    status: str = 'ok'
    ts_event_ms: int = field(default_factory=_ms_now)
    ts_exec_start_ms: int | None = None
    ts_queue_ms: int | None = None
    ts_state_commit_ms: int | None = None
    severity: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    def to_stream_fields(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            'sid': str(self.sid),
            'symbol': str(self.symbol),
            'action': str(self.action),
            'event_type': str(self.event_type),
            'status': str(self.status),
            'ts_event_ms': int(self.ts_event_ms),
        }
        if self.ts_exec_start_ms is not None:
            out['ts_exec_start_ms'] = int(self.ts_exec_start_ms)
        if self.ts_queue_ms is not None:
            out['ts_queue_ms'] = int(self.ts_queue_ms)
        if self.ts_state_commit_ms is not None:
            out['ts_state_commit_ms'] = int(self.ts_state_commit_ms)
        if self.severity:
            out['severity'] = str(self.severity)
        out.update(self.payload or {})
        return out


def collect_tp_algo_refs(state: dict[str, Any]) -> list[BinanceAlgoOrderRef]:
    refs: list[BinanceAlgoOrderRef] = []
    idx = 1
    while True:
        algo_id = _i(state.get(f'tp{idx}_algo_id'))
        client_algo_id = _s(state.get(f'tp{idx}_client_algo_id'))
        if algo_id is None and client_algo_id is None:
            break
        refs.append(BinanceAlgoOrderRef(
            algo_id=algo_id,
            client_algo_id=client_algo_id,
            trigger_price=_f(state.get(f'tp{idx}_trigger_price')),
            working_type=_s(state.get(f'tp{idx}_working_type')),
            status=_s(state.get(f'tp{idx}_state')),
        ))
        idx += 1
    return refs


def build_materialized_state_view(state: dict[str, Any]) -> dict[str, Any]:
    doc = dict(state or {})
    entry = BinancePlainOrderRef(
        order_id=_i(doc.get('binance_order_id') or doc.get('entry_order_id')),
        client_order_id=_s(doc.get('entry_client_order_id')),
        status=_s(doc.get('entry_status') or doc.get('status')),
        qty=_f(doc.get('qty')),
        avg_price=_f(doc.get('exec_price')),
    ).to_dict()
    sl = BinanceAlgoOrderRef(
        algo_id=_i(doc.get('sl_algo_id')),
        client_algo_id=_s(doc.get('sl_client_algo_id')),
        trigger_price=_f(doc.get('sl_trigger_price') or doc.get('sl')),
        working_type=_s(doc.get('sl_working_type')),
        status='ARMED' if _i(doc.get('sl_algo_id')) is not None else None,
    ).to_dict()
    trail = BinanceAlgoOrderRef(
        algo_id=_i(doc.get('trail_algo_id')),
        client_algo_id=_s(doc.get('trail_client_algo_id') or doc.get('trail_client_id')),
        trigger_price=_f(doc.get('trail_activate_price')),
        working_type=_s(doc.get('trail_working_type')),
        status=_s(doc.get('trail_status')),
    ).to_dict()
    tp_refs = collect_tp_algo_refs(doc)
    protective: dict[str, Any] = {
        'tp_algo_ids': [int(r.algo_id) for r in tp_refs if r.algo_id is not None],
        'tp_client_algo_ids': [str(r.client_algo_id) for r in tp_refs if r.client_algo_id],
        'tp_refs': [r.to_dict() for r in tp_refs],
    }
    if sl:
        protective.update({
            'sl_algo_id': sl.get('algo_id'),
            'sl_client_algo_id': sl.get('client_algo_id'),
            'sl': sl,
        })
    if trail:
        doc['trailing'] = trail
    if entry:
        doc['entry'] = entry
    doc['protective'] = {**dict(doc.get('protective') or {}), **protective}
    doc.setdefault('state_schema_ver', 'execution_state:v2')
    doc.setdefault('ts_event_ms', int(doc.get('ts_ms') or _ms_now()))
    return doc
