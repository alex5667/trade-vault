from __future__ import annotations

import argparse
import json
import os

try:  # pragma: no cover
    from services.active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics
    from services.active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine
    from services.active_symbol_guard_runbook import ActiveSymbolGuardRunbookExecutor
    from services.binance_futures_client import BinanceFuturesClient
    from services.execution_projection_worker import _redis_from_env
except Exception:  # pragma: no cover
    from active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics  # type: ignore
    from active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine  # type: ignore
    from active_symbol_guard_runbook import ActiveSymbolGuardRunbookExecutor  # type: ignore
    from binance_futures_client import BinanceFuturesClient  # type: ignore
    from execution_projection_worker import _redis_from_env  # type: ignore


def _client_from_env():  # pragma: no cover
    try:
        return BinanceFuturesClient.from_env()
    except Exception:
        return None


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='Active-symbol guard diagnostics CLI')
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--symbol', help='Exchange symbol, e.g. BTCUSDT')
    group.add_argument('--sid', help='Execution sid')
    group.add_argument('--snapshot', action='store_true', help='Print full guard snapshot')
    group.add_argument('--heatmap', action='store_true', help='Print windowed hot-symbol heatmap')
    group.add_argument('--dashboard', action='store_true', help='P13: Print operator audit dashboard (active holds, acks, recent runbook history)')
    group.add_argument('--ticket-history', help='P13: Print audit history for a ticket id')
    group.add_argument('--incident-symbol', help='Print full incident bundle for symbol')
    group.add_argument('--incident-sid', help='Print full incident bundle for sid')
    group.add_argument('--triage-symbol', help='Print triaged incident payload for symbol')
    group.add_argument('--triage-sid', help='Print triaged incident payload for sid')
    group.add_argument('--runbook-symbol', help='Print runbook state for symbol')
    group.add_argument('--runbook-sid', help='Print runbook state for sid')
    group.add_argument('--apply-hold-symbol', help='Apply manual hold for symbol')
    group.add_argument('--revoke-hold-symbol', help='Revoke manual hold for symbol')
    group.add_argument('--force-release-symbol', help='Guarded force release for symbol')
    group.add_argument('--ack-symbol', help='Ack escalation for symbol')
    group.add_argument('--renew-symbol', help='Renew escalation for symbol')
    p.add_argument('--exchange', action='store_true', help='Include live Binance truth if client is configured')
    p.add_argument('--pretty', action='store_true', help='Pretty-print JSON')
    p.add_argument('--operator', help='Operator id/email for runbook action')
    p.add_argument('--ticket', help='Ticket/change id for runbook action')
    p.add_argument('--reason', default='', help='Free-form reason/note for runbook action')
    p.add_argument('--ttl-sec', type=int, default=0, help='Optional TTL override for hold/ack/renew')
    p.add_argument('--expected-sid', default='', help='Expected sid for guarded force release')
    return p


def main(argv=None) -> int:  # pragma: no cover
    args = _build_arg_parser().parse_args(argv)
    redis_obj = _redis_from_env()
    client = BinanceFuturesClient.from_env() if args.exchange else None
    diag = ActiveSymbolGuardDiagnostics(
        redis_obj,
        client=client,
        active_symbol_key_prefix=os.getenv('ORDERS_ACTIVE_SYMBOL_KEY_PREFIX', 'orders:active_symbol_sid:'),
        state_key_prefix=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'),
        state_ttl_sec=int(os.getenv('ORDERS_STATE_TTL_SEC', '86400')),
        tombstone_ttl_sec=int(os.getenv('ACTIVE_SYMBOL_GUARD_TOMBSTONE_TTL_SEC', '120')),
        stale_tombstone_ms=int(os.getenv('ACTIVE_SYMBOL_GUARD_STALE_TOMBSTONE_MS', '600000')),
        hot_symbol_limit=int(os.getenv('ACTIVE_SYMBOL_GUARD_EXPORTER_HOT_LIMIT', '10')),
    )
    runbook = ActiveSymbolGuardRunbookExecutor(redis_obj, diagnostics=diag, policy=ActiveSymbolGuardIncidentPolicyEngine(redis_obj, diag), client=client)
    if args.snapshot:
        payload = diag.snapshot()
    elif args.heatmap:
        payload = diag.heatmap()
    elif getattr(args, 'dashboard', False):
        # P13: operator audit dashboard
        payload = runbook.runbook_dashboard()
    elif getattr(args, 'ticket_history', None):
        # P13: ticket-linked audit history
        payload = {'ticket': args.ticket_history, 'history': runbook.audit_history(ticket=args.ticket_history, limit=100)}
    elif args.incident_symbol:
        payload = diag.incident_bundle_symbol(args.incident_symbol, include_exchange=bool(args.exchange))
    elif args.incident_sid:
        payload = diag.incident_bundle_sid(args.incident_sid, include_exchange=bool(args.exchange))
    elif args.triage_symbol:
        payload = ActiveSymbolGuardIncidentPolicyEngine(redis_obj, diag).triage_symbol(args.triage_symbol, include_exchange=bool(args.exchange))
    elif args.triage_sid:
        payload = ActiveSymbolGuardIncidentPolicyEngine(redis_obj, diag).triage_sid(args.triage_sid, include_exchange=bool(args.exchange))
    elif args.runbook_symbol:
        payload = runbook.runbook_state_symbol(args.runbook_symbol)
    elif args.runbook_sid:
        payload = runbook.runbook_state_sid(args.runbook_sid)
    elif args.apply_hold_symbol:
        payload = runbook.apply_hold_symbol(symbol=args.apply_hold_symbol, operator=args.operator or '', ticket=args.ticket or '', reason=args.reason or '', ttl_sec=args.ttl_sec or None)
    elif args.revoke_hold_symbol:
        payload = runbook.revoke_hold_symbol(symbol=args.revoke_hold_symbol, operator=args.operator or '', ticket=args.ticket or '', reason=args.reason or '')
    elif args.force_release_symbol:
        payload = runbook.guarded_force_release(symbol=args.force_release_symbol, operator=args.operator or '', ticket=args.ticket or '', expected_sid=args.expected_sid or '', reason=args.reason or '')
    elif args.ack_symbol:
        payload = runbook.escalation_ack(symbol=args.ack_symbol, operator=args.operator or '', ticket=args.ticket or '', reason=args.reason or '', ttl_sec=args.ttl_sec or None)
    elif args.renew_symbol:
        payload = runbook.escalation_renew(symbol=args.renew_symbol, operator=args.operator or '', ticket=args.ticket or '', reason=args.reason or '', ttl_sec=args.ttl_sec or None)
    elif args.symbol:
        payload = diag.debug_symbol(args.symbol, include_exchange=bool(args.exchange))
    else:
        payload = diag.debug_sid(args.sid, include_exchange=bool(args.exchange))
    print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None, sort_keys=bool(args.pretty)))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
