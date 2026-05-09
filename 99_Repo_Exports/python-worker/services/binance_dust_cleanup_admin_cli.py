#!/usr/bin/env python3
from __future__ import annotations

"""CLI for Binance dust cleanup admin control and ACK workflow.

Usage examples::

  # Show current denylist/cooldown state
  python -m services.binance_dust_cleanup_admin_cli --show-state --pretty

  # Acknowledge a stale denylist reminder
  python -m services.binance_dust_cleanup_admin_cli \\
    --ack-reminder --ack-kind old_denylist --ack-symbol APTUSDT \\
    --operator alice --reason investigating --ticket INC-42 --ttl-sec 1800

  # Renew an existing ACK
  python -m services.binance_dust_cleanup_admin_cli \\
    --renew-ack --ack-kind old_denylist --ack-symbol APTUSDT \\
    --operator alice --reason still-investigating --ticket INC-42 --ttl-sec 3600

  # Revoke an ACK (re-enables reminders)
  python -m services.binance_dust_cleanup_admin_cli \\
    --revoke-ack --ack-kind old_denylist --ack-symbol APTUSDT \\
    --operator alice --reason resolved --ticket INC-42

  # Show ACK dashboard
  python -m services.binance_dust_cleanup_admin_cli --show-ack-dashboard --pretty
"""

import argparse
import json
import os
import sys
from typing import Any

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(CURRENT_DIR)
for _p in (REPO_ROOT,):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

from services.binance_dust_cleanup_admin import BinanceDustCleanupAdmin
from services.binance_dust_cleanup_admin_ack import (
    ack_dashboard,
    ack_reminder,
    renew_reminder_ack,
    revoke_reminder_ack,
)


def _dump(doc: Any, pretty: bool) -> None:
    """Pretty-print or compact-print a dict to stdout."""
    if pretty:
        print(json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(json.dumps(doc, ensure_ascii=False, separators=(',', ':')))


def main() -> int:
    ap = argparse.ArgumentParser(description='Manual admin control for Binance dust cleanup worker.')
    ap.add_argument('--pretty', action='store_true')

    # ── existing state/audit commands ─────────────────────────────────────
    ap.add_argument('--show-state', action='store_true')
    ap.add_argument('--show-symbol')
    ap.add_argument('--show-audit', action='store_true')
    ap.add_argument('--audit-symbol')
    ap.add_argument('--limit', type=int, default=50)

    # ── existing mutation commands ─────────────────────────────────────────
    ap.add_argument('--add-denylist-symbol')
    ap.add_argument('--remove-denylist-symbol')
    ap.add_argument('--clear-cooldown-symbol')
    ap.add_argument('--ttl-sec', type=int, default=0)
    ap.add_argument('--operator', default='')
    ap.add_argument('--reason', default='')
    ap.add_argument('--ticket', default='')

    # ── P14: ACK workflow commands ─────────────────────────────────────────
    ap.add_argument('--show-ack-dashboard', action='store_true',
                    help='Show all active reminder ACKs from Redis')
    ap.add_argument('--ack-kind', default='',
                    help='ACK kind, e.g. old_denylist or cooldown_loop')
    ap.add_argument('--ack-symbol', default='',
                    help='Symbol to ACK/renew/revoke (e.g. APTUSDT)')
    ap.add_argument('--ack-reminder', action='store_true',
                    help='Create or overwrite a reminder ACK')
    ap.add_argument('--renew-ack', action='store_true',
                    help='Extend TTL of an existing reminder ACK')
    ap.add_argument('--revoke-ack', action='store_true',
                    help='Delete an existing reminder ACK (re-enables notifications)')
    ap.add_argument('--fingerprint', default='',
                    help='Optional fingerprint for ACK mismatch detection')

    ns = ap.parse_args()

    # Build admin (lazily initialises Redis from ENV)
    admin = BinanceDustCleanupAdmin()
    # Reuse the same redis client for ACK commands
    redis_client = admin.r  # type: ignore[attr-defined]

    # ── existing commands ──────────────────────────────────────────────────
    if ns.show_state:
        _dump(admin.current_state(), ns.pretty)
        return 0
    if ns.show_symbol:
        _dump(admin.symbol_state(ns.show_symbol), ns.pretty)
        return 0
    if ns.show_audit:
        _dump(admin.recent_audit(symbol=ns.audit_symbol, limit=ns.limit), ns.pretty),
        return 0,
    if ns.add_denylist_symbol:
        _dump(admin.add_denylist_symbol(ns.add_denylist_symbol, operator=ns.operator, reason=ns.reason, ticket=ns.ticket, ttl_sec=ns.ttl_sec), ns.pretty),
        return 0,
    if ns.remove_denylist_symbol:
        _dump(admin.remove_denylist_symbol(ns.remove_denylist_symbol, operator=ns.operator, reason=ns.reason, ticket=ns.ticket), ns.pretty),
        return 0,
    if ns.clear_cooldown_symbol:
        _dump(admin.clear_cooldown(ns.clear_cooldown_symbol, operator=ns.operator, reason=ns.reason, ticket=ns.ticket), ns.pretty),
        return 0,

    # ── P14: ACK workflow commands ─────────────────────────────────────────
    if ns.show_ack_dashboard:
        _dump(ack_dashboard(redis_client, limit=max(ns.limit, 50)), ns.pretty),
        return 0,
    if ns.ack_reminder:
        _dump(
            ack_reminder(
                redis_client,
                kind=ns.ack_kind,
                symbol=ns.ack_symbol,
                operator=ns.operator,
                reason=ns.reason,
                ticket=ns.ticket,
                ttl_sec=ns.ttl_sec if ns.ttl_sec else 1800,
                fingerprint=ns.fingerprint,
            ),
            ns.pretty,
        )
        return 0
    if ns.renew_ack:
        _dump(
            renew_reminder_ack(
                redis_client,
                kind=ns.ack_kind,
                symbol=ns.ack_symbol,
                operator=ns.operator,
                reason=ns.reason,
                ticket=ns.ticket,
                ttl_sec=ns.ttl_sec if ns.ttl_sec else 1800,
            ),
            ns.pretty,
        )
        return 0
    if ns.revoke_ack:
        _dump(
            revoke_reminder_ack(
                redis_client,
                kind=ns.ack_kind,
                symbol=ns.ack_symbol,
                operator=ns.operator,
                reason=ns.reason,
                ticket=ns.ticket,
            ),
            ns.pretty,
        )
        return 0

    ap.error('no_action_selected')
    return 2


if __name__ == '__main__':
    raise SystemExit(main())
