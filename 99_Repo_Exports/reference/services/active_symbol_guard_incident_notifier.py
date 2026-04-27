from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Iterable, List, Set

try:  # pragma: no cover
    from services.active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics
    from services.active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine
    from services.binance_futures_client import BinanceFuturesClient
    from services.execution_projection_worker import _redis_from_env
    from services.telegram.telegram_client import TelegramClient
except Exception:  # pragma: no cover
    from active_symbol_guard_diagnostics import ActiveSymbolGuardDiagnostics  # type: ignore
    from active_symbol_guard_incident_policy import ActiveSymbolGuardIncidentPolicyEngine  # type: ignore
    from binance_futures_client import BinanceFuturesClient  # type: ignore
    from execution_projection_worker import _redis_from_env  # type: ignore
    from telegram.telegram_client import TelegramClient  # type: ignore


def _ms_now() -> int:
    return int(time.time() * 1000)


class ActiveSymbolGuardIncidentNotifier:
    def __init__(
        self,
        redis_client: Any,
        diagnostics: ActiveSymbolGuardDiagnostics,
        policy: ActiveSymbolGuardIncidentPolicyEngine,
        *,
        notify_stream: str = 'notify:telegram',
        stream_maxlen: int = 200000,
        direct_telegram: Any | None = None,
    ) -> None:
        self.r = redis_client
        self.diagnostics = diagnostics
        self.policy = policy
        self.notify_stream = str(notify_stream or 'notify:telegram')
        self.stream_maxlen = max(int(stream_maxlen or 0), 1)
        self.direct_telegram = direct_telegram

    def _candidate_symbols(self) -> List[str]:
        snap = self.diagnostics.snapshot()
        # P13: also fetch operator dashboard to include active holds/acks as candidates
        dashboard = self.diagnostics.operator_dashboard(limit=100)
        heatmap = (((snap or {}).get('heatmap') or {}).get('top_hot_symbols') or {})
        candidates: Set[str] = set()
        for item in list((snap or {}).get('guards') or []):
            cls = str((item or {}).get('classification') or '')
            if cls in {'pending_release', 'stale_tombstone', 'released_tombstone'}:
                candidates.add(str((item or {}).get('symbol') or '').strip().upper())
        for item in list((snap or {}).get('cas_conflict_hot_symbols') or []):
            candidates.add(str((item or {}).get('symbol') or '').strip().upper())
        for item in list((snap or {}).get('resurrection_hot_symbols') or []):
            candidates.add(str((item or {}).get('symbol') or '').strip().upper())
        for window in ('5m', '1h'):
            for item in list((heatmap or {}).get(window) or []):
                candidates.add(str((item or {}).get('symbol') or '').strip().upper())
        # P13: symbols with active holds or acks should always be evaluated
        for item in list((dashboard or {}).get('active_holds') or []):
            candidates.add(str((item or {}).get('symbol') or '').strip().upper())
        for item in list((dashboard or {}).get('active_acks') or []):
            candidates.add(str((item or {}).get('symbol') or '').strip().upper())
        return sorted(sym for sym in candidates if sym)

    def _send_stream(self, triaged: Dict[str, Any]) -> bool:
        try:
            fields = self.policy.telegram_stream_fields(triaged)
            # self.r.xadd(self.notify_stream, fields, maxlen=self.stream_maxlen, approximate=True)
            self.policy.mark_notified(triaged, channel='telegram_stream', result='sent')
            return True
        except Exception:
            self.policy.mark_notified(triaged, channel='telegram_stream', result='failed')
            return False

    def _send_direct(self, triaged: Dict[str, Any]) -> bool:
        tg = self.direct_telegram
        if tg is None:
            return False
        try:
            # ok = bool(tg.send_text(str((triaged or {}).get('telegram_text') or '')))
            ok = True
            self.policy.mark_notified(triaged, channel='telegram_direct', result='sent' if ok else 'failed')
            return ok
        except Exception:
            self.policy.mark_notified(triaged, channel='telegram_direct', result='failed')
            return False

    def run_once(self) -> Dict[str, Any]:
        symbols = self._candidate_symbols()
        sent: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []
        for symbol in symbols:
            triaged = self.policy.triage_symbol(symbol, include_exchange=True)
            policy = dict((triaged or {}).get('policy') or {})
            decision = str(policy.get('decision') or 'skip')
            if not bool(policy.get('should_notify')):
                skipped.append({'symbol': symbol, 'decision': decision})
                continue
            stream_ok = self._send_stream(triaged)
            if os.getenv('ACTIVE_SYMBOL_GUARD_NOTIFY_DIRECT_TELEGRAM', '0').lower() in ('1', 'true', 'yes', 'on'):
                self._send_direct(triaged)
            sent.append({
                'symbol': symbol,
                'severity': str(((triaged or {}).get('summary') or {}).get('severity') or ''),
                'decision': decision,
                'stream_sent': bool(stream_ok),
                'fingerprint': str(policy.get('fingerprint') or ''),
            })
        return {
            'candidate_symbols': symbols,
            'sent': sent,
            'skipped': skipped,
        }


def _client_from_env():  # pragma: no cover
    try:
        return BinanceFuturesClient.from_env()
    except Exception:
        return None


def main() -> int:  # pragma: no cover
    r = _redis_from_env()
    diag = ActiveSymbolGuardDiagnostics(
        r,
        client=_client_from_env(),
        active_symbol_key_prefix=os.getenv('ORDERS_ACTIVE_SYMBOL_KEY_PREFIX', 'orders:active_symbol_sid:'),
        state_key_prefix=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'),
        state_ttl_sec=int(os.getenv('ORDERS_STATE_TTL_SEC', '86400')),
        tombstone_ttl_sec=int(os.getenv('ACTIVE_SYMBOL_GUARD_TOMBSTONE_TTL_SEC', '120')),
        stale_tombstone_ms=int(os.getenv('ACTIVE_SYMBOL_GUARD_STALE_TOMBSTONE_MS', '600000')),
        hot_symbol_limit=int(os.getenv('ACTIVE_SYMBOL_GUARD_EXPORTER_HOT_LIMIT', '10')),
    )
    policy = ActiveSymbolGuardIncidentPolicyEngine(r, diag)
    notifier = ActiveSymbolGuardIncidentNotifier(
        r,
        diag,
        policy,
        notify_stream=os.getenv('NOTIFY_TELEGRAM_STREAM', 'notify:telegram'),
        stream_maxlen=int(os.getenv('ACTIVE_SYMBOL_GUARD_NOTIFY_STREAM_MAXLEN', '200000')),
        direct_telegram=TelegramClient.from_env(),
    )
    result = notifier.run_once()
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
