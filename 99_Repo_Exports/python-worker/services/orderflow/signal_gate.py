"""
SignalGate — единственная точка решения «публиковать ли сигнал».

Ранее эта логика была размазана по _pre_publish_allows_signal
_build_redis_dq_snapshot, _build_portfolio_risk_input
_refresh_quarantine_sid_cache, _persist_risk_decisions
внутри CryptoOrderflowService.

Три проверки в порядке fail-fast:
  1. Quarantine denylist  (EXEC_QUARANTINE_DENYLIST_ENABLE)
  2. Redis DQ hard veto   (TRADE_DQ_HARD_VETO_ENABLE)
  3. Portfolio risk       (TRADE_RISK_ENGINE_V2_ENABLE)

Всегда fail-open: любое неожиданное исключение → allow=True.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
from typing import Any, Dict, Optional, Set

logger = logging.getLogger("signal_gate")

# ── Опциональные импорты (те же try/except что были в сервисе) ────────────────

try:
    from services.redis_dq_policy import RedisDQSnapshot, RedisDQThresholds, evaluate_redis_dq
except Exception:
    try:
        from redis_dq_policy import RedisDQSnapshot, RedisDQThresholds, evaluate_redis_dq  # type: ignore
    except Exception:
        RedisDQSnapshot = RedisDQThresholds = evaluate_redis_dq = None  # type: ignore

try:
    from services.risk.risk_policy_engine import (
        PortfolioPosition, PortfolioRiskInput, PortfolioRiskLimits, evaluate_portfolio_risk
        infer_symbol_tier, RISK_DENY_HARD, RISK_DENY_SOFT, RISK_FORCE_FLATTEN
    )
except Exception:
    try:
        from risk.risk_policy_engine import (  # type: ignore
            PortfolioPosition, PortfolioRiskInput, PortfolioRiskLimits, evaluate_portfolio_risk
            infer_symbol_tier, RISK_DENY_HARD, RISK_DENY_SOFT, RISK_FORCE_FLATTEN
        )
    except Exception:
        try:
            from services.risk.portfolio_risk_engine import (  # type: ignore
                PortfolioPosition, PortfolioRiskInput, PortfolioRiskLimits, evaluate_portfolio_risk
                RISK_DENY_HARD, RISK_DENY_SOFT, RISK_FORCE_FLATTEN
            )
            infer_symbol_tier = None
        except Exception:
            try:
                from risk.portfolio_risk_engine import (  # type: ignore
                    PortfolioPosition, PortfolioRiskInput, PortfolioRiskLimits, evaluate_portfolio_risk
                    RISK_DENY_HARD, RISK_DENY_SOFT, RISK_FORCE_FLATTEN
                )
                infer_symbol_tier = None
            except Exception:
                PortfolioPosition = PortfolioRiskInput = PortfolioRiskLimits = None  # type: ignore
                evaluate_portfolio_risk = infer_symbol_tier = None  # type: ignore
                RISK_DENY_HARD = "DENY_HARD"
                RISK_DENY_SOFT = "DENY_SOFT"
                RISK_FORCE_FLATTEN = "FORCE_FLATTEN"

try:
    from services.risk.risk_audit_sql import RiskAuditSqlSink
except Exception:
    try:
        from risk.risk_audit_sql import RiskAuditSqlSink  # type: ignore
    except Exception:
        RiskAuditSqlSink = None  # type: ignore

try:
    from services.quarantine_denylist import check_signal_against_quarantine_cache
except Exception:
    try:
        from quarantine_denylist import check_signal_against_quarantine_cache  # type: ignore
    except Exception:
        check_signal_against_quarantine_cache = None  # type: ignore


def _utc_epoch_ms() -> int:
    from utils.time_utils import get_ny_time_millis
    return get_ny_time_millis()


def _runtime_ms(runtime: Any, *names: str) -> int:
    """Read first non-zero int from runtime attributes (in priority order)."""
    for name in names:
        try:
            v = int(getattr(runtime, name, 0) or 0)
            if v > 0:
                return v
        except Exception:
            continue
    return 0


def _ensure_audit_chain_fields(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Материализует signal_id / execution_plan_id / decision_id перед публикацией."""
    if not isinstance(signal, dict):
        return signal
    decision_id = str(signal.get("decision_id") or signal.get("id") or "").strip()
    signal_id = str(signal.get("signal_id") or decision_id or signal.get("sid") or "").strip()
    execution_plan_id = str(signal.get("execution_plan_id") or decision_id or signal_id or "").strip()
    if signal_id:
        signal["signal_id"] = signal_id
    if execution_plan_id:
        signal["execution_plan_id"] = execution_plan_id
    if decision_id:
        signal["decision_id"] = decision_id
    signal.setdefault("audit_chain_ver", "p5_execution_audit_v1")
    return signal


class SignalGate:
    """Решает, может ли сигнал быть опубликован.

    Создаётся в CryptoOrderflowService и инжектируется зависимостями —
    не лезет в атрибуты сервиса напрямую.
    """

    def __init__(
        self
        *
        redis_main: Any,                              # aioredis.Redis для quarantine cache
        publisher: Any,                               # AsyncSignalPublisher (для outbox_backlog)
        risk_limits: Optional[Any],                   # PortfolioRiskLimits | None
        dq_thresholds: Optional[Any],                 # RedisDQThresholds | None
        risk_hard_veto: bool = True
        risk_audit_sink: Optional[Any] = None
        quarantine_enable: bool = True
        quarantine_sids_key: str = "orders:quarantine:state:sids"
        quarantine_cache_ms: int = 1000
    ) -> None:
        self._redis = redis_main
        self._publisher = publisher
        self._risk_limits = risk_limits
        self._dq_thresholds = dq_thresholds
        self._risk_hard_veto = risk_hard_veto
        self._audit_sink = risk_audit_sink
        self._quarantine_enable = quarantine_enable
        self._quarantine_sids_key = quarantine_sids_key
        self._quarantine_cache_ms = quarantine_cache_ms

        self._sid_cache: Set[str] = set()
        self._sid_cache_ts_ms: int = 0

    @classmethod
    def from_service(cls, svc: Any) -> "SignalGate":
        """Фабрика из сервиса — принимает уже созданные объекты, не дублирует from_env()."""
        rc = svc._svc_cfg.risk
        return cls(
            redis_main=svc._pools.main
            publisher=svc.publisher
            risk_limits=svc.portfolio_risk_limits
            dq_thresholds=svc.redis_dq_thresholds
            risk_hard_veto=rc.risk_hard_veto
            risk_audit_sink=svc.risk_audit_sql_sink
            quarantine_enable=rc.quarantine_enable
            quarantine_sids_key=rc.quarantine_sids_key
            quarantine_cache_ms=rc.quarantine_cache_ms
        )

    # ── Public API ────────────────────────────────────────────────────────────

    async def allows(self, runtime: Any, signal: Dict[str, Any]) -> bool:
        """Возвращает True если сигнал разрешён к публикации.

        Мутирует signal: добавляет audit-поля, dq_snapshot, risk_snapshot.
        Всегда fail-open: исключение → True.
        """
        try:
            return await self._check(runtime, signal)
        except Exception as exc:
            logger.error("SignalGate.allows unexpected error (fail-open): %s", exc, exc_info=True)
            return True

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _check(self, runtime: Any, signal: Dict[str, Any]) -> bool:
        now_ms = _utc_epoch_ms()
        signal = _ensure_audit_chain_fields(signal)
        signal.setdefault("ts_event_ms", now_ms)
        signal["ts_publish_ms"] = now_ms

        if not await self._check_quarantine(signal, now_ms):
            return False
        if not self._check_dq(runtime, signal, now_ms):
            return False
        if not self._check_risk(runtime, signal):
            return False
        return True

    async def _check_quarantine(self, signal: Dict[str, Any], now_ms: int) -> bool:
        if not self._quarantine_enable or check_signal_against_quarantine_cache is None:
            return True
        await self._refresh_sid_cache(now_ms)
        deny = check_signal_against_quarantine_cache(signal, self._sid_cache)
        signal["quarantine_snapshot"] = deny.to_dict()
        if not deny.allowed:
            signal["quarantine_denylist_hit"] = True
            signal["quarantine_sid"] = str(deny.matched_sid)
            logger.warning(
                "\U0001f6ab (%s) Quarantine denylist veto: sid=%s candidates=%s"
                signal.get("symbol", "?"), deny.matched_sid, deny.candidates
            )
            return False
        return True

    def _check_dq(self, runtime: Any, signal: Dict[str, Any], now_ms: int) -> bool:
        if not self._dq_thresholds or evaluate_redis_dq is None:
            return True
        snap = self._build_dq_snapshot(runtime, now_ms)
        if snap is None:
            return True
        decision = evaluate_redis_dq(snap, self._dq_thresholds)
        signal["dq_snapshot"] = decision.to_dict()
        signal["dq_level"] = int(decision.level)
        signal["dq_hard_veto"] = not bool(decision.allow_trade_publish)
        if not decision.allow_trade_publish:
            logger.warning(
                "\U0001f6ab (%s) DQ hard veto: reasons=%s snapshot=%s"
                signal.get("symbol", "?"), decision.reasons, decision.snapshot
            )
            return False
        return True

    def _check_risk(self, runtime: Any, signal: Dict[str, Any]) -> bool:
        if not self._risk_limits or evaluate_portfolio_risk is None:
            return True
        risk_input = self._build_risk_input(runtime, signal)
        if risk_input is None:
            return True
        decision = evaluate_portfolio_risk(risk_input, self._risk_limits)
        signal["risk_snapshot"] = decision.to_dict()
        signal["risk_level"] = str(decision.level)
        signal["risk_leverage_cap"] = float(decision.leverage_cap)
        signal["risk_tier"] = str(decision.tier_policy.name)
        signal["symbol_tier"] = str(decision.tier_policy.name)
        signal["risk_min_confidence_required"] = float(decision.min_confidence_required)
        signal["risk_watchdog_timeout_ms"] = int(decision.watchdog_timeout_ms)
        signal["risk_maker_policy_allowed"] = bool(decision.maker_policy_allowed)
        signal["execution_policy"] = str(decision.effective_execution_policy)
        snap = decision.snapshot or {}
        signal["risk_decision_latency_ms"] = float(snap.get("decision_latency_ms") or 0.0)
        signal["risk_clamp_ratio"] = float(snap.get("clamp_ratio") or 0.0)
        self._persist_risk_decision(signal=signal, risk_input=risk_input, decision=decision)
        if decision.level in {RISK_DENY_SOFT, RISK_DENY_HARD, RISK_FORCE_FLATTEN} and self._risk_hard_veto:
            logger.warning(
                "\U0001f6ab (%s) Portfolio risk veto: level=%s reasons=%s"
                signal.get("symbol", "?"), decision.level, decision.reasons
            )
            return False
        if decision.allow_trade_publish and decision.adjusted_notional_usd > 0:
            signal["planned_notional_usd"] = float(decision.adjusted_notional_usd)
        return True

    # ── Builders ──────────────────────────────────────────────────────────────

    def _build_dq_snapshot(self, runtime: Any, now_ms: int) -> Optional[Any]:
        if RedisDQSnapshot is None:
            return None
        last_tick_ms = _runtime_ms(
            runtime
            "last_tick_ts_ms"
            "last_ts_ms"
            "last_tick_ts"
        )
        last_book_ms = _runtime_ms(
            runtime
            "last_book_ts_ms"
            "last_book_ts"
        )
        tick_stale = max(0, now_ms - last_tick_ms) if last_tick_ms > 0 else 0
        book_stale = max(0, now_ms - last_book_ms) if last_book_ms > 0 else 0
        outbox_backlog = 0
        try:
            q = getattr(self._publisher, "_retry_queue", None)
            if q is not None and hasattr(q, "qsize"):
                outbox_backlog = int(q.qsize())
        except Exception:
            pass
        return RedisDQSnapshot(
            symbol=str(getattr(runtime, "symbol", "") or "UNKNOWN")
            queue_lag_ms=tick_stale
            tick_staleness_ms=tick_stale
            book_staleness_ms=book_stale
            redis_timeout_events=int(getattr(runtime, "redis_timeout_events", 0) or 0)
            negative_age_events=int(getattr(runtime, "negative_age_events", 0) or 0)
            xack_fail_events=int(getattr(runtime, "xack_fail_events", 0) or 0)
            outbox_backlog=outbox_backlog
            stream_timeout_burst=int(getattr(runtime, "stream_timeout_burst", 0) or 0)
            force_hard_veto=bool(getattr(runtime, "force_hard_veto", False))
        )

    def _build_risk_input(self, runtime: Any, signal: Dict[str, Any]) -> Optional[Any]:
        if PortfolioRiskInput is None:
            return None
        positions = []
        for p in (signal.get("portfolio_positions") or []):
            if not isinstance(p, dict):
                continue
            try:
                positions.append(PortfolioPosition(
                    symbol=str(p.get("symbol") or "")
                    notional_usd=float(p.get("notional_usd") or 0.0)
                    side=str(p.get("side") or "LONG")
                    cluster=str(p.get("cluster") or "default")
                    tier=str(p.get("tier") or "B")
                    beta=float(p.get("beta") or 1.0)
                ))
            except Exception:
                continue

        symbol = str(signal.get("symbol") or getattr(runtime, "symbol", "") or "")
        tier = str(signal.get("symbol_tier") or signal.get("tier") or "").strip().upper()
        if (not tier or tier not in {"A", "B", "C"}) and infer_symbol_tier is not None:
            tier = infer_symbol_tier(symbol, self._risk_limits)

        def _f(*keys: str, default: float = 0.0) -> float:
            for k in keys:
                v = signal.get(k)
                if v is not None:
                    return float(v)
            return default

        return PortfolioRiskInput(
            symbol=symbol
            cluster=str(
                signal.get("risk_cluster")
                or signal.get("cluster")
                or signal.get("symbol")
                or getattr(runtime, "symbol", "")
            )
            tier=tier or "B"
            requested_notional_usd=_f("planned_notional_usd", "notional_usd")
            current_positions=positions
            equity_usd=_f("account_equity_usd"
                          default=float(os.getenv("ACCOUNT_DEPOSIT_USD", "0") or 0))
            daily_pnl_pct=_f("daily_pnl_pct")
            stop_distance_bps=_f("stop_distance_bps", "planned_stop_distance_bps", "sl_bps", "stop_bps")
            volatility_bps=_f("volatility_bps", "atr_bps", "realized_vol_bps")
            spread_bps=_f("spread_bps")
            expected_slippage_bps=_f("expected_slippage_bps", "slippage_bps")
            expected_edge_bps=_f("expected_edge_bps", "edge_bps")
            fee_bps=_f("fee_bps", "estimated_fee_bps")
            confidence=_f("confidence", "signal_confidence")
            maker_policy_requested=bool(
                signal.get("maker_policy_requested")
                or signal.get("prefer_maker")
                or str(signal.get("execution_policy") or "").strip().upper() == "MAKER_FIRST"
            )
            infra_degraded=bool(signal.get("infra_degraded") or signal.get("dq_hard_veto"))
            high_vol=bool(signal.get("high_vol") or signal.get("regime_high_vol"))
            kill_switch=bool(signal.get("risk_kill_switch") or signal.get("kill_switch"))
            net_beta=_f("net_beta", "beta", default=1.0)
            leader_symbol=str(signal.get("leader_symbol") or "BTCUSDT")
            leader_drawdown_bps=_f("leader_drawdown_bps")
            news_blackout=bool(
                signal.get("news_blackout")
                or signal.get("high_vol_blackout")
                or signal.get("news_blackout_active")
            )
            shadow_only=bool(signal.get("shadow_only") or signal.get("paper_only"))
        )

    # ── Side effects ──────────────────────────────────────────────────────────

    async def _refresh_sid_cache(self, now_ms: int) -> None:
        if not self._quarantine_sids_key:
            return
        if (now_ms - self._sid_cache_ts_ms) < self._quarantine_cache_ms:
            return
        try:
            values = await self._redis.smembers(self._quarantine_sids_key)
            self._sid_cache = {str(v) for v in (values or set()) if str(v)}
            self._sid_cache_ts_ms = now_ms
        except Exception:
            pass

    def _persist_risk_decision(
        self
        *
        signal: Dict[str, Any]
        risk_input: Any
        decision: Any
    ) -> None:
        if not self._audit_sink:
            return
        try:
            import uuid
            from utils.task_manager import safe_create_task
            decision_id = str(signal.get("decision_id") or signal.get("id") or uuid.uuid4().hex)
            signal["decision_id"] = decision_id

            async def _bg():
                try:
                    await asyncio.to_thread(
                        self._audit_sink.record_decision
                        decision_id=decision_id
                        signal=signal
                        risk_input=risk_input
                        risk_decision=decision
                    )
                except Exception:
                    pass

            safe_create_task(_bg(), name=f"risk-audit-{signal.get('symbol', '?')}")
        except Exception as exc:
            if random.random() < 0.01:
                logger.warning("Risk SQL audit fire failed: %s", exc)
