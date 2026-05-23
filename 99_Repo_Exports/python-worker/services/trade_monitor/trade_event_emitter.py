# services/trade_monitor/trade_event_emitter.py
"""
Event emission helpers: append_event, AB/backtest event logging,
trailing audit stream, io_tasks runner, periodic reports trigger.

Extracted from TradeMonitorService methods:
  _log_ab_closed_event  (monolith lines 5124-5512)
  _emit_trailing_audit   (2469-2498)
  _pvd_record_closed     (2305-2309)
  _maybe_paper_vs_demo_report / _send_paper_vs_demo_report  (2311-2464)
  _run_io_tasks          (1964-1969)
  _safe_trigger_report   (990-1003)

Design:
  - All methods are fail-open (exceptions caught and logged, never raised).
  - No locks inside this class — locking is the caller's responsibility.
  - Injected dependencies; no direct Redis/DB construction.
"""
from __future__ import annotations

import contextlib
import logging
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from utils.time_utils import get_ny_time_millis

if TYPE_CHECKING:
    from domain.models import TradeClosed, TradeEvent
    from domain.models import PositionState

logger = logging.getLogger(__name__)


class TradeEventEmitter:
    """
    Thin facade over repo.append_event + TradeEventsLogger.

    Args:
        repo          — RedisTradeRepository.
        events_logger — TradeEventsLogger (may be None; methods become no-ops).
        redis         — Redis client (for trailing audit stream).
        trailing_audit_stream — Redis stream key for trailing audit (may be "").
        trailing_audit_maxlen — MAXLEN for the audit stream.
    """

    def __init__(
        self,
        repo: Any,
        events_logger: Any | None = None,
        *,
        redis: Any = None,
        trailing_audit_stream: str = "",
        trailing_audit_maxlen: int = 200_000,
        log: logging.Logger | None = None,
    ) -> None:
        self._repo = repo
        self._events_logger = events_logger
        self._redis = redis
        self._trailing_audit_stream = trailing_audit_stream
        self._trailing_audit_maxlen = trailing_audit_maxlen
        self._logger = log or logger

    # ------------------------------------------------------------------
    # Core event emission
    # ------------------------------------------------------------------

    def emit_event(self, ev: "TradeEvent") -> None:
        """Append a single TradeEvent to the repo stream. Fail-open."""
        try:
            self._repo.append_event(ev)
        except Exception as e:
            self._logger.warning("⚠️ append_event failed (%s): %s", getattr(ev, "event_type", "?"), e)

    # ------------------------------------------------------------------
    # IO task runner
    # ------------------------------------------------------------------

    def run_io_tasks(self, tasks: list[Any]) -> None:
        """
        Execute a list of _IOTask (fn, desc) outside the global lock.
        Fail-open on each task.
        """
        for t in tasks:
            try:
                t.fn()
            except Exception as e:
                self._logger.warning("⚠️ IO task failed: %s (%s)", t.desc, e)

    # ------------------------------------------------------------------
    # Periodic report trigger
    # ------------------------------------------------------------------

    def trigger_report(
        self,
        source: str,
        symbol: str,
        counter_type: str,
        order_id: str,
        *,
        demo_only: bool = False,
    ) -> None:
        """
        Fire check_and_trigger_report for a closed trade.  Fail-open.
        """
        try:
            from services.periodic_reporter import check_and_trigger_report  # lazy

            check_and_trigger_report(
                source, symbol, counter_type=counter_type, order_id=order_id
            )
        except Exception as e:
            self._logger.warning("⚠️ Ошибка при триггере отчёта: %s", e)

    # ------------------------------------------------------------------
    # Trailing audit stream
    # ------------------------------------------------------------------

    def emit_trailing_audit(
        self,
        event_type: str,
        pos: "PositionState",
        new_sl: float,
        prev_sl: float,
        ts_ms: int,
    ) -> None:
        """
        Emit a trailing event to the unified audit stream.
        Fail-open; never breaks trade execution.
        """
        if not self._trailing_audit_stream or not self._redis:
            return
        try:
            self._redis.xadd(
                self._trailing_audit_stream,
                {
                    "source": "trade_monitor",
                    "event_type": event_type,
                    "sid": str(getattr(pos, "sid", "") or ""),
                    "symbol": str(getattr(pos, "symbol", "") or ""),
                    "direction": str(getattr(pos, "direction", "") or ""),
                    "prev_sl": str(prev_sl),
                    "new_sl": str(new_sl),
                    "entry_price": str(getattr(pos, "entry_price", 0.0)),
                    "trailing_distance": str(getattr(pos, "trailing_distance", 0.0)),
                    "ts_ms": str(ts_ms),
                },
                maxlen=self._trailing_audit_maxlen,
            )
        except Exception:
            pass  # fail-open: never break trade execution

    # ------------------------------------------------------------------
    # AB / backtest event logging
    # ------------------------------------------------------------------

    def emit_ab_closed(
        self,
        pos: "PositionState",
        closed: "TradeClosed",
        close_reason: str,
        *,
        get_spec_fn: Any = None,
    ) -> None:
        """
        Log a closed position to TradeEventsLogger for AB/backtest evaluation.

        Ported verbatim from monolith _log_ab_closed_event (lines 5124-5512).
        Fail-open throughout.
        """
        if not self._events_logger:
            return

        try:
            self._emit_ab_closed_impl(pos, closed, close_reason, get_spec_fn=get_spec_fn)
        except Exception:
            pass  # outermost safety net

    def _emit_ab_closed_impl(
        self,
        pos: "PositionState",
        closed: "TradeClosed",
        close_reason: str,
        *,
        get_spec_fn: Any = None,
    ) -> None:
        """Internal implementation — may raise; caller wraps in try/except."""
        from domain.evidence_keys import MetaKeys
        from services.horizon_contract import build_horizon_event_scalars

        md: dict[str, Any] = {}
        sp = getattr(pos, "signal_payload", None)
        if isinstance(sp, dict):
            for k in ("ab_arm", "ab_group", "ab_key", "ab_ver", "arm_ver",
                      "regime", "zone_id", "zone_type", "bundle", "decision", "leader"):
                if k in sp:
                    md[k] = sp.get(k)

        # ------ Risk/PnL R calculation ------
        extra: dict[str, Any] = {}
        try:
            risk_usd = 0.0
            # 1. Try pos.risk_usd
            with contextlib.suppress(Exception):
                ru = getattr(pos, "risk_usd", None)
                if ru is not None and float(ru) > 0:
                    risk_usd = float(ru)

            # 2. Try spec.risk_money
            if risk_usd <= 1e-9 and get_spec_fn is not None:
                with contextlib.suppress(Exception):
                    spec = get_spec_fn(pos.symbol)
                    if spec:
                        side = str(getattr(pos, "direction", "") or "").upper()
                        risk_usd = float(
                            spec.risk_money(
                                float(pos.entry_price or 0.0),
                                float(pos.sl or 0.0),
                                float(pos.lot or 0.0),
                                side,
                                str(pos.symbol or ""),
                            ) or 0.0
                        )

            # 3. Fallback
            if risk_usd <= 1e-9:
                with contextlib.suppress(Exception):
                    risk_usd = float(
                        abs(float(pos.entry_price or 0.0) - float(pos.sl or 0.0))
                        * float(pos.lot or 0.0)
                    )

            ab_arm = ab_group = rg = ""
            sp_dict = getattr(pos, "signal_payload", None) or {}
            if isinstance(sp_dict, dict):
                ab_arm = sp_dict.get("ab_arm") or ""
                ab_group = sp_dict.get("ab_group") or ""
                rg = sp_dict.get("regime") or ""

            extra = {
                "risk_usd": float(risk_usd),
                "ab_arm": ab_arm,
                "ab_group": ab_group,
                "regime": rg or "na",
            }

            if isinstance(sp_dict, dict):
                ab = sp_dict.get("ab", {}) if isinstance(sp_dict, dict) else {}
                ctx = sp_dict.get("ctx", {}) if isinstance(sp_dict, dict) else {}
                dec = sp_dict.get("decision", "na") if isinstance(sp_dict, dict) else "na"
                pnl_usd = float(getattr(closed, "pnl_net", 0.0) or 0.0)
                r_usd = float(risk_usd or getattr(pos, "risk_usd", 0.0) or 0.0)
                extra.update({
                    "ab_arm": (ab.get("arm", getattr(pos, "ab_arm", "A"))).upper(),
                    "ab_group": (ab.get("group", getattr(pos, "ab_group", "default"))).lower(),
                    "ab_key": (ab.get("key", getattr(pos, "ab_key", ""))),
                    "arm_ver": int(ab.get("arm_ver", getattr(pos, "arm_ver", 0))),
                    "ab_split_reason": (ab.get("split_reason", "")),
                    "scenario": str(dec).lower(),
                    "regime": (ctx.get("regime", getattr(pos, "regime", "na"))).lower(),
                    "entry_adx_q": float(ctx.get("adx_q", 0.5) or 0.5),
                    "entry_spread_z": float(ctx.get("spread_z", 0.0) or 0.0),
                    "entry_pressure_sps": float(ctx.get("pressure_sps", 0.0) or 0.0),
                    "entry_cooldown_sps": float(ctx.get("cooldown_sps", 0.0) or 0.0),
                    "entry_obi_age_ms": int(ctx.get("obi_age_ms", 0) or 0),
                    "entry_abs_th_unstable": int(ctx.get("abs_th_unstable", 0) or 0),
                    "entry_news_blocked": int(ctx.get("news_blocked", 0) or 0),
                    "risk_usd": r_usd,
                    "signal_payload": sp_dict,
                })
                pol = sp_dict.get("policy") or {}
                if isinstance(pol, dict):
                    extra.update({
                        "abs_lvl_tier": int(pol.get("abs_lvl_tier", -1)),
                        "dn_tier": int(pol.get("dn_tier", -1)),
                        "book_health_ok": int(pol.get("book_health_ok", -1)),
                        "of_confirm_ok": int(pol.get("of_confirm_ok", 0)),
                        "of_confirm_score": float(pol.get("of_confirm_score", 0.0)),
                        "spread_bp": float(pol.get("spread_bp", 0.0)),
                        "book_age_ms": int(pol.get("book_age_ms", 0)),
                        "book_rate_hz": float(pol.get("book_rate_hz", 0.0)),
                    })
                if r_usd > 1e-9:
                    r_val = float(pnl_usd / r_usd)
                    md["pnl_r"] = r_val
                    extra["r_mult"] = r_val
                    md["risk_usd"] = r_usd
        except Exception:
            extra = {}

        # ------ Extract all AB/indicator fields ------
        ab_arm = ab_group = ab_key = ""
        arm_ver = 0
        regime = regime_group = "na"
        scenario = scenario_v4 = ""
        risk_usd = 0.0
        abs_lvl_tier = dn_tier = book_health_ok = book_age_ms = of_confirm_ok = -1
        spread_bp = of_confirm_score = atr_bps_exec = atr_unified_th_bps = -1.0
        atr_floor_th_bps = atr_fees_th_bps = -1.0
        meta_enforce_applied = None
        meta_veto = 0
        meta_enforce_key = ""
        meta_enforce_salt = "enf_v1"

        try:
            sp_dict = getattr(pos, "signal_payload", None)
            if isinstance(sp_dict, dict):
                ab_arm = (sp_dict.get("ab_arm", "") or "")
                ab_group = (sp_dict.get("ab_group", "") or "")
                ab_key = (sp_dict.get("ab_key", "") or "")
                arm_ver = int(sp_dict.get("arm_ver", 0) or 0)
                regime = str(sp_dict.get("regime", (sp_dict.get("ctx") or {}).get("regime", "na")) or "na")
                regime_group = str(
                    sp_dict.get("regime_group")
                    or (sp_dict.get("ctx") or {}).get("regime_group")
                    or (sp_dict.get("indicators") or {}).get("regime_group")
                    or regime or "na"
                )
                raw_scenario = str(
                    sp_dict.get("scenario") or sp_dict.get("decision")
                    or sp_dict.get("strong_gate_scn")
                    or (sp_dict.get("of") or {}).get("strong_gate_scn") or ""
                ).lower()
                if raw_scenario not in ("continuation", "reversal"):
                    with contextlib.suppress(Exception):
                        from core.autopilot_fields import normalize_scenario
                        raw_scenario = normalize_scenario(raw_scenario)
                scenario = raw_scenario

                with contextlib.suppress(Exception):
                    of_dict = sp_dict.get("of") or {}
                    if isinstance(of_dict, dict):
                        ev_dict = of_dict.get("evidence") or {}
                        if isinstance(ev_dict, dict):
                            scenario_v4 = (ev_dict.get("scenario_v4", "") or "")

                risk_usd = float(sp_dict.get("risk_usd", 0.0) or 0.0)

                ind = (
                    sp_dict.get("indicators")
                    or (sp_dict.get("config_snapshot") or {}).get("indicators")
                    or {}
                )
                if isinstance(ind, dict):
                    abs_lvl_tier = int(ind.get("abs_lvl_tier", ind.get("abs_lvl_tier_used", -1)) or -1)
                    dn_tier = int(ind.get("dn_tier", -1) or -1)
                    book_health_ok = int(ind.get("book_health_ok", -1) or -1)
                    book_age_ms = int(ind.get("book_age_ms", -1) or -1)
                    spread_bp = float(ind.get("spread_bp", ind.get("spread_bps", -1.0)) or -1.0)
                    of_confirm_ok = int(ind.get("of_confirm_ok", ind.get("strong_gate_ok", -1)) or -1)
                    of_confirm_score = float(ind.get("of_confirm_score", -1.0) or -1.0)
                    atr_bps_exec = float(ind.get("atr_bps_exec", -1.0) or -1.0)
                    atr_unified_th_bps = float(ind.get("atr_unified_th_bps", -1.0) or -1.0)
                    atr_floor_th_bps = float(ind.get("atr_floor_th_bps", -1.0) or -1.0)
                    atr_fees_th_bps = float(ind.get("atr_fees_th_bps", -1.0) or -1.0)

                    with contextlib.suppress(Exception):
                        of_dict = sp_dict.get("of") or {}
                        if isinstance(of_dict, dict):
                            evidence = of_dict.get("evidence") or {}
                            if isinstance(evidence, dict):
                                meta_enforce_applied = int(evidence.get(MetaKeys.ENFORCE_APPLIED, 0) or 0)
                                meta_veto = int(evidence.get(MetaKeys.VETO, 0) or 0)
                                meta_enforce_key = (evidence.get(MetaKeys.ENFORCE_KEY, "") or "")
                                meta_enforce_salt = (evidence.get(MetaKeys.ENFORCE_SALT, "enf_v1") or "enf_v1")
                        if meta_enforce_applied is None and isinstance(ind, dict):
                            meta_enforce_applied = int(ind.get(MetaKeys.ENFORCE_APPLIED, 0) or 0)
                            if meta_veto == 0:
                                meta_veto = int(ind.get(MetaKeys.VETO, 0) or 0)
                            if not meta_enforce_key:
                                meta_enforce_key = (ind.get(MetaKeys.ENFORCE_KEY, "") or "")
                            if meta_enforce_salt == "enf_v1":
                                meta_enforce_salt = (ind.get(MetaKeys.ENFORCE_SALT, "enf_v1") or "enf_v1")

            if risk_usd <= 0:
                risk_usd = float(getattr(pos, "risk_usd", 0.0) or 0.0)
        except Exception:
            pass

        r_mult = 0.0
        with contextlib.suppress(Exception):
            if risk_usd > 0:
                r_mult = float(getattr(closed, "pnl_net", 0.0) or 0.0) / float(risk_usd)

        exit_ts_ms = 0
        try:
            exit_ts_ms = int(
                getattr(closed, "exit_ts_ms", None) or getattr(pos, "exit_ts_ms", None) or 0
            )
            if exit_ts_ms <= 0:
                from datetime import datetime
                ca = getattr(closed, "closed_at", None)
                if ca:
                    if isinstance(ca, (int, float)):
                        exit_ts_ms = int(ca)
                    elif isinstance(ca, datetime):
                        exit_ts_ms = int(ca.timestamp() * 1000)
                if exit_ts_ms <= 0:
                    exit_ts_ms = get_ny_time_millis()
        except Exception:
            exit_ts_ms = get_ny_time_millis()

        order_id = ""
        fee_bps = 0.0
        with contextlib.suppress(Exception):
            order_id = str(
                getattr(closed, "order_id", None)
                or getattr(closed, "exit_order_id", None)
                or getattr(pos, "close_order_id", None)
                or getattr(pos, "order_id", None)
                or getattr(pos, "id", "") or ""
            )
        with contextlib.suppress(Exception):
            fees_usd_val = float(getattr(closed, "fees", 0.0) or getattr(pos, "fees", 0.0) or 0.0)
            turnover_val = float(getattr(closed, "turnover_roundtrip", 0.0) or 0.0)
            if turnover_val > 0:
                fee_bps = (fees_usd_val / turnover_val) * 10_000.0  # type: ignore

        _exit_mid = float(getattr(pos, "exit_mid_price", 0.0) or 0.0)
        _exit_spread_bps = float(getattr(pos, "exit_spread_bps", 0.0) or 0.0)
        if _exit_mid <= 0:
            _exit_mid = float(getattr(closed, "exit_price", 0.0) or 0.0)
        _bbo_bid = _bbo_ask = _bbo_mid = None
        if _exit_mid > 0 and _exit_spread_bps > 0:
            _half = _exit_mid * _exit_spread_bps / 20000.0
            _bbo_mid = _exit_mid
            _bbo_bid = _exit_mid - _half
            _bbo_ask = _exit_mid + _half

        self._events_logger.log_position_closed(  # type: ignore
            sid=str(getattr(pos, "sid", "")),
            symbol=str(getattr(pos, "symbol", "")),
            ts_ms=int(exit_ts_ms or 0),
            exit_ts_ms=int(exit_ts_ms or 0),
            order_id=order_id,
            side=str(getattr(pos, "direction", "") or "").upper(),
            venue=str(getattr(pos, "source", "") or ""),
            qty=float(getattr(pos, "lot", 0.0) or 0.0),
            fee_bps=float(fee_bps),
            close_price=float(getattr(closed, "exit_price", 0.0) or 0.0),
            bid_at_fill=_bbo_bid,
            ask_at_fill=_bbo_ask,
            mid_at_fill=_bbo_mid,
            pnl=float(getattr(closed, "pnl_net", 0.0) or 0.0),
            position_id=str(getattr(pos, "pos_id", "") or getattr(pos, "id", "")),
            lot=float(getattr(pos, "lot", 0.0) or 0.0),
            source=str(getattr(pos, "source", "mt5")),
            close_reason=str(close_reason),
            metadata=md,
            payload={
                "ab_arm": ab_arm,
                "ab_group": ab_group,
                "ab_key": ab_key,
                "arm_ver": int(arm_ver),
                "regime": regime,
                "regime_group": regime_group,
                "scenario": scenario,
                "scenario_v4": scenario_v4,
                "risk_usd": float(risk_usd),
                "r_mult": float(r_mult),
                "exit_ts_ms": int(exit_ts_ms or 0),
                "ts_fill_ms": int(exit_ts_ms or 0),
                "order_id": str(order_id),
                "qty": float(getattr(pos, "lot", 0.0) or 0.0),
                "side": str(getattr(pos, "direction", "") or "").upper(),
                "venue": str(getattr(pos, "source", "") or ""),
                "fee_bps": float(fee_bps),
                "abs_lvl_tier": int(abs_lvl_tier),
                "dn_tier": int(dn_tier),
                "book_health_ok": int(book_health_ok),
                "book_age_ms": int(book_age_ms),
                "spread_bp": float(spread_bp),
                "p0_book_age_ms": int(getattr(pos, "p0_book_age_ms", 0) or 0),
                "fees_usd": float(getattr(closed, "fees", 0.0) or getattr(pos, "fees", 0.0) or 0.0),
                "turnover_roundtrip": float(getattr(closed, "turnover_roundtrip", 0.0) or 0.0),
                "of_confirm_ok": int(of_confirm_ok),
                "of_confirm_score": float(of_confirm_score),
                "atr_bps_exec": float(atr_bps_exec),
                "atr_unified_th_bps": float(atr_unified_th_bps),
                "atr_floor_th_bps": float(atr_floor_th_bps),
                "atr_fees_th_bps": float(atr_fees_th_bps),
                "meta_enforce_applied": int(meta_enforce_applied) if meta_enforce_applied is not None else None,
                "meta_veto": int(meta_veto),
                "meta_enforce_key": str(meta_enforce_key),
                "meta_enforce_salt": str(meta_enforce_salt),
                **build_horizon_event_scalars(pos),
            },
            extra_payload=extra,
        )
