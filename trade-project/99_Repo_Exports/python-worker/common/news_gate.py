from __future__ import annotations

import json
import math
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any


@dataclass
class NewsBlock:
    blocked: bool
    reason: str
    until_ts_ms: int
    meta: dict[str, Any]


@dataclass(kw_only=True)
class GateDecision:
    """Unified result for hard-block + soft risk scaling.

    risk_factor_bps:
      10000 -> no reduction
      0     -> full disable (equivalent to hard block in strategy layer)
    """

    hard_block: bool
    hard_reason: str
    until_ts_ms: int
    risk_factor_bps: int  # 0..10000
    dq_flags: dict[str, Any]
    meta: dict[str, Any]
    soft_reasons: list[str] = dataclass_field(default_factory=list)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def _clamp01(x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    return x


def _clamp_int(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x


class NewsGate:
    """Redis-based high-impact news gate.

    Sources (in order):
      1) Manual JSON key with TTL:
         news:hi:active -> {"active":1, "until_ts_ms":..., "reason":"CPI", "symbols":[...]}
      2) Calendar aggregated hash:
         calendar:agg:<asset_class> -> fields:
           event_grade_id, event_ts_ms/next_ts_ms, (legacy) event_tminus_sec, title/event_id

    Determinism:
      - caller must pass now_ts_ms (event time), not wall-time.
    """

    def __init__(
        self,
        *,
        redis_client: Any,
        asset_class: str = "crypto",
        window_sec: int = 300,
        grade_min: int = 4,
        manual_key: str = "news:hi:active",
        cal_agg_prefix: str = "calendar:agg:",
        # soft gate
        soft_enabled: bool = True,
        soft_window_sec: int | None = None,
        soft_grade_min: int = 2,
        soft_grade2_bps: int = 5000,
        soft_grade3_bps: int = 3500,
        soft_grade4_bps: int = 2500,
        soft_news_k: float = 0.9,
        soft_news_min_bps: int = 2500,
    ) -> None:
        self.redis = redis_client

        ac = str(asset_class).strip().lower()
        if ac == "forex":
            ac = "fx"
        self.asset_class = ac

        self.window_sec = int(window_sec)
        self.grade_min = int(grade_min)
        self.manual_key = str(manual_key)

        self.cal_key = f"{str(cal_agg_prefix)}{self.asset_class}"

        self.soft_enabled = bool(soft_enabled)
        self.soft_window_sec = int(soft_window_sec) if soft_window_sec is not None else int(window_sec)
        self.soft_grade_min = int(soft_grade_min)
        self.soft_grade2_bps = int(soft_grade2_bps)
        self.soft_grade3_bps = int(soft_grade3_bps)
        self.soft_grade4_bps = int(soft_grade4_bps)
        self.soft_news_k = float(soft_news_k)
        self.soft_news_min_bps = int(soft_news_min_bps)

    def _manual(self, now_ts_ms: int, symbols: tuple[str, ...] | None = None) -> NewsBlock | None:
        try:
            raw = self.redis.get(self.manual_key)
            if not raw:
                return None
            d = json.loads(raw)
            active = int(d.get("active", 0) or 0)
            if active != 1:
                return None
            until_ts_ms = _i(d.get("until_ts_ms", 0), 0)
            if until_ts_ms <= 0 or now_ts_ms <= 0:
                return None
            if now_ts_ms >= until_ts_ms:
                return None
            if symbols:
                allow = d.get("symbols")
                if isinstance(allow, list) and allow:
                    allow_u = {str(x).upper() for x in allow}
                    if not any(str(s).upper() in allow_u for s in symbols):
                        return None
            reason = (d.get("reason", "manual_hi_impact") or "manual_hi_impact")
            meta = {"src": "manual", "reason": reason}
            return NewsBlock(blocked=True, reason=reason, until_ts_ms=until_ts_ms, meta=meta)
        except Exception:
            return None

    def _calendar_fields(self) -> dict[str, Any]:
        try:
            d = self.redis.hgetall(self.cal_key) or {}
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}

    def decide(
        self,
        *,
        now_ts_ms: int,
        symbols: tuple[str, ...] | None = None,
        # optional news features (pass from ctx.news to get full soft-gate)
        news_risk: float | None = None,
        news_grade_id: int | None = None,
        confidence: float | None = None,
        horizon_sec: int | None = None,
        asof_ts_ms: int | None = None,
    ) -> GateDecision:
        dq: dict[str, Any] = {}
        meta: dict[str, Any] = {}

        if now_ts_ms <= 0:
            dq["no_ts"] = True
            return GateDecision(
                hard_block=False,
                hard_reason="no_ts",
                until_ts_ms=0,
                risk_factor_bps=10000,
                dq_flags=dq,
                meta=meta,
                soft_reasons=[],
            )

        # 1) Manual hard block
        m = self._manual(now_ts_ms, symbols=symbols)
        if m is not None:
            return GateDecision(
                hard_block=True,
                hard_reason=m.reason,
                until_ts_ms=m.until_ts_ms,
                risk_factor_bps=0,
                dq_flags=dq,
                meta=m.meta,
                soft_reasons=[],
            )

        # 2) Calendar hard/soft
        cal = self._calendar_fields()
        grade = _i(cal.get("event_grade_id", 0), 0)

        event_ts_ms = _i(cal.get("event_ts_ms", 0), 0) or _i(cal.get("next_ts_ms", 0), 0)
        tminus = 1e9
        if event_ts_ms > 0:
            tminus = float(event_ts_ms - now_ts_ms) / 1000.0
        else:
            # Legacy fallback (stale by design) - keep only to avoid breaking old deployments.
            tminus = _f(cal.get("event_tminus_sec", 1e9), 1e9)
            dq["calendar_missing_event_ts_ms"] = True

        title = str(cal.get("title", "") or cal.get("event_title", "") or "")
        ev_id = (cal.get("event_id", "") or "")

        meta.update(
            {
                "src": "calendar:agg",
                "cal_key": self.cal_key,
                "cal_grade": grade,
                "tminus_sec": float(tminus),
                "event_ts_ms": event_ts_ms if event_ts_ms > 0 else 0,
                "title": title,
                "event_id": ev_id,
            }
        )

        # Hard block window
        if grade >= self.grade_min and abs(tminus) <= float(self.window_sec):
            until_ts_ms = int(event_ts_ms + self.window_sec * 1000) if event_ts_ms > 0 else int(now_ts_ms + self.window_sec * 1000)
            if now_ts_ms < until_ts_ms:
                return GateDecision(
                    hard_block=True,
                    hard_reason="calendar_hi_impact",
                    until_ts_ms=until_ts_ms,
                    risk_factor_bps=0,
                    dq_flags=dq,
                    meta=meta,
                    soft_reasons=[],
                )

        # Soft gate defaults
        rf_bps = 10000
        soft_reasons: list[str] = []
        if self.soft_enabled:
            # Calendar soft: reduce risk around grade>=soft_grade_min
            if grade >= self.soft_grade_min and abs(tminus) <= float(self.soft_window_sec):
                soft_reasons.append("soft_cal")
                if grade >= 4:
                    rf_bps = min(rf_bps, self.soft_grade4_bps)
                    meta["soft_calendar_bps"] = self.soft_grade4_bps
                elif grade == 3:
                    rf_bps = min(rf_bps, self.soft_grade3_bps)
                    meta["soft_calendar_bps"] = self.soft_grade3_bps
                elif grade == 2:
                    rf_bps = min(rf_bps, self.soft_grade2_bps)
                    meta["soft_calendar_bps"] = self.soft_grade2_bps

            # News soft: requires features
            if news_risk is not None and news_grade_id is not None and confidence is not None:
                # staleness by horizon (if provided)
                if asof_ts_ms is not None and horizon_sec is not None and horizon_sec > 0 and asof_ts_ms > 0:
                    if (now_ts_ms - asof_ts_ms) > horizon_sec * 1000:
                        dq["news_stale_over_horizon"] = True
                    else:
                        soft_reasons.append("soft_news")
                        rr = _clamp01(float(news_risk))
                        g = news_grade_id
                        if g >= 3:
                            gw = 1.0
                        elif g == 2:
                            gw = 0.6
                        elif g == 1:
                            gw = 0.3
                        else:
                            gw = 0.0
                        cw = max(0.2, min(1.0, float(confidence)))
                        impact = rr * gw * cw
                        factor = max(float(self.soft_news_min_bps) / 10000.0, 1.0 - self.soft_news_k * impact)
                        news_bps = int(10000.0 * factor)
                        rf_bps = min(rf_bps, news_bps)
                        meta["soft_news_bps"] = news_bps
                        meta["soft_news_impact"] = float(impact)
                else:
                    soft_reasons.append("soft_news")
                    rr = _clamp01(float(news_risk))
                    g = news_grade_id
                    gw = 1.0 if g >= 3 else 0.6 if g == 2 else 0.3 if g == 1 else 0.0
                    cw = max(0.2, min(1.0, float(confidence)))
                    impact = rr * gw * cw
                    factor = max(float(self.soft_news_min_bps) / 10000.0, 1.0 - self.soft_news_k * impact)
                    news_bps = int(10000.0 * factor)
                    rf_bps = min(rf_bps, news_bps)
                    meta["soft_news_bps"] = news_bps
                    meta["soft_news_impact"] = float(impact)

        # Apply news reco reader hot-path overrides
        from services.news_reco_reader import get_reco
        if symbols:
            for sym in symbols:
                snap = get_reco(sym)
                if snap:
                    profile = snap.payload.get("suggest_profile")
                    if profile:
                        meta["news_reco_profile"] = profile
                        if profile == "hard":
                            return GateDecision(
                                hard_block=True,
                                hard_reason="news_reco_hard",
                                until_ts_ms=now_ts_ms + 60000, # Block for a short period
                                risk_factor_bps=0,
                                dq_flags=dq,
                                meta=meta,
                                soft_reasons=soft_reasons,
                            )
                        elif profile == "tighten":
                            rf_bps = min(rf_bps, 2500)
                        elif profile == "soft":
                            rf_bps = min(rf_bps, 5000)

        rf_bps = _clamp_int(rf_bps, 0, 10000)

        return GateDecision(
            hard_block=False,
            hard_reason="ok",
            until_ts_ms=0,
            risk_factor_bps=rf_bps,
            dq_flags=dq,
            meta=meta,
            soft_reasons=soft_reasons,
        )

    def check(self, *, now_ts_ms: int, symbols: tuple[str, ...] | None = None) -> NewsBlock:
        """Backward-compatible hard-block API."""
        d = self.decide(now_ts_ms=now_ts_ms, symbols=symbols)
        return NewsBlock(
            blocked=bool(d.hard_block),
            reason=str(d.hard_reason),
            until_ts_ms=int(d.until_ts_ms),
            meta=dict(d.meta),
        )
