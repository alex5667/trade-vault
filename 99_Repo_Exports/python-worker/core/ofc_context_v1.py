from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict


@dataclass(frozen=True)
class OFCContextV1:
    symbol: str
    direction: str
    ts_ms: int
    session: str
    dow: int
    hour_utc: int
    scenario_base: str
    scenario_v4: str
    liq_regime: str
    vol_regime: str
    book_regime: str
    notional_bucket: str
    artifact_version: str = ""


def _pick_session(indicators: Dict[str, Any]) -> str:
    for key in ("ctx_session", "session_name", "session"):
        v = str(indicators.get(key, "") or "").strip().lower()
        if v:
            return v
    for key in ("session_asia", "session_eu", "session_us", "session_off"):
        try:
            if int(indicators.get(key, 0) or 0) == 1:
                return key.replace("session_", "")
        except Exception:
            continue
    return "off"


def build_ofc_context(
    *,
    symbol: str,
    direction: str,
    ts_ms: int,
    indicators: Dict[str, Any],
    runtime: Any,
    scenario_base: str,
    scenario_v4: str,
) -> OFCContextV1:
    dt = datetime.fromtimestamp(max(0, int(ts_ms)) / 1000.0, tz=timezone.utc)
    liq_regime = str(
        indicators.get("liq_regime", getattr(runtime, "liq_regime", getattr(runtime, "last_regime", "na"))) or "na"
    )
    vol_regime = str(
        indicators.get("vol_regime", indicators.get("vol_shock_regime", getattr(runtime, "vol_regime", "na"))) or "na"
    )
    book_regime = str(
        indicators.get("book_regime", indicators.get("book_health_regime", "na")) or "na"
    )
    notional_bucket = str(
        indicators.get("dn_tier_active", indicators.get("notional_bucket", "na")) or "na"
    )
    return OFCContextV1(
        symbol=str(symbol or ""),
        direction=str(direction or "").upper(),
        ts_ms=int(ts_ms or 0),
        session=_pick_session(indicators),
        dow=int(dt.weekday()),
        hour_utc=int(dt.hour),
        scenario_base=str(scenario_base or ""),
        scenario_v4=str(scenario_v4 or scenario_base or ""),
        liq_regime=liq_regime,
        vol_regime=vol_regime,
        book_regime=book_regime,
        notional_bucket=notional_bucket,
        artifact_version="",
    )
