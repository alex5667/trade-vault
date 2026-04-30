# session_service.py
"""
Session and execution management functionality extracted from base_orderflow_handler.py
"""

from __future__ import annotations

from typing import Optional, Dict, Any, TYPE_CHECKING, List, Mapping, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)

if TYPE_CHECKING:
    from contexts import OrderflowSignalContext, ExecutionContext

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


@dataclass(frozen=True)
class SessionWindow:
    """
    Session window in local time of `tz`.
    start_min/end_min: minutes since 00:00 in that timezone.
    If end_min < start_min -> wraps over midnight.
    """
    label: str
    tz: str = "UTC"
    start_min: int = 0
    end_min: int = 24 * 60
    weekdays: Optional[Tuple[int, ...]] = None  # 0=Mon .. 6=Sun


class SessionService:
    """
    Service for session analysis and execution planning.
    """

    def __init__(self, symbol: str, config: Any = None, *, asset_class: Optional[str] = None):
        self.symbol = symbol
        self.logger = setup_logger(f"SessionService:{symbol}")
        self.config = config
        self.asset_class = (asset_class or "").lower().strip() or None

        # Defaults: non-overlapping UTC buckets for crypto-like flows
        # Priority: US > Europe > Asia > Overnight
        self._default_windows_crypto: List[SessionWindow] = [
            SessionWindow("us_main", "UTC", 14 * 60, 21 * 60, weekdays=None)
            SessionWindow("european", "UTC", 8 * 60, 14 * 60, weekdays=None)
            SessionWindow("asian", "UTC", 0 * 60, 8 * 60, weekdays=None)
            SessionWindow("overnight", "UTC", 21 * 60, 24 * 60, weekdays=None)
        ]

        # "По-взрослому" для FX/металлов: DST-aware зоны.
        # Приоритет сохраняем (US > London > Tokyo > Overnight).
        self._default_windows_fx_like: List[SessionWindow] = [
            SessionWindow("us_main", "America/New_York", 9 * 60, 16 * 60, weekdays=(0, 1, 2, 3, 4))
            SessionWindow("european", "Europe/London", 8 * 60, 16 * 60, weekdays=(0, 1, 2, 3, 4))
            SessionWindow("asian", "Asia/Tokyo", 9 * 60, 17 * 60, weekdays=(0, 1, 2, 3, 4))
            # Overnight: fallback for everything else (UTC, all days)
            SessionWindow("overnight", "UTC", 0, 24 * 60, weekdays=None)
        ]

        # Bias defaults (can be overridden via config)
        self._default_biases: Dict[str, float] = {
            "us_main": 0.1
            "european": -0.05
            "asian": 0.0
            "overnight": -0.1
            "weekend": 0.0
            "unknown": 0.0
        }

    def _normalize_ts_ms(self, ts: Any) -> int:
        """
        Normalize timestamp to epoch ms.
        Heuristic: if 0 < ts < 1e12 => seconds, else ms.
        """
        if ts is None:
            return 0
        try:
            if isinstance(ts, str):
                ts = ts.strip()
                if not ts:
                    return 0
                ts = int(float(ts))
            elif isinstance(ts, float):
                # allow float seconds/ms
                ts = int(ts)
            else:
                ts = int(ts)
        except Exception:
            return 0
        if ts <= 0:
            return 0
        # seconds -> ms
        if ts < 1_000_000_000_000:
            return ts * 1000
        return ts

    def _safe_zoneinfo(self, tz_name: str):
        if tz_name == "UTC":
            return timezone.utc
        if ZoneInfo is None:
            return timezone.utc
        try:
            return ZoneInfo(tz_name)
        except Exception:
            return timezone.utc

    def _minutes_of_day(self, dt: datetime) -> int:
        return dt.hour * 60 + dt.minute

    def _match_window(self, dt_local: datetime, w: SessionWindow) -> bool:
        if w.weekdays is not None:
            if dt_local.weekday() not in w.weekdays:
                return False
        m = self._minutes_of_day(dt_local)
        if w.end_min >= w.start_min:
            return (w.start_min <= m) and (m < w.end_min)
        # wraps over midnight
        return (m >= w.start_min) or (m < w.end_min)

    def _config_get(self, path: str, default: Any = None) -> Any:
        """
        Best-effort config getter supporting:
          - objects with attributes
          - dict-like configs
        path like "sessions.windows" is supported.
        """
        cur: Any = self.config
        if cur is None:
            return default
        for part in (path or "").split("."):
            if not part:
                continue
            try:
                if isinstance(cur, Mapping):
                    cur = cur.get(part, default)
                else:
                    cur = getattr(cur, part, default)
            except Exception:
                return default
            if cur is None:
                return default
        return cur if cur is not None else default

    def _infer_asset_class(self, ctx: "OrderflowSignalContext") -> str:
        """
        Infer asset class for session logic.
        Priority:
          1) ctor asset_class
          2) config.asset_class
          3) ctx.asset_class
          4) heuristic by symbol
        """
        if self.asset_class:
            return self.asset_class
        ac = (self._config_get("asset_class", None) or getattr(ctx, "asset_class", None) or "").lower().strip()
        if ac:
            return ac
        sym = (getattr(ctx, "symbol", None) or self.symbol or "").upper()
        if sym.startswith("XAU") or sym.startswith("XAG") or "XAU" in sym:
            return "metals"
        # default to crypto because your stack is crypto-first
        return "crypto"

    def _load_windows_from_config(self, asset_class: str) -> Optional[List[SessionWindow]]:
        """
        Optional config format:
          config.sessions = {
            "crypto": {
              "weekend_label": "weekend"
              "windows": [
                {"label":"us_main","tz":"UTC","start":"14:00","end":"21:00","weekdays":[0,1,2,3,4,5,6]}
                ...
              ]
            }
            "metals": {...}
          }
        """
        sessions_cfg = self._config_get("sessions", None)
        if not sessions_cfg:
            return None
        try:
            ac_cfg = sessions_cfg.get(asset_class) if isinstance(sessions_cfg, Mapping) else getattr(sessions_cfg, asset_class, None)
        except Exception:
            ac_cfg = None
        if not ac_cfg:
            return None
        try:
            windows_raw = ac_cfg.get("windows") if isinstance(ac_cfg, Mapping) else getattr(ac_cfg, "windows", None)
        except Exception:
            windows_raw = None
        if not windows_raw:
            return None

        def _hm_to_min(x: Any) -> Optional[int]:
            if x is None:
                return None
            if isinstance(x, (int, float)):
                v = int(x)
                return max(0, min(24 * 60, v))
            s = str(x).strip()
            if not s:
                return None
            if ":" in s:
                hh, mm = s.split(":", 1)
                try:
                    return int(hh) * 60 + int(mm)
                except Exception:
                    return None
            try:
                return int(s)
            except Exception:
                return None

        out: List[SessionWindow] = []
        for w in windows_raw:
            if not isinstance(w, Mapping):
                continue
            label = str(w.get("label") or "").strip()
            if not label:
                continue
            tz = str(w.get("tz") or "UTC").strip() or "UTC"
            sm = _hm_to_min(w.get("start"))
            em = _hm_to_min(w.get("end"))
            if sm is None or em is None:
                continue
            weekdays = w.get("weekdays", None)
            wd_t: Optional[Tuple[int, ...]] = None
            if isinstance(weekdays, (list, tuple)):
                try:
                    wd_t = tuple(int(x) for x in weekdays)
                except Exception:
                    wd_t = None
            out.append(SessionWindow(label=label, tz=tz, start_min=int(sm), end_min=int(em), weekdays=wd_t))
        return out or None

    def _weekend_label_enabled(self, asset_class: str) -> bool:
        """
        Weekend labeling must be configurable (default: enabled).
        """
        sessions_cfg = self._config_get("sessions", None)
        if isinstance(sessions_cfg, Mapping) and asset_class in sessions_cfg:
            ac_cfg = sessions_cfg.get(asset_class) or {}
            if isinstance(ac_cfg, Mapping) and "label_weekend" in ac_cfg:
                return bool(ac_cfg.get("label_weekend"))
        # default: enabled for all
        return True

    def _weekend_label(self, asset_class: str) -> str:
        sessions_cfg = self._config_get("sessions", None)
        if isinstance(sessions_cfg, Mapping) and asset_class in sessions_cfg:
            ac_cfg = sessions_cfg.get(asset_class) or {}
            if isinstance(ac_cfg, Mapping):
                wl = ac_cfg.get("weekend_label")
                if wl:
                    return str(wl)
        return "weekend"

    def _infer_session_label(self, ctx: "OrderflowSignalContext") -> str:
        """Infer trading session label from context (DST-aware when possible)."""
        ts_ms = self._normalize_ts_ms(getattr(ctx, "ts", 0))
        if ts_ms <= 0:
            return "unknown"

        try:
            dt_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            asset_class = self._infer_asset_class(ctx)

            # Configurable weekend labeling
            if self._weekend_label_enabled(asset_class) and dt_utc.weekday() >= 5:
                return self._weekend_label(asset_class)

            # Windows: config override -> defaults by asset_class
            windows = self._load_windows_from_config(asset_class)
            if windows is None:
                if asset_class in ("fx", "metals", "forex"):
                    windows = self._default_windows_fx_like
                else:
                    windows = self._default_windows_crypto

            # Priority is order of `windows` list (keep US > EU > Asia > Overnight)
            for w in windows:
                tzinfo = self._safe_zoneinfo(w.tz)
                dt_local = dt_utc.astimezone(tzinfo)
                if self._match_window(dt_local, w):
                    return w.label

        except Exception as e:
            self.logger.warning("Failed to infer session label: %s", e)
            return "unknown"

    def _session_bias(self, ctx: "OrderflowSignalContext") -> float | None:
        """Calculate session bias."""
        session = self._infer_session_label(ctx)

        # Config override:
        #   config.session_biases = {"us_main":0.05, ...}
        cfg_biases = self._config_get("session_biases", None)
        if isinstance(cfg_biases, Mapping):
            try:
                if session in cfg_biases:
                    return float(cfg_biases.get(session))
            except Exception:
                pass

        return self._default_biases.get(session, 0.0)

    def _daily_open_cross_bias_from_freq(self, cross_freq: float) -> float:
        """Calculate bias from daily open crossing frequency."""
        # Clamp frequency to reasonable range
        cross_freq = max(0.0, min(1.0, cross_freq))

        # Higher crossing frequency suggests more volatile/trending day
        # Lower frequency suggests ranging day
        if cross_freq > 0.7:
            return 0.2  # Bullish bias for very volatile days
        elif cross_freq > 0.5:
            return 0.1  # Slightly bullish
        elif cross_freq > 0.3:
            return 0.0  # Neutral
        else:
            return -0.1  # Bearish bias for ranging days

    def attach_to_ctx(self, ctx: "OrderflowSignalContext") -> None:
        """
        Single place to attach session fields to the signal context.
        This is what you should call from build_signal_ctx().
        """
        try:
            label = self._infer_session_label(ctx)
            bias = self._session_bias(ctx)
            setattr(ctx, "session", label)
            setattr(ctx, "session_bias", bias)
        except Exception as e:
            self.logger.warning("Failed to attach session fields: %s", e)

    # ---- Execution plan stubs (keep compatibility, but make coherent) ----
    def create_execution_plan_from_signal(self, sig_ctx: "OrderflowSignalContext") -> Optional[Dict[str, Any]]:
        """
        Coherent stub: builds a minimal plan from OrderflowSignalContext.
        If you later introduce a real ExecutionContext, keep this as adapter.
        """
        try:
            return {
                "symbol": getattr(sig_ctx, "symbol", self.symbol)
                "direction": int(getattr(sig_ctx, "direction", 0) or 0)
                "quantity": float(getattr(sig_ctx, "quantity", 1.0) or 1.0)
                "execution_type": "market"
                "created_at_ms": self._normalize_ts_ms(getattr(sig_ctx, "ts", 0))
                "session": getattr(sig_ctx, "session", None)
                "session_bias": getattr(sig_ctx, "session_bias", None)
            }
        except Exception as e:
            self.logger.warning("Failed to create execution plan from signal ctx: %s", e)
            return None

    def _create_execution_plan(self, ctx: "ExecutionContext") -> Optional[Any]:
        """Create execution plan from context."""
        try:
            # This would integrate with execution planning logic
            # For now, return a simple placeholder
            return {
                "symbol": getattr(ctx, 'symbol', self.symbol)
                "direction": getattr(ctx, 'direction', 0)
                "quantity": getattr(ctx, 'quantity', 1.0)
                "execution_type": "market"
                "created_at": getattr(ctx, 'ts', 0)
            }
        except Exception as e:
            self.logger.warning(f"Failed to create execution plan: {e}")
            return None

    def _save_execution_plan(self, plan: Any) -> None:
        """Save execution plan for later use."""
        # Placeholder - would save to database/cache
        self.logger.debug("Saved execution plan: %s", plan)

    def _execution_plan_to_dict(self, plan: Optional[Any]) -> Optional[dict]:
        """Convert execution plan to dictionary."""
        if plan is None:
            return None

        try:
            if isinstance(plan, dict):
                return plan
            elif hasattr(plan, '__dict__'):
                return plan.__dict__
            else:
                return {"plan": str(plan)}
        except Exception as e:
            self.logger.warning("Failed to convert execution plan to dict: %s", e)
            return None

    def analyze_session(self, ctx: "OrderflowSignalContext") -> Dict[str, Any]:
        """Perform complete session analysis."""
        ts_ms = self._normalize_ts_ms(getattr(ctx, "ts", 0))
        session_label = self._infer_session_label(ctx)
        session_bias = self._session_bias(ctx)
        asset_class = self._infer_asset_class(ctx)

        # Additional session metrics could be calculated here

        return {
            "session_label": session_label
            "session_bias": session_bias
            "asset_class": asset_class
            "analysis_ts_ms": ts_ms
        }
