from __future__ import annotations

import hashlib
import logging
import os
from typing import Any

from common.dq_flags import append_dq_flag
from common.math_safe import safe_float
from common.runtime_snapshot import RuntimeSnapshot
from handlers.crypto_orderflow.utils.risk_cfg_resolver import RiskCfgResolver

_logger = logging.getLogger(__name__)


def _get_sync_redis() -> Any:
    """
    Lazy singleton for sync Redis client.
    RiskCfgResolver needs sync .get() / .hget(); the handler only has async redis.
    """
    if not hasattr(_get_sync_redis, "_client"):
        try:
            import redis
            url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            _get_sync_redis._client = redis.from_url(url, decode_responses=True)
        except Exception as e:
            _logger.warning("Failed to create sync Redis for RiskCfgResolver: %s", e)
            _get_sync_redis._client = None
    return _get_sync_redis._client


class CryptoOrderFlowConfigManager:
    """
    Manages configuration, runtime snapshots, and environment variable parsing
    for CryptoOrderFlowHandler.
    """

    def __init__(self, handler: Any, symbol: str, config: Any):
        self._handler = handler
        self._symbol = symbol
        self._config = config
        self._runtime: RuntimeSnapshot | None = None
        # FIX: use sync Redis client (RiskCfgResolver calls .get()/.hget() synchronously).
        # handler.redis is async (aioredis.Redis), which returns coroutines instead of values.
        self._risk_resolver = RiskCfgResolver(redis_client=_get_sync_redis())

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def config(self) -> Any:
        return self._config

    def _env_float(self, name: str, default: float) -> float:
        try:
            val = os.getenv(name)
            return float(val) if val is not None else default
        except Exception:
            return default

    def _sym_env_float(self, base: str, symbol: str, default: float) -> float:
        """
        Get float ENV with symbol-specific override.
        Example: MIN_CONF_BTCUSDT override style.
        """
        val = self._env_float(f"{base}_{symbol}", self._env_float(base, default))
        return val

    def get_runtime_snapshot(self) -> RuntimeSnapshot:
        """
        Single place to access cached ENV values.
        Must be extremely cheap in hot-path.
        """
        rt = self._runtime
        if rt is None:
            rt = RuntimeSnapshot.load()
            self._runtime = rt
            return rt

        # Soft refresh attempt (fail-open)
        # Note: Original code tried to call rt.maybe_refresh(redis=r) which might not exist
        # on RuntimeSnapshot from common.runtime_snapshot. We preserve the try-block
        # structure but if methods coincide with common lib, it works.
        try:
            r = getattr(self._handler, "redis", None)
            if hasattr(rt, "maybe_refresh"):
                rt2 = rt.maybe_refresh(redis=r) # type: ignore
                if rt2 is not rt:
                    self._runtime = rt2
                    return rt2
        except Exception:
            pass
    def resolve_risk_cfg(self) -> dict[str, Any]:
        """Resolves symbol-specific risk configuration."""
        return self._risk_resolver.resolve(str(self.symbol))

    def min_conf_thresholds(self, symbol: str) -> tuple[float, float]:
        """Replaces per-call os.getenv parsing."""
        rt = self.get_runtime_snapshot()
        return float(rt.min_conf(symbol)), float(rt.min_conf_factor(symbol))

    def estimate_fees_bps(self, ctx: Any) -> float:
        """
        Estimate fees in basis points (bps).
        Priority:
        1. Explicit ctx fields (fees_bps, taker_fee_bps, fee_bps)
        2. ENV variable EDGE_FEES_BPS_DEFAULT (with symbol override)
        """
        for k in ("fees_bps", "taker_fee_bps", "fee_bps"):
            v = getattr(ctx, k, None)
            try:
                if v is not None and float(v) > 0:
                    return float(v)
            except Exception:
                pass

        # fallback: env default
        sym = str(getattr(ctx, "symbol", "") or self.symbol)
        return self._sym_env_float("EDGE_FEES_BPS_DEFAULT", sym, 4.0)

    def estimate_slippage_bps(self, ctx: Any) -> float:
        """
        Estimate slippage in basis points (bps).
        Priority:
        1. Realized spread tracker from ctx
        2. Current spread * 0.5 (if EDGE_SLIPPAGE_USE_SPREAD_HALF=1)
        3. ENV default EDGE_SLIPPAGE_BPS_DEFAULT
        """
        sym = str(getattr(ctx, "symbol", "") or self.symbol)

        # 1) Realized spread tracker
        rs = 0.0
        for k in ("realized_spread_bps", "realized_spread_ema_bps", "rs_ema_bps"):
            v = getattr(ctx, k, None)
            fv = safe_float(v, default=0.0)
            if fv > 0:
                rs = float(fv)
                break

        # 2) Fallback from spread_bps
        spread_bps = 0.0
        sb = safe_float(getattr(ctx, "spread_bps", None), default=0.0)
        if sb > 0:
            spread_bps = float(sb)

        # Optional DQ marker
        if rs <= 0.0 and spread_bps <= 0.0:
            append_dq_flag(ctx, "spread_missing_or_zero")

        base = self._sym_env_float("EDGE_SLIPPAGE_BPS_DEFAULT", sym, 2.0)

        use_spread_half = (os.getenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "1") or "1").lower() not in {"0","false","no"}
        if use_spread_half and spread_bps > 0:
            base = max(base, 0.5 * spread_bps)

        return max(base, rs, 0.0)

    def expected_move_bps(self, ctx: Any, *, kind: str, side: int) -> float:
        """
        Estimate expected move in bps to nearest realistic fixation.
        Modes: tp1 | rr | atr
        """
        mode = (os.getenv("EDGE_EXPECTED_MOVE_MODE", "tp1") or "tp1").strip().lower()
        price = getattr(ctx, "last_price", None) or getattr(ctx, "price", None)
        try:
            px = float(price) # type: ignore
        except Exception:
            return 0.0
        if px <= 0:
            return 0.0

        # TP1 Mode
        if mode == "tp1":
            tp1 = getattr(ctx, "tp1_price", None) or getattr(ctx, "tp1", None)
            entry = getattr(ctx, "entry_price", None) or getattr(ctx, "price", None)
            try:
                tp1f = tp1 if tp1 is not None else None
                enf = entry if entry is not None else px
            except Exception:
                tp1f = None
                enf = px
            if tp1f is not None and enf > 0:
                return abs(tp1f - enf) / enf * 10_000.0

        # RR Mode
        if mode == "rr":
            stop_dist = getattr(ctx, "stop_dist", None)
            if stop_dist is None:
                sl = getattr(ctx, "sl_price", None) or getattr(ctx, "sl", None)
                try:
                    slf = sl if sl is not None else None
                    stop_dist = abs(px - slf) if slf is not None else None
                except Exception:
                    stop_dist = None

            try:
                sd = stop_dist if stop_dist is not None else 0.0
            except Exception:
                sd = 0.0

            tp_rr = getattr(self.config, "tp_rr", None)
            try:
                rr = float(tp_rr) if tp_rr is not None else 1.0
            except Exception:
                rr = 1.0

            if sd > 0 and rr > 0:
                return (sd * rr) / px * 10_000.0

        # ATR Mode
        if mode == "atr":
            atr = getattr(ctx, "atr_5m", None) or getattr(ctx, "atr", None) or getattr(ctx, "atr_1m", None) or getattr(ctx, "atr_intraday", None)
            try:
                atrf = atr if atr is not None else 0.0
            except Exception:
                atrf = 0.0

            mult = 1.0
            tp_atr_mults = getattr(self.config, "tp_atr_mults", None)
            try:
                if isinstance(tp_atr_mults, (list, tuple)) and tp_atr_mults:
                    mult = float(tp_atr_mults[0])
            except Exception:
                mult = 1.0
            if atrf > 0 and mult > 0:
                return (atrf * mult) / px * 10_000.0

        return 0.0

    @staticmethod
    def run_jsonify(v: Any) -> Any:
        """Safe type conversion for JSON serialization."""
        if v is None or isinstance(v, (str, int, float, bool)):
            return v
        if isinstance(v, (list, tuple)):
            return [CryptoOrderFlowConfigManager.run_jsonify(x) for x in v]
        if isinstance(v, dict):
            return {str(k): CryptoOrderFlowConfigManager.run_jsonify(val) for k, val in v.items()}
        try:
            return float(v)
        except Exception:
            return str(v)

    def build_config_params_from_cfg(self) -> dict[str, Any]:
        """
        Build config_params for sidecar meta (NOT payload).
        """
        cfg = self.config
        if cfg is None:
            return {}

        raw = {
            "delta_window_ticks": getattr(cfg, "delta_window_ticks", None),
            "delta_z_threshold": getattr(cfg, "delta_z_threshold", None),
            "weak_progress_atr": getattr(cfg, "weak_progress_atr", None),
            "obi_threshold": getattr(cfg, "obi_threshold", None),
            "obi_min_duration": getattr(cfg, "obi_min_duration", None),
            "iceberg_refresh_count": getattr(cfg, "iceberg_refresh_count", None),
            "iceberg_min_duration": getattr(cfg, "iceberg_min_duration", None),
            "iceberg_refresh_min_abs": getattr(cfg, "iceberg_refresh_min_abs", None),
            "dist_atr_threshold": getattr(cfg, "dist_atr_threshold", None),
            "min_signal_interval_sec": getattr(cfg, "min_signal_interval_sec", None),
            "stop_mode": getattr(cfg, "stop_mode", None),
            "stop_atr_mult": getattr(cfg, "stop_atr_mult", None),
            "stop_pct": getattr(cfg, "stop_pct", None),
            "stop_points": getattr(cfg, "stop_points", None),
            "tp_mode": getattr(cfg, "tp_mode", None),
            "tp_rr": getattr(cfg, "tp_rr", None),
            "tp_atr_mults": getattr(cfg, "tp_atr_mults", None),
        }
        # Minimize size: remove None
        return {k: v for k, v in raw.items() if v is not None}

    def stable_signal_id(self, payload: dict[str, Any]) -> str:
        """
        Stable signal_id for replay/regression tests.
        Key: symbol|kind|side|ts_bucket|level_price_rounded|venue|timeframe
        """
        ts = int(payload.get("ts", 0) or 0)
        bucket_ms = int(os.getenv("OUTBOX_SEM_DEDUP_BUCKET_MS", "1000") or 1000)
        ts_bucket = (ts // max(bucket_ms, 1)) * max(bucket_ms, 1)

        lvl = payload.get("level_price")
        try:
            lvl_f = float(lvl) if lvl is not None else 0.0
        except Exception:
            lvl_f = 0.0

        lvl_dec = int(os.getenv("OUTBOX_SEM_DEDUP_LEVEL_DECIMALS", "2") or 2)
        lvl_r = round(lvl_f, max(0, lvl_dec))

        sym = (payload.get("symbol", "") or "")
        kind = (payload.get("kind", "") or "")
        side = (payload.get("side", "") or "")
        venue = str(payload.get("venue", "") or payload.get("exchange", "") or "")
        tf = (payload.get("timeframe", "") or "")

        base = f"{sym}|{kind}|{side}|{ts_bucket}|{lvl_r}|{venue}|{tf}"
        return hashlib.sha1(base.encode("utf-8")).hexdigest()
