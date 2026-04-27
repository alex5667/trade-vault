from __future__ import annotations

import math
import json
import hashlib
from typing import Any, Optional, Tuple, Dict

from common.dq_flags import append_dq_flag
from common.ctx_cache import cached_on_ctx
from handlers.context_helpers.context_utils import ensure_levels
from signals.level_enricher import attach_trade_levels_to_ctx
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import (
    L2Level, SimpleL2Snapshot, LiquidityContext, ClusterVol
)

try:
    from prometheus_client import Counter
    side_norm_fallback_total = Counter("side_norm_fallback_total", "Side normalization fallback", ["reason"])
except Exception:
    side_norm_fallback_total = None

# Helper alias for compatibility if needed, or just use append_dq_flag directly
def _mark_dq(ctx: Any, flag: str, logger: Any = None, key: str = "", exc: Optional[Exception] = None) -> None:
    append_dq_flag(ctx, flag)
    if logger and exc:
        try:
            logger.warning(f"DQ {flag}: {exc}")
        except Exception:
            pass

class CryptoLiquidity:
    """
    Manages liquidity analysis (walls, depth) and trade level enrichment.
    """
    
    @staticmethod
    def _cfg_hash(cfg: dict | None) -> str:
        """Stable cfg hash for cache keys."""
        try:
            s = json.dumps(cfg or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()
        except Exception:
            return "cfg:err"

    def find_near_liquidity_wall(
        self,
        ctx: Any,
        l2: SimpleL2Snapshot,
        max_levels: int = 10,
        max_dist_bps: float = 15.0,
        size_z_thr: float = 1.5,
    ) -> Tuple[Optional[str], Optional[L2Level], Optional[float]]:
        price = ctx.last_price
        if price is None:
            return None, None, None

        # Helper to extract size/notional
        def _lvl_metric(lvl: Any) -> float:
            try:
                n = float(getattr(lvl, "notional", 0.0) or 0.0)
                if n > 0.0 and math.isfinite(n):
                    return n
            except Exception:
                pass
            try:
                s = float(getattr(lvl, "size", 0.0) or 0.0)
                if s > 0.0 and math.isfinite(s):
                    return s
            except Exception:
                pass
            return 0.0

        sizes = [_lvl_metric(lvl) for lvl in l2.bids[:max_levels]] + [_lvl_metric(lvl) for lvl in l2.asks[:max_levels]]
        sizes = [x for x in sizes if x > 0.0]
        if not sizes:
            return None, None, None

        mu = sum(sizes) / len(sizes)
        var = sum((s - mu) ** 2 for s in sizes) / max(len(sizes) - 1, 1)
        std = math.sqrt(max(var, 1e-9))

        best_side = None
        best_level = None
        best_size_z = None

        def process_side(levels: list[L2Level], side: str):
            nonlocal best_side, best_level, best_size_z
            for lvl in levels[:max_levels]:
                dist_rel = abs(lvl.price - price) / max(price, 1e-6)
                dist_bps = dist_rel * 10_000.0
                if dist_bps > max_dist_bps:
                    continue

                m = _lvl_metric(lvl)
                size_z = (m - mu) / max(std, 1e-6)
                if size_z < size_z_thr:
                    continue

                if best_level is None or size_z > (best_size_z or -1e9):
                    best_side = side
                    best_level = lvl
                    best_size_z = size_z

        process_side(l2.bids, "bid")
        process_side(l2.asks, "ask")

        return best_side, best_level, best_size_z

    def calculate_book_metrics(
        self,
        l2: SimpleL2Snapshot,
    ) -> Dict[str, Any]:
        """
        Calculate common book metrics: spread, depth_5, best prices/qtys.
        """
        best_bid = l2.bids[0] if l2.bids else None
        best_ask = l2.asks[0] if l2.asks else None

        metrics = {
            "best_bid_px": getattr(best_bid, "price", 0.0) if best_bid else 0.0,
            "best_bid_qty": getattr(best_bid, "size", 0.0) if best_bid else 0.0,
            "best_ask_px": getattr(best_ask, "price", 0.0) if best_ask else 0.0,
            "best_ask_qty": getattr(best_ask, "size", 0.0) if best_ask else 0.0,
            "spread_bps": 0.0,
            "depth_5_bid_vol": sum(x.size for x in l2.bids[:5]),
            "depth_5_ask_vol": sum(x.size for x in l2.asks[:5]),
            "top5_bids": [(x.price, x.size) for x in l2.bids[:5]],
            "top5_asks": [(x.price, x.size) for x in l2.asks[:5]],
        }

        if metrics["best_bid_px"] > 0 and metrics["best_ask_px"] > 0:
            mid = (metrics["best_bid_px"] + metrics["best_ask_px"]) / 2.0
            spread = metrics["best_ask_px"] - metrics["best_bid_px"]
            metrics["spread_bps"] = (spread / mid) * 10_000.0 if mid > 0 else 0.0

        return metrics

    def build_liquidity_context(
        self,
        ctx: Any,
        l2: SimpleL2Snapshot,
        cluster: Optional[ClusterVol] = None,
    ) -> LiquidityContext:
        lc = LiquidityContext()
        
        # 1) Calculate basic metrics
        bm = self.calculate_book_metrics(l2)
        lc.depth_5_vol = bm["depth_5_bid_vol"] + bm["depth_5_ask_vol"] # Or side-specific? Handled below.

        # 2) Find near wall
        side, lvl, size_z = self.find_near_liquidity_wall(ctx, l2)
        if side is None or lvl is None or size_z is None:
            return lc

        lc.near_wall_side = side
        lc.near_wall_price = lvl.price
        lc.near_wall_size = lvl.size
        lc.near_wall_size_z = size_z

        # 3) Override depth_5 with side-specific if needed by existing logic
        # (Original code used: sum(x.size for x in (l2.bids[:5] if side == "bid" else l2.asks[:5])))
        lc.depth_5_vol = bm["depth_5_bid_vol"] if side == "bid" else bm["depth_5_ask_vol"]
        
        return lc

    def ensure_levels_once(self, ctx: Any, *, side: Any, logger: Any = None) -> None:
        """
        ensure_levels(...) cheap, but called many times.
        """
        key = (str(side),)
        def _compute():
            try:
                ensure_levels(ctx, side=side)
            except Exception:
                try:
                    _mark_dq(ctx, "ensure_levels_failed", logger=logger, key="ensure_levels_failed")
                except Exception:
                    pass
            return True

        cached_on_ctx(ctx, slot="_cache_ensure_levels", key=key, compute=_compute)

    def ensure_trade_levels_once(
        self,
        *,
        ctx: Any,
        side: Any,
        symbol: str,
        kind: str,
        cfg: dict | None,
        regime: Any = None,
        empirical: Any = None,
        overwrite: bool = False,
        logger: Any = None,
    ) -> None:
        """
        Attach trade levels to ctx with caching and normalization.
        """
        if ctx is None:
            return

        # 0) ensure minimal invariants first
        self.ensure_levels_once(ctx, side=side, logger=logger)

        # 1) Normalize side
        def _norm_side(s) -> Optional[str]:
            try:
                if s in (1, +1, True): return "LONG"
                if s in (-1, -1.0, False): return "SHORT"
            except Exception: pass
            try:
                if isinstance(s, str):
                    u = s.strip().upper()
                    if u in {"LONG", "BUY", "+1", "1"}: return "LONG"
                    if u in {"SHORT", "SELL", "-1"}: return "SHORT"
                # enums/objects
                u = str(getattr(s, "name", None) or getattr(s, "value", None) or s).strip().upper()
                if u in {"LONG", "BUY"}: return "LONG"
                if u in {"SHORT", "SELL"}: return "SHORT"
            except Exception: pass
            return None

        side_s = _norm_side(side if side is not None else getattr(ctx, "side", None))
        if side_s is None:
            if side_norm_fallback_total:
                try:
                    side_norm_fallback_total.labels(reason="unrecognized_side").inc()
                except Exception:
                    pass
            assert False, f"Unrecognized or invalid side: {side}"
        try:
            cfgd = dict(cfg or {})
        except Exception:
            cfgd = {}

        # 2) Cache key
        rg_key = None
        try:
            if empirical is not None and regime is not None:
                rg_key = str(regime)
        except Exception:
            rg_key = None
        key = (str(symbol), str(side_s), str(kind), CryptoLiquidity._cfg_hash(cfgd), rg_key, bool(empirical is not None))
        
        try:
            prev = getattr(ctx, "_trade_levels_key", None)
            if (not overwrite) and prev == key:
                return
        except Exception:
            pass

        # 3) Compute
        force_overwrite = False
        if getattr(ctx, "_trade_levels_key", None) is not None and prev != key and not overwrite:
             # Logic from handler: existing levels might be wrong side -> force overwrite
             # But simplified here: if we are here, we proceed to attach.
             # Note: logic in handler had force_overwrite variable but used it to call attach with force=True?
             # Let's check handler code again.
             pass

        try:
            # We call attach_trade_levels_to_ctx which does the heavy lifting
            res = attach_trade_levels_to_ctx(
                ctx=ctx,
                side=side_s,
                symbol=str(symbol),
                cfg=cfgd,
                empirical=empirical,
                regime=regime,
                overwrite=overwrite,
            )
            
            # Sizing (RR-mode fixed risk)
            try:
                from services.position_sizing import apply_position_sizing_to_ctx
                apply_position_sizing_to_ctx(ctx, cfg=cfgd, symbol=str(symbol), logger=logger)
            except Exception:
                pass

            # Update cache key
            setattr(ctx, "_trade_levels_key", key)
        except Exception as e:
            _mark_dq(ctx, "trade_levels_attach_error", logger=logger, key="trade_levels_attach_error", exc=e)
