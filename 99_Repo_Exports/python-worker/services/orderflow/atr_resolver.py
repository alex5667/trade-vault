import math
import logging
from typing import Any
from core.dyn_cfg_keys import DynCfgKeys as DK

logger = logging.getLogger("crypto_atr_resolver")

class ATRResolver:
    def __init__(self, facade: Any):
        self.facade = facade
        self.atr_cache = facade.atr_cache

    def get_atr_for_symbol(self, symbol: str, cfg: dict[str, Any], tf_override: str | None = None, runtime: Any | None = None) -> float | None:
        """
        Extract the dynamically configured ATR tf and its value for the symbol.
        """
        from core.atr_floor_policy import compute_atr_bps_threshold
        
        tf = tf_override
        if not tf:
            tf = "1m"
            if runtime:
                sel = runtime.dynamic_cfg.get(DK.ATR_TF_SEL, "")
                if sel and isinstance(sel, str):
                    tf = sel
                else:
                     tf = cfg.get("atr_tf_default", "1m")
            else:
                 tf = cfg.get("atr_tf_default", "1m")

        val = self.atr_cache.get_atr(symbol, tf)
        if val is not None and val > 0:
             # V1 Sanity: check if bps is too large/small vs limits
             if runtime:
                 px = float(getattr(runtime, "last_price", 0.0) or 0.0)
                 bps_val = (val / px) * 10000.0 if px > 0 else 0.0

                 min_bps, max_bps = compute_atr_bps_threshold(cfg)
                 enable_sanity = int(runtime.config.get("atr_sanity_enable", 1) or 1)
                 
                 if enable_sanity == 1 and px > 0:
                     if min_bps is not None and bps_val < min_bps:
                          pass # Could log or modify, but V1 implementation just returns
                     elif max_bps is not None and bps_val > max_bps:
                          pass
             return val

        # Fallbacks
        for fallback_tf in ["1m", "5m", "15m"]:
            val = self.atr_cache.get_atr(symbol, fallback_tf)
            if val is not None and val > 0:
                return val
        return None
