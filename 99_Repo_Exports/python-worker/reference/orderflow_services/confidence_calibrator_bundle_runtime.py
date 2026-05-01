
import json
import os
import time
import logging
import math
import asyncio
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

class ConfidenceCalibratorBundleRuntime:
    """
    Runtime loader for Confidence Calibration Bundle V2 (Global + Buckets).
    Supports hot-reload via file modification time check.
    """
    def __init__(self, bundle_path: str, poll_interval_ms: int = 5000):
        self.bundle_path = bundle_path
        self.poll_interval_ms = poll_interval_ms
        self.last_check_ms = 0
        self.last_mtime = 0
        self.bundle: Optional[Dict[str, Any]] = None
        self.config_loaded = False
        
        # Determine strictness from env (fail safe)
        self.fail_open = True 

    def _load_bundle(self):
        """Loads the bundle from disk if changed."""
        try:
            if not os.path.exists(self.bundle_path):
                if not self.config_loaded:
                    logger.warning(f"Confidence bundle not found: {self.bundle_path}")
                return

            mt = os.path.getmtime(self.bundle_path)
            if mt == self.last_mtime and self.config_loaded:
                return

            logger.info(f"Loading/Reloading confidence bundle from {self.bundle_path}")
            with open(self.bundle_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Basic validation
            schema_ver = data.get("schema_version")
            if schema_ver not in (2, 3):
                logger.warning(f"Unknown schema version {schema_ver}, expected 2 or 3. Proceeding anyway.")
            
            self.bundle = data
            self.last_mtime = mt
            self.config_loaded = True
            logger.info(f"Loaded bundle v{data.get('version', '?')} (schema {schema_ver}) generated at {data.get('generated_at', '?')}")
            
        except Exception as e:
            logger.error(f"Failed to load specific confidence bundle: {e}")
            if not self.config_loaded:
                self.bundle = None 

    def maybe_reload(self, now_ms: int):
        """Polls for updates throttled by poll_interval."""
        if now_ms - self.last_check_ms > self.poll_interval_ms:
            self.last_check_ms = now_ms
            self._load_bundle()

    def get_calibrated_confidence(self, raw_conf: float, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns calibrated confidence and metadata.
        Output keys: result (float), method (str), bucket_key (str), bucket_by (str), schema_version (int)
        """
        # Default: fallback to identity (raw)
        res = {
            "result": raw_conf, 
            "method": "identity", 
            "bucket_key": "global", 
            "bucket_by": "none",
            "bucket_level": "none",
            "fallback_depth": 0,
            "schema_version": 0
        }

        if not self.config_loaded or not self.bundle:
            return res
            
        schema_ver = self.bundle.get("schema_version", 2)
        res["schema_version"] = schema_ver

        try:
            meta = self.bundle.get("meta", {})
            bucket_by = meta.get("bucket_by", "none")
            buckets = self.bundle.get("buckets", {})
            
            # 1. Determine Bucket Key(s) Hierarchically
            bkeys = ["global"]
            
            # Context extraction
            s = str(context.get("session", "OFF"))
            r = str(context.get("regime", "neutral"))
            sym = str(context.get("symbol", "unknown"))
            
            if schema_ver >= 3:
                # V3 Hierarchical Lookup
                # Pattern: SYM|sess|reg -> SYM|sess|any -> SYM|any|reg -> SYM|any|any -> GLOBAL|... -> GLOBAL
                # Actually, standard "bucket_by" might control the hierarchy structure. 
                # But implementation plan says: Fallback logic: SYM|sess|reg -> ...
                # Let's assume bucket_by="hierarchical" or we just try specific paths if available.
                # If bucket_by is specific (e.g. session_regime), we might still want symbol specific overrides?
                # The prompt implies a standard hierarchy check.
                
                # Check 1: Symbol Specific
                # Structure: "SYM|{sym}|{s}_{r}" or "{sym}|{s}|{r}"? 
                # Let's stick to a reliable key format. 
                # If the trainer generates keys like "BTCUSDT|ASIA|trend_up", we match that.
                
                # Full specific
                bkeys = [
                    f"{sym}|{s}|{r}",       # Symbol + Session + Regime
                    f"{sym}|{s}|any",       # Symbol + Session
                    f"{sym}|any|{r}",       # Symbol + Regime
                    f"{sym}|any|any",       # Symbol Only
                    # Global fallbacks
                    f"GLOBAL|{s}|{r}",
                    f"GLOBAL|{s}|any",
                    f"GLOBAL|any|{r}",
                    "global"
                ]
            else:
                # V2 Legacy Logic
                if bucket_by == "session":
                    bkeys = [s, "global"]
                elif bucket_by == "regime":
                    bkeys = [r, "global"]
                elif bucket_by == "session_regime":
                    bkeys = [f"{s}_{r}", s, r, "global"]
                elif bucket_by == "symbol":
                    bkeys = [sym, "global"]

            
            # 2. Find Calibrator (First match in buckets)
            cal_cfg = None
            bkey = "global" # Default
            bucket_level = "global"
            fallback_depth = 0
            
            for i, k in enumerate(bkeys):
                if k in buckets:
                    cal_cfg = buckets[k]
                    bkey = k
                    bucket_level = k.split("|")[0] if "|" in k else k # rough level
                    fallback_depth = i
                    break
            
            if not cal_cfg:
                # Should not happen if "global" is in bkeys and in buckets
                return res
            
            # 3. Apply Calibration
            method = cal_cfg.get("method", "identity")
            params = cal_cfg.get("params", {})
            
            cal_val = raw_conf
            
            if method == "input" or method == "identity":
                cal_val = raw_conf
            
            elif method in ("platt", "platt_logit"):
                # Platt Scaling: 1 / (1 + exp(-(A * x + B)))
                # V3: platt_logit expects input to be logit? Or just standard Platt on output?
                # Standard Platt maps score -> probability via sigmoid.
                # If score is already prob, we usually convert to logit first.
                # Legacy "platt" in V2 seemed to do a*prob + b? No, checks line 135: a*raw_conf+b.
                # Wait, line 139 was `logit = a * raw_conf + b`.
                # If raw_conf is [0,1], then a*raw_conf+b is linear. Then sigmoid. 
                # This is technically not correct Platt if input is probability.
                # True Platt is logistic regression on valid logits.
                # Correct Platt: a * logit(p) + b.
                
                # Support old V2 behavior for "platt" if needed, or assume trained model matches.
                # If "platt_logit" is used, we definitely convert to logit first.
                
                x_val = raw_conf
                if method == "platt_logit":
                    # Convert p -> logit
                    if 0.0 < raw_conf < 1.0:
                        x_val = math.log(raw_conf / (1.0 - raw_conf))
                    else:
                        # Edge cases
                        x_val = -15.0 if raw_conf <= 0.0 else 15.0
                
                # Check for slope/intercept or a/b
                a = float(params.get("a") if params.get("a") is not None else params.get("slope", 1.0))
                b = float(params.get("b") if params.get("b") is not None else params.get("intercept", 0.0))

                logit = a * x_val + b
                # clip logit
                logit = max(-100.0, min(100.0, logit))
                cal_val = 1.0 / (1.0 + math.exp(-logit))
                
            elif method == "temperature_scaling" or method == "temp_logit":
                t = float(params.get("temperature", 1.0))
                if t <= 0: t = 1.0
                
                if 0.0 < raw_conf < 1.0:
                    logit = math.log(raw_conf / (1.0 - raw_conf))
                    cal_val = 1.0 / (1.0 + math.exp(-(logit / t)))
                else:
                    cal_val = raw_conf

            elif method in ("beta", "beta_simplified"):
                a = float(params.get("a", 1.0))
                b = float(params.get("b", 1.0))
                c = float(params.get("c", 0.0))
                
                if 0.0 < raw_conf < 1.0:
                    try:
                        ln_p = math.log(raw_conf)
                        ln_1_p = math.log(1.0 - raw_conf)
                        logit = a * ln_p + b * ln_1_p + c
                        logit = max(-100.0, min(100.0, logit))
                        cal_val = 1.0 / (1.0 + math.exp(-logit))
                    except:
                        cal_val = raw_conf
                else:
                    cal_val = raw_conf
            
            elif method == "isotonic":
                boundaries = params.get("boundaries") or params.get("f_x") or cal_cfg.get("boundaries") or cal_cfg.get("f_x") or []
                values = params.get("values") or params.get("f_y") or cal_cfg.get("values") or cal_cfg.get("f_y") or []

                if not boundaries:
                    cal_val = raw_conf
                else:
                    x = raw_conf
                    if x <= boundaries[0]:
                        cal_val = values[0]
                    elif x >= boundaries[-1]:
                        cal_val = values[-1]
                    else:
                        # Find interval
                        # Binary search via bisect could be faster but linear is fine for small Arrays
                        for i in range(len(boundaries) - 1):
                            if boundaries[i] <= x <= boundaries[i+1]:
                                x0, x1 = boundaries[i], boundaries[i+1]
                                y0, y1 = values[i], values[i+1]
                                if x1 != x0:
                                    cal_val = y0 + (x - x0) * (y1 - y0) / (x1 - x0)
                                else:
                                    cal_val = y0
                                break

            # Clamp result
            cal_val = max(0.0, min(1.0, cal_val))
            
            res["result"] = cal_val
            res["method"] = method
            res["bucket_key"] = bkey
            res["bucket_by"] = bucket_by
            res["bucket_level"] = bucket_level
            res["fallback_depth"] = fallback_depth
            
        except Exception as e:
            # Fallback on error (silent)
            pass
            
        return res
