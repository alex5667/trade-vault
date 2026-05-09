from __future__ import annotations

import math
import os
from typing import Any

from common.normalization import SIGNAL_ID_ALGO_V1, generate_signal_id
from services.atr_horizon_live_surface import build_live_risk_surface
from services.atr_horizon_live_surface_canary import should_apply_live_surface
from services.atr_horizon_shadow_surface import build_risk_surface_shadow
from services.atr_policy_provenance import build_policy_provenance
from services.atr_policy_resolver import get_atr_policy_resolver
from services.atr_policy_rollout_router import build_rollout_sticky_key, should_apply_rollout
from services.horizon_contract import attach_phase0_contract
from utils.time_utils import get_ny_time_millis

# Cache environment variables at module level (Zero I/O in Hot Path)
_DQ_TICK_GAP_FLAG_MS = int(os.getenv("DQ_TICK_GAP_FLAG_MS", "5000"))
_DQ_BOOK_STALE_FLAG_MS = int(os.getenv("DQ_BOOK_STALE_FLAG_MS", "1500"))
_DQ_SPREAD_WIDE_FLAG_BPS = float(os.getenv("DQ_SPREAD_WIDE_FLAG_BPS", "12.0"))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except Exception:
        return default


def _dedup_str_list(xs: list[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for x in xs:
        s = (x or "").strip().lower()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def preprocess_signal_for_publish(signal: dict[str, Any], symbol: str, source: str, logger: Any, fast_path: bool = False) -> dict[str, Any]:
    """
    In-place normalize + attach *data-quality* flags that downstream gates can use.

    Contract goals:
      - deterministic time fields (epoch ms)
      - numeric coercions for price/entry
      - micro defaults (spread/book staleness)
      - `data_quality_flags`: list[str] (lowercase) for hard veto gates (optional)

    This function MUST be safe to call multiple times.
    """
    # Safe-check for dictionary interface
    if not hasattr(signal, "get") or not isinstance(signal, dict):
        return signal

    # Time
    now_ms = get_ny_time_millis()
    ts = signal.get("tick_ts") or signal.get("ts_ms") or signal.get("ts") or now_ms
    try:
        ts_ms = int(ts)
    except Exception:
        ts_ms = now_ms
    if ts_ms <= 0:
        ts_ms = now_ms

    signal["v"] = 1

    signal["symbol"] = str(symbol or signal.get("symbol") or "").upper()
    signal["ts_ms"] = int(ts_ms)
    signal["tick_ts"] = int(signal.get("tick_ts") or ts_ms)

    # Direction / side (keep legacy `side` for consumers)
    direction = (signal.get("direction") or "").upper().strip()
    if not direction:
        direction = (signal.get("side") or "").upper().strip()

    if direction in {"LONG", "SHORT", "BUY", "SELL"}:
        if direction in {"BUY", "LONG"}:
            norm_dir = "LONG"
            side_int = 1
        else:
            norm_dir = "SHORT"
            side_int = -1

        exec_side = "BUY" if norm_dir == "LONG" else "SELL"
        signal["direction"] = norm_dir          # LONG | SHORT (strategy)
        signal["side"] = exec_side              # BUY  | SELL  (execution)
        signal["side_lc"] = exec_side.lower()
        signal["side_uc"] = exec_side
        signal["side_int"] = side_int

    # Signal ID / sid
    if not signal.get("signal_id"):
        kind = (signal.get("kind") or "crypto-of")
        direction_for_id = (signal.get("direction") or "LONG")
        signal["signal_id"] = generate_signal_id(
            symbol=signal["symbol"],
            ts_ms=signal["ts_ms"],
            direction=direction_for_id,
            kind=kind,
        )
        signal["id_algo"] = SIGNAL_ID_ALGO_V1

    signal.setdefault("id_algo", SIGNAL_ID_ALGO_V1)
    signal["sid"] = signal["signal_id"]

    # Confidence mirrors
    if "confidence" in signal:
        conf = _safe_float(signal["confidence"], 0.0)
        # normalize to 0..1 and 0..100
        if conf > 1.0:
            c01 = conf / 100.0
            cpct = conf
        else:
            c01 = conf
            cpct = conf * 100.0
        signal["confidence01"] = round(float(c01), 4)
        signal["confidence_pct"] = round(float(cpct), 2)

    # Numeric coercions
    if "price" in signal:
        signal["price"] = _safe_float(signal.get("price"), 0.0)
    if "entry" in signal:
        signal["entry"] = _safe_float(signal.get("entry"), signal.get("price") or 0.0)
        signal["entry_price"] = signal["entry"]
        if "price" not in signal:
            signal["price"] = signal["entry"]

    # Fast path for high-frequency telemetry (BBO, CVD, etc)
    if fast_path:
        return signal

    # Micro defaults
    micro = signal.get("micro")
    if not isinstance(micro, dict):
        micro = {}
        signal["micro"] = micro

    micro.setdefault("spread_bps", 0.0)
    micro.setdefault("book_stale_ms", 10**9)

    # Indicators may already hold DQ hints
    indicators = signal.get("indicators")
    if not isinstance(indicators, dict):
        indicators = {}
        signal["indicators"] = indicators

    # ------------------------------------------------------------------
    # Data-quality flags (fail-open by default; veto is controlled elsewhere)
    # ------------------------------------------------------------------
    flags: list[str] = []
    if isinstance(signal.get("data_quality_flags"), list):
        flags.extend([str(x) for x in signal.get("data_quality_flags") if x is not None])

    # Tick health hints from indicators
    if int(indicators.get("tick_ts_missing", 0) or 0) == 1:
        flags.append("tick_ts_missing")
    if int(indicators.get("tick_oood", 0) or 0) == 1:
        flags.append("tick_oood")
    if "tick_gap_ms" in indicators:
        try:
            gap = int(indicators.get("tick_gap_ms") or 0)
            if gap > _DQ_TICK_GAP_FLAG_MS:
                flags.append("tick_gap")
        except Exception:
            pass

    # L2/book freshness derived from micro
    try:
        book_stale = int(micro.get("book_stale_ms") or 0)
        if book_stale > _DQ_BOOK_STALE_FLAG_MS:
            flags.append("stale_l2")
    except Exception:
        pass

    # Spread widening flag
    try:
        spread_bps = float(micro.get("spread_bps") or 0.0)
        if spread_bps > _DQ_SPREAD_WIDE_FLAG_BPS:
            flags.append("wide_spread")
    except Exception:
        pass

    # Optional: missing trade_id (not a veto by default; useful for diagnostics)
    if signal.get("trade_id") is None:
        flags.append("missing_trade_id")

    signal["data_quality_flags"] = _dedup_str_list(flags)

    # ------------------------------------------------------------------
    # Phase 0: horizon-aware contract surface (observe-only, idempotent)
    # ------------------------------------------------------------------
    try:
        attach_phase0_contract(
            signal,
            symbol=str(symbol or signal.get("symbol") or ""),
            source=(source or "unknown"),
        ),
    except Exception as exc:  # noqa: BLE001
        # fail-open: contract emission must never block trading
        try:
            if logger:
                logger.debug("phase0 contract attach failed: %s", exc)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Phase 2.2: shadow stop/entry risk surface from selected ATR.
    # Does NOT change sl_price / tp1_price — only emits shadow surface
    # into meta for post-trade comparison and diagnostics.
    # ------------------------------------------------------------------
    try:
        if os.getenv("ATR_HORIZON_SHADOW_RISK_SURFACE_ENABLE", "1") == "1":
            meta = signal.setdefault("meta", {})
            if "risk_surface_shadow" not in meta:
                meta["risk_surface_shadow"] = build_risk_surface_shadow(signal)
    except Exception as exc:  # noqa: BLE001
        try:
            if logger:
                logger.debug("phase2.2 risk surface shadow attach failed: %s", exc)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Phase 2.4: canary live stop/tp1/max_signal_age from selected ATR.
    # trailing remains untouched downstream (get_atr path after TP1/BE).
    # signal_id is NOT changed — stable hash does not depend on sl/tp.
    # ------------------------------------------------------------------
    try:
        if os.getenv("ATR_HORIZON_LIVE_SURFACE_ENABLE", "1") == "1":
            meta = signal.setdefault("meta", {})
            horizon = meta.get("horizon", {}) if isinstance(meta.get("horizon"), dict) else {}
            policy = get_atr_policy_resolver().resolve(
                source=str(source or signal.get("source") or "CryptoOrderFlow"),
                symbol=str(signal.get("symbol") or symbol or ""),
                scenario=str(signal.get("kind") or signal.get("scenario") or "default"),
                regime=str(meta.get("regime") or signal.get("regime") or "na"),
                risk_horizon_bucket=str(horizon.get("risk_horizon_bucket") or signal.get("risk_horizon_bucket") or "unknown"),
            ),
            meta["atr_policy_resolution"] = policy

            # Phase 4 metadata
            meta["atr_policy_ver"] = int(policy.get("policy_ver", 0))
            meta["atr_policy_snapshot_kind"] = (policy.get("level", "unknown"))
            meta["atr_policy_applied_key"] = (policy.get("active_key", ""))
            meta["atr_policy_kill_switch"] = bool(policy.get("kill_switch_active", False))

            # Phase 4.1: attach resolved ATR policy snapshot metadata
            meta.setdefault("atr_policy_snapshot", {
                "policy_ver": int(policy.get("policy_ver", 0)),
                "source": (policy.get("source", "")),
                "symbol": (policy.get("symbol", "")),
                "scenario": (policy.get("scenario", "")),
                "regime": (policy.get("regime", "")),
                "risk_horizon_bucket": (policy.get("risk_horizon_bucket", "")),
                "stop_ttl_mode": (policy.get("stop_ttl_mode", "canary")),
                "trailing_mode": (policy.get("trailing_mode", "canary")),
                "active_key": (policy.get("active_key", "")),
                "updated_at_ms": int(policy.get("updated_at_ms", 0)),
            })

            signal["atr_policy_ver"] = int(policy.get("policy_ver", 0))
            signal["atr_policy_level"] = (policy.get("level", "miss"))
            signal["atr_policy_key"] = (policy.get("active_key", ""))
            signal["atr_policy_reason_code"] = (policy.get("reason_code", ""))

            decision = should_apply_live_surface(
                symbol=str(signal.get("symbol") or symbol or ""),
                sid=str(signal.get("signal_id") or signal.get("sid") or ""),
                regime=str(meta.get("regime") or signal.get("regime") or ""),
                scenario=str(signal.get("kind") or signal.get("scenario") or ""),
            ),
            meta["live_surface_canary"] = decision

            # always compute candidate surface for observability / diagnostics
            live_surface = build_live_risk_surface(signal)
            meta["risk_surface_live_candidate"] = live_surface

            # keep baseline snapshot once (idempotent) for analytics / rollback
            meta.setdefault("live_surface_baseline", {
                "sl_price": float(signal.get("sl_price") or 0.0),
                "tp1_price": float(signal.get("tp1_price") or 0.0),
                "max_signal_age_ms": int(
                    signal.get("max_signal_age_ms")
                    or meta.get("horizon", {}).get("max_signal_age_ms")
                    or 0
                ),
            })

            apply_live = False
            apply_reason = ""

            rollout_stage = (policy.get("rollout_stage_stop_ttl", "shadow"))
            if rollout_stage == "shadow":
                apply_live = False
                apply_reason = "ATR_POLICY_ROLLOUT_SHADOW"
            elif rollout_stage in {"frozen", "rolled_back"}:
                apply_live = False
                apply_reason = f"ATR_POLICY_ROLLOUT_{rollout_stage.upper()}"
            else:
                sticky_key = build_rollout_sticky_key(signal)
                if should_apply_rollout(sticky_key=sticky_key, rollout_stage=rollout_stage):
                    if (policy.get("stop_ttl_mode") or "canary") == "live":
                        apply_live = True
                        apply_reason = "ATR_POLICY_ACTIVE_STOP_TTL"
                    elif bool(decision.get("should_apply", False)):
                        apply_live = True
                        apply_reason = (decision.get("reason_code") or "LIVE_SURFACE_CANARY_APPLY")
                else:
                    apply_live = False
                    apply_reason = f"ATR_POLICY_ROLLOUT_{rollout_stage.upper()}_MISS"

            if apply_live and live_surface.get("reason_code") == "LIVE_SURFACE_OK":
                signal["sl_price"] = float(live_surface["selected_sl_price"])
                signal["tp1_price"] = float(live_surface["selected_tp1_price"])
                signal["max_signal_age_ms"] = int(live_surface["selected_max_signal_age_ms"])
                meta["live_surface_applied"] = {
                    "applied": True,
                    "reason_code": apply_reason,
                    "atr_tf_ms": int(live_surface.get("atr_tf_ms") or 0),
                    "atr_value": float(live_surface.get("atr_value") or 0.0),
                    "policy_level": (policy.get("level") or "miss"),
                }
            else:
                meta["live_surface_applied"] = {
                    "applied": False,
                    "reason_code": str(policy.get("reason_code") or decision.get("reason_code") or "LIVE_SURFACE_SHADOW_ONLY"),
                    "atr_tf_ms": int(live_surface.get("atr_tf_ms") or 0),
                    "atr_value": float(live_surface.get("atr_value") or 0.0),
                    "policy_level": (policy.get("level") or "miss"),
                }
    except Exception as exc:  # noqa: BLE001
        try:
            if logger:
                logger.debug("phase2.4 live surface attach failed: %s", exc)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Phase 5: runtime policy provenance
    # ------------------------------------------------------------------
    try:
        meta = signal.get("meta")
        if not isinstance(meta, dict):
            meta = {}
            signal["meta"] = meta

        provenance = build_policy_provenance(signal)
        meta["policy_provenance"] = provenance

        # compact top-level aliases for downstream consumers
        signal["atr_policy_ver"] = int(provenance.get("policy_ver", 0) or 0)
        signal["atr_policy_tag"] = (provenance.get("policy_tag") or "")
        signal["atr_recovery_run_id"] = (provenance.get("recovery_run_id") or "")
        signal["atr_restore_cert_status"] = (provenance.get("restore_cert_status") or "")
    except Exception:
        pass

    return signal
