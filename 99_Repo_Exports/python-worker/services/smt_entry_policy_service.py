from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis  # type: ignore

from core.active_arm_stabilizer import ActiveArmStabilizer
from core.entry_policy_freeze import EntryPolicyFreezeV1
from core.entry_policy_overrides_v1 import EntryPolicyOverridesV1
from services.entry_policy_ab_gate import decide_active_arm, norm_arm, regime_group

# P0: normalized derivatives context (funding/basis/OI crowding) overlay
from services.orderflow.derivatives_context import aread_derivatives_context
from services.orderflow.derivatives_context_gate import evaluate_derivatives_context

# P6: hard consumer hook — convert P5 autoguard freeze key into real entry-path stop
from services.orderflow.exec_health_freeze_hook import (
    aread_exec_health_auto_freeze,
    build_exec_health_auto_freeze_decision,
)
from services.orderflow.exec_health_observability import (
    record_exec_health_observability,
    record_exec_health_reader_error,
)
from services.orderflow.exec_health_rollups import aread_exec_health_rollups, decide_exec_health_from_env

# P4: SLO contract state writer (fail-open, rate-limited flush)
from services.orderflow.exec_health_slo_contract import (
    flush_exec_health_contract_state_async,
    record_exec_health_contract_reader_error,
    record_exec_health_contract_state,
)

# P6: execution health (TCA rollups) for entry policy
from services.orderflow.utils import session_utc
from utils.time_utils import get_ny_time_millis
import contextlib


def _now_ms() -> int:
    return get_ny_time_millis()

def _i(x: Any, d: int = 0) -> int:
    try: return int(x)
    except Exception: return d

def _f(x: Any, d: float = 0.0) -> float:
    try: return float(x)
    except Exception: return d

def _s(x: Any, d: str = "") -> str:
    try: return str(x) if x is not None else d
    except Exception: return d

def _b(x: Any) -> bool:
    try: return int(x) == 1
    except Exception: return False

def _sha1(s: str) -> str:
    import hashlib
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def _entry_id(cand: dict[str, Any], snap: dict[str, Any], bundle: dict[str, Any]) -> str:
    base = {
        "sym": (cand.get("symbol", "")).upper(),
        "side": (cand.get("side", "")).upper(),
        "bundle": (cand.get("bundle", "")),
        "setup_ts_ms": int(cand.get("setup_ts_ms", 0) or 0),
        "zone_id": (snap.get("zone_id", cand.get("zone_id", "")) or ""),
        "ab_key": (cand.get("ab_key", "") or ""),
        "kind": (bundle.get("decision", "") or ""),
    }
    return _sha1(json.dumps(base, sort_keys=True, separators=(",", ":")))

@dataclass
class PolicyCfg:
    in_stream: str
    out_stream: str
    audit_stream: str
    group: str
    consumer: str
    snap_prefix: str
    bundle_prefix: str
    coh_thr: float
    leader_conf_min: float
    min_of_score: float
    max_zone_bp: float
    max_zone_bp_thin: float
    obi_min_sec: float
    shadow: bool
    dedup_ms: int
    poll_ms: int
    max_age_ms: int
    allow_zone_id_change_if_near: bool
    active_arm_cache_ttl_ms: int
    audit_stream_maxlen: int
    out_stream_maxlen: int

    @staticmethod
    def from_env() -> PolicyCfg:
        return PolicyCfg(
            in_stream=os.getenv("SMT_ENTRY_STREAM", "stream:trade:entry_candidate"),
            out_stream=os.getenv("TRADE_ENTRY_STREAM", "stream:trade:entry"),
            audit_stream=os.getenv("TRADE_ENTRY_AUDIT_STREAM", "stream:trade:entry_audit"),
            group=os.getenv("ENTRY_POLICY_GROUP", "entry-policy"),
            consumer=os.getenv("ENTRY_POLICY_CONSUMER", "c1"),
            snap_prefix=os.getenv("SMT_SNAP_PREFIX", "smt:snap:"),
            bundle_prefix=os.getenv("SMT_BUNDLE_PREFIX", "smt:bundle:v1:"),
            coh_thr=float(os.getenv("SMT_COH_THRESHOLD", "0.65")),
            leader_conf_min=float(os.getenv("SMT_LEADER_CONF_MIN_SCORE", "0.65")),
            min_of_score=float(os.getenv("SMT_ENTRY_MIN_OF_SCORE", "1.0")),
            max_zone_bp=float(os.getenv("SMT_ENTRY_MAX_ZONE_BP", "15.0")),
            max_zone_bp_thin=float(os.getenv("SMT_ENTRY_MAX_ZONE_BP_THIN", "10.0")),
            obi_min_sec=float(os.getenv("SMT_ENTRY_OBI_MIN_SEC", "1.5")),
            shadow=bool(int(os.getenv("ENTRY_POLICY_SHADOW", "0"))),
            dedup_ms=int(os.getenv("SMT_ENTRY_DEDUP_MS", "60000")),
            poll_ms=int(os.getenv("ENTRY_POLICY_POLL_MS", "100")),
            max_age_ms=int(os.getenv("ENTRY_POLICY_MAX_CANDIDATE_AGE_MS", "120000")),
            allow_zone_id_change_if_near=bool(int(os.getenv("ENTRY_POLICY_ALLOW_ZONE_CHANGE_IF_NEAR", "0"))),
            active_arm_cache_ttl_ms=int(os.getenv("ENTRY_POLICY_ACTIVE_ARM_CACHE_TTL_MS", "2000")),
            audit_stream_maxlen=int(os.getenv("TRADE_ENTRY_AUDIT_MAXLEN", "5000")),
            out_stream_maxlen=int(os.getenv("TRADE_ENTRY_MAXLEN", "20000")),
        )

class EntryPolicyService:
    def __init__(self) -> None:
        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)
        self.cfg = PolicyCfg.from_env()
        self._dedup: dict[str, int] = {}
        self.use_event_ts = bool(int(os.getenv("ENTRY_POLICY_USE_EVENT_TS", "0")))

        # === Overrides V1 (strict) ===
        self._ovr_prefix = os.getenv("ENTRY_POLICY_OVR_PREFIX", "cfg:entry_policy:overrides:v1")
        self._ovr_group: dict[str, EntryPolicyOverridesV1] = {} # effective overrides per group
        self._ovr_loaded_ts_ms: dict[str, int] = {}            # per group: last loaded updated_ts_ms
        self._ovr_last_apply_ts_ms: dict[str, int] = {}        # per group: last effective application wall-time
        self._ovr_last_seen_hash: dict[str, str] = {}          # per group: applied raw hash
        self._ovr_cand_hash: dict[str, str] = {}               # per group: candidate hash
        self._ovr_cand_first_ts: dict[str, int] = {}           # per group: first seen ts
        self._ovr_cand_raw: dict[str, str] = {}                # per group: raw json staged

        # === Active arm stabilizer ===
        self._arm_stab = ActiveArmStabilizer(
            hold_down_ms=int(os.getenv("ACTIVE_ARM_HOLD_DOWN_MS", "600000")),
            min_switch_gap_ms=int(os.getenv("ACTIVE_ARM_MIN_SWITCH_GAP_MS", "1800000")),
        )

        from services.entry_policy_core import EntryPolicyCfg
        self.core_cfg = EntryPolicyCfg(
            coh_thr=self.cfg.coh_thr,
            leader_conf_min=self.cfg.leader_conf_min,
            min_of_score=self.cfg.min_of_score,
            max_zone_bp=self.cfg.max_zone_bp,
            max_zone_bp_thin=self.cfg.max_zone_bp_thin,
            obi_min_sec=self.cfg.obi_min_sec,
            dedup_ms=self.cfg.dedup_ms,
            allow_zone_id_change_if_near=self.cfg.allow_zone_id_change_if_near,
        )

        # Optional: max expected slippage veto (used by entry_policy_core).
        # If unset (=0), core keeps this gate disabled.
        try:
            self.core_cfg.max_expected_slippage_bps = float(os.getenv("SMT_ENTRY_MAX_EXPECTED_SLIPPAGE_BPS", "0") or 0.0)
        except Exception:
            self.core_cfg.max_expected_slippage_bps = 0.0

        # P6: execution health (TCA rollups) configuration
        self._exec_entry_policy_enable = bool(int(os.getenv("EXEC_HEALTH_ENTRY_POLICY_ENABLED", "1") or 1))
        self._exec_venue = (os.getenv("EXEC_HEALTH_VENUE", "binance") or "binance").lower()
        self._exec_tf = (os.getenv("EXEC_HEALTH_TF", "all") or "all").lower()

    async def _get_active_arm(self, *, symbol: str, regime: str, group: str, scenario: str, raw_only: bool = False) -> str:
        sym, rg, g, scn = symbol.upper(), regime.lower(), group.lower(), (scenario or "").lower()
        now = _now_ms()

        # Build key candidates (most specific -> fallback)
        keys = []
        if sym and scn in ("continuation", "reversal"):
            keys.append(f"cfg:entry_policy:active_arm:{sym}:{rg}:{g}:{scn}")
        if sym:
            keys.append(f"cfg:entry_policy:active_arm:{sym}:{rg}:{g}")
        keys.append(f"cfg:entry_policy:active_arm:{g}")

        raw_v = ""
        for k in keys:
            v = await self.r.get(k)
            raw_v = (v or "").strip().upper()
            if raw_v: break

        if raw_only:
            return raw_v

        # apply hold-down + gap using stabilizer
        # Stability key should be specific if we found a specific key, but generally tracking per scenario intent is good
        # If we fell back to group, do we stabilize on group key?
        # The prompt implies: "Active arm must be per scenario".
        # Let's use the full scenario key for stabilization to avoid flapping if we switch keys.
        arm_key = f"{sym}:{rg}:{g}:{scn}"
        return self._arm_stab.update(key=arm_key, raw=raw_v, now_ms=now)

    async def _get_freeze(self, *, symbol: str, group: str, scenario: str, now_ms: int) -> tuple[int, int, str, str]:
        sym, grp, scn = symbol.upper(), group.lower(), scenario.lower()
        if scn not in ("reversal", "continuation"): return 0, 0, "", ""
        ck = f"{sym}:{grp}:{scn}"
        if (now_ms - self._freeze_cache_ts.get(ck, 0)) < self._freeze_cache_ttl_ms:
            v = self._freeze_cache.get(ck)
            if v:
                obj, _ = EntryPolicyFreezeV1.from_json(v)
                if obj and obj.is_active(now_ms): return 1, int(obj.until_ts_ms), str(obj.mode), str(obj.reason_code)

        raw = await self.r.get(f"cfg:entry_policy:freeze:v1:{sym}:{grp}:{scn}")
        self._freeze_cache[ck], self._freeze_cache_ts[ck] = (raw or ""), now_ms
        if raw:
            obj, _ = EntryPolicyFreezeV1.from_json(str(raw))
            if obj and obj.is_active(now_ms): return 1, int(obj.until_ts_ms), str(obj.mode), str(obj.reason_code)
        return 0, 0, "", ""

    async def _get_context(self, cand: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        snap = await self._get_snap(cand["symbol"])
        bundle = await self._get_bundle(cand["bundle"])
        return snap, bundle

    async def _get_snap(self, sym: str) -> dict[str, Any]:
        # Merge base SMT snapshot + optional sidecar fields.
        # Sidecar is used for evolving metrics without breaking SymbolSnapshot schema.
        base_key = f"{self.cfg.snap_prefix}{sym.upper()}"
        extra_prefix = os.getenv("SMT_SNAP_EXTRA_PREFIX", "smt:snap_extra:")
        extra_key = f"{extra_prefix}{sym.upper()}"
        raw = None
        rawx = None
        try:
            raw, rawx = await self.r.mget(base_key, extra_key)
        except Exception:
            raw = await self.r.get(base_key)
            rawx = await self.r.get(extra_key)
        snap = json.loads(raw) if raw else {}
        if rawx:
            try:
                extra = json.loads(rawx)
                if isinstance(extra, dict):
                    snap.update(extra)
            except Exception:
                pass
        return snap

    async def _get_bundle(self, bid: str) -> dict[str, Any]:
        d = await self.r.hgetall(f"{self.cfg.bundle_prefix}{bid}")
        return d or {}

    async def _maybe_attach_exec_health(self, *, now_ms: int, cand: dict[str, Any], snap: dict[str, Any], bundle: dict[str, Any]) -> tuple[bool, str, str]:
        """Apply the canonical execution-health overlay to entry policy.

        Behaviour is intentionally fail-open:
          - missing Redis / missing rollups -> allow without mutation
          - monitor/default/soft -> annotate only
          - strict/tighten -> annotate + tighten expected_slippage_bps
          - hard/veto -> annotate + tighten + veto
        """
        if not self._exec_entry_policy_enable:
            return True, "", ""

        try:
            sym = (cand.get("symbol") or "").upper()
            side = (cand.get("side") or "NA").upper()
            kind = str(bundle.get("decision") or cand.get("scenario") or "all").lower()
            sess = str(session_utc(int(now_ms)))
            profile = os.getenv("ENTRY_POLICY_PROFILE", os.getenv("GATE_PROFILE", "default") or "default").strip().lower()

            try:
                roll = await aread_exec_health_rollups(
                    redis=self.r,
                    sym=sym,
                    venue=self._exec_venue,
                    session=sess,
                    tf=self._exec_tf,
                    kind=kind,
                    side=side,
                )
            except Exception:
                record_exec_health_reader_error(scope="entry_policy", where="read_rollups")
                record_exec_health_contract_reader_error(scope="entry_policy")
                raise

            dec = decide_exec_health_from_env(profile=profile, rollups=roll, scope="entry_policy") if roll else None
            record_exec_health_observability(
                symbol=sym,
                scope="entry_policy",
                profile=profile,
                rollups=roll,
                decision=dec,
                now_ms=int(now_ms),
            )
            # P4 SLO contract: record decision outcome (fail-open)
            try:
                record_exec_health_contract_state(
                    scope="entry_policy",
                    profile=str(profile),
                    symbol=str(sym),
                    decision=dec,
                    now_ms=int(now_ms),
                )
                await flush_exec_health_contract_state_async(redis_client=self.r, scope="entry_policy")
            except Exception:
                pass  # never block trading on contract write failure
            if not roll or dec is None:
                return True, "", ""

            snap["tca_is_p95_bps"] = float(roll.get("is_p95_bps", 0.0) or 0.0)
            snap["tca_perm_impact_p95_bps"] = float(roll.get("perm_impact_p95_bps", 0.0) or 0.0)
            snap["tca_realized_spread_p50_bps"] = float(roll.get("realized_spread_p50_bps", 0.0) or 0.0)
            if "perm_impact_p95_bps_delta_sec" in roll:
                snap["tca_perm_impact_p95_bps_delta_sec"] = int(roll.get("perm_impact_p95_bps_delta_sec", 0) or 0)
            if "realized_spread_p50_bps_delta_sec" in roll:
                snap["tca_realized_spread_p50_bps_delta_sec"] = int(roll.get("realized_spread_p50_bps_delta_sec", 0) or 0)

            snap["exec_health_apply"] = int(1 if dec.apply else 0)
            snap["exec_health_veto"] = int(1 if dec.veto else 0)
            snap["exec_health_mode"] = str(dec.mode)
            snap["exec_health_flags"] = ",".join(dec.flags)
            snap["exec_health_reason"] = str(dec.reason_code or "")
            snap["exec_health_tighten_add_bps"] = float(dec.tighten_add_bps or 0.0)
            snap["exec_health_tighten_k"] = float(dec.tighten_k_mult or 1.0)

            # strict/tighten: raise expected_slippage_bps in snap (and micro if present)
            if float(dec.tighten_add_bps or 0.0) > 0.0:
                micro = snap.get("micro") if isinstance(snap.get("micro"), dict) else {}
                exp0 = _f(snap.get("expected_slippage_bps", micro.get("expected_slippage_bps", 0.0)), 0.0)
                exp1 = float(exp0 + float(dec.tighten_add_bps))
                snap["expected_slippage_bps"] = exp1
                if isinstance(micro, dict):
                    micro["expected_slippage_bps"] = exp1
                    snap["micro"] = micro

            # hard/veto: block entry with structured reason
            if dec.veto:
                notes = f"flags={snap.get('exec_health_flags','')} is_p95={snap.get('tca_is_p95_bps', 0.0):.2f} perm_impact_p95={snap.get('tca_perm_impact_p95_bps', 0.0):.2f} realized_spread_p50={snap.get('tca_realized_spread_p50_bps', 0.0):.2f}"
                return False, str(dec.reason_code or "VETO_EXEC_HEALTH"), notes
            return True, "", ""
        except Exception:
            return True, "", ""

    async def _maybe_enforce_exec_health_auto_freeze(
        self, *, now_ms: int, cand: dict[str, Any], snap: dict[str, Any], bundle: dict[str, Any]
    ) -> tuple[bool, str, str]:
        """P6 hard hook: turn autoguard freeze key into real entry-path deny.

        Fail-open on Redis/read errors. This is the consumer-side enforcement for the
        global freeze key set by exec_health_slo_autoguard_v1.py.
        Early pre-evaluate deny (before entry_policy_core decision) stops expensive
        evaluation; final safety check in _emit_entry covers race window.
        """
        try:
            fr = await aread_exec_health_auto_freeze(
                redis=self.r,
                scope="entry_policy",
                now_ms=int(now_ms),
            )
            snap["exec_health_auto_freeze_active"] = int(1 if fr.active else 0)
            snap["exec_health_auto_freeze_until_ts_ms"] = int(fr.freeze_until_ts_ms or 0)
            snap["exec_health_auto_freeze_reason"] = str(fr.freeze_reason or "")
            if not fr.active:
                return True, "", ""
            dec = build_exec_health_auto_freeze_decision(
                scope="entry_policy",
                state=fr,
                reason_code="DENY_EXEC_HEALTH_AUTO_FREEZE",
            )
            return False, str(dec.reason_code), str(dec.notes)
        except Exception:
            # Fail-open: never block entry on hook errors
            return True, "", ""

    async def _maybe_poll_overrides(self, now_ms: int, *, cand: dict[str, Any], snap: dict[str, Any], bundle: dict[str, Any]) -> None:
        """
        Poll overrides using strict schema + hold-down + hysteresis.
        Key precedence (most specific first):
          1) cfg:entry_policy:overrides:v1:{symbol}:{regime}:{scenario}:{group}
          2) cfg:entry_policy:overrides:v1:{symbol}:{regime}:{group}
          3) cfg:entry_policy:overrides:v1:{group}
          4) cfg:entry_policy:overrides:v1
        Deterministic: apply only if updated_ts_ms increases.
        """
        sym = (cand.get("symbol") or "").strip().upper()
        rg = (snap.get("regime", cand.get("regime", "na")) or "na").strip().lower()
        grp = (cand.get("ab_group") or "default").strip().lower()
        scn = (bundle.get("decision", cand.get("scenario", "")) or "").strip().lower()
        prefix = "cfg:entry_policy:overrides:v1"
        keys = [
            f"{prefix}:{sym}:{rg}:{scn}:{grp}" if (sym and rg and scn) else "",
            f"{prefix}:{sym}:{rg}:{grp}" if (sym and rg) else "",
            f"{prefix}:{grp}" if grp else "",
            prefix,
        ]
        keys = [k for k in keys if k]
        raw = ""
        try:
            for k in keys:
                raw = str(await self.r.get(k) or "")
                if raw:
                    break
        except Exception:
            return

        o, status = EntryPolicyOverridesV1.from_json(raw)
        if o is None:
            return
        ok, _ = o.validate()
        if not ok:
            return

        # Apply only if updated_ts_ms moves forward (hysteresis)
        try:
            if int(o.updated_ts_ms or 0) <= int(self._ovr_loaded_ts_ms.get(grp, 0)):
                return
        except Exception:
            pass

        self._ovr_group[grp] = o
        self._ovr_loaded_ts_ms[grp] = int(o.updated_ts_ms or now_ms)
        self._ovr_last_apply_ts_ms[grp] = int(now_ms)

    async def _audit(self, *, now_ms: int, cand: dict[str, Any], ok: bool, reason_code: str, notes: str, snap: dict[str, Any], bundle: dict[str, Any], arm: str = "NA", ovr: EntryPolicyOverridesV1 = None) -> None:
        try:
            o = ovr or EntryPolicyOverridesV1()
            eid = _entry_id(cand, snap, bundle)
            payload = {
                "ts_ms": now_ms, "entry_id": eid, "ok": 1 if ok else 0, "reason_code": str(reason_code), "notes": str(notes),
                "regime": (snap.get("regime", cand.get("regime", "na"))), "ab_arm": (cand.get("ab_arm", arm)),
                "ab_group": (cand.get("ab_group", "default")), "symbol": cand.get("symbol"), "side": cand.get("side"),
                "ab_split_reason": (cand.get("ab_split_reason","") or ""),
                "ab_split_a": float(cand.get("ab_split_a", 0.0) or 0.0),
                "ab_split_b": float(cand.get("ab_split_b", 0.0) or 0.0),
                "ab_split_c": float(cand.get("ab_split_c", 0.0) or 0.0),
                "pressure_sps": float(snap.get("pressure_sps", 0.0) or 0.0),
                "adx_q": float(snap.get("adx_q", 0.5) or 0.5),
                "spread_z": float(snap.get("spread_z", 0.0) or 0.0),
                "policy_runtime": {
                    "overrides": {
                        "enabled": int(getattr(o, "enabled", 1) or 1),
                        "updated_ts_ms": int(getattr(o, "updated_ts_ms", 0) or 0),
                        "force_active_arm": str(getattr(o, "force_active_arm", "") or ""),
                        "freeze_active": int(getattr(o, "freeze_active", 0) or 0),
                        "freeze_mode": str(getattr(o, "freeze_mode", "shadow") or "shadow"),
                        "overrides_apply_hold_ms": int(getattr(o, "overrides_apply_hold_ms", 0) or 0),
                        "overrides_min_switch_gap_ms": int(getattr(o, "overrides_min_switch_gap_ms", 0) or 0),
                        "active_arm_hold_down_ms": int(getattr(o, "active_arm_hold_down_ms", 0) or 0),
                        "active_arm_min_switch_gap_ms": int(getattr(o, "active_arm_min_switch_gap_ms", 0) or 0),
                    },
                    "active_arm_dbg": getattr(self, "_last_active_arm_dbg", None),
                    # Liquidity geometry / resiliency (Phase C)
                    # These fields are *best-effort* and may be missing depending on
                    # how SMT snapshots are produced.
                    "liq_geom": {
                        "profile": (snap.get("liq_geom_profile", "")),
                        "flags": (snap.get("liq_geom_flags", "")),
                        "slope_min": float(snap.get("liq_geom_slope_min", 0.0) or 0.0),
                        "dws_bps": float(snap.get("liq_geom_dws_bps", 0.0) or 0.0),
                        "recovery_time_ms": int(snap.get("liq_geom_recovery_time_ms", 0) or 0),
                        "tighten_add_bps": float(snap.get("liq_geom_tighten_add_bps", 0.0) or 0.0),
                    },
                    "flow_toxic": {
                        "profile": (snap.get("flow_toxic_profile", "")),
                        "flags": (snap.get("flow_toxic_flags", "")),
                        "ofi_norm_z": float(snap.get("ofi_norm_z", snap.get("flow_toxic_ofi_norm_z", 0.0)) or 0.0),
                        "vpin_cdf": float(snap.get("vpin_cdf", snap.get("flow_toxic_vpin_cdf", 0.0)) or 0.0),
                        "tighten_add_bps": float(snap.get("flow_toxic_tighten_add_bps", 0.0) or 0.0),
                    },
                },
                "snap": {k: snap.get(k) for k in ("close_px", "spread_bp", "pressure_sps", "cooldown_sps", "obi_age_ms", "regime")},
            }
            await self.r.xadd(self.cfg.audit_stream, {"data": json.dumps(payload)}, maxlen=self.cfg.audit_stream_maxlen, approximate=True)
        except Exception: pass

    def _apply_liq_geom_policy(self, *, cand: dict[str, Any], snap: dict[str, Any]) -> tuple[bool, str, str]:
        """Phase C (P2): Liquidity geometry/resiliency policy for EntryPolicy.

        Requirements from product:
          - default/soft: annotate only
          - strict: tighten expected_slippage_bps
          - hard: tighten + veto

        This gate is fail-open: missing fields => no action.
        """
        try:
            profile = os.getenv("ENTRY_POLICY_PROFILE", os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
            if profile not in {"default", "soft", "strict", "hard"}:
                profile = "default"

            micro = snap.get("micro") if isinstance(snap.get("micro"), dict) else {}

            slope_bid = _f(snap.get("book_slope_bid", micro.get("book_slope_bid", 0.0)), 0.0)
            slope_ask = _f(snap.get("book_slope_ask", micro.get("book_slope_ask", 0.0)), 0.0)
            dws_bps = _f(snap.get("dws_bps", micro.get("dws_bps", 0.0)), 0.0)
            rec_ms = _i(snap.get("liq_recovery_time_ms", micro.get("liq_recovery_time_ms", 0)), 0)

            thr_slope = float(os.getenv("ENTRY_LIQ_MIN_BOOK_SLOPE", os.getenv("LIQ_MIN_BOOK_SLOPE", "0")) or 0.0)
            thr_dws = float(os.getenv("ENTRY_LIQ_MAX_DWS_BPS", os.getenv("LIQ_MAX_DWS_BPS", "0")) or 0.0)
            thr_rec = int(os.getenv("ENTRY_LIQ_MAX_RECOVERY_TIME_MS", os.getenv("LIQ_MAX_RECOVERY_TIME_MS", "0")) or 0)

            cap = float(os.getenv("ENTRY_LIQ_TIGHTEN_ADD_CAP_BPS", os.getenv("LIQ_GEOM_TIGHTEN_ADD_CAP_BPS", "10.0")) or 10.0)
            mult = float(os.getenv("ENTRY_LIQ_TIGHTEN_ADD_MULT", os.getenv("LIQ_GEOM_TIGHTEN_ADD_MULT", "1.0")) or 1.0)

            from services.orderflow.liquidity_geom_policy import evaluate_liq_geom
            decg = evaluate_liq_geom(
                profile=profile,
                slope_bid=slope_bid,
                slope_ask=slope_ask,
                dws_bps=dws_bps,
                recovery_ms=rec_ms,
                thr_slope=thr_slope,
                thr_dws=thr_dws,
                thr_recovery_ms=thr_rec,
                tighten_cap_bps=cap,
                tighten_mult=mult,
            )

            # Always annotate (for downstream audit + observability)
            snap["liq_geom_profile"] = profile
            snap["liq_geom_flags"] = ",".join(decg.flags) if decg.flags else ""
            snap["liq_geom_slope_min"] = float(decg.slope_min)
            snap["liq_geom_dws_bps"] = float(dws_bps)
            snap["liq_geom_recovery_time_ms"] = int(rec_ms)

            if (not decg.flags) or profile in {"default", "soft"}:
                return True, "", ""

            # strict/hard: tighten expected_slippage_bps in snap (and micro if present)
            if decg.tighten_add_bps > 0.0:
                exp0 = _f(snap.get("expected_slippage_bps", micro.get("expected_slippage_bps", 0.0)), 0.0)
                exp1 = float(exp0 + float(decg.tighten_add_bps))
                snap["expected_slippage_bps"] = exp1
                if isinstance(micro, dict):
                    micro["expected_slippage_bps"] = exp1
                    snap["micro"] = micro
                snap["liq_geom_tighten_add_bps"] = float(decg.tighten_add_bps)

            # hard: veto with DENY_LIQ_GEOM
            if decg.veto:
                notes = f"flags={snap.get('liq_geom_flags','')} slope_min={decg.slope_min:.1f} dws_bps={dws_bps:.2f} rec_ms={rec_ms}"
                return False, "DENY_LIQ_GEOM", notes

            return True, "", ""
        except Exception:
            # Fail-open: never block entry on missing/invalid geometry data
            return True, "", ""

    def _apply_flow_toxicity_policy(self, *, cand: dict[str, Any], snap: dict[str, Any]) -> tuple[bool, str, str]:
        """Phase D (P3): Flow toxicity overlay for EntryPolicy.

        Inputs (expected in `snap`, possibly via sidecar `smt:snap_extra:*`):
          - ofi_norm_z: robust z-score of OFI normalized by near depth
          - vpin_cdf: optional, derived from L3-lite tracker

        Profiles:
          - default/soft: annotate only
          - strict: tighten expected_slippage_bps
          - hard: tighten + veto (by default requires TCA-bad, unless FLOW_TOX_VETO_WITHOUT_TCA=1)

        Fail-open: missing metrics => allow.
        """
        try:
            from services.orderflow.flow_toxicity import evaluate_flow_toxicity

            profile = os.getenv("ENTRY_POLICY_PROFILE", os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
            if profile not in {"default", "soft", "strict", "hard"}:
                profile = "default"

            thr_z = float(os.getenv("FLOW_OFI_NORM_Z_MAX", "0") or 0.0)
            thr_vpin = float(os.getenv("FLOW_VPIN_CDF_MAX", "0") or 0.0)

            cap = float(os.getenv("ENTRY_FLOW_TOX_TIGHTEN_ADD_CAP_BPS", os.getenv("FLOW_TOX_TIGHTEN_ADD_CAP_BPS", "6.0")) or 6.0)
            mult = float(os.getenv("ENTRY_FLOW_TOX_TIGHTEN_ADD_MULT", os.getenv("FLOW_TOX_TIGHTEN_ADD_MULT", "1.0")) or 1.0)

            veto_wo_tca = bool(int(os.getenv("FLOW_TOX_VETO_WITHOUT_TCA", "0") or 0))
            thr_is = float(os.getenv("ENTRY_POLICY_MAX_IS_P95_BPS", os.getenv("EXEC_MAX_IS_P95_BPS", "0")) or 0.0)
            thr_imp = float(os.getenv("ENTRY_POLICY_MAX_PERM_IMPACT_P95_BPS", os.getenv("EXEC_MAX_PERM_IMPACT_P95_BPS", "0")) or 0.0)

            ofi_z = float(snap.get("ofi_norm_z", 0.0) or 0.0)
            vpin_cdf = float(snap.get("vpin_cdf", 0.0) or 0.0)

            tca_is = float(snap.get("tca_is_p95_bps", snap.get("is_p95_bps", 0.0)) or 0.0)
            tca_imp = float(snap.get("tca_perm_impact_p95_bps", snap.get("perm_impact_p95_bps", 0.0)) or 0.0)

            dec = evaluate_flow_toxicity(
                profile=profile,
                ofi_norm_z=ofi_z,
                thr_ofi_norm_z=thr_z,
                vpin_cdf=vpin_cdf,
                thr_vpin_cdf=thr_vpin,
                tca_is_p95_bps=tca_is,
                tca_perm_impact_p95_bps=tca_imp,
                thr_is_p95_bps=thr_is,
                thr_perm_impact_p95_bps=thr_imp,
                tighten_mult=mult,
                tighten_cap_bps=cap,
                veto_without_tca=veto_wo_tca,
            )

            snap["flow_toxic_profile"] = profile
            snap["flow_toxic_flags"] = ",".join(dec.flags) if dec.flags else ""
            snap["flow_toxic_ofi_norm_z"] = float(ofi_z)
            snap["flow_toxic_vpin_cdf"] = float(vpin_cdf)

            if dec.tighten_add_bps > 0.0:
                exp0 = _f(snap.get("expected_slippage_bps", snap.get("expected_slippage", 0.0)), 0.0)
                exp1 = float(exp0 + float(dec.tighten_add_bps))
                snap["expected_slippage_bps"] = exp1
                snap["flow_toxic_tighten_add_bps"] = float(dec.tighten_add_bps)

            if dec.veto:
                notes = f"flags={snap.get('flow_toxic_flags','')} ofi_z={ofi_z:.2f} thr={thr_z:.2f} vpin_cdf={vpin_cdf:.3f} thr={thr_vpin:.3f} tca_is_p95={tca_is:.2f} thr={thr_is:.2f} tca_imp_p95={tca_imp:.2f} thr={thr_imp:.2f}"
                return False, "VETO_FLOW_TOXIC", notes

            notes = "" if not dec.hit else f"flags={snap.get('flow_toxic_flags','')}"
            return True, "", notes
        except Exception:
            return True, "", ""

    def _apply_manip_gate(self, *, cand: dict[str, Any], snap: dict[str, Any]) -> tuple[bool, str, str]:
        """Phase E (P4): Manipulation patterns overlay for EntryPolicy.

        Reads quote_stuffing_score / layering_score / otr_z from merged snap
        (base + sidecar `smt:snap_extra:*`).

        Profiles:
          - default/soft/monitor: annotate only
          - strict/tighten: tighten expected_slippage_bps
          - hard/veto: tighten + veto

        Fail-open: missing metrics => allow.
        """
        try:
            profile = os.getenv("MANIP_GATE_PROFILE", os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
            manip_mode_ov = (os.getenv("MANIP_MODE", "") or "").strip().lower()
            if manip_mode_ov in {"monitor", "tighten", "veto"}:
                profile = manip_mode_ov
            if profile not in {"default", "soft", "strict", "hard", "monitor", "tighten", "veto"}:
                profile = "default"

            thr_qs = float(os.getenv("MANIP_QUOTE_STUFF_SCORE_MAX", "0") or 0.0)
            thr_lay = float(os.getenv("MANIP_LAYERING_SCORE_MAX", "0") or 0.0)
            thr_otr_z = float(os.getenv("MANIP_OTR_Z_MAX", "0") or 0.0)

            tighten_cap = float(os.getenv("MANIP_TIGHTEN_ADD_CAP_BPS", "6.0") or 6.0)
            tighten_mult = float(os.getenv("MANIP_TIGHTEN_ADD_MULT", "1.0") or 1.0)

            qs_score = float(snap.get("quote_stuffing_score", 0.0) or 0.0)
            lay_score = float(snap.get("layering_score", 0.0) or 0.0)
            otr_z_val = float(snap.get("otr_z", 0.0) or 0.0)
            manip_flags_val = (snap.get("manip_flags", "") or "")

            # Annotate snap (for audit)
            snap["manip_gate_profile"] = profile
            snap["manip_flags"] = manip_flags_val

            hit_qs = thr_qs > 0.0 and qs_score >= thr_qs
            hit_lay = thr_lay > 0.0 and lay_score >= thr_lay
            hit_otr = thr_otr_z > 0.0 and otr_z_val >= thr_otr_z
            hit_any = hit_qs or hit_lay or hit_otr

            if not hit_any:
                return True, "", ""

            # Annotate always
            snap["manip_gate_hit"] = 1

            if profile in {"default", "soft", "monitor"}:
                return True, "", f"manip_flags={manip_flags_val} monitor_only"

            # strict/tighten/hard/veto: tighten
            manip_score = max(qs_score, lay_score)
            if manip_score <= 0.0 and hit_otr:
                manip_score = min(1.0, max(0.1, (otr_z_val - thr_otr_z) / max(thr_otr_z, 1.0)))
            add_bps = float(min(tighten_cap, manip_score * tighten_mult * 3.0))
            if add_bps > 0.0:
                exp0 = _f(snap.get("expected_slippage_bps", snap.get("micro", {}).get("expected_slippage_bps", 0.0)), 0.0)
                exp1 = float(exp0 + add_bps)
                snap["expected_slippage_bps"] = exp1
                snap["manip_tighten_add_bps"] = float(add_bps)
                micro = snap.get("micro") if isinstance(snap.get("micro"), dict) else {}
                if isinstance(micro, dict):
                    micro["expected_slippage_bps"] = exp1
                    snap["micro"] = micro

            # hard/veto: veto with reason code
            if profile in {"hard", "veto"}:
                veto_reason = "VETO_QUOTE_STUFFING" if hit_qs else ("VETO_LAYERING" if hit_lay else "VETO_OTR_SPIKE")
                notes = f"flags={manip_flags_val} qs={qs_score:.3f} lay={lay_score:.3f} otr_z={otr_z_val:.2f}"
                return False, veto_reason, notes

            notes = f"flags={manip_flags_val} tighten_bps={add_bps:.2f}"
            return True, "", notes
        except Exception:
            # Fail-open: never block entry on missing/invalid manip data
            return True, "", ""

    async def _apply_derivatives_context_policy(
        self, *, cand: dict[str, Any], snap: dict[str, Any]
    ) -> tuple[bool, str, str]:
        """P0: normalized derivatives context overlay for EntryPolicy.

        Requirements from product:
          - default/soft: annotate only
          - strict: tighten expected_slippage_bps
          - hard: tighten + veto on multi-flag crowding

        This gate is fail-open: missing Redis / snapshot => allow.
        """
        try:
            profile = str(
                os.getenv("ENTRY_DERIV_CTX_PROFILE", os.getenv("DERIV_CTX_PROFILE", os.getenv("GATE_PROFILE", "default")))
                or "default"
            ).strip().lower()
            if profile not in {"default", "soft", "strict", "hard", "monitor", "tighten", "veto"}:
                profile = "default"

            sym = (cand.get("symbol") or "").upper()
            ctx_snap = await aread_derivatives_context(self.r, symbol=sym)
            if ctx_snap is None:
                return True, "", ""

            snap["deriv_ctx_profile"] = profile
            snap["funding_rate"] = float(ctx_snap.funding_rate)
            snap["funding_rate_z"] = float(ctx_snap.funding_rate_z)
            snap["basis_bps"] = float(ctx_snap.basis_bps)
            snap["oi_notional_usd"] = float(ctx_snap.oi_notional_usd)
            snap["funding_extreme"] = int(ctx_snap.funding_extreme)
            snap["basis_extreme"] = int(ctx_snap.basis_extreme)
            snap["oi_accel"] = int(ctx_snap.oi_accel)

            decd = evaluate_derivatives_context(
                profile=profile,
                funding_rate_z=float(ctx_snap.funding_rate_z),
                basis_bps=float(ctx_snap.basis_bps),
                funding_extreme=int(ctx_snap.funding_extreme),
                basis_extreme=int(ctx_snap.basis_extreme),
                oi_accel=int(ctx_snap.oi_accel),
                thr_funding_z=float(os.getenv("DERIV_CTX_FUNDING_Z_MAX", "3.0") or 3.0),
                thr_basis_bps=float(os.getenv("DERIV_CTX_BASIS_BPS_MAX", "10.0") or 10.0),
                require_oi_for_veto=bool(int(os.getenv("DERIV_CTX_REQUIRE_OI_FOR_VETO", "1") or 1)),
                tighten_mult=float(os.getenv("ENTRY_DERIV_CTX_TIGHTEN_ADD_MULT", os.getenv("DERIV_CTX_TIGHTEN_ADD_MULT", "1.0")) or 1.0),
                tighten_cap_bps=float(os.getenv("ENTRY_DERIV_CTX_TIGHTEN_ADD_CAP_BPS", os.getenv("DERIV_CTX_TIGHTEN_ADD_CAP_BPS", "8.0")) or 8.0),
            )

            snap["deriv_ctx_flags"] = ",".join(decd.flags) if decd.flags else ""
            snap["deriv_ctx_hit"] = 1 if decd.hit else 0
            snap["deriv_ctx_crowding_score"] = float(decd.crowding_score)

            if decd.tighten_add_bps > 0.0:
                micro = snap.get("micro") if isinstance(snap.get("micro"), dict) else {}
                exp0 = _f(snap.get("expected_slippage_bps", micro.get("expected_slippage_bps", 0.0)), 0.0)
                exp1 = float(exp0 + float(decd.tighten_add_bps))
                snap["expected_slippage_bps"] = exp1
                snap["deriv_ctx_tighten_add_bps"] = float(decd.tighten_add_bps)
                if isinstance(micro, dict):
                    micro["expected_slippage_bps"] = exp1
                    snap["micro"] = micro

            if decd.veto:
                notes = (
                    f"flags={snap.get('deriv_ctx_flags','')} "
                    f"funding_z={ctx_snap.funding_rate_z:.2f} "
                    f"basis_bps={ctx_snap.basis_bps:.2f} "
                    f"oi_accel={ctx_snap.oi_accel}"
                )
                return False, "DENY_DERIV_CTX", notes

            notes = f"flags={snap.get('deriv_ctx_flags','')}" if decd.hit else ""
            return True, "", notes
        except Exception:
            return True, "", ""

    async def _emit_entry(self, *, now_ms: int, cand: dict[str, Any], snap: dict[str, Any], bundle: dict[str, Any]) -> bool:
        """Publish the approved trade entry to the output stream.

        Returns True if emitted, False if blocked (P6 final safety freeze check)
        or on unexpected errors (fail-safe: do not emit stale entries).
        """
        try:
            # P6 final safety check: even if pre-evaluate allowed, re-check freeze key
            # before the real xadd to close the evaluation-to-emit race window.
            fr = await aread_exec_health_auto_freeze(
                redis=self.r,
                scope="entry_policy",
                now_ms=int(now_ms),
                force=True,  # bypass cache: this is the last gate before xadd
            )
            snap["exec_health_auto_freeze_active"] = int(1 if fr.active else 0)
            snap["exec_health_auto_freeze_until_ts_ms"] = int(fr.freeze_until_ts_ms or 0)
            snap["exec_health_auto_freeze_reason"] = str(fr.freeze_reason or "")
            if fr.active:
                # Freeze became active between pre-evaluate and emit: hard stop
                return False

            eid = _entry_id(cand, snap, bundle)
            payload = {
                "ts_ms": now_ms,
                "entry_id": eid,
                "symbol": cand["symbol"],
                "side": cand["side"],
                "bundle": cand.get("bundle", ""),
                # --- AB routing (must survive into PositionState.signal_payload) ---
                "ab_arm": (cand.get("ab_arm") or "A"),
                "ab_group": (cand.get("ab_group") or "default"),
                "ab_key": (cand.get("ab_key") or ""),
                "ab_ver": _i(cand.get("arm_ver", 0), 0),
                "leader": _s(bundle.get("leader", "")),
                "decision": _s(bundle.get("decision", "")),  # scenario taxonomy: continuation|reversal
                # ------------------------------------------------------------
                # Policy/quality fields (for TradeMonitor autopilot analytics)
                # These must survive into PositionState.signal_payload and then into POSITION_CLOSED.
                # ------------------------------------------------------------
                "policy": {
                    # tiers (если нет — будет -1, tuner пропустит)
                    "abs_lvl_tier": _i(snap.get("abs_lvl_tier", snap.get("abs_lvl_tier_used", -1)), -1),
                    "dn_tier": _i(snap.get("dn_tier", -1), -1),
                    # book quality
                    "book_health_ok": _i(snap.get("book_health_ok", -1), -1),
                    "book_age_ms": _i(snap.get("book_age_ms", snap.get("book_age", 0)), 0),
                    "book_rate_hz": _f(snap.get("book_rate_hz", snap.get("book_rate_ema", 0.0)), 0.0),
                    # of/strong gate summary
                    "of_confirm_ok": _i(snap.get("of_confirm_ok", snap.get("of_strong", 0)), 0),
                    "of_confirm_score": _f(snap.get("of_confirm_score", 0.0), 0.0),
                    "strong_gate_have": _i(snap.get("strong_gate_have", 0), 0),
                    "strong_gate_need": _i(snap.get("strong_gate_need", 0), 0),
                    "strong_gate_scn": _s(snap.get("strong_gate_scn", "")),
                    # microstructure
                    "spread_bp": _f(snap.get("spread_bp", 0.0), 0.0),
                },
                # === AB routing (must reach TradeMonitor -> POSITION_CLOSED) ===
                "ab": {
                    "arm": _s(cand.get("ab_arm", "A")).upper(),
                    "group": _s(cand.get("ab_group", "default")).lower(),
                    "key": _s(cand.get("ab_key", "")),
                    "arm_ver": _i(cand.get("arm_ver", 0)),
                    "split_reason": _s(cand.get("ab_split_reason", "")),
                    "split_a": _f(cand.get("ab_split_a", 0.0)),
                    "split_b": _f(cand.get("ab_split_b", 0.0)),
                    "split_c": _f(cand.get("ab_split_c", 0.0)),
                },
                "ctx": {
                    "regime": _s(snap.get("regime", "na")),
                    "atr": _f(snap.get("atr", 0.0)),
                    "coh": _f(bundle.get("coh", 0.0)),
                    "leader_conf_score": _f(bundle.get("leader_conf_score", 0.0)),
                    # === microstructure penalties (entry-time snapshot) ===
                    "adx_q": _f(snap.get("adx_q", 0.5)),
                    "spread_z": _f(snap.get("spread_z", 0.0)),
                    "pressure_sps": _f(snap.get("pressure_sps", 0.0)),
                    "cooldown_sps": _f(snap.get("cooldown_sps", 0.0)),
                    "obi_age_ms": _i(snap.get("obi_age_ms", 0)),
                    "abs_th_unstable": _i(snap.get("abs_lvl_th_unstable", 0)),
                    "news_blocked": _i(bundle.get("news_blocked", 0)),
                },
                "source": "smt_entry_policy",
            }
            await self.r.xadd(self.cfg.out_stream, {"type": "trade_entry", "ts_ms": str(now_ms), "payload": json.dumps(payload)}, maxlen=self.cfg.out_stream_maxlen)
            return True
        except Exception:
            return False

    async def _record_shadow_block(
        self, *, sym: str, grp: str, scn: str,
        spread_z: float, obi_age: float, pressure: float,
        blocked: bool,
    ) -> None:
        """Increment shadow block/seen counters for FreezePromotionService.

        Hash key: cfg:entry_policy:freeze:shadow_stats:{sym}:{grp}:{scn}
        Fields:
          blocked_count - how many candidates were blocked (shadow-vetoed)
          seen_count    - total candidates seen while shadow freeze active
          last_spread_z, last_obi_age_ms, last_pressure_sps - latest metrics snapshot
          last_ts_ms    - wall-time of last update
        TTL: 4h (auto-cleaned when freeze expires)
        """
        k = f"cfg:entry_policy:freeze:shadow_stats:{sym.upper()}:{grp.lower()}:{scn.lower()}"
        try:
            pipe = self.r.pipeline()
            if blocked:
                pipe.hincrby(k, "blocked_count", 1)
            pipe.hincrby(k, "seen_count", 1)
            pipe.hset(k, mapping={
                "last_spread_z": str(spread_z),
                "last_obi_age_ms": str(obi_age),
                "last_pressure_sps": str(pressure),
                "last_ts_ms": str(_now_ms()),
            })
            pipe.expire(k, 14400)  # 4 hours
            await pipe.execute()
        except Exception:
            pass  # fail-open: never block entry on counter write failure

    def _parse_candidate(self, fields: dict[str, Any]) -> dict[str, Any] | None:
        try:
            if (fields.get("type")) != "entry_candidate": return None
            pl = json.loads(fields.get("payload", "{}"))
            return {
                "symbol": _s(fields.get("symbol")).upper(), "ts_ms": _i(fields.get("ts_ms")), "side": _s(fields.get("side")).upper(),
                "bundle": _s(fields.get("bundle")), "ab_arm": _s(pl.get("ab_arm", "A")).upper(), "ab_group": _s(pl.get("ab_group", "default")).lower(),
                "ab_key": _s(pl.get("ab_key", "")),
                "regime": _s(pl.get("regime", "na")).lower(), "payload": pl,
                "ab_split_reason": (pl.get("ab_split_reason", "")),
                "ab_split_a": _f(pl.get("ab_split_a", 0.0)),
                "ab_split_b": _f(pl.get("ab_split_b", 0.0)),
                "ab_split_c": _f(pl.get("ab_split_c", 0.0)),
            }
        except Exception: return None

    async def process_one(self, fields: dict[str, Any]) -> None:
        cand = self._parse_candidate(fields)
        if not cand: return
        now = _now_ms()

        # Expert recommendation: Auto-assign ab_group based on regime if missing
        if not cand.get("payload", {}).get("ab_group"):
             cand["ab_group"] = regime_group(cand.get("regime", "na"))

        # Normalize arm from candidate if present
        if cand.get("ab_arm"):
            cand["ab_arm"] = norm_arm(cand["ab_arm"])

        grp = (cand.get("ab_group") or "default").lower()
        snap, bundle = await self._get_context(cand)
        scn = (bundle.get("decision", "na")).lower()

        await self._maybe_poll_overrides(now, cand=cand, snap=snap, bundle=bundle)
        ovr = self._ovr_group.get(grp) or EntryPolicyOverridesV1()
        ovr, _ = ovr.validate()

        # ------------------------------------------------------------
        # NEW: Tier-policy gate (strict enforcement)
        # ------------------------------------------------------------
        try:
            req_tier = -1
            reg = (snap.get("regime", "na")).lower()
            if reg == "trend": req_tier = int(ovr.abs_lvl_tier_trend)
            elif reg == "range": req_tier = int(ovr.abs_lvl_tier_range)
            elif reg == "thin": req_tier = int(ovr.abs_lvl_tier_thin)

            if req_tier != -1:
                obs_tier = int(snap.get("abs_lvl_tier", 0))
                mode = str(ovr.abs_lvl_tier_mode).lower()
                deny = False
                if mode == "exact":
                    if obs_tier != req_tier: deny = True
                else: # "min"
                    if obs_tier < req_tier: deny = True

                if deny:
                    await self._audit(
                        now_ms=_now_ms(), cand=cand, ok=False,
                        reason_code="DENY_ABS_LVL_TIER_POLICY",
                        notes=f"req={req_tier} obs={obs_tier} mode={mode}",
                        snap=snap, bundle=bundle, ovr=ovr
                    )
                    return
        except Exception:
            pass

        # Apply policy knobs into core_cfg (safe / optional)
        try:
            if float(getattr(ovr, "coh_thr", 0.0) or 0.0) > 0:
                self.core_cfg.coh_thr = float(ovr.coh_thr)
        except Exception:
            pass

        frz_active, frz_until, frz_mode, frz_reason = 0, 0, "", ""

        try:
            if ovr.enabled:
                # 1) Force active arm (hard override)
                fa = str(getattr(ovr, "force_active_arm", "") if hasattr(ovr, "force_active_arm") else "").upper()
                if fa in ("A", "B", "C"):
                    cand["ab_arm"] = fa
        except Exception:
            pass

        # ADX-aware execution: in chop force Arm A
        try:
            adx_q = float(snap.get("adx_q", 0.5) or 0.5)
            adx_chop_lo = float(ovr.adx_chop_lo_q)
            if adx_q <= adx_chop_lo:
                cand["ab_arm"] = "A"
        except Exception: pass



        active_val_raw = await self._get_active_arm(
            symbol=cand["symbol"],
            regime=snap.get("regime", "na"),
            group=grp,
            scenario=scn,
            raw_only=True
        )

        # apply hold-down + gap using overrides values
        try:
            self._arm_stab.hold_down_ms = int(getattr(ovr, "active_arm_hold_down_ms", self._arm_stab.hold_down_ms) or self._arm_stab.hold_down_ms)
            self._arm_stab.min_switch_gap_ms = int(getattr(ovr, "active_arm_min_switch_gap_ms", self._arm_stab.min_switch_gap_ms) or self._arm_stab.min_switch_gap_ms)
        except Exception:
            pass

        arm_key = f"{cand.get('symbol','')}:{snap.get('regime','na')}:{grp}:{scn}"
        active_val = self._arm_stab.update(key=arm_key, raw=(active_val_raw or ""), now_ms=now)

        act = decide_active_arm(cand_arm=cand["ab_arm"], active_arm_value=active_val)

        # Shadow mode: arm shadow or forced-shadow due to spread guard
        arm_shadow = not act.is_active

        if not act.is_active:
            # audit includes raw/effective active arm and stabilizer snapshot
            with contextlib.suppress(Exception):
                self._last_active_arm_dbg = {"raw": active_val_raw, "eff": active_val, "key": arm_key, "ovr": ovr, "stab": self._arm_stab.snapshot(arm_key)}
            await self._audit(now_ms=_now_ms(), cand=cand, ok=True, reason_code="ALLOW_SHADOW_AB_ARM", notes=f"Shadow (active_raw={active_val_raw} active_eff={active_val})", snap=snap, bundle=bundle, ovr=ovr)
            return

        if not frz_active:
            frz_active, _, frz_mode, frz_reason = await self._get_freeze(symbol=cand["symbol"], group=cand["ab_group"], scenario=scn, now_ms=now)
        if frz_active:
            if frz_mode == "hard":
                await self._audit(now_ms=_now_ms(), cand=cand, ok=False, reason_code="FROZEN_HARD", notes=frz_reason, snap=snap, bundle=bundle, ovr=ovr)
                return
            elif cand["ab_arm"] != "A": # Shadow covers non-A
                # Record block for promoter decision-making
                await self._record_shadow_block(
                    sym=cand["symbol"], grp=cand["ab_group"], scn=scn,
                    spread_z=float(snap.get("spread_z", 0.0) or 0.0),
                    obi_age=float(snap.get("obi_age_ms", 0.0) or 0.0),
                    pressure=float(snap.get("pressure_sps", 0.0) or 0.0),
                    blocked=True,
                )
                await self._audit(now_ms=_now_ms(), cand=cand, ok=True, reason_code="ALLOW_SHADOW_FROZEN_NONA", notes=frz_reason, snap=snap, bundle=bundle, ovr=ovr)
                return
            else:
                # Arm A passes through shadow freeze; still count as seen
                await self._record_shadow_block(
                    sym=cand["symbol"], grp=cand["ab_group"], scn=scn,
                    spread_z=float(snap.get("spread_z", 0.0) or 0.0),
                    obi_age=float(snap.get("obi_age_ms", 0.0) or 0.0),
                    pressure=float(snap.get("pressure_sps", 0.0) or 0.0),
                    blocked=False,
                )

        # Phase C (P2): Liquidity geometry/resiliency overlay for EntryPolicy
        # - default/soft: annotate only (audit)
        # - strict: tighten expected_slippage_bps (more conservative)
        # - hard: tighten + veto
        ok_geom, geom_rc, geom_notes = self._apply_liq_geom_policy(cand=cand, snap=snap)
        if not ok_geom:
            await self._audit(now_ms=_now_ms(), cand=cand, ok=False, reason_code=str(geom_rc), notes=str(geom_notes), snap=snap, bundle=bundle, ovr=ovr)
            return

        # Phase D (P3): Flow toxicity overlay for EntryPolicy
        # Reads ofi_norm_z / vpin_cdf from merged snap (base + sidecar).
        # Profiles: default/soft=annotate, strict=tighten, hard=tighten+veto.
        ok_flow, flow_rc, flow_notes = self._apply_flow_toxicity_policy(cand=cand, snap=snap)
        if not ok_flow:
            await self._audit(now_ms=_now_ms(), cand=cand, ok=False, reason_code=str(flow_rc), notes=str(flow_notes), snap=snap, bundle=bundle, ovr=ovr)
            return

        # Phase E (P4): Manipulation patterns overlay for EntryPolicy
        # Reads quote_stuffing_score / layering_score / otr_z from merged snap (sidecar).
        # Profiles: default/soft/monitor=annotate, strict/tighten=tighten, hard/veto=veto.
        ok_manip, manip_rc, manip_notes = self._apply_manip_gate(cand=cand, snap=snap)
        if not ok_manip:
            await self._audit(now_ms=_now_ms(), cand=cand, ok=False, reason_code=str(manip_rc), notes=str(manip_notes), snap=snap, bundle=bundle, ovr=ovr)
            return

        # P0: normalized derivatives context (funding/basis/OI crowding) overlay.
        # Uses aread (async, fail-open). Must run before exec_health to allow snapping
        # expected_slippage_bps before core evaluation.
        ok_deriv, deriv_rc, deriv_notes = await self._apply_derivatives_context_policy(cand=cand, snap=snap)
        if not ok_deriv:
            await self._audit(now_ms=_now_ms(), cand=cand, ok=False, reason_code=str(deriv_rc), notes=str(deriv_notes), snap=snap, bundle=bundle, ovr=ovr)
            return

        # P6: apply execution health overlay (same SoT reader/policy as EdgeCostGate/Pipeline)
        ok_exec, exec_rc, exec_notes = await self._maybe_attach_exec_health(now_ms=now, cand=cand, snap=snap, bundle=bundle)
        if not ok_exec:
            await self._audit(now_ms=_now_ms(), cand=cand, ok=False, reason_code=str(exec_rc), notes=str(exec_notes), snap=snap, bundle=bundle, ovr=ovr)
            return

        # P6: global auto-freeze must stop real entry emission (pre-evaluate deny).
        # Placed after exec_health TCA check, before entry_policy_core evaluation to
        # avoid unnecessary work when frozen.
        ok_frz, frz_rc, frz_notes = await self._maybe_enforce_exec_health_auto_freeze(now_ms=now, cand=cand, snap=snap, bundle=bundle)
        if not ok_frz:
            await self._audit(now_ms=_now_ms(), cand=cand, ok=False, reason_code=str(frz_rc), notes=str(frz_notes), snap=snap, bundle=bundle, ovr=ovr)
            return

        from services.entry_policy_core import evaluate_entry_policy
        dec = evaluate_entry_policy(now_ms=_now_ms(), cand=cand, snap=snap, bundle=bundle, cfg=self.core_cfg, dedup_state=self._dedup)

        if dec.ok and dec.emit:
            # Extra strict gates from overrides (deterministic from snap fields)
            try:
                if int(getattr(ovr, "enabled", 1) or 1) == 1:
                    rg = (snap.get("regime", "na") or "na")
                    min_of = float(ovr.min_of_score(rg))
                    of_score = float(snap.get("of_confirm_score", 0.0) or 0.0)
                    if of_score < min_of:
                        await self._audit(now_ms=_now_ms(), cand=cand, ok=False, reason_code="OVR_MIN_OF_SCORE", notes=f"of_score={of_score:.2f}<min={min_of:.2f}", snap=snap, bundle=bundle, ovr=ovr)
                        return
                    # Spread z guard (shadow safer than hard veto)
                    spr_z = float(snap.get("spread_z", 0.0) or 0.0)
                    if spr_z > float(getattr(ovr, "spread_z_max", 3.0) or 3.0):
                        # allow shadow, do not emit
                        await self._audit(now_ms=_now_ms(), cand=cand, ok=True, reason_code="ALLOW_SHADOW_SPREAD_Z", notes=f"spread_z={spr_z:.2f}", snap=snap, bundle=bundle, ovr=ovr)
                        return
            except Exception:
                pass

            emitted = await self._emit_entry(now_ms=_now_ms(), cand=cand, snap=snap, bundle=bundle)
            if not emitted:
                # P6 final safety: freeze became active between pre-evaluate and emit
                fr_notes = f"freeze_reason={snap.get('exec_health_auto_freeze_reason','')} freeze_until_ts_ms={int(snap.get('exec_health_auto_freeze_until_ts_ms', 0) or 0)}"
                await self._audit(now_ms=_now_ms(), cand=cand, ok=False, reason_code="DENY_EXEC_HEALTH_AUTO_FREEZE", notes=fr_notes, snap=snap, bundle=bundle, ovr=ovr)
                return
            await self._audit(now_ms=_now_ms(), cand=cand, ok=True, reason_code="ALLOW", notes=dec.notes, snap=snap, bundle=bundle, ovr=ovr)
        else:
            await self._audit(now_ms=_now_ms(), cand=cand, ok=False, reason_code=dec.reason_code, notes=dec.notes, snap=snap, bundle=bundle, ovr=ovr)

    async def run_forever(self) -> None:
        with contextlib.suppress(Exception): await self.r.xgroup_create(self.cfg.in_stream, self.cfg.group, id="0", mkstream=True)
        while True:
            try:
                msgs = await self.r.xreadgroup(self.cfg.group, self.cfg.consumer, {self.cfg.in_stream: ">"}, count=100, block=1000)
                if not msgs:
                    await asyncio.sleep(0.1); continue
                for _, entries in msgs:
                    for msg_id, fields in entries:
                        try: await self.process_one(fields)
                        finally: await self.r.xack(self.cfg.in_stream, self.cfg.group, msg_id)
            except Exception: await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(EntryPolicyService().run_forever())
