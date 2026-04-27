from __future__ import annotations

import os
import json
import inspect
import asyncio
import math
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class GateDecision:
    """
    Unified gate decision (same shape as other pre-publish gates):
      - apply: gate evaluated (had enough info)
      - veto: block signal (ONLY in veto mode and ONLY narrow rule)
      - reason_code: stable code for metrics/logs
      - gate: gate name for observability
      - notes: optional debug
    """
    apply: bool
    veto: bool
    reason_code: str
    gate: str = ""
    notes: str = ""


def _safe_float(x: Any, default: float = float("nan")) -> float:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return float(default)


def _boolish(x: Any) -> bool:
    if x is True:
        return True
    if x is False or x is None:
        return False
    try:
        if isinstance(x, (int, float)):
            return float(x) != 0.0
        s = str(x).strip().lower()
        return s in {"1", "true", "yes", "y", "on"}
    except Exception:
        return False


def _dir_to_ud(direction: str) -> str:
    """
    Normalize direction to UP/DOWN for SMT alignment checks.
    Supports LONG/SHORT, BUY/SELL, UP/DOWN already.
    """
    d = (direction or "").strip().upper()
    if d in {"LONG", "BUY", "UP"}:
        return "UP"
    if d in {"SHORT", "SELL", "DOWN"}:
        return "DOWN"
    return "NA"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def _env_str(name: str, default: str = "") -> str:
    try:
        return str(os.getenv(name, default) or default)
    except Exception:
        return default


def _env_set(name: str) -> Optional[set]:
    """
    Comma-separated lowercased set. Empty -> None.
    """
    raw = _env_str(name, "").strip()
    if not raw:
        return None
    xs = [x.strip().lower() for x in raw.split(",") if x.strip()]
    return set(xs) if xs else None



def _sync_get(val: Any) -> Any:
    if inspect.isawaitable(val):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                val.close()
                return None
            return loop.run_until_complete(val)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(val)
    return val


def _redis_read_bundle_state(redis_client: Any, key: str) -> Optional[Dict[str, Any]]:
    """
    Read bundle state in best-effort manner:
      - if GET(key) returns JSON -> parse
      - else try HGETALL(key) (hash) -> parse fields
    Fail-open: return None on any error.
    """
    if redis_client is None:
        return None

    # 1) try GET(JSON)
    try:
        v = _sync_get(redis_client.get(key))
        if v is not None:
            if isinstance(v, bytes):
                v = v.decode("utf-8", errors="ignore")
            if isinstance(v, str):
                s = v.strip()
                if s:
                    obj = json.loads(s)
                    if isinstance(obj, dict):
                        return obj
    except Exception:
        pass

    # 2) try HGETALL(hash)
    try:
        d = _sync_get(redis_client.hgetall(key)) or {}
        if not isinstance(d, dict) or not d:
            return None

        def _b2s(x: Any) -> str:
            if isinstance(x, bytes):
                return x.decode("utf-8", errors="ignore")
            return str(x)

        out: Dict[str, Any] = {}
        for k, v in dict(d).items():
            out[_b2s(k)] = _b2s(v)
        return out if out else None
    except Exception:
        return None


class SmtLeaderCoherenceGate:
    """
    SMT leader/coherence gate (pre-publish).

    Modes:
      - observe: never veto; only attach ctx.smt_* audit fields
      - veto: veto ONLY narrow rule:
          leader_confirm=1 AND coh_hi=1 AND align=0 (countertrend vs confirmed leader)

    Why this design:
      - maximum stability: fail-open; missing state never blocks signals
      - audit-first: ctx fields always set when state available
      - supports later post-calibration (reliability curves) without cutting flow
    """

    def __init__(
        self,
        *,
        redis_client: Any,
        bundle_id: str,
        mode: str,
        coh_hi_thr: float,
        veto_kinds: Optional[set],
        diag_stream: str,
        diag_sample: int,
    ) -> None:
        self.redis = redis_client
        self.bundle_id = (bundle_id or "").strip()
        self.mode = (mode or "observe").strip().lower()
        self.coh_hi_thr = float(coh_hi_thr)
        self.veto_kinds = veto_kinds
        self.diag_stream = (diag_stream or "").strip()
        self.diag_sample = int(diag_sample) if int(diag_sample) > 0 else 1

    @staticmethod
    def from_env(*, redis_client: Any) -> "SmtLeaderCoherenceGate":
        # Bundle id is mandatory to enable the gate.
        bundle_id = _env_str("SMT_COH_BUNDLE", "").strip()
        mode = _env_str("SMT_LEADER_MODE", "observe").strip().lower()
        # Keep one threshold across system; fallback to reliability threshold for consistency.
        coh_hi_thr = _env_float("SMT_COH_HI_THRESHOLD", _env_float("RELIABILITY_SMT_COH_THR", 0.65))
        veto_kinds = _env_set("SMT_LEADER_VETO_KINDS")  # optional allowlist
        diag_stream = _env_str("SMT_LEADER_DIAG_STREAM", "")  # optional diagnostics stream
        diag_sample = int(_env_float("SMT_LEADER_DIAG_SAMPLE", 1))
        return SmtLeaderCoherenceGate(
            redis_client=redis_client,
            bundle_id=bundle_id,
            mode=mode,
            coh_hi_thr=coh_hi_thr,
            veto_kinds=veto_kinds,
            diag_stream=diag_stream,
            diag_sample=diag_sample,
        )

    def _maybe_diag(self, payload: Dict[str, Any]) -> None:
        """
        Optional diagnostics stream writer (fail-open).
        Note: sampling is coarse; for high-volume streams keep sample=10..100.
        """
        if not self.diag_stream or self.redis is None:
            return
        try:
            # ultra-cheap sampling (no RNG needed): hash-like by modulo on ts_ms if present
            ts = int(payload.get("ts_ms") or 0)
            if self.diag_sample > 1 and ts > 0 and (ts % self.diag_sample) != 0:
                return
        except Exception:
            pass
        try:
            _sync_get(self.redis.xadd(self.diag_stream, {"data": json.dumps(payload, ensure_ascii=False)}, maxlen=50000))
        except Exception:
            return

    def evaluate(self, *, ctx: Any, symbol: str, kind: str, direction: str) -> GateDecision:
        """
        Attach SMT audit fields into ctx (dynamic attrs; no protocol break).
        Returns GateDecision (veto only in veto mode, only narrow rule).
        """
        # ── Fail-open price:latest write ──────────────────────────────────────
        # Feed the SmtBundleAggregator with the latest close price each time
        # the gate is evaluated (per-signal, ~every candle close).
        # Writes HSET price:latest:{symbol} {mid, ts_ms, venue} to redis-worker-1.
        # Completely non-blocking: guarded by try/except, never affects gate logic.
        try:
            _price = float(
                getattr(ctx, "price", None)
                or getattr(ctx, "last_price", None)
                or getattr(ctx, "close", None)
                or 0.0
            )
            _ts_ms = int(
                getattr(ctx, "ts_ms", None)
                or getattr(ctx, "ts", None)
                or 0
            )
            # ts may be in seconds → convert
            if 0 < _ts_ms < 10_000_000_000:
                _ts_ms = _ts_ms * 1000
            if self.redis is not None and _price > 0.0 and _ts_ms > 0:
                _sync_get(self.redis.hset(
                    f"price:latest:{str(symbol).strip().upper()}",
                    mapping={
                        "mid": f"{_price:.10f}",
                        "ts_ms": str(_ts_ms),
                        "venue": "crypto",
                    },
                ))
        except Exception:
            pass

        # If bundle is not configured -> gate disabled (fail-open).
        if not self.bundle_id:
            return GateDecision(apply=False, veto=False, reason_code="SMT_DISABLED", gate="SmtLeaderCoherenceGate", notes="SMT_COH_BUNDLE empty")

        # Optional kind allowlist (for veto mode especially).
        kind_l = (kind or "").strip().lower()
        if self.veto_kinds is not None and kind_l and (kind_l not in self.veto_kinds):
            # Still may attach audit if state exists, but never veto for this kind.
            # We keep apply=True only if state exists.
            pass

        key = f"smt:bundle:v1:{self.bundle_id}"
        st = _redis_read_bundle_state(self.redis, key)
        if not isinstance(st, dict) or not st:
            # no state -> no audit, no veto (fail-open)
            return GateDecision(apply=False, veto=False, reason_code="SMT_NO_STATE", gate="SmtLeaderCoherenceGate", notes=key)

        leader = str(st.get("leader") or st.get("smt_leader") or "NA").strip().upper()
        leader_dir = str(st.get("leader_dir") or st.get("smt_leader_dir") or "NA").strip().upper()
        leader_confirm = 1 if _boolish(st.get("leader_confirm") or st.get("smt_leader_confirm")) else 0
        coh = _safe_float(st.get("coh") or st.get("smt_coh"), float("nan"))
        coh_hi = 1 if (math.isfinite(coh) and float(coh) >= float(self.coh_hi_thr)) else 0

        sig_ud = _dir_to_ud(direction)
        align = 1 if (leader_dir in {"UP", "DOWN"} and sig_ud in {"UP", "DOWN"} and leader_dir == sig_ud) else 0

        decision = str(st.get("decision") or "none").lower()
        pick = str(st.get("pick") or "").upper()
        news_blocked = 1 if _boolish(st.get("news_blocked")) else 0
        news_until_ts_ms = int(_safe_float(st.get("news_until_ts_ms"), 0.0) or 0)
        leader_conf_score = _safe_float(st.get("leader_conf_score"), float("nan"))
        
        # Attach audit fields (these are used later by reliability curves).
        try:
            setattr(ctx, "smt_bundle_id", str(self.bundle_id))
            setattr(ctx, "smt_bundle", str(self.bundle_id))  # backward-friendly alias
            setattr(ctx, "smt_leader", str(leader))
            setattr(ctx, "smt_leader_dir", str(leader_dir))
            setattr(ctx, "smt_leader_confirm", int(leader_confirm))
            setattr(ctx, "smt_coh", float(coh) if math.isfinite(coh) else float("nan"))
            setattr(ctx, "smt_coh_hi", int(coh_hi))
            setattr(ctx, "smt_align", int(align))
            setattr(ctx, "smt_mode", str(self.mode))
            setattr(ctx, "smt_decision", decision)
            setattr(ctx, "smt_pick", pick)
            setattr(ctx, "smt_news_blocked", int(news_blocked))
            setattr(ctx, "smt_news_until_ts_ms", int(news_until_ts_ms))
            setattr(ctx, "smt_leader_conf_score", float(leader_conf_score) if math.isfinite(leader_conf_score) else float("nan"))
        except Exception:
            pass

        # Hard news veto in veto mode
        if self.mode == "veto" and news_blocked == 1:
            try:
                setattr(ctx, "smt_blocked", 1)
                setattr(ctx, "smt_block_reason", "NEWS_GATE")
            except Exception:
                pass
            return GateDecision(apply=True, veto=True, reason_code="VETO_SMT_NEWS_GATE", gate="SmtLeaderCoherenceGate", notes=f"until={news_until_ts_ms}")

        # Narrow block rule (only meaningful when leader_confirm & coh_hi).
        blocked = 1 if (leader_confirm == 1 and coh_hi == 1 and align == 0) else 0
        block_reason = "COUNTERTREND_VS_CONFIRMED_LEADER" if blocked else ""

        # Write preliminary blocked state early so audit is always set on ctx
        # (golden ticket / continuation may update these fields after).
        try:
            setattr(ctx, "smt_blocked", int(blocked))
            setattr(ctx, "smt_block_reason", str(block_reason))
        except Exception:
            pass

        # SMT V2 Logic:
        # 1. Reversal Golden Ticket: If decision='reversal' and symbol==pick => ALLOW (override block)
        if decision == "reversal" and pick and symbol.upper() == pick:
            blocked = 0
            block_reason = "GOLDEN_REVERSAL"
            try:
                setattr(ctx, "smt_golden", 1)
                setattr(ctx, "smt_blocked", 0)
                setattr(ctx, "smt_block_reason", "GOLDEN_REVERSAL")
            except Exception:
                pass
            return GateDecision(apply=True, veto=False, reason_code="SMT_GOLDEN_REVERSAL", gate="SmtLeaderCoherenceGate", notes=f"picked {pick}")

        # 2. Continuation Enforcement: If decision='continuation', enforce alignment strictly
        if decision == "continuation":
            if align == 0:
                blocked = 1
                block_reason = "COUNTER_CONTINUATION"
                try:
                    setattr(ctx, "smt_blocked", int(blocked))
                    setattr(ctx, "smt_block_reason", str(block_reason))
                except Exception:
                    pass

        # Diagnostics (optional).
        self._maybe_diag({
            "event": "SMT_GATE",
            "bundle": self.bundle_id,
            "symbol": str(symbol or ""),
            "kind": str(kind or ""),
            "direction": str(direction or ""),
            "leader": leader,
            "leader_dir": leader_dir,
            "leader_confirm": leader_confirm,
            "coh": float(coh) if math.isfinite(coh) else None,
            "coh_thr": float(self.coh_hi_thr),
            "coh_hi": coh_hi,
            "decision": decision,
            "pick": pick,
            "align": align,
            "blocked": blocked,
            "mode": self.mode,
            "ts_ms": int(getattr(ctx, "ts_ms", 0) or getattr(ctx, "ts", 0) or 0),
        })

        if self.mode != "veto":
            return GateDecision(apply=True, veto=False, reason_code="SMT_OBSERVE", gate="SmtLeaderCoherenceGate", notes=block_reason)

        # veto mode: veto ONLY narrow rule + optional kind allowlist
        if blocked == 1:
            if self.veto_kinds is not None and kind_l and (kind_l not in self.veto_kinds):
                return GateDecision(apply=True, veto=False, reason_code="SMT_BLOCKED_BUT_KIND_NOT_APPLICABLE", gate="SmtLeaderCoherenceGate", notes=block_reason)
            return GateDecision(apply=True, veto=True, reason_code="VETO_SMT_COUNTERTREND", gate="SmtLeaderCoherenceGate", notes=block_reason)

        return GateDecision(apply=True, veto=False, reason_code="SMT_OK", gate="SmtLeaderCoherenceGate", notes="")
